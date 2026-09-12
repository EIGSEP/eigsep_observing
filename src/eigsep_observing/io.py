from collections import defaultdict
import datetime
import logging
import math
import numpy as np
import os
import queue
import tempfile
import threading
import time
from pathlib import Path

from .linear_range import load_linear_range, validate_operating_point

# Shared IO contract -- canonical home is eigsep_base.io. eigsep_observing
# writes through it, eigsep_data reads through it; re-exported here for
# backward compatibility with existing call sites in this package.
from eigsep_base.io import (  # noqa: F401
    read_hdf5,
    write_hdf5,
    write_metadata_hdf5,
    read_metadata_hdf5,
    write_s11_file,
    read_s11_file,
    CORR_HEADER_SCHEMA,
    SENSOR_SCHEMAS,
    VNA_S11_MODE_DATA_KEYS,
    RFSWITCH_TRANSITION_WINDOW_S,
    avg_metadata,
    # corr data layout / wiring helpers -- also canonical in
    # eigsep_base.io (shared with eigsep_data's read side).
    data_shape,
    reshape_data,
    effective_input_to_ant,
    pair_label,
    corr_pair_labels,
)
from eigsep_base.io import append_corr_header as _base_append_corr_header

# Private helpers also re-exported for internal consumers
from eigsep_base.io import (  # noqa: F401
    _write_attr,
    _write_dataset,
    _read_dataset,
    _write_header_item,
    _validate_corr_header,
    _validate_metadata,
    _validate_vna_s11_header,
    _validate_vna_s11_data,
    _avg_rfswitch_metadata,
    _avg_sensor_values,
    _log_invariant_disagreement,
    _last_invariant_log,
    _IMU_BASE,
    _IMU_AZ_SCHEMA,
    _IMU_EL_SCHEMA,
)

logger = logging.getLogger(__name__)


def append_corr_header(header, acc_cnts, sync_times):
    """
    Append header for correlation files with useful computed
    quantities: times, frequencies, and (when configured via
    ``linear_range_file``) the per-channel linear-range bounds.

    Thin wrapper around :func:`eigsep_base.io.append_corr_header` that
    injects this package's packaged-calibration-product loader
    (:func:`eigsep_observing.linear_range.load_linear_range`) and
    operating-point check (:func:`eigsep_observing.linear_range.
    validate_operating_point`) -- ``eigsep_base`` stays free of the
    hardware-package dependency, so the loader is passed in rather than
    imported there.

    Parameters
    ----------
    header : dict
        Header dictionary for correlation file.
    acc_cnts : array_like
        Array of accumulation counts for each time step.
    sync_times : array_like
        Synchronization time for the measurements, used to calculate
        the times. This is when `acc_cnts` starts.

    Returns
    -------
    new_header : dict
        Updated header dictionary with additional computed quantities.
        Computed fields may be missing if the source header was
        malformed (the failure is logged at ERROR level).

    """
    return _base_append_corr_header(
        header,
        acc_cnts,
        sync_times,
        load_linear_range=load_linear_range,
        validate_operating_point=validate_operating_point,
    )


class File:
    def __init__(
        self,
        save_dir,
        pairs,
        ntimes,
        cfg,
        writer_timeout=30.0,
        on_write=None,
    ):
        """
        Initialize the File object for saving correlation data.
        Uses a double-buffered async writer so that HDF5 I/O never
        blocks the data-reading loop.

        Parameters
        ----------
        save_dir : Path or str
            Directory where the data will be saved. Must be able to
            instantiate a Path object.
        pairs : list
            List of correlation pairs to write.
        ntimes : int
            Number of time steps to accumulate per file.
        cfg : dict
            Observing configuration.
        writer_timeout : float
            Maximum seconds ``corr_write`` will wait for the writer
            thread to release the standby buffer before dropping the
            active buffer with a loud ERROR. Bounds the worst-case
            behavior on a stuck writer (slow disk, NFS stall, etc.):
            corr data is sacred, but staying alive to capture future
            data is more important than blocking forever to save the
            current buffer. Default 30s — well above a normal HDF5
            write of one buffer (sub-second) and well below the
            shortest realistic buffer cadence.
        on_write : callable or None
            Optional ``on_write(path, mtime_unix)`` callback invoked
            from the writer thread after a successful ``os.rename``.
            Used by ``EigObserver`` to publish the live-status
            file-write heartbeat to Redis so a dashboard on a
            different host can see new files land without needing
            access to ``save_dir`` on disk. Exceptions raised by the
            callback are caught and logged at ERROR — corr data is
            sacred, a flaky heartbeat must not corrupt the writer.

        """
        self.logger = logger
        self.save_dir = Path(save_dir)
        self.ntimes = ntimes
        self.pairs = pairs
        self.cfg = cfg
        self._writer_timeout = writer_timeout
        self._on_write = on_write
        self._dropped_buffers = 0
        # RF switch transition tracking — see Phase 11 in
        # add_data. Forward-only: never mutates previously-written
        # samples. Both fields persist across buffer swaps and
        # writer drops since they live on File, not the buffer.
        self._prev_rfswitch_state = None
        self._rfswitch_unknown_remaining = 0
        self.set_header()

        acc_bins = cfg["acc_bins"]
        nchan = cfg["nchan"]
        dtype = np.dtype(cfg["dtype"])

        # active buffer
        self.acc_cnts = np.zeros(self.ntimes)
        self.sync_times = np.zeros(self.ntimes)
        self.metadata = defaultdict(list)
        self.data = {}
        for p in pairs:
            shape = data_shape(self.ntimes, acc_bins, nchan, cross=len(p) > 1)
            self.data[p] = np.zeros(shape, dtype=dtype)

        # standby buffer
        self._standby_acc_cnts = np.zeros(self.ntimes)
        self._standby_sync_times = np.zeros(self.ntimes)
        self._standby_metadata = defaultdict(list)
        self._standby_data = {}
        for p in pairs:
            shape = data_shape(self.ntimes, acc_bins, nchan, cross=len(p) > 1)
            self._standby_data[p] = np.zeros(shape, dtype=dtype)

        self.counter = 0

        # async writer
        self._write_queue = queue.Queue(maxsize=1)
        self._write_error = None  # set by writer thread on failure
        self._standby_ready = threading.Event()
        self._standby_ready.set()
        self._writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True
        )
        self._writer_thread.start()

    def __len__(self):
        return self.counter

    def reset(self):
        """
        Reset the active data arrays to zero.

        """
        self.metadata.clear()
        for p in self.pairs:
            self.data[p].fill(0)
        self.acc_cnts.fill(0)
        self.sync_times.fill(0)
        self.counter = 0

    def set_header(self, header=None):
        """
        Set the header for the correlation file.

        Validates the merged header against ``CORR_HEADER_SCHEMA`` and
        logs ERROR per violation. Does NOT raise: corr data is sacred,
        and a header bug must not stop the script. The writer path is
        hardened to skip fields that depend on missing or malformed
        values, so producers see loud logs but data continues to flow.

        Parameters
        ----------
        header : dict
            Header information to be written to the file. This specifies
            static configuration, settings, etc. Values are expected to be
            primarily strings or numbers, but may also include small
            arrays, lists, or dictionaries.

        """
        if header is None:
            self.header = {}
        else:
            self.header = header.copy()
        for key, val in self.cfg.items():
            if key not in self.header:
                self.header[key] = val
        violations = _validate_corr_header(self.header)
        for v in violations:
            self.logger.error(
                f"Header contract violation: {v}. Producer must be "
                f"fixed; affected fields will be missing from "
                f"written files."
            )

    def add_data(self, acc_cnt, sync_time, data, metadata=None):
        """
        Populate the data arrays with the given data. The data is expected
        to be of the dtype specified in the header.

        Parameters
        ----------
        acc_cnt : int
            Accumulation count.
        sync_time : float
            Synchronization time for the measurements, used to calculate
            the times. This is when `acc_cnt` starts.
        data : dict
            Dictionary of data arrays to be added for one time step.
        metadata : dict
            Dynamic metadata, such as sensor readings, timestamps, etc.
            Expected format from ``get_metadata()``:
            ``{stream_name: [list_of_dicts]}``.

        """
        if data is None:
            self.logger.error(
                "SNAP contract violation: data is None, dropping "
                "sample. Producer must be fixed."
            )
            return
        if acc_cnt is None:
            # Keep the sample so corr data is preserved; mark the
            # acc_cnt slot as NaN so downstream can detect that this
            # row's timestamp is unknown. _prev_cnt becomes NaN too,
            # which means gap detection across this sample is lost
            # until the next valid acc_cnt re-anchors the sequence.
            self.logger.error(
                "SNAP contract violation: acc_cnt is None, storing "
                "NaN and saving sample anyway. Producer must be "
                "fixed."
            )
            acc_cnt = float("nan")
        try:
            delta_cnt = acc_cnt - self._prev_cnt
        except AttributeError:  # first call
            delta_cnt = 1

        # iterative gap-fill with zeros (avoids recursion for large gaps)
        if delta_cnt > 1:
            zero_data = {p: np.zeros_like(self.data[p][0]) for p in self.pairs}
            base_cnt = self._prev_cnt
            for i in range(1, delta_cnt):
                self._insert_sample(base_cnt + i, sync_time, zero_data)

        # Process metadata from get_metadata (stream format:
        # {stream_name: [list_of_dicts]}).  Each list contains all
        # readings since the last call; we average them down to one
        # entry per sample to resample onto the correlator cadence.
        # Strip the "stream:" prefix — it's a Redis artifact — and
        # split temp sensors' A/B channels into separate entries.
        processed_md = {}
        metadata = metadata or {}
        for key in metadata:
            value = metadata[key]
            if not (isinstance(value, list) and len(value) > 0):
                self.logger.error(
                    f"Producer contract violation: metadata for "
                    f"stream '{key}' must be a non-empty list, got "
                    f"{value!r}. Dropping this stream for this sample."
                )
                continue
            # Per-stream safety net: corr data is sacred. A producer
            # contract violation that escapes avg_metadata must never
            # block the corr-data write. Log at ERROR so the producer
            # gets fixed; drop only this stream's metadata for this
            # sample and fall through to _insert_sample.
            try:
                # strip stream prefix
                name = key.removeprefix("stream:")
                processed_md[name] = avg_metadata(value)
            except Exception as e:
                self.logger.error(
                    f"Metadata contract violation processing stream "
                    f"'{key}': {e}. Producer must be fixed; dropping "
                    f"this stream's metadata for this sample."
                )

        # RF switch transition detection (Phase 11). The pico
        # reports the *commanded* switch state synchronously when
        # it receives a switch command, but the physical actuation
        # takes ~200ms and the pico has no way to know when it
        # finished. Detect transitions by comparing consecutive
        # samples' raw rfswitch states; on a change, flag a forward
        # window of samples as UNKNOWN to cover the contamination.
        # Forward-only — never mutates previously-written samples.
        new_rfswitch = processed_md.get("rfswitch")
        if (
            new_rfswitch not in (None, "UNKNOWN")
            and self._prev_rfswitch_state not in (None, "UNKNOWN")
            and new_rfswitch != self._prev_rfswitch_state
        ):
            try:
                int_time = float(self.header["integration_time"])
                n_to_flag = max(
                    1,
                    math.ceil(RFSWITCH_TRANSITION_WINDOW_S / int_time),
                )
            except (KeyError, TypeError, ValueError):
                n_to_flag = 2  # safe default for typical 0.25s int
            self._rfswitch_unknown_remaining = n_to_flag
            self.logger.info(
                f"RF switch transition detected: "
                f"{self._prev_rfswitch_state}→{new_rfswitch}. "
                f"Flagging next {n_to_flag} sample(s) as UNKNOWN to "
                f"cover the ~{int(RFSWITCH_TRANSITION_WINDOW_S * 1000)}ms "
                f"actuation+cadence window."
            )
        # Update prev only when we saw a real raw state — UNKNOWN
        # and None do not advance the comparison anchor.
        if new_rfswitch not in (None, "UNKNOWN"):
            self._prev_rfswitch_state = new_rfswitch
        # Apply the forward flag if we're inside a transition
        # window. This overrides any raw state in processed_md, and
        # is also applied when the sample carried no rfswitch
        # reading at all — the corr data is contaminated regardless
        # of whether we got a switch reading.
        if self._rfswitch_unknown_remaining > 0:
            processed_md["rfswitch"] = "UNKNOWN"
            self._rfswitch_unknown_remaining -= 1

        self._insert_sample(acc_cnt, sync_time, data, processed_md)

    def _insert_sample(
        self, acc_cnt, sync_time, sample_data, sample_metadata=None
    ):
        """
        Insert one sample into the active buffer, flushing to disk
        when the buffer is full.

        Parameters
        ----------
        acc_cnt : int
            Accumulation count for this sample.
        sync_time : float
            Synchronization time.
        sample_data : dict
            One spectrum per correlation pair.
        sample_metadata : dict, optional
            Pre-processed metadata: ``{key: scalar_value}``.
            Keys absent from the active metadata are back-filled
            with ``None``; active keys absent from
            *sample_metadata* get ``None`` appended.

        """
        sample_metadata = sample_metadata or {}
        self.acc_cnts[self.counter] = acc_cnt
        self.sync_times[self.counter] = sync_time
        # Per-pair safety net: a SNAP contract violation on one pair
        # (missing pair, half-spectrum, wrong dtype) must not cost us
        # the other pairs in the same sample. Skip the bad pair —
        # its slot stays at zero from buffer init/reset, which is
        # visually distinguishable from real data downstream — and
        # keep going. Half-spectra are not partially saved (the
        # ValueError catches them and the pair is dropped wholesale).
        for p in self.pairs:
            try:
                self.data[p][self.counter] = sample_data[p]
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(
                    f"SNAP contract violation: cannot write pair "
                    f"'{p}' at sample {self.counter} "
                    f"({type(e).__name__}: {e}). Zeroing slot. "
                    f"Producer must be fixed."
                )
                # Belt-and-suspenders: enforce the zero-on-drop
                # contract at the use site so it does not depend on
                # __init__/reset having previously zeroed the slot.
                self.data[p][self.counter] = 0
        # pad new keys so indices align 1:1 with samples
        for key in sample_metadata:
            if key not in self.metadata:
                self.metadata[key] = [None] * self.counter
            self.metadata[key].append(sample_metadata[key])
        # pad missing keys with None for 1:1 correspondence
        for key in self.metadata:
            if key not in sample_metadata:
                self.metadata[key].append(None)
        self.counter += 1
        self._prev_cnt = acc_cnt
        if self.counter == self.ntimes:
            self.corr_write()

    # ----------- double-buffered async writer -----------

    def _swap_buffers(self):
        """O(1) reference swap between active and standby buffers."""
        self.data, self._standby_data = (
            self._standby_data,
            self.data,
        )
        self.acc_cnts, self._standby_acc_cnts = (
            self._standby_acc_cnts,
            self.acc_cnts,
        )
        self.sync_times, self._standby_sync_times = (
            self._standby_sync_times,
            self.sync_times,
        )
        self.metadata, self._standby_metadata = (
            self._standby_metadata,
            self.metadata,
        )

    def _writer_loop(self):
        """Background thread that dequeues write jobs."""
        while True:
            job = self._write_queue.get()
            if job is None:  # shutdown signal
                self._standby_ready.set()
                self._write_queue.task_done()
                break
            (
                fname,
                data,
                acc_cnts,
                sync_times,
                metadata,
                counter,
                header,
            ) = job
            try:
                self._do_write(
                    fname,
                    data,
                    acc_cnts,
                    sync_times,
                    metadata,
                    counter,
                    header,
                )
            except Exception as e:
                self.logger.error(f"Failed to write {fname}: {e}")
                self._write_error = e
            finally:
                self._standby_ready.set()
                self._write_queue.task_done()

    def _do_write(
        self, fname, data, acc_cnts, sync_times, metadata, counter, header
    ):
        """
        Atomic write: write to a temp file, then rename. This prevents
        a crash mid-write from leaving a corrupted .h5 file — either
        the complete file exists or it doesn't (rename is atomic on
        POSIX).

        The rename is deliberately *outside* the cleanup ``try/except``:
        if ``write_hdf5`` succeeds but ``os.rename`` then raises (e.g.
        a transient NFS / filesystem error), we let the exception
        propagate without deleting the temp file. The just-written data
        is preserved on disk as ``corr_*.h5.tmp`` for an operator to
        recover by hand. Corr data is sacred — never destroy a
        successful write because of a downstream filesystem hiccup.

        """
        if fname is None:
            date = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%d_%H%M%SZ"
            )
            fname = self.save_dir / f"corr_{date}.h5"
            # Disambiguate if a file with the same second-resolution
            # timestamp already exists. In production, file_time is
            # 60-240s so this almost never triggers; the loop is
            # bounded by the number of writes per second (typically
            # zero) and each iteration is a single stat() call (~10
            # μs). An explicit fname (passed by the caller) is left
            # alone — that's the existing API contract.
            suffix = 1
            while fname.exists():
                fname = self.save_dir / f"corr_{date}-{suffix}.h5"
                suffix += 1
        self.logger.info(f"Writing correlation data to {fname}")

        # slice to counter so short final files don't include trailing zeros
        data = {p: d[:counter] for p, d in data.items()}
        acc_cnts = acc_cnts[:counter]
        sync_times = sync_times[:counter]
        metadata = {k: v[:counter] for k, v in metadata.items()}

        reshaped = reshape_data(
            data,
            acc_bins=header.get("acc_bins", 2),
            avg_even_odd=header.get("avg_even_odd", True),
        )
        full_header = append_corr_header(header, acc_cnts, sync_times)

        fd, tmp_path = tempfile.mkstemp(dir=self.save_dir, suffix=".h5.tmp")
        os.close(fd)
        try:
            write_hdf5(tmp_path, reshaped, full_header, metadata=metadata)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
        os.rename(tmp_path, fname)
        if self._on_write is not None:
            try:
                self._on_write(fname, time.time())
            except Exception as exc:
                self.logger.error(
                    f"on_write callback raised for {fname}: {exc}. "
                    "File is already on disk; heartbeat will be stale "
                    "until the next successful write."
                )

    def corr_write(self, fname=None):
        """
        Enqueue the current buffer for async writing, swap to the
        standby buffer, and reset.

        Parameters
        ----------
        fname : str, optional
            Filename where the data will be written. If not provided, a
            timestamped filename will be generated.

        """
        if self.counter == 0:
            return

        if self._write_error is not None:
            self.logger.error(
                f"Previous write failed: {self._write_error}. "
                "Data from that buffer was lost."
            )
            self._write_error = None

        # pad any short metadata lists to match counter
        for key in self.metadata:
            while len(self.metadata[key]) < self.counter:
                self.metadata[key].append(None)

        # Bounded wait for the writer to release the standby buffer.
        # If the writer is stuck (slow disk, NFS stall, etc.), drop
        # the active buffer rather than block forever — corr data is
        # sacred, but staying alive to capture future data is more
        # important than blocking the data loop indefinitely. The
        # script keeps running and resumes normal writes once the
        # writer unblocks.
        if not self._standby_ready.wait(timeout=self._writer_timeout):
            self._dropped_buffers += 1
            self.logger.error(
                f"Writer thread blocked for >{self._writer_timeout}s; "
                f"dropping buffer of {self.counter} samples (total "
                f"dropped: {self._dropped_buffers}). Script continues; "
                f"resolve the underlying I/O issue."
            )
            self.reset()
            return
        self._standby_ready.clear()

        # package job with current buffer references
        job = (
            fname,
            self.data,
            self.acc_cnts,
            self.sync_times,
            self.metadata,
            self.counter,
            self.header.copy(),
        )

        # swap buffers and reset active
        self._swap_buffers()
        self.reset()

        # enqueue for async write
        self._write_queue.put(job)

    def close(self):
        """
        Flush any pending data, shut down the writer thread, and
        surface final-state errors.

        Calls ``corr_write`` first if the active buffer is non-empty,
        so a caller can simply call ``close()`` at the end of a run
        without remembering to flush manually. The flush goes through
        the normal writer path (bounded wait, drop-on-timeout, full
        ERROR logging) — corr data is sacred but ``close()`` must
        remain bounded.

        """
        if self.counter > 0:
            self.corr_write()
        self._write_queue.put(None)
        self._writer_thread.join(timeout=30)
        if self._writer_thread.is_alive():
            self.logger.error("Writer thread did not shut down within timeout")
        if self._write_error is not None:
            self.logger.error(
                f"Pending write error at shutdown: {self._write_error}. "
                f"Final buffer data was lost."
            )
            self._write_error = None
        if self._dropped_buffers > 0:
            self.logger.error(
                f"Total buffers dropped due to writer hang: "
                f"{self._dropped_buffers}. Investigate I/O performance."
            )
