import json
import logging

import numpy as np
from eigsep_redis import SingleStreamReader, SingleStreamWriter

from .keys import ADC_SNAPSHOT_STREAM

logger = logging.getLogger(__name__)


class AdcSnapshotWriter(SingleStreamWriter):
    """
    Publish raw ADC snapshot arrays onto the diagnostic snapshot stream.

    Each entry carries the bare int8 samples pulled from the SNAP's
    ``input_snapshot_bram`` by :meth:`Input.get_adc_snapshot`, stacked
    across all antennas, plus a JSON sidecar with timing context and
    wiring so a downstream live-status app can label cores without
    hardcoding the layout. Not routed through ``MetadataWriter`` —
    snapshots are binary and bypass the metadata-averaging path; they
    live in Redis only and are not folded into the HDF5 corr file.

    ``maxlen`` is a dead-reader failsafe: at the default 20 s publish
    cadence (``adc_snapshot_period_s``), 60 entries is 20 minutes of
    headroom, which covers any realistic live-status reconnect window.
    Snapshots are diagnostic and not recoverable from a different
    source, so we don't need hours of buffering — if the reader fell
    behind that far, the data it would pull is already stale for
    debugging purposes.
    """

    stream = ADC_SNAPSHOT_STREAM
    maxlen = 60

    def _encode(
        self,
        data,
        unix_ts,
        sync_time=None,
        corr_acc_cnt=None,
        wiring=None,
    ):
        if not isinstance(data, np.ndarray):
            raise ValueError("data must be a numpy array")
        # Force C-contiguous so tobytes() and the reader's reshape
        # always agree on memory layout regardless of what the caller
        # passed (views, F-contig, strided, etc.).
        data = np.ascontiguousarray(data)
        arr_meta = {
            "dtype": data.dtype.str,
            "shape": list(data.shape),
        }
        sidecar = {
            "arr_meta": arr_meta,
            "unix_ts": float(unix_ts),
            "sync_time": sync_time,
            "corr_acc_cnt": corr_acc_cnt,
            "wiring": wiring,
        }
        return {
            "data": data.tobytes(),
            "sidecar": json.dumps(sidecar),
        }

    def add(
        self,
        data,
        unix_ts,
        sync_time=None,
        corr_acc_cnt=None,
        wiring=None,
    ):
        """
        Publish one ADC snapshot frame.

        Parameters
        ----------
        data : np.ndarray
            Raw ADC samples, shape ``(n_antennas, 2, n_samples)``,
            dtype ``int8``. Axis 0 is the antenna index as consumed by
            :meth:`Input.get_adc_snapshot`; axis 1 is pol (0=x, 1=y);
            axis 2 is the sample time axis.
        unix_ts : float
            Wall clock time at which the snapshot was captured.
        sync_time : float or None
            The SNAP sync_time at capture, if available. Lets the
            consumer place the snapshot on the same time axis as the
            corr stream.
        corr_acc_cnt : int or None
            The ``corr_acc_cnt`` register value at capture, if
            available. Same alignment purpose as ``sync_time``.
        wiring : dict or None
            Subset of ``wiring.yaml`` describing the antenna-to-core
            mapping. Included so the live-status app can label cores
            without parsing the corr header separately.

        Raises
        ------
        ValueError
            If ``data`` is not a numpy array.
        """
        self.publish(
            data,
            unix_ts,
            sync_time=sync_time,
            corr_acc_cnt=corr_acc_cnt,
            wiring=wiring,
        )


class AdcSnapshotReader(SingleStreamReader):
    """
    Consume ADC snapshot frames from the diagnostic snapshot stream.

    Returns ``(data, sidecar)`` from :meth:`read`; the
    ``(None, None)`` tuple is returned when the snapshot stream
    isn't registered yet (producer hasn't started).
    """

    stream = ADC_SNAPSHOT_STREAM
    absent_warning = (
        "No ADC snapshot stream found. Publisher may not have started yet."
    )

    def _absent_sentinel(self):
        return None, None

    def _decode(self, entry_id, fields):
        sidecar = json.loads(fields[b"sidecar"].decode("utf-8"))
        arr_meta = sidecar["arr_meta"]
        data = np.frombuffer(
            fields[b"data"], dtype=np.dtype(arr_meta["dtype"])
        ).reshape(arr_meta["shape"])
        return data, sidecar
