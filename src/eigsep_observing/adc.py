import json
import logging

import numpy as np
from eigsep_redis.keys import DATA_STREAMS_SET

from .keys import ADC_SNAPSHOT_STREAM

logger = logging.getLogger(__name__)


class AdcSnapshotWriter:
    """
    Publish raw ADC snapshot arrays onto the diagnostic snapshot stream.

    Each entry carries the bare int8 samples pulled from the SNAP's
    ``input_snapshot_bram`` by :meth:`Input.get_adc_snapshot`, stacked
    across all antennas, plus a JSON sidecar with timing context and
    wiring so a downstream live-status app can label cores without
    hardcoding the layout. Not routed through ``MetadataWriter`` —
    snapshots are binary and bypass the metadata-averaging path; they
    live in Redis only and are not folded into the HDF5 corr file.

    ``maxlen`` is a dead-reader failsafe: at the 1 Hz default publish
    cadence, 60 entries is one minute of headroom, which covers any
    realistic live-status reconnect window. Snapshots are diagnostic
    and not recoverable from a different source, so we don't need
    hours of buffering — if the reader fell behind that far, the data
    it would pull is already stale for debugging purposes.
    """

    maxlen = 60

    def __init__(self, transport):
        self.transport = transport

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
        payload = {
            "data": data.tobytes(),
            "sidecar": json.dumps(sidecar),
        }
        r = self.transport.r
        r.xadd(
            ADC_SNAPSHOT_STREAM,
            payload,
            maxlen=self.maxlen,
            approximate=True,
        )
        r.sadd(DATA_STREAMS_SET, ADC_SNAPSHOT_STREAM)


class AdcSnapshotReader:
    """Consume ADC snapshot frames from the diagnostic snapshot stream."""

    def __init__(self, transport):
        self.transport = transport

    def read(self, timeout=0):
        """
        Blocking read of one ADC snapshot frame.

        Parameters
        ----------
        timeout : float
            Timeout in seconds. Pass 0 to block indefinitely.

        Returns
        -------
        (data, sidecar) : tuple
            ``(np.ndarray, dict)``. Returns ``(None, None)`` if no
            snapshot stream exists yet.

        Raises
        ------
        TimeoutError
            If no entry arrives within ``timeout``.
        """
        r = self.transport.r
        if not r.sismember(DATA_STREAMS_SET, ADC_SNAPSHOT_STREAM):
            self.transport.logger.warning(
                "No ADC snapshot stream found. "
                "Publisher may not have started yet."
            )
            return None, None
        last_id = self.transport._streams_from_set(DATA_STREAMS_SET)[
            ADC_SNAPSHOT_STREAM
        ]
        out = r.xread(
            {ADC_SNAPSHOT_STREAM: last_id},
            count=1,
            block=int(timeout * 1000),
        )
        if not out:
            raise TimeoutError("No ADC snapshot received within timeout.")
        eid, fields = out[0][1][0]
        self.transport._set_last_read_id(ADC_SNAPSHOT_STREAM, eid)
        sidecar = json.loads(fields[b"sidecar"].decode("utf-8"))
        arr_meta = sidecar["arr_meta"]
        data = np.frombuffer(
            fields[b"data"], dtype=np.dtype(arr_meta["dtype"])
        ).reshape(arr_meta["shape"])
        return data, sidecar
