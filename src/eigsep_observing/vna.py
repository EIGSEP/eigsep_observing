import json
import logging

import numpy as np

from eigsep_redis.keys import DATA_STREAMS_SET

from .keys import VNA_STREAM

logger = logging.getLogger(__name__)


class VnaWriter:
    """
    Publish VNA S11 measurements onto the VNA stream.

    Each entry carries per-trace raw bytes, an ``arr_meta`` sidecar
    (dtype/shape/order), and optional JSON-encoded ``header`` and
    ``metadata`` blobs. Numpy arrays inside ``header`` / ``metadata``
    are flattened to lists before encoding.
    """

    maxlen = 1000

    def __init__(self, transport):
        self.transport = transport

    def add(self, data, header=None, metadata=None):
        """
        Publish one VNA measurement.

        Parameters
        ----------
        data : dict[str, np.ndarray]
            Keys are measurement modes (e.g. ``"ant"``, ``"rec"``,
            ``"cal:open"``), values are complex arrays.
        header : dict or None
            VNA configuration. Placed in the file header downstream.
        metadata : dict or None
            Live sensor snapshot at VNA trigger time.

        Raises
        ------
        ValueError
            If ``data`` is empty.
        """
        arr = next(iter(data.values()), None)
        if arr is None:
            raise ValueError("Data cannot be empty")
        arr_meta = {
            "dtype": arr.dtype.str,
            "shape": arr.shape,
            "order": "C" if arr.flags["C_CONTIGUOUS"] else "F",
        }
        payload = {k: arr.tobytes() for k, arr in data.items()}
        payload["arr_meta"] = json.dumps(arr_meta)
        if header is not None:
            _hdr = header.copy()
            for k, v in _hdr.items():
                if isinstance(v, np.ndarray):
                    _hdr[k] = v.tolist()
            payload["header"] = json.dumps(_hdr)
        if metadata is not None:
            _md = metadata.copy()
            for k, v in _md.items():
                if isinstance(v, np.ndarray):
                    _md[k] = v.tolist()
            payload["metadata"] = json.dumps(_md)
        r = self.transport.r
        r.xadd(
            VNA_STREAM,
            payload,
            maxlen=self.maxlen,
            approximate=True,
        )
        r.sadd(DATA_STREAMS_SET, VNA_STREAM)


class VnaReader:
    """Consume VNA S11 measurements from the VNA stream."""

    def __init__(self, transport):
        self.transport = transport

    def read(self, timeout=0):
        """
        Blocking read of one VNA entry.

        Parameters
        ----------
        timeout : int
            Timeout in seconds. Pass 0 to block indefinitely.

        Returns
        -------
        (data, header, metadata) : tuple
            ``(dict[str, np.ndarray], dict | None, dict | None)``.
            Returns ``(None, None, None)`` if no VNA stream exists yet.

        Raises
        ------
        TimeoutError
            If no entry arrives within ``timeout``.
        """
        r = self.transport.r
        if not r.sismember(DATA_STREAMS_SET, VNA_STREAM):
            self.transport.logger.warning(
                "No VNA data stream found. "
                "Please ensure the VNA is running and sending data."
            )
            return None, None, None
        last_id = self.transport._streams_from_set(DATA_STREAMS_SET)[
            VNA_STREAM
        ]
        out = r.xread(
            {VNA_STREAM: last_id},
            count=1,
            block=int(timeout * 1000),
        )
        if not out:
            raise TimeoutError("No VNA data received within timeout.")
        eid, fields = out[0][1][0]
        self.transport._set_last_read_id(VNA_STREAM, eid)
        arr_meta = json.loads(fields.pop(b"arr_meta").decode("utf-8"))
        if b"header" in fields:
            header = json.loads(fields.pop(b"header").decode("utf-8"))
        else:
            header = None
        if b"metadata" in fields:
            metadata = json.loads(fields.pop(b"metadata").decode("utf-8"))
        else:
            metadata = None
        vna_data = {}
        for k, v in fields.items():
            arr = np.frombuffer(v, dtype=np.dtype(arr_meta["dtype"])).reshape(
                arr_meta["shape"], order=arr_meta["order"]
            )
            vna_data[k.decode()] = arr
        return vna_data, header, metadata
