import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import h5py
import numpy as np

from eigsep_redis.keys import DATA_STREAMS_SET

from .keys import VNA_STREAM
from .vna_calibration import calibrate_s11

logger = logging.getLogger(__name__)


class VnaWriter:
    """
    Publish VNA S11 measurements onto the VNA stream.

    Each entry carries per-trace raw bytes, an ``arr_meta`` sidecar
    (dtype/shape/order), and optional JSON-encoded ``header`` and
    ``metadata`` blobs. Numpy arrays inside ``header`` / ``metadata``
    are flattened to lists before encoding.

    ``maxlen`` is a dead-reader failsafe: each ``measure_s11`` call
    produces one bundled entry (ant+noise+load+OSL or rec+OSL), and
    the ground reader drains it synchronously. 200 covers ~100 full
    ant+rec sweeps of headroom even during VNA-only campaigns —
    well beyond any realistic ground-reader outage window.
    """

    maxlen = 200

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


def save_vna_manual_h5(s11, header, metadata, *, save_dir, mode):
    """Write one VNA bundle to a local HDF5 file for bring-up tests.

    The companion to ``scripts/vna_manual.py``. Stores raw S11 arrays
    under ``/raw/``, ideal-OSL calibrated S11 (matching the live-status
    pane's calibration) under ``/calibrated/``, the freq axis under
    ``/freqs``, the header on root attrs (nested dicts JSON-encoded),
    and the metadata snapshot under ``/metadata_snapshot/`` attrs.

    Calibration failures (shape mismatch from ``calibrate_s11``) are
    logged at ERROR and the ``/calibrated/`` group is skipped — the
    raw arrays still land so the operator can re-calibrate offline.

    Parameters
    ----------
    s11 : dict[str, np.ndarray]
        The first element of the tuple returned by
        ``PandaClient.measure_s11``.
    header : dict
        The second element. Must contain ``"freqs"`` (sequence of Hz).
    metadata : dict
        The third element (panda metadata snapshot at trigger time).
    save_dir : pathlib.Path
        Directory the file is written into. Must already exist.
    mode : str
        Either ``"ant"`` or ``"rec"``. Used in the filename and to
        pick which keys count as DUTs (everything that is not a
        ``cal:*`` standard).

    Returns
    -------
    pathlib.Path
        The path of the file that was written.
    """
    if mode not in {"ant", "rec"}:
        raise ValueError(f"mode must be 'ant' or 'rec', got {mode!r}")
    save_dir = Path(save_dir)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = save_dir / f"vna_manual_{mode}_{stamp}.h5"

    dut_keys = sorted(k for k in s11 if not k.startswith("cal:"))
    cal_o = np.asarray(s11["cal:VNAO"])
    cal_s = np.asarray(s11["cal:VNAS"])
    cal_l = np.asarray(s11["cal:VNAL"])

    calibrated = {}
    cal_error = None
    for dut in dut_keys:
        try:
            calibrated[dut] = calibrate_s11(
                np.asarray(s11[dut]), cal_o, cal_s, cal_l
            )
        except ValueError as exc:
            cal_error = exc
            calibrated = {}
            break
    if cal_error is not None:
        logger.error(
            "vna_manual: calibration failed for mode=%r, skipping "
            "/calibrated/ group: %s",
            mode,
            cal_error,
        )

    with h5py.File(path, "w") as f:
        raw_grp = f.create_group("raw")
        for key, arr in s11.items():
            raw_grp.create_dataset(key, data=np.asarray(arr))
        if calibrated:
            cal_grp = f.create_group("calibrated")
            for dut, arr in calibrated.items():
                cal_grp.create_dataset(dut, data=arr)
        f.create_dataset(
            "freqs", data=np.asarray(header["freqs"], dtype=float)
        )
        for k, v in header.items():
            if k in {"freqs", "mode"}:
                continue
            if isinstance(v, (dict, list, tuple)):
                f.attrs[k] = json.dumps(v)
            elif isinstance(v, np.ndarray):
                f.attrs[k] = json.dumps(v.tolist())
            else:
                f.attrs[k] = v
        f.attrs["mode"] = mode
        f.attrs["vna_manual_script_version"] = "1"
        meta_grp = f.create_group("metadata_snapshot")
        for k, v in (metadata or {}).items():
            if isinstance(v, (dict, list, tuple)):
                meta_grp.attrs[k] = json.dumps(v)
            elif isinstance(v, np.ndarray):
                meta_grp.attrs[k] = json.dumps(v.tolist())
            else:
                meta_grp.attrs[k] = v
    return path
