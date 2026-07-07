import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import h5py
import numpy as np

from cmt_vna import VNA
from eigsep_redis import (
    MetadataSnapshotReader,
    SingleStreamReader,
    SingleStreamWriter,
)
from picohost.proxy import PicoProxy

from . import imu_calibration, obs_config_owner, run_tag, vna_service
from ._scripts_util import require_pico
from .io import _validate_vna_s11_data, _validate_vna_s11_header
from .keys import VNA_STREAM
from .vna_calibration import calibrate_s11

logger = logging.getLogger(__name__)


class VnaWriter(SingleStreamWriter):
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

    stream = VNA_STREAM
    maxlen = 200

    def _encode(self, data, header=None, metadata=None):
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
        return payload

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
        self.publish(data, header=header, metadata=metadata)


class VnaReader(SingleStreamReader):
    """
    Consume VNA S11 measurements from the VNA stream.

    Returns ``(data, header, metadata)`` from :meth:`read`; the
    ``(None, None, None)`` tuple is returned when the VNA stream
    isn't registered yet (producer hasn't started).
    """

    stream = VNA_STREAM
    absent_warning = (
        "No VNA data stream found. "
        "Please ensure the VNA is running and sending data."
    )

    def _absent_sentinel(self):
        return None, None, None

    def _decode(self, entry_id, fields):
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


@dataclass
class VnaSubsystem:
    """Minimal VNA-producer surface a bring-up script needs to call
    :func:`measure_s11`.

    Returned by :func:`build_vna_subsystem`. See ``scripts/CLAUDE.md``
    for the broader bring-up-script contract this dataclass embodies.
    """

    vna: VNA
    vna_writer: "VnaWriter"
    metadata_snapshot: MetadataSnapshotReader
    cleanup: Callable[[], None]


def build_vna_subsystem(transport, cfg, *, source, dummy=False):
    """Assemble the minimum VNA-producer subsystem for bring-up scripts.

    The bring-up-script counterpart to ``PandaClient.init_VNA``: builds
    the configured ``cmt_vna.VNA`` (or ``DummyVNA``), wires its
    ``switch_fn`` through a ``PicoProxy("rfswitch")`` so the Pico's
    command stream arbitrates switching across concurrent producers,
    and returns the producer bus surface (``VnaWriter``) plus the
    consumer snapshot (``MetadataSnapshotReader``) needed to call
    :func:`measure_s11`.

    Unlike ``PandaClient.__init__`` this:

    - does **not** start a panda heartbeat thread (``panda:hb*``
      belongs to the real panda process);
    - does **not** force-switch the rig to RFANT at startup
      (would fight a running observer);
    - does **not** upload ``cfg`` to ``ConfigStore`` (the running
      observer owns it);
    - does **not** build a ``MotionSwitchCoordinator`` (its in-process
      lock would not serialize against the real observer or sibling
      bring-up scripts in other terminals; the Pico does the
      cross-process arbitration).

    These omissions are the contract documented in ``scripts/CLAUDE.md``.

    Parameters
    ----------
    transport : eigsep_redis.Transport
        Bare transport — typically from
        ``_scripts_util.build_transport_bare``. Must NOT have a
        ``DummyPandaClient`` already attached (would double-register
        dummy picos when ``dummy=True``).
    cfg : dict
        Loaded obs_config (``vna_ip`` / ``vna_port`` / ``vna_timeout``
        / ``vna_settings``). Not uploaded to Redis.
    source : str
        Identifier stamped onto the ``PicoProxy("rfswitch", ...)`` so
        operator log lines distinguish ``vna_manual`` /
        ``record_vna`` / etc.
    dummy : bool, optional
        If True, use ``DummyVNA`` and start an in-process dummy
        ``PicoManager`` (so the ``rfswitch`` proxy resolves). The
        manager is shut down by the returned ``cleanup`` callback. If
        False (default), start ``cmtvna.service`` and wait for the R60
        to answer before opening the socket; the returned ``cleanup``
        stops the service again.

    Returns
    -------
    VnaSubsystem
        Dataclass with ``vna``, ``vna_writer``, ``metadata_snapshot``
        and a ``cleanup`` callable the caller must invoke on teardown
        (stops the dummy ``PicoManager`` when ``dummy=True``, stops
        ``cmtvna.service`` when ``dummy=False``).

    Raises
    ------
    SystemExit
        If ``rfswitch`` is not registered with ``PicoManager`` (raised
        from :func:`require_pico` with an operator-actionable message).
    """
    sw_proxy = PicoProxy("rfswitch", transport, source=source)

    def switch_fn(state):
        if sw_proxy.send_command("switch", state=state) is None:
            raise RuntimeError(
                f"RF switch to {state} failed: rfswitch not registered "
                "with PicoManager."
            )

    manager = None
    vna_cls = VNA
    if dummy:
        from cmt_vna.testing import DummyVNA
        from eigsep_observing.testing import start_dummy_pico_manager

        vna_cls = DummyVNA
        manager = start_dummy_pico_manager(transport)
    else:
        # Real hardware: bring cmtvna.service up before opening the
        # socket. cleanup() (below) stops it again — including on any
        # failure during the build, so a half-built subsystem never
        # leaves the CPU-pegging service running.
        vna_service.start()

    def cleanup():
        if manager is not None:
            manager.stop()
        if not dummy:
            try:
                vna_service.stop()
            except Exception:
                logger.warning("cmtvna stop failed", exc_info=True)

    try:
        if not dummy:
            vna_service.wait_ready(cfg["vna_ip"], cfg["vna_port"])
        vna = vna_cls(
            ip=cfg["vna_ip"],
            port=cfg["vna_port"],
            timeout=cfg["vna_timeout"],
            switch_fn=switch_fn,
        )
        require_pico(sw_proxy)
        setup_kwargs = cfg["vna_settings"].copy()
        setup_kwargs["power_dBm"] = setup_kwargs["power_dBm"]["ant"]
        vna.setup(**setup_kwargs)
    except Exception:
        cleanup()
        raise

    return VnaSubsystem(
        vna=vna,
        vna_writer=VnaWriter(transport),
        metadata_snapshot=MetadataSnapshotReader(transport),
        cleanup=cleanup,
    )


def measure_s11(
    vna,
    mode,
    *,
    cfg,
    transport,
    vna_writer,
    metadata_snapshot,
    on_contract_violation=None,
    logger=logger,
):
    """Run one VNA S11 bundle and publish it on the VNA stream.

    The eigsep-side VNA producer protocol: drive the cmt_vna primitives
    to get OSL + DUT traces, build a header with provenance overlays
    and a metadata snapshot, validate against the schema, and publish
    via :class:`VnaWriter`. Returns the published payload so callers
    that want a local artifact (e.g. ``scripts/vna_manual.py``) do not
    need to re-drain the VNA stream.

    Used by :meth:`eigsep_observing.client.PandaClient.measure_s11`
    (production observing) and ``scripts/vna_manual.py`` (interactive
    bring-up) — the two pathways share this protocol so a future
    contract change lands in one place.

    Parameters
    ----------
    vna : cmt_vna.VNA
        Configured VNA instance with ``switch_fn`` already wired.
    mode : str
        ``"ant"`` or ``"rec"``.
    cfg : dict
        Observing config. ``cfg["vna_settings"]["power_dBm"][mode]``
        is set on ``vna.power_dBm`` before the sweep; the full ``cfg``
        is embedded into the header as ``obs_config``.
    transport : eigsep_redis.Transport
        Used to read the active ``run_tag`` for the header overlay.
    vna_writer : VnaWriter
        Sink for the bundled S11 + header + metadata payload.
    metadata_snapshot : eigsep_redis.MetadataSnapshotReader
        Captures the panda-side metadata at the moment of publish.
    on_contract_violation : callable, optional
        ``f(msg: str) -> None``. Called once per contract violation
        with a formatted message. Defaults to ``logger.warning``.
        Production callers pass ``PandaClient._warn_with_status`` so
        violations also reach the Redis status stream that the ground
        observer relays.
    logger : logging.Logger, optional
        Where to log info-level progress. Defaults to this module's
        logger.

    Returns
    -------
    s11 : dict[str, np.ndarray]
        Bundle published to Redis (raw DUT traces plus ``cal:VNAO`` /
        ``cal:VNAS`` / ``cal:VNAL`` standards).
    header : dict
        Header published alongside the bundle (instrument header plus
        ``mode``, ``metadata_snapshot_unix``, ``run_tag``,
        ``run_started_at_unix``, ``obs_config_owner``,
        ``obs_config_owner_uploaded_unix``, ``obs_config``,
        ``imu_calibration``, ``imu_calibration_upload_unix``).
    metadata : dict
        Panda-side metadata snapshot captured at trigger time.

    Raises
    ------
    ValueError
        If ``mode`` is not ``"ant"`` or ``"rec"``.
    RuntimeError
        If ``vna`` is ``None``.

    Notes
    -----
    Contract validation is loud-but-non-blocking: a violation fires
    ``on_contract_violation`` and the bundle is *still* published, so
    a producer-side contract drift never blocks data flow. cmt_vna
    handles all RF switching internally via its ``switch_fn``.
    """
    if mode not in ("ant", "rec"):
        raise ValueError(f"Unknown VNA mode: {mode}. Must be 'ant' or 'rec'.")
    if vna is None:
        raise RuntimeError(
            "VNA not initialized. Open a VNA session first "
            "(PandaClient.vna_open()/vna_session(), or "
            "build_vna_subsystem)."
        )

    vna.power_dBm = cfg["vna_settings"]["power_dBm"][mode]
    osl_s11 = vna.measure_OSL()
    if mode == "ant":
        logger.info("Measuring antenna, noise, load S11")
        s11 = vna.measure_ant(measure_noise=True, measure_load=True)
    else:  # mode is rec
        logger.info("Measuring receiver S11")
        s11 = vna.measure_rec()
    for k, v in osl_s11.items():
        s11[f"cal:{k}"] = v

    header = vna.header
    header["mode"] = mode
    header["metadata_snapshot_unix"] = time.time()
    tag = run_tag.read(transport)
    owner = obs_config_owner.read_owner(transport)
    header["run_tag"] = (
        tag["run_tag"] if tag["run_tag"] is not None else "UNKNOWN"
    )
    header["run_started_at_unix"] = (
        tag["run_started_at_unix"]
        if tag["run_started_at_unix"] is not None
        else 0.0
    )
    header["obs_config_owner"] = (
        owner["owner"] if owner["owner"] is not None else "UNKNOWN"
    )
    header["obs_config_owner_uploaded_unix"] = (
        owner["uploaded_at_unix"]
        if owner["uploaded_at_unix"] is not None
        else 0.0
    )
    header["obs_config"] = dict(cfg)
    cal = imu_calibration.read_calibration(transport)
    header["imu_calibration"] = cal
    header["imu_calibration_upload_unix"] = imu_calibration.upload_unix(cal)
    metadata = metadata_snapshot.get()

    violations = _validate_vna_s11_header(header) + _validate_vna_s11_data(
        s11, mode
    )
    if violations:
        warn = on_contract_violation or logger.warning
        warn(
            f"VNA S11 producer contract violation (mode={mode!r}): "
            + "; ".join(violations)
        )

    vna_writer.add(s11, header=header, metadata=metadata)
    logger.info("Vna data added to redis")
    return s11, header, metadata


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
