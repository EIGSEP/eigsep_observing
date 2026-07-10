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
from picohost.base import PicoPotentiometer
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
    produces one bundled entry (ant+load+noise+amb+sp1_short+sp1_open
    +OSL or rec+OSL), and the ground reader drains it synchronously. 200
    covers ~100 full ant+rec sweeps of headroom even during VNA-only
    campaigns — well beyond any realistic ground-reader outage
    window.
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
    sp1_term_fn: Callable[[str], None]
    cleanup: Callable[[], None]


def build_vna_subsystem(transport, cfg, *, source, dummy=False):
    """Assemble the minimum VNA-producer subsystem for bring-up scripts.

    The bring-up-script counterpart to ``PandaClient.init_VNA``: builds
    the configured ``cmt_vna.VNA`` (or ``DummyVNA``), wires its
    ``switch_fn`` through a ``PicoProxy("rfswitch")`` so the Pico's
    command stream arbitrates switching across concurrent producers,
    builds an analogous ``sp1_term_fn`` through a
    ``PicoProxy("potmon")`` for the SP1 failsafe termination that
    ``measure_s11(mode="ant")`` requires, and returns the producer bus
    surface (``VnaWriter``) plus the consumer snapshot
    (``MetadataSnapshotReader``) needed to call :func:`measure_s11`.

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
        Identifier stamped onto the ``PicoProxy("rfswitch", ...)`` /
        ``PicoProxy("potmon", ...)`` so operator log lines distinguish
        ``vna_manual`` / ``record_vna`` / etc.
    dummy : bool, optional
        If True, use ``DummyVNA`` and start an in-process dummy
        ``PicoManager`` (so the ``rfswitch`` and ``potmon`` proxies
        resolve). The manager is shut down by the returned ``cleanup``
        callback. If False (default), start ``cmtvna.service`` and wait
        for the R60 to answer before opening the socket; the returned
        ``cleanup`` stops the service again.

    Returns
    -------
    VnaSubsystem
        Dataclass with ``vna``, ``vna_writer``, ``metadata_snapshot``,
        ``sp1_term_fn`` (pass through to ``measure_s11`` for ant mode)
        and a ``cleanup`` callable the caller must invoke on teardown
        (stops the dummy ``PicoManager`` when ``dummy=True``, stops
        ``cmtvna.service`` when ``dummy=False``).

    Raises
    ------
    SystemExit
        If ``rfswitch`` is not registered with ``PicoManager`` (raised
        from :func:`require_pico` with an operator-actionable message).
        ``potmon`` is deliberately *not* required at build time — see
        the ``sp1_term_fn`` closure below.
    """
    sw_proxy = PicoProxy("rfswitch", transport, source=source)
    pot_proxy = PicoProxy("potmon", transport, source=source)

    def switch_fn(state):
        if sw_proxy.send_command("switch", state=state) is None:
            raise RuntimeError(
                f"RF switch to {state} failed: rfswitch not registered "
                "with PicoManager."
            )

    # Deliberate deferral: potmon availability is NOT checked at build
    # time (no ``require_pico(pot_proxy)``). Only ant-mode bundles
    # touch the SP1 termination, so rec-only / probe-only sessions
    # must keep working with the potmon pico down. When an ant bundle
    # *is* attempted, ``measure_s11`` calls this closure and the
    # ``send_command(...) is None`` check below surfaces the absence
    # as a clear RuntimeError at use time.
    def sp1_term_fn(term):
        if term not in PicoPotentiometer.SP1_TERMINATIONS:
            raise ValueError(
                f"Invalid SP1 termination {term!r}; valid: "
                f"{sorted(PicoPotentiometer.SP1_TERMINATIONS)}"
            )
        if pot_proxy.send_command("set_sp1_termination", state=term) is None:
            raise RuntimeError(
                f"SP1 termination to {term} failed: potmon not "
                "registered with PicoManager."
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
        sp1_term_fn=sp1_term_fn,
        cleanup=cleanup,
    )


def _stamp_provenance(header, transport, cfg):
    """Overlay Redis-side provenance onto a VNA header, in place.

    The shared header block for every VNA capture: snapshot timestamp,
    active ``run_tag``, ``obs_config`` ownership, the full ``cfg``, and
    the IMU calibration. Absent Redis state lands as the sentinel
    ``"UNKNOWN"`` / ``0.0`` values so headers stay schema-shaped.
    """
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


def measure_s11(
    vna,
    mode,
    *,
    cfg,
    transport,
    vna_writer,
    metadata_snapshot,
    sp1_term_fn=None,
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
    sp1_term_fn : callable, optional
        ``f(term: str) -> None``. Drives the Spare-1 failsafe
        termination between ``"SHORT"`` and ``"OPEN"``; raises on
        failure, same contract as ``vna.switch_fn``. Required for
        ``mode="ant"`` (raises ``RuntimeError`` if ``None``); ignored
        for ``mode="rec"``. Production callers pass
        ``PandaClient._set_sp1_term``.
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
        Bundle published to Redis (raw DUT traces — ``ant``/``load``/
        ``noise``/``amb``/``sp1_short``/``sp1_open`` in ant mode,
        ``rec`` in rec mode — plus ``cal:VNAO`` / ``cal:VNAS`` /
        ``cal:VNAL`` standards).
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
        If ``vna`` is ``None``, or if ``mode="ant"`` and
        ``sp1_term_fn`` is ``None``.

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
        if sp1_term_fn is None:
            raise RuntimeError(
                "measure_s11(mode='ant') requires sp1_term_fn to drive "
                "the SP1 failsafe termination (PandaClient passes "
                "_set_sp1_term)."
            )
        logger.info("Measuring antenna, noise, load S11")
        s11 = vna.measure_ant(measure_noise=True, measure_load=True)
        logger.info("Measuring ambient-load S11")
        s11["amb"] = vna.measure_dut("VNAAMB")
        try:
            logger.info("Measuring Spare-1 S11, SHORT termination")
            sp1_term_fn("SHORT")
            s11["sp1_short"] = vna.measure_dut("VNASP1")
            logger.info("Measuring Spare-1 S11, OPEN termination")
            sp1_term_fn("OPEN")
            # Same rfswitch path — only the far-end termination moved,
            # so sweep again without re-switching.
            s11["sp1_open"] = vna.measure_S11()
        finally:
            # Best-effort failsafe restore: must not mask a measurement
            # error, and every observing-mode transition re-asserts
            # SHORT anyway.
            try:
                sp1_term_fn("SHORT")
            except Exception as exc:
                logger.warning(
                    f"Failed to restore SP1 termination to SHORT: "
                    f"{type(exc).__name__}: {exc}"
                )
    else:  # mode is rec
        logger.info("Measuring receiver S11")
        s11 = vna.measure_rec()
    for k, v in osl_s11.items():
        s11[f"cal:{k}"] = v

    header = vna.header
    header["mode"] = mode
    _stamp_provenance(header, transport, cfg)
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


def measure_dut(vna, state, *, cfg, transport, metadata_snapshot):
    """Run one one-off S11 probe of an arbitrary switch path.

    The bring-up companion to :func:`measure_s11`: routes the RF
    switch to ``state`` via ``cmt_vna.VNA.measure_dut`` and takes a
    single sweep — no OSL standards and no publish on the VNA stream.
    The stream protocol is bundle-shaped (OSL + DUT + validated
    header), so a lone probe trace is a local artifact only; keep it
    with :func:`save_vna_dut_h5`.

    Power is whatever the VNA is currently set to
    (``build_vna_subsystem`` applies ``vna_settings`` with the ``ant``
    power); set ``vna.power_dBm`` first if a different level is
    needed.

    Parameters
    ----------
    vna : cmt_vna.VNA
        Configured VNA instance with ``switch_fn`` already wired.
    state : str
        Switch path name, passed verbatim to the switch (e.g.
        ``"VNAANT"``, ``"VNANON"``).
    cfg : dict
        Observing config, embedded into the header as ``obs_config``.
    transport : eigsep_redis.Transport
        Used to read the active ``run_tag`` for the header overlay.
    metadata_snapshot : eigsep_redis.MetadataSnapshotReader
        Captures the panda-side metadata at the moment of the probe.

    Returns
    -------
    s11 : np.ndarray
        Complex S11 sweep of the selected path.
    header : dict
        Instrument header plus the provenance overlays of
        :func:`measure_s11`; ``mode`` is ``"dut:<state>"``.
    metadata : dict
        Panda-side metadata snapshot captured at probe time.

    Raises
    ------
    RuntimeError
        If ``vna`` is ``None``.
    """
    if vna is None:
        raise RuntimeError(
            "VNA not initialized. Open a VNA session first "
            "(PandaClient.vna_open()/vna_session(), or "
            "build_vna_subsystem)."
        )
    logger.info("Measuring one-off DUT S11: %s", state)
    s11 = vna.measure_dut(state)
    header = vna.header
    header["mode"] = f"dut:{state}"
    _stamp_provenance(header, transport, cfg)
    metadata = metadata_snapshot.get()
    return s11, header, metadata


def _write_json_attrs(target, mapping, skip=()):
    """Write a dict onto an h5 node's attrs, JSON-encoding nested values."""
    for k, v in mapping.items():
        if k in skip:
            continue
        if isinstance(v, (dict, list, tuple)):
            target.attrs[k] = json.dumps(v)
        elif isinstance(v, np.ndarray):
            target.attrs[k] = json.dumps(v.tolist())
        else:
            target.attrs[k] = v


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
        _write_json_attrs(f, header, skip={"freqs", "mode"})
        f.attrs["mode"] = mode
        f.attrs["vna_manual_script_version"] = "1"
        _write_json_attrs(f.create_group("metadata_snapshot"), metadata or {})
    return path


def save_vna_dut_h5(s11, header, metadata, *, save_dir, state):
    """Write one one-off DUT probe to a local HDF5 file.

    Raw-only companion to :func:`save_vna_manual_h5` for
    :func:`measure_dut` captures: the sweep under ``/raw/<state>``,
    the freq axis under ``/freqs``, the header on root attrs, and the
    metadata snapshot under ``/metadata_snapshot/`` attrs. There is no
    ``/calibrated/`` group — a probe carries no OSL standards to
    calibrate against.

    Parameters
    ----------
    s11 : np.ndarray
        Complex S11 sweep from :func:`measure_dut`.
    header : dict
        Header from :func:`measure_dut`. Must contain ``"freqs"``.
    metadata : dict
        Panda metadata snapshot captured at probe time.
    save_dir : pathlib.Path
        Directory the file is written into. Must already exist.
    state : str
        Switch path name; used in the filename and dataset name.

    Returns
    -------
    pathlib.Path
        The path of the file that was written.
    """
    save_dir = Path(save_dir)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = save_dir / f"vna_dut_{state}_{stamp}.h5"
    with h5py.File(path, "w") as f:
        f.create_group("raw").create_dataset(state, data=np.asarray(s11))
        f.create_dataset(
            "freqs", data=np.asarray(header["freqs"], dtype=float)
        )
        _write_json_attrs(f, header, skip={"freqs", "mode"})
        f.attrs["mode"] = header.get("mode", f"dut:{state}")
        f.attrs["vna_manual_script_version"] = "1"
        _write_json_attrs(f.create_group("metadata_snapshot"), metadata or {})
    return path
