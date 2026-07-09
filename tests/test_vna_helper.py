"""Direct tests for the ``eigsep_observing.vna.measure_s11`` helper.

The helper is exercised end-to-end through :meth:`PandaClient.measure_s11`
in ``tests/test_client.py``; the tests here pin behaviours that are
only meaningful at the module-level callsite shape that bring-up
scripts use (no PandaClient orchestration, callback-based contract
violation reporting).
"""

import logging
from unittest.mock import patch

import numpy as np
import pytest
from cmt_vna.testing import DummyVNA
from eigsep_redis import MetadataSnapshotReader
from picohost.buses import ImuCalStore

from eigsep_observing import obs_config_owner, run_tag
from eigsep_observing._test_fixtures import IMU_CALIBRATION
from eigsep_observing.keys import VNA_STREAM
from eigsep_observing.vna import VnaWriter, measure_dut, measure_s11


def _make_vna(cfg, switch_fn=lambda state: None):
    vna = DummyVNA(
        ip=cfg["vna_ip"],
        port=cfg["vna_port"],
        timeout=cfg["vna_timeout"],
        switch_fn=switch_fn,
    )
    setup_kwargs = cfg["vna_settings"].copy()
    setup_kwargs["power_dBm"] = setup_kwargs["power_dBm"]["ant"]
    vna.setup(**setup_kwargs)
    return vna


def _make_sinks(transport):
    return VnaWriter(transport), MetadataSnapshotReader(transport)


def test_measure_s11_helper_ant_publishes_bundle(transport, dummy_cfg):
    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    s11, header, _ = measure_s11(
        vna,
        "ant",
        cfg=dummy_cfg,
        transport=transport,
        vna_writer=writer,
        metadata_snapshot=snap,
    )

    # ant bundle: DUT keys + OSL standards
    assert "ant" in s11 and "load" in s11 and "noise" in s11
    assert "amb" in s11 and "sp1" in s11
    assert "cal:VNAO" in s11 and "cal:VNAS" in s11 and "cal:VNAL" in s11
    assert header["mode"] == "ant"

    # A single bundle landed on the VNA stream.
    assert transport.r.xlen(VNA_STREAM) == 1


def test_measure_s11_ant_switch_sequence(transport, dummy_cfg):
    """Pin the full ant-cycle switch order: OSL standards, then the
    four DUTs. A reorder silently changes which physical path each
    trace was taken on."""
    calls = []
    vna = _make_vna(dummy_cfg, switch_fn=calls.append)
    writer, snap = _make_sinks(transport)

    measure_s11(
        vna,
        "ant",
        cfg=dummy_cfg,
        transport=transport,
        vna_writer=writer,
        metadata_snapshot=snap,
    )

    assert calls == [
        "VNAO",
        "VNAS",
        "VNAL",
        "VNAANT",
        "VNANOFF",
        "VNANON",
        "VNAAMB",
        "VNASP1",
    ]


def test_measure_s11_helper_rec_publishes_bundle(transport, dummy_cfg):
    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    s11, header, _ = measure_s11(
        vna,
        "rec",
        cfg=dummy_cfg,
        transport=transport,
        vna_writer=writer,
        metadata_snapshot=snap,
    )

    # rec bundle has the rec DUT only, plus OSL standards.
    assert "rec" in s11 and "ant" not in s11 and "noise" not in s11
    assert "cal:VNAO" in s11 and "cal:VNAS" in s11 and "cal:VNAL" in s11
    assert header["mode"] == "rec"


def test_measure_s11_helper_contract_violation_fires_callback_and_publishes(
    transport, dummy_cfg, caplog
):
    """Violations must fire the callback and still publish — the helper
    is loud-but-non-blocking by design."""
    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    canned = ["missing key 'npoints'"]
    seen = []

    def on_violation(msg):
        seen.append(msg)

    with patch(
        "eigsep_observing.vna._validate_vna_s11_header",
        return_value=canned,
    ):
        s11, header, _ = measure_s11(
            vna,
            "ant",
            cfg=dummy_cfg,
            transport=transport,
            vna_writer=writer,
            metadata_snapshot=snap,
            on_contract_violation=on_violation,
        )

    assert len(seen) == 1
    assert "missing key 'npoints'" in seen[0]
    assert "mode='ant'" in seen[0]
    # The bundle still landed on the stream — loud-but-non-blocking.
    assert transport.r.xlen(VNA_STREAM) == 1


def test_measure_s11_helper_default_callback_logs_warning(
    transport, dummy_cfg, caplog
):
    """No callback provided → helper falls back to ``logger.warning``."""
    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    canned = ["bogus violation"]
    with patch(
        "eigsep_observing.vna._validate_vna_s11_header",
        return_value=canned,
    ):
        caplog.set_level(logging.WARNING, logger="eigsep_observing.vna")
        measure_s11(
            vna,
            "ant",
            cfg=dummy_cfg,
            transport=transport,
            vna_writer=writer,
            metadata_snapshot=snap,
        )

    warns = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "VNA S11 producer contract violation" in r.getMessage()
    ]
    assert len(warns) == 1
    assert "bogus violation" in warns[0]


def test_measure_s11_helper_empty_redis_yields_unknown_overlays(
    transport, dummy_cfg
):
    """A transport with no ``run_tag`` / ``obs_config_owner`` published
    gives the documented sentinels (``"UNKNOWN"`` / 0.0) rather than
    ``None``."""
    # Sanity: both stores start empty.
    assert run_tag.read(transport)["run_tag"] is None
    assert obs_config_owner.read_owner(transport)["owner"] is None

    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    _, header, _ = measure_s11(
        vna,
        "rec",
        cfg=dummy_cfg,
        transport=transport,
        vna_writer=writer,
        metadata_snapshot=snap,
    )

    assert header["run_tag"] == "UNKNOWN"
    assert header["run_started_at_unix"] == 0.0
    assert header["obs_config_owner"] == "UNKNOWN"
    assert header["obs_config_owner_uploaded_unix"] == 0.0
    assert header["obs_config"] == dict(dummy_cfg)
    assert header["imu_calibration"] == {}
    assert header["imu_calibration_upload_unix"] == 0.0


def test_measure_s11_helper_injects_published_owner(transport, dummy_cfg):
    """``publish_owner`` flows into the VNA header on the next
    ``measure_s11`` call."""
    obs_config_owner.publish_owner(
        transport, "panda_observe", uploaded_at_unix=7.5
    )

    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    _, header, _ = measure_s11(
        vna,
        "ant",
        cfg=dummy_cfg,
        transport=transport,
        vna_writer=writer,
        metadata_snapshot=snap,
    )

    assert header["obs_config_owner"] == "panda_observe"
    assert header["obs_config_owner_uploaded_unix"] == 7.5


def test_measure_s11_helper_embeds_imu_calibration(transport, dummy_cfg):
    """A seeded ``ImuCalStore`` blob flows into the VNA header."""
    ImuCalStore(transport).upload(IMU_CALIBRATION)

    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    _, header, _ = measure_s11(
        vna,
        "ant",
        cfg=dummy_cfg,
        transport=transport,
        vna_writer=writer,
        metadata_snapshot=snap,
    )

    assert header["imu_calibration"]["imu_el"] == IMU_CALIBRATION["imu_el"]
    assert header["imu_calibration_upload_unix"] > 0.0


def test_measure_s11_helper_rejects_invalid_mode(transport, dummy_cfg):
    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    with pytest.raises(ValueError, match="Unknown VNA mode"):
        measure_s11(
            vna,
            "bogus",
            cfg=dummy_cfg,
            transport=transport,
            vna_writer=writer,
            metadata_snapshot=snap,
        )


def test_measure_s11_helper_requires_initialized_vna(transport, dummy_cfg):
    writer, snap = _make_sinks(transport)

    with pytest.raises(RuntimeError, match="VNA not initialized"):
        measure_s11(
            None,
            "ant",
            cfg=dummy_cfg,
            transport=transport,
            vna_writer=writer,
            metadata_snapshot=snap,
        )


def test_measure_s11_helper_uses_mode_specific_power(transport, dummy_cfg):
    """Power is set per-mode from cfg, not left at whatever ``setup`` left it."""
    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    # setup() leaves ant power; rec call must overwrite to rec power.
    measure_s11(
        vna,
        "rec",
        cfg=dummy_cfg,
        transport=transport,
        vna_writer=writer,
        metadata_snapshot=snap,
    )
    assert vna.power_dBm == dummy_cfg["vna_settings"]["power_dBm"]["rec"]

    measure_s11(
        vna,
        "ant",
        cfg=dummy_cfg,
        transport=transport,
        vna_writer=writer,
        metadata_snapshot=snap,
    )
    assert vna.power_dBm == dummy_cfg["vna_settings"]["power_dBm"]["ant"]


def test_measure_s11_helper_freqs_match_setup(transport, dummy_cfg):
    """Header carries the frequency axis from cfg/vna setup."""
    vna = _make_vna(dummy_cfg)
    writer, snap = _make_sinks(transport)

    _, header, _ = measure_s11(
        vna,
        "ant",
        cfg=dummy_cfg,
        transport=transport,
        vna_writer=writer,
        metadata_snapshot=snap,
    )
    freqs = np.asarray(header["freqs"], dtype=float)
    assert freqs[0] == pytest.approx(dummy_cfg["vna_settings"]["fstart"])
    assert freqs[-1] == pytest.approx(dummy_cfg["vna_settings"]["fstop"])
    assert freqs.size == dummy_cfg["vna_settings"]["npoints"]


def test_build_vna_subsystem_real_manages_service(monkeypatch, dummy_cfg):
    from eigsep_redis.testing import DummyTransport
    from eigsep_observing import vna, vna_service
    from cmt_vna.testing import DummyVNA

    events = []
    monkeypatch.setattr(vna_service, "start", lambda: events.append("start"))
    monkeypatch.setattr(vna_service, "stop", lambda: events.append("stop"))
    monkeypatch.setattr(
        vna_service, "wait_ready", lambda ip, port, **k: events.append("ready")
    )
    # Build the real (non-dummy) subsystem but with the VNA class faked
    # so no real socket is opened.
    monkeypatch.setattr(vna, "VNA", DummyVNA)

    transport = DummyTransport()
    from eigsep_observing.testing import start_dummy_pico_manager

    mgr = start_dummy_pico_manager(transport)
    try:
        sub = vna.build_vna_subsystem(
            transport, dummy_cfg_vna(dummy_cfg), source="test", dummy=False
        )
        assert events == ["start", "ready"]
        sub.cleanup()
        assert events == ["start", "ready", "stop"]
    finally:
        mgr.stop()


def dummy_cfg_vna(dummy_cfg):
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    return cfg


def test_build_vna_subsystem_real_stops_service_on_wait_ready_failure(
    monkeypatch, dummy_cfg
):
    from eigsep_redis.testing import DummyTransport
    from eigsep_observing import vna, vna_service
    from cmt_vna.testing import DummyVNA
    from eigsep_observing.testing import start_dummy_pico_manager

    events = []
    monkeypatch.setattr(vna_service, "start", lambda: events.append("start"))
    monkeypatch.setattr(vna_service, "stop", lambda: events.append("stop"))

    def boom(ip, port, **k):
        raise TimeoutError("not ready")

    monkeypatch.setattr(vna_service, "wait_ready", boom)
    monkeypatch.setattr(vna, "VNA", DummyVNA)

    transport = DummyTransport()
    mgr = start_dummy_pico_manager(transport)
    try:
        with pytest.raises(TimeoutError):
            vna.build_vna_subsystem(
                transport,
                dummy_cfg_vna(dummy_cfg),
                source="test",
                dummy=False,
            )
        assert events == ["start", "stop"]
    finally:
        mgr.stop()


def test_build_vna_subsystem_real_stops_service_on_build_failure(
    monkeypatch, dummy_cfg
):
    from eigsep_redis.testing import DummyTransport
    from eigsep_observing import vna, vna_service
    from cmt_vna.testing import DummyVNA
    from eigsep_observing.testing import start_dummy_pico_manager

    events = []
    monkeypatch.setattr(vna_service, "start", lambda: events.append("start"))
    monkeypatch.setattr(vna_service, "stop", lambda: events.append("stop"))
    monkeypatch.setattr(vna_service, "wait_ready", lambda ip, port, **k: None)

    class BoomVNA(DummyVNA):
        def setup(self, **kwargs):
            raise RuntimeError("setup boom")

    monkeypatch.setattr(vna, "VNA", BoomVNA)

    transport = DummyTransport()
    mgr = start_dummy_pico_manager(transport)
    try:
        with pytest.raises(RuntimeError, match="setup boom"):
            vna.build_vna_subsystem(
                transport,
                dummy_cfg_vna(dummy_cfg),
                source="test",
                dummy=False,
            )
        assert events == ["start", "stop"]
    finally:
        mgr.stop()


def test_measure_dut_helper_probes_state_no_publish(transport, dummy_cfg):
    switched = []
    vna = _make_vna(dummy_cfg, switch_fn=switched.append)
    _, snap = _make_sinks(transport)

    s11, header, metadata = measure_dut(
        vna,
        "VNAAMB",
        cfg=dummy_cfg,
        transport=transport,
        metadata_snapshot=snap,
    )

    assert switched == ["VNAAMB"]
    assert np.iscomplexobj(s11)
    assert len(s11) == dummy_cfg["vna_settings"]["npoints"]
    assert header["mode"] == "dut:VNAAMB"
    # same provenance overlays as measure_s11 (empty redis → sentinels)
    assert header["run_tag"] == "UNKNOWN"
    assert header["obs_config"] == dict(dummy_cfg)
    assert "metadata_snapshot_unix" in header
    assert isinstance(metadata, dict)
    # a lone probe is a local artifact only: nothing on the VNA stream
    assert transport.r.xlen(VNA_STREAM) == 0


def test_measure_dut_helper_requires_initialized_vna(transport, dummy_cfg):
    _, snap = _make_sinks(transport)
    with pytest.raises(RuntimeError, match="VNA not initialized"):
        measure_dut(
            None,
            "VNAANT",
            cfg=dummy_cfg,
            transport=transport,
            metadata_snapshot=snap,
        )
