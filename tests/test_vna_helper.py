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

from eigsep_observing import obs_config_owner, run_tag
from eigsep_observing.keys import VNA_STREAM
from eigsep_observing.vna import VnaWriter, measure_s11


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
    assert "cal:VNAO" in s11 and "cal:VNAS" in s11 and "cal:VNAL" in s11
    assert header["mode"] == "ant"

    # A single bundle landed on the VNA stream.
    assert transport.r.xlen(VNA_STREAM) == 1


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
