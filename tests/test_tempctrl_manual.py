"""Tests for the seed-state polling helper in ``scripts/tempctrl_manual.py``.

The script previously seeded the operator-facing setpoints with a
hardcoded 20 deg C fallback whenever the firmware hadn't yet published
``T_target``. That left the UI disagreeing with the firmware (firmware
default 30 deg C) for the brief startup race. The new ``_seed_state``
polls the snapshot until both ``tempctrl_lna`` and ``tempctrl_load``
have published ``T_target``, then seeds from those values directly —
the pico is the single source of truth.
"""

import importlib.util
from pathlib import Path

import pytest
from eigsep_redis import MetadataSnapshotReader, MetadataWriter


SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load(name):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _publish(transport, *, lna=None, load=None):
    """Minimal tempctrl_lna / tempctrl_load entries — only the fields
    ``_seed_state`` reads. Other fields are absent on purpose so the
    test fails loudly if the helper grows a dependency on them."""
    writer = MetadataWriter(transport)
    if lna is not None:
        writer.add("tempctrl_lna", lna)
    if load is not None:
        writer.add("tempctrl_load", load)


def test_seed_state_uses_firmware_t_target(transport):
    """When both streams have published ``T_target``, the seed picks
    those values up — no hardcoded fallback."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        lna={
            "sensor_name": "tempctrl_lna",
            "status": "update",
            "T_target": 30.0,
            "enabled": False,
            "Kp": 0.25,
            "Ki": 0.01,
        },
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_target": 28.5,
            "enabled": True,
            "Kp": 0.18,
            "Ki": 0.0,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    state = mod._seed_state(snapshot, timeout_s=1.0, poll_interval_s=0.01)
    assert state.lna_setpoint == 30.0
    assert state.load_setpoint == 28.5
    assert state.lna_enabled is False
    assert state.load_enabled is True
    assert state.lna_Kp == 0.25
    assert state.lna_Ki == 0.01
    assert state.load_Kp == 0.18
    assert state.load_Ki == 0.0


def test_seed_state_reads_cooling_enabled(transport):
    """``cooling_enabled`` is seeded from the firmware-published value
    per channel, so the readout and the bump keys start in sync with
    what the firmware is enforcing."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        lna={
            "sensor_name": "tempctrl_lna",
            "status": "update",
            "T_target": 30.0,
            "cooling_enabled": False,
        },
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_target": 30.0,
            "cooling_enabled": True,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    state = mod._seed_state(snapshot, timeout_s=1.0, poll_interval_s=0.01)
    assert state.lna_cooling_enabled is False
    assert state.load_cooling_enabled is True


def test_seed_state_cooling_enabled_defaults_true_when_absent(transport):
    """A firmware that predates ``cooling_enabled`` (field absent)
    seeds True, matching the firmware default (cooling permitted)."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        lna={
            "sensor_name": "tempctrl_lna",
            "status": "update",
            "T_target": 30.0,
        },
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_target": 30.0,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    state = mod._seed_state(snapshot, timeout_s=1.0, poll_interval_s=0.01)
    assert state.lna_cooling_enabled is True
    assert state.load_cooling_enabled is True


def test_seed_state_falls_back_to_default_gains_only(transport):
    """``T_target`` and ``enabled`` come from the pico; missing
    ``Kp`` / ``Ki`` fall back to the firmware-side defaults."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        lna={
            "sensor_name": "tempctrl_lna",
            "status": "update",
            "T_target": 30.0,
        },
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_target": 30.0,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    state = mod._seed_state(snapshot, timeout_s=1.0, poll_interval_s=0.01)
    assert state.lna_Kp == mod.DEFAULT_KP
    assert state.lna_Ki == mod.DEFAULT_KI
    assert state.load_Kp == mod.DEFAULT_KP
    assert state.load_Ki == mod.DEFAULT_KI


def test_seed_state_times_out_when_streams_silent(transport):
    """If the pico is registered (``require_pico`` already passed) but
    never publishes a ``T_target``, the seed exits with a SystemExit
    message naming the silent streams — rather than papering over the
    silence with a hardcoded default."""
    mod = _load("tempctrl_manual")
    snapshot = MetadataSnapshotReader(transport)
    with pytest.raises(SystemExit) as exc:
        mod._seed_state(snapshot, timeout_s=0.05, poll_interval_s=0.01)
    msg = str(exc.value)
    assert "tempctrl_lna" in msg
    assert "tempctrl_load" in msg
    assert "T_target" in msg


def test_seed_state_times_out_when_one_stream_silent(transport):
    """One stream silent is still a timeout — both must publish before
    the UI is allowed to come up with consistent state."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        lna={
            "sensor_name": "tempctrl_lna",
            "status": "update",
            "T_target": 30.0,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    with pytest.raises(SystemExit) as exc:
        mod._seed_state(snapshot, timeout_s=0.05, poll_interval_s=0.01)
    msg = str(exc.value)
    assert "tempctrl_load" in msg
    assert "tempctrl_lna" not in msg
