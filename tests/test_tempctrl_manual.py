"""Tests for the seed-state polling helper in ``scripts/tempctrl_manual.py``.

The script previously seeded the operator-facing setpoint with a
hardcoded 20 deg C fallback whenever the firmware hadn't yet published
``T_target``. That left the UI disagreeing with the firmware (firmware
default 30 deg C) for the brief startup race. ``_seed_state`` polls the
snapshot until ``tempctrl_load`` (the sole tempctrl channel — the
LNA/Peltier channel and its PI control were removed) has published
``T_target``, then seeds from that value directly — the pico is the
single source of truth.
"""

import importlib.util
import math
from pathlib import Path

from eigsep_redis import MetadataSnapshotReader, MetadataWriter


SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load(name):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _publish(transport, *, load=None):
    """Minimal ``tempctrl_load`` entry — only the fields the consumer
    under test reads: ``_seed_state``'s ``T_target`` / ``enabled`` /
    ``hysteresis`` in the seed tests, and ``_History``'s
    ``PLOT_FIELDS`` (``T_now`` / ``T_target`` / ``drive_level``) in the
    history/plot tests. The remaining ``_LOAD_HEATER_SCHEMA`` fields
    are absent on purpose: omitting them is safe because neither code
    path reads them (``_History.record`` NaN-fills anything missing),
    and it makes the test fail loudly if either helper grows a
    dependency on a field this fixture doesn't supply."""
    writer = MetadataWriter(transport)
    if load is not None:
        writer.add("tempctrl_load", load)


def test_seed_state_uses_firmware_t_target(transport):
    """When the stream has published ``T_target``, the seed picks that
    value up — no hardcoded fallback."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_target": 28.5,
            "enabled": True,
            "hysteresis": 0.75,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    state = mod._seed_state(snapshot, timeout_s=1.0, poll_interval_s=0.01)
    assert state.setpoint == 28.5
    assert state.enabled is True
    assert state.hysteresis == 0.75
    assert state.installed is True


def test_seed_state_falls_back_to_default_hysteresis_only(transport):
    """``T_target`` and ``enabled`` come from the pico; a missing
    ``hysteresis`` falls back to the client-side default."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_target": 30.0,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    state = mod._seed_state(snapshot, timeout_s=1.0, poll_interval_s=0.01)
    assert state.hysteresis == mod.DEFAULT_HYSTERESIS_C
    assert state.enabled is False


def test_seed_state_silent_stream_marks_not_installed(transport):
    """A stream that stays silent through the seed window is the
    descoped-channel shape (firmware ``installed=false`` publishes
    nothing): the UI still comes up, marked not-installed and seeded
    from firmware defaults so a later re-install (`u`) starts from sane
    values. This is not an error — inspecting or re-installing a
    deliberately descoped channel is a supported flow."""
    mod = _load("tempctrl_manual")
    snapshot = MetadataSnapshotReader(transport)
    state = mod._seed_state(snapshot, timeout_s=0.05, poll_interval_s=0.01)
    assert state.installed is False
    assert state.setpoint == mod.DEFAULT_T_TARGET_C
    assert state.enabled is False
    assert state.hysteresis == mod.DEFAULT_HYSTERESIS_C


def test_seed_state_live_stream_marks_installed(transport):
    """A publishing stream marks the channel installed."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_target": 30.0,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    state = mod._seed_state(snapshot, timeout_s=1.0, poll_interval_s=0.01)
    assert state.installed is True


def _record_n(mod, snapshot, n):
    """Build a history and append ``n`` samples from ``snapshot``."""
    history = mod._History()
    for i in range(n):
        history.record(snapshot, now=float(i))
    return history


def test_history_records_numeric_and_gaps(transport):
    """Numeric firmware fields are buffered as floats; missing or
    non-numeric ones become NaN gaps rather than crashing or zeroing."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_now": 25.0,
            "T_target": 30.0,
            "drive_level": 0.4,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    history = _record_n(mod, snapshot, 3)

    assert len(history) == 3
    assert history.t == [0.0, 1.0, 2.0]  # elapsed seconds from first sample
    assert history.values["T_now"] == [25.0, 25.0, 25.0]
    assert history.values["drive_level"] == [0.4, 0.4, 0.4]


def test_history_records_gaps_when_stream_absent(transport):
    """A silent stream buffers as an all-NaN row rather than crashing."""
    mod = _load("tempctrl_manual")
    snapshot = MetadataSnapshotReader(transport)
    history = _record_n(mod, snapshot, 3)

    assert len(history) == 3
    assert all(math.isnan(v) for v in history.values["T_now"])


def test_plot_history_writes_png(transport, tmp_path):
    """A non-empty history renders a PNG whose path is returned."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_now": 24.0,
            "T_target": 28.0,
            "drive_level": 0.2,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    history = _record_n(mod, snapshot, 5)

    path = mod._plot_history(
        history, outdir=str(tmp_path), timestamp="20260528_120000"
    )
    assert path is not None
    assert path.name == "tempctrl_20260528_120000.png"
    assert path.exists()
    assert path.stat().st_size > 0


def test_plot_history_with_gaps_does_not_raise(transport, tmp_path):
    """An all-NaN history (sensor/stream dropout) still renders without
    error."""
    mod = _load("tempctrl_manual")
    snapshot = MetadataSnapshotReader(transport)
    history = _record_n(mod, snapshot, 4)

    path = mod._plot_history(
        history, outdir=str(tmp_path), timestamp="20260528_120001"
    )
    assert path is not None
    assert path.exists()


def test_plot_history_empty_returns_none(tmp_path):
    """An empty history writes nothing and returns None."""
    mod = _load("tempctrl_manual")
    assert mod._plot_history(mod._History(), outdir=str(tmp_path)) is None
    assert list(tmp_path.iterdir()) == []


def test_handle_p_key_plots_and_continues(transport, tmp_path):
    """The `p` key writes a PNG, reports it in the footer, and keeps the
    loop running (returns True)."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_now": 24.0,
            "T_target": 28.0,
            "drive_level": 0.2,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    history = _record_n(mod, snapshot, 3)
    state = mod._seed_state(snapshot, timeout_s=1.0, poll_interval_s=0.01)

    keep_going = mod._handle_key(
        ord("p"),
        proxy=None,
        state=state,
        history=history,
        outdir=str(tmp_path),
    )
    assert keep_going is True
    assert state.last_message.startswith("wrote ")
    assert len(list(tmp_path.glob("tempctrl_*.png"))) == 1


class _FakeProxy:
    """Records send_command calls; returns a truthy result like a
    successful PicoProxy round-trip."""

    def __init__(self):
        self.sent = []

    def send_command(self, action, **kwargs):
        self.sent.append((action, kwargs))
        return {"action": action}


def _make_state(mod):
    return mod._State(
        setpoint=30.0,
        hysteresis=0.5,
        enabled=False,
    )


def test_enable_hotkeys_push_set_enable(transport):
    """`o`/`O` toggle the LOAD enable flag and push it to the pico."""
    mod = _load("tempctrl_manual")
    proxy = _FakeProxy()
    state = _make_state(mod)

    assert mod._handle_key(ord("o"), proxy, state) is True
    assert state.enabled is True
    assert proxy.sent[-1] == ("set_enable", {"LOAD": True})

    mod._handle_key(ord("O"), proxy, state)
    assert state.enabled is False
    assert proxy.sent[-1] == ("set_enable", {"LOAD": False})


def test_setpoint_hotkeys_push_set_temperature(transport):
    """`]`/`[` bump the setpoint and push both setpoint + hysteresis."""
    mod = _load("tempctrl_manual")
    proxy = _FakeProxy()
    state = _make_state(mod)

    mod._handle_key(ord("]"), proxy, state)
    assert state.setpoint == 30.5
    assert proxy.sent[-1] == (
        "set_temperature",
        {"T_LOAD": 30.5, "LOAD_hyst": 0.5},
    )

    mod._handle_key(ord("["), proxy, state)
    mod._handle_key(ord("["), proxy, state)
    assert state.setpoint == 29.5
    assert proxy.sent[-1] == (
        "set_temperature",
        {"T_LOAD": 29.5, "LOAD_hyst": 0.5},
    )


def test_hysteresis_hotkeys_push_set_temperature_and_floor(transport):
    """`}`/`{` bump hysteresis and floor at ``HYSTERESIS_MIN_C`` instead
    of going to zero or negative."""
    mod = _load("tempctrl_manual")
    proxy = _FakeProxy()
    state = _make_state(mod)

    mod._handle_key(ord("}"), proxy, state)
    assert state.hysteresis == 0.6
    assert proxy.sent[-1] == (
        "set_temperature",
        {"T_LOAD": 30.0, "LOAD_hyst": 0.6},
    )

    state.hysteresis = mod.HYSTERESIS_MIN_C
    mod._handle_key(ord("{"), proxy, state)
    assert state.hysteresis == mod.HYSTERESIS_MIN_C


def test_installed_hotkeys_push_set_installed(transport):
    """`u`/`U` toggle LOAD installed."""
    mod = _load("tempctrl_manual")
    proxy = _FakeProxy()
    state = _make_state(mod)
    state.installed = True

    assert mod._handle_key(ord("U"), proxy, state) is True
    assert state.installed is False
    assert proxy.sent[-1] == ("set_installed", {"LOAD": False})

    mod._handle_key(ord("u"), proxy, state)
    assert state.installed is True
    assert proxy.sent[-1] == ("set_installed", {"LOAD": True})


def test_reenable_hotkey_pushes_enable_and_temperature(transport):
    """`r` re-enables at the last setpoint/hysteresis — the operator's
    trip-clear ack."""
    mod = _load("tempctrl_manual")
    proxy = _FakeProxy()
    state = _make_state(mod)

    assert mod._handle_key(ord("r"), proxy, state) is True
    assert state.enabled is True
    assert ("set_enable", {"LOAD": True}) in proxy.sent
    assert (
        "set_temperature",
        {"T_LOAD": 30.0, "LOAD_hyst": 0.5},
    ) in proxy.sent


def test_handle_p_key_no_data(transport, tmp_path):
    """Pressing `p` before any sample is buffered reports 'no data'."""
    mod = _load("tempctrl_manual")
    _publish(
        transport,
        load={
            "sensor_name": "tempctrl_load",
            "status": "update",
            "T_target": 28.0,
        },
    )
    snapshot = MetadataSnapshotReader(transport)
    state = mod._seed_state(snapshot, timeout_s=1.0, poll_interval_s=0.01)

    keep_going = mod._handle_key(
        ord("p"),
        proxy=None,
        state=state,
        history=mod._History(),
        outdir=str(tmp_path),
    )
    assert keep_going is True
    assert state.last_message == "no data to plot yet"
    assert list(tmp_path.glob("*.png")) == []


def test_handle_q_key_stops_loop(transport):
    """`q` (and ESC) return False to end the curses main loop."""
    mod = _load("tempctrl_manual")
    proxy = _FakeProxy()
    state = _make_state(mod)
    assert mod._handle_key(ord("q"), proxy, state) is False
    assert mod._handle_key(27, proxy, state) is False
