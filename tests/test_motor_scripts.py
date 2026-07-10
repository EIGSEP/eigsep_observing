"""Smoke tests for the migrated motor scripts.

The scripts live under ``scripts/`` (bring-up tools) or
``src/eigsep_observing/scripts/`` (alt-mode observers like
``no_switch_observation`` / ``vna_position_sweep``, per
``scripts/CLAUDE.md``). ``_load`` resolves either location by file
path so each test can drive the script's ``main`` callable against
the dummy transport used everywhere else.
"""

import importlib.util
import json
import sys
import threading
import time
from argparse import Namespace
from pathlib import Path

from eigsep_redis.testing import DummyTransport

import numpy as np
import pytest
import yaml

from eigsep_observing import run_tag
from eigsep_observing.keys import VNA_STREAM


_REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIRS = (
    _REPO_ROOT / "scripts",
    _REPO_ROOT / "src" / "eigsep_observing" / "scripts",
)


def _load(name):
    for base in SCRIPT_DIRS:
        path = base / f"{name}.py"
        if path.exists():
            spec = importlib.util.spec_from_file_location(name, path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
    raise FileNotFoundError(f"{name}.py not found in {SCRIPT_DIRS}")


def _scan_args(**overrides):
    """A small-grid arg namespace for driving ``motor_scan.main``
    quickly. Grid bounds default to a 3-point az/el sweep; the
    ``--az``/``--el`` hold flags default to unset (full 2-D grid)."""
    base = dict(
        el_first=False,
        count=1,
        pause_s=None,
        sleep_s=None,
        az_start=-1.0,
        az_stop=1.0,
        az_step=1.0,
        az=None,
        el_start=-1.0,
        el_stop=1.0,
        el_step=1.0,
        el=None,
    )
    base.update(overrides)
    return Namespace(**base)


def test_motor_scan_main_runs_short_scan(client):
    """``motor_scan.main`` runs a full scan to completion against the
    dummy manager, building the grid from CLI args (no numpy patching)."""
    mc = _load("motor_scan")
    mc.main(client.transport, _scan_args())


def test_motor_scan_axis_range_inclusive_endpoint():
    """``_axis_range`` includes the stop endpoint when it lands on the
    step grid, so ``0 -> 180`` actually reaches 180."""
    mc = _load("motor_scan")
    rng = mc._axis_range(0.0, 180.0, 5.0)
    assert rng[0] == 0.0
    assert rng[-1] == 180.0
    assert len(rng) == 37
    np.testing.assert_allclose(np.diff(rng), 5.0)


def test_motor_scan_axis_range_offgrid_stop_does_not_overshoot():
    """A stop that doesn't land on the grid stops at the last point
    <= stop rather than overshooting the boundary."""
    mc = _load("motor_scan")
    rng = mc._axis_range(0.0, 10.0, 3.0)
    np.testing.assert_allclose(rng, [0.0, 3.0, 6.0, 9.0])


def test_motor_scan_axis_range_rejects_nonpositive_step():
    mc = _load("motor_scan")
    with pytest.raises(ValueError):
        mc._axis_range(0.0, 10.0, 0.0)


def test_motor_scan_axis_range_rejects_inverted_bounds():
    mc = _load("motor_scan")
    with pytest.raises(ValueError):
        mc._axis_range(180.0, 0.0, 5.0)


def test_motor_scan_parse_args_accepts_scan_bounds(monkeypatch):
    """The new per-axis bound/step flags parse as floats into the
    underscore namespace ``main`` consumes."""
    mc = _load("motor_scan")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "motor_scan",
            "--az_start",
            "0",
            "--az_stop",
            "180",
            "--az_step",
            "10",
            "--el_start",
            "-45",
            "--el_stop",
            "45",
            "--el_step",
            "15",
        ],
    )
    args = mc._parse_args()
    assert (args.az_start, args.az_stop, args.az_step) == (0.0, 180.0, 10.0)
    assert (args.el_start, args.el_stop, args.el_step) == (-45.0, 45.0, 15.0)


def _capture_moves(mc, monkeypatch):
    """Spy on ``MotorClient.move_to`` and ``MotorClient.scan``, making
    both no-ops. Returns ``(moves, scan_calls)``: the per-call kwargs
    dicts recorded for each."""
    moves = []
    scan_calls = []

    def fake_move_to(self, **kwargs):
        moves.append(kwargs)

    def fake_scan(self, **kwargs):
        scan_calls.append(kwargs)

    monkeypatch.setattr(mc.MotorClient, "move_to", fake_move_to)
    monkeypatch.setattr(mc.MotorClient, "scan", fake_scan)
    return moves, scan_calls


def test_motor_scan_el_flag_moves_el_once_then_sweeps_az(client, monkeypatch):
    """``--el`` selects the az-sweep mode: one move takes elevation to
    the hold angle, then azimuth repositions to the sweep start and
    runs to the stop. ``MotorClient.scan`` (home + serpentine) is
    bypassed entirely."""
    mc = _load("motor_scan")
    moves, scan_calls = _capture_moves(mc, monkeypatch)
    mc.main(client.transport, _scan_args(el=30.0))
    assert moves == [{"el_deg": 30.0}, {"az_deg": -1.0}, {"az_deg": 1.0}]
    assert scan_calls == []


def test_motor_scan_az_flag_moves_az_once_then_sweeps_el(client, monkeypatch):
    """``--az`` is the mirrored el-sweep at a fixed azimuth."""
    mc = _load("motor_scan")
    moves, scan_calls = _capture_moves(mc, monkeypatch)
    mc.main(client.transport, _scan_args(az=15.0))
    assert moves == [{"az_deg": 15.0}, {"el_deg": -1.0}, {"el_deg": 1.0}]
    assert scan_calls == []


def test_motor_scan_single_axis_same_direction_every_pass(client, monkeypatch):
    """Repeat passes re-run start -> stop identically — no serpentine
    direction reversal between passes."""
    mc = _load("motor_scan")
    moves, _ = _capture_moves(mc, monkeypatch)
    mc.main(client.transport, _scan_args(el=30.0, count=2))
    assert [m["az_deg"] for m in moves[1:]] == [-1.0, 1.0, -1.0, 1.0]


def test_motor_scan_single_axis_stepped_grid_with_pause(client, monkeypatch):
    """``--pause_s`` steps the sweep axis through every grid point
    (dwelling at each) instead of one continuous start-to-stop move."""
    mc = _load("motor_scan")
    moves, _ = _capture_moves(mc, monkeypatch)
    mc.main(client.transport, _scan_args(el=30.0, pause_s=0.0))
    assert [m["az_deg"] for m in moves[1:]] == [-1.0, 0.0, 1.0]


def test_motor_scan_single_axis_never_homes(client, monkeypatch):
    """End-to-end against the dummy manager: the single-axis sweep
    neither homes at start nor at completion — when done the rig stays
    parked with the held axis at its hold angle."""
    mc = _load("motor_scan")
    home_calls = {"n": 0}

    def fake_home(self, *a, **kw):
        home_calls["n"] += 1

    monkeypatch.setattr(mc.MotorClient, "home", fake_home)
    mc.main(client.transport, _scan_args(el=1.0))
    assert home_calls["n"] == 0


def test_motor_scan_both_hold_flags_is_an_error(client):
    """``--az`` and ``--el`` together are ambiguous (which axis
    sweeps?) and must fail fast before any hardware or run_tag is
    touched."""
    mc = _load("motor_scan")
    with pytest.raises(ValueError):
        mc.main(client.transport, _scan_args(az=1.0, el=2.0))
    assert run_tag.read(client.transport) == {
        "run_tag": None,
        "run_started_at_unix": None,
    }


def test_motor_scan_default_full_grid_forwards_to_scan(client, monkeypatch):
    """With neither hold flag the script keeps the original behavior:
    the full 2-D grid is handed to ``MotorClient.scan`` with
    ``--el_first`` forwarded unchanged."""
    mc = _load("motor_scan")
    moves, scan_calls = _capture_moves(mc, monkeypatch)
    mc.main(client.transport, _scan_args(el_first=True))
    assert moves == []
    (kwargs,) = scan_calls
    np.testing.assert_allclose(kwargs["az_range_deg"], [-1.0, 0.0, 1.0])
    np.testing.assert_allclose(kwargs["el_range_deg"], [-1.0, 0.0, 1.0])
    assert kwargs["el_first"] is True


def test_motor_scan_parse_args_hold_flags_default_none(monkeypatch):
    """There is no ``--axis`` flag; the ``--az``/``--el`` hold flags
    default to unset, which selects the full 2-D grid."""
    mc = _load("motor_scan")
    monkeypatch.setattr(sys, "argv", ["motor_scan"])
    args = mc._parse_args()
    assert not hasattr(args, "axis")
    assert args.az is None
    assert args.el is None


def test_motor_scan_parse_args_hold_flag_coexists_with_bounds(monkeypatch):
    """The ``--el`` hold flag coexists with the ``--el_start/stop/step``
    and ``--az_*`` bound flags without an argparse prefix clash."""
    mc = _load("motor_scan")
    monkeypatch.setattr(
        sys,
        "argv",
        ["motor_scan", "--el", "30", "--az_stop", "90"],
    )
    args = mc._parse_args()
    assert args.el == 30.0
    assert args.az is None
    assert args.az_stop == 90.0


def _patch_scan_to_interrupt(mc, monkeypatch):
    """Make ``MotorClient.scan`` raise KeyboardInterrupt and spy on
    ``home``. Returns the call counter dict."""
    home_calls = {"n": 0}

    def fake_scan(self, **kwargs):
        raise KeyboardInterrupt

    def fake_home(self, *a, **kw):
        home_calls["n"] += 1

    monkeypatch.setattr(mc.MotorClient, "scan", fake_scan)
    monkeypatch.setattr(mc.MotorClient, "home", fake_home)
    return home_calls


def test_motor_scan_interrupt_prompt_yes_homes(client, monkeypatch):
    """A Ctrl-C during the scan prompts the operator; answering 'y'
    drives back to (0,0) via ``MotorClient.home``."""
    mc = _load("motor_scan")
    home_calls = _patch_scan_to_interrupt(mc, monkeypatch)
    monkeypatch.setattr("builtins.input", lambda *_a: "y")
    mc.main(client.transport, _scan_args())
    assert home_calls["n"] == 1


def test_motor_scan_interrupt_prompt_no_skips_home(client, monkeypatch):
    """The prompt defaults to No — an empty answer leaves the motors
    halted in place rather than homing."""
    mc = _load("motor_scan")
    home_calls = _patch_scan_to_interrupt(mc, monkeypatch)
    monkeypatch.setattr("builtins.input", lambda *_a: "")
    mc.main(client.transport, _scan_args())
    assert home_calls["n"] == 0


def test_motor_scan_interrupt_prompt_eof_skips_home(client, monkeypatch):
    """A non-interactive stdin (EOFError on input) is treated as No and
    must not crash the exit path."""
    mc = _load("motor_scan")
    home_calls = _patch_scan_to_interrupt(mc, monkeypatch)

    def raise_eof(*_a):
        raise EOFError

    monkeypatch.setattr("builtins.input", raise_eof)
    mc.main(client.transport, _scan_args())
    assert home_calls["n"] == 0


def test_motor_scan_second_interrupt_skips_home(client, monkeypatch):
    """A second Ctrl-C at the prompt declines the home cleanly."""
    mc = _load("motor_scan")
    home_calls = _patch_scan_to_interrupt(mc, monkeypatch)

    def raise_interrupt(*_a):
        raise KeyboardInterrupt

    monkeypatch.setattr("builtins.input", raise_interrupt)
    mc.main(client.transport, _scan_args())
    assert home_calls["n"] == 0


def test_motor_manual_helpers_exist():
    """``motor_manual`` should expose the curses frame and arg parser."""
    mm = _load("motor_manual")
    assert callable(mm._curses_main)
    assert callable(mm._parse_args)
    assert callable(mm._render)
    assert callable(mm._build_zeroer)


def test_motor_manual_build_zeroer_override_limits():
    """_build_zeroer with override_limits=True builds enforce_limits=False."""
    mm = _load("motor_manual")
    ns = Namespace(override_limits=True, az_step0_fallback=False)
    zeroer = mm._build_zeroer(DummyTransport(), ns)
    assert zeroer._motor_client.enforce_limits is False


def test_motor_manual_build_zeroer_default_enforces_limits():
    """_build_zeroer with override_limits=False preserves enforce_limits=True."""
    mm = _load("motor_manual")
    ns = Namespace(override_limits=False, az_step0_fallback=False)
    zeroer = mm._build_zeroer(DummyTransport(), ns)
    assert zeroer._motor_client.enforce_limits is True


def test_motor_manual_parse_args_accepts_override_limits(monkeypatch):
    """_parse_args recognises --override-limits flag."""
    mm = _load("motor_manual")
    monkeypatch.setattr(sys, "argv", ["motor_manual", "--override-limits"])
    args = mm._parse_args()
    assert args.override_limits is True


def test_motor_manual_parse_args_accepts_az_step0_fallback(monkeypatch):
    """_parse_args recognises --az-step0-fallback flag (default off)."""
    mm = _load("motor_manual")
    monkeypatch.setattr(sys, "argv", ["motor_manual", "--az-step0-fallback"])
    assert mm._parse_args().az_step0_fallback is True
    monkeypatch.setattr(sys, "argv", ["motor_manual"])
    assert mm._parse_args().az_step0_fallback is False


def test_motor_manual_build_zeroer_forwards_az_step0_fallback():
    """_build_zeroer threads the flag through to the homer."""
    mm = _load("motor_manual")
    ns = Namespace(override_limits=False, az_step0_fallback=True)
    zeroer = mm._build_zeroer(DummyTransport(), ns)
    assert zeroer._homer.az_step0_fallback is True


# ---------------------------------------------------------------------
# vna_position_sweep / no_switch_observation
# ---------------------------------------------------------------------


def _write_cfg(tmp_path, dummy_cfg, **overrides):
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["use_motor"] = True
    cfg.update(overrides)
    path = tmp_path / "test_cfg.yaml"
    path.write_text(yaml.dump(cfg))
    return path


def test_vna_position_sweep_visits_grid_and_writes(
    transport, dummy_cfg, tmp_path
):
    """End-to-end smoke against the dummy manager: a 2x2 grid produces
    one ``vna_writer.add`` per (mode, position), so 4 positions × 2
    modes = 8 stream entries.
    """
    cfg_path = _write_cfg(
        tmp_path,
        dummy_cfg,
        vna_position_sweep={
            "az_grid_deg": [-1.0, 1.0],
            "el_grid_deg": [-1.0, 1.0],
            "settle_s": 0.0,
        },
    )
    vps = _load("vna_position_sweep")
    args = Namespace(cfg_file=cfg_path, dummy=True)
    vps.main(transport, args)
    assert transport.r.xlen(VNA_STREAM) == 8


def test_vna_position_sweep_serialize_forced_on(
    transport, dummy_cfg, tmp_path, monkeypatch
):
    """Even when the deployed yaml says ``serialize_motion_and_switching:
    false``, the characterization script must force ``serialize=True``
    on the panda's coordinator before driving the grid.
    """
    cfg_path = _write_cfg(
        tmp_path,
        dummy_cfg,
        serialize_motion_and_switching=False,
        vna_position_sweep={
            "az_grid_deg": [0.0],
            "el_grid_deg": [0.0],
            "settle_s": 0.0,
        },
    )
    vps = _load("vna_position_sweep")

    captured = {}
    real_build = vps._build_client

    def capture_client(transport_arg, cfg, dummy):
        client = real_build(transport_arg, cfg, dummy)
        captured["client"] = client
        return client

    monkeypatch.setattr(vps, "_build_client", capture_client)
    args = Namespace(cfg_file=cfg_path, dummy=True)
    vps.main(transport, args)
    assert captured["client"].coord.serialize is True


def test_no_switch_observation_runs_and_writes_calibration(
    transport, dummy_cfg, tmp_path
):
    """End-to-end: the script does pre-scan calibration (2 VNA
    measurements), a tiny scan, and post-scan calibration (2 more),
    so the VNA stream must contain 4 entries by exit.
    """
    cfg_path = _write_cfg(
        tmp_path,
        dummy_cfg,
        switch_schedule={"RFANT": 0.0, "RFNOFF": 0.05, "RFNON": 0.05},
        motor_scan={
            "az_range_deg": [-1.0, 1.0],
            "el_range_deg": [-1.0, 1.0],
            "repeat_count": 1,
            "pause_s": 0.0,
        },
    )
    nso = _load("no_switch_observation")
    args = Namespace(cfg_file=cfg_path, dummy=True)
    nso.main(transport, args)
    # 2 calibration brackets × 2 vna_modes = 4 vna stream entries.
    assert transport.r.xlen(VNA_STREAM) == 4


def test_no_switch_observation_pins_rfant_during_scan(
    transport, dummy_cfg, tmp_path, monkeypatch
):
    """The rfswitch must hold RFANT throughout the scan phase. Probe
    by snapshotting ``sw_state_name`` from a watcher thread while the
    scan is running and asserting it never deviates from RFANT.
    """
    cfg_path = _write_cfg(
        tmp_path,
        dummy_cfg,
        switch_schedule={"RFNOFF": 0.0, "RFNON": 0.0},  # skip dwells
        motor_scan={
            "az_range_deg": [-1.0, 1.0],
            "el_range_deg": [-1.0, 1.0],
            "repeat_count": 1,
            "pause_s": 0.05,  # slow enough for the watcher to sample
        },
    )
    nso = _load("no_switch_observation")

    captured = {}
    real_build = nso._build_client

    def capture_client(transport_arg, cfg, dummy):
        client = real_build(transport_arg, cfg, dummy)
        captured["client"] = client
        return client

    monkeypatch.setattr(nso, "_build_client", capture_client)

    scan_observed_modes = []
    scan_started = threading.Event()
    scan_done = threading.Event()

    real_scan = None

    def _watcher(client):
        scan_started.wait(timeout=5.0)
        # The script's sw("RFANT") ack precedes the rfswitch's
        # periodic redis publication: the manager-side cmd returns
        # as soon as the cmd is delivered to firmware, but the
        # firmware then enters a settle window before its next
        # status broadcast. During pre-scan calibration we just
        # burst-sent 11 switch cmds (VNAO..VNARF), which can stall
        # the firmware's broadcaster long enough that the metadata
        # hash still holds a *pre-cmd* RFANT (the boot publication
        # before the VNA cycle). Gating the break on `rfswitch_ts`
        # crossing a post-`sw("RFANT")` baseline guarantees we wait
        # for an actual post-cmd publication before deciding the
        # switch has settled to RFANT — otherwise the stale RFANT
        # would let us through, and the firmware's still-pending
        # UNKNOWN settle window would leak into the sample loop and
        # falsely fail the deviation assertion.
        sample_floor_ts = time.time()
        deadline = time.monotonic() + 2.0
        while not scan_done.is_set() and time.monotonic() < deadline:
            try:
                ts = client.metadata_snapshot.get("rfswitch_ts")
            except KeyError:
                ts = 0.0
            if (
                isinstance(ts, (int, float))
                and ts > sample_floor_ts
                and client._read_switch_mode_from_redis() == "RFANT"
            ):
                break
            time.sleep(0.005)
        while not scan_done.is_set():
            mode = client._read_switch_mode_from_redis()
            if mode is not None and not scan_done.is_set():
                scan_observed_modes.append(mode)
            time.sleep(0.02)

    def patched_scan(self, **kw):
        scan_started.set()
        try:
            return real_scan(self, **kw)
        finally:
            scan_done.set()

    # Defer the patch until after _build_client (so we have the real
    # scan reference). monkeypatch the MotorClient.scan on the
    # captured client when capture fires.
    args = Namespace(cfg_file=cfg_path, dummy=True)

    def capture_and_patch(transport_arg, cfg, dummy):
        nonlocal real_scan
        client = real_build(transport_arg, cfg, dummy)
        captured["client"] = client
        real_scan = type(client.motor_client).scan
        monkeypatch.setattr(type(client.motor_client), "scan", patched_scan)
        watcher = threading.Thread(
            target=_watcher, args=(client,), daemon=True
        )
        watcher.start()
        captured["watcher"] = watcher
        return client

    monkeypatch.setattr(nso, "_build_client", capture_and_patch)
    nso.main(transport, args)
    captured["watcher"].join(timeout=2.0)

    assert scan_observed_modes, "watcher saw no rfswitch state during scan"
    deviations = [m for m in scan_observed_modes if m != "RFANT"]
    assert not deviations, f"rfswitch left RFANT during scan: {deviations[:5]}"


# ---------------------------------------------------------------------
# Cleanup-on-precondition-failure path
# ---------------------------------------------------------------------
#
# Both scripts validate ``client.motor_client``/``client.vna`` after
# building the client. If the validation fires, the heartbeat thread
# spun up in ``PandaClient.__init__`` must still be told to exit via
# ``client.stop()`` so the ground observer sees the ``alive=False``
# farewell. Regression guard against the resource-cleanup gap fixed
# alongside this test.


def _force_motor_none(real_build):
    """Wrap _build_client so the returned client has motor_client=None."""

    captured = {}

    def wrapper(transport_arg, cfg, dummy):
        client = real_build(transport_arg, cfg, dummy)
        client.motor_client = None
        captured["client"] = client
        return client

    return wrapper, captured


def test_vna_position_sweep_calls_stop_when_motor_missing(
    transport, dummy_cfg, tmp_path, monkeypatch
):
    cfg_path = _write_cfg(
        tmp_path,
        dummy_cfg,
        vna_position_sweep={
            "az_grid_deg": [0.0],
            "el_grid_deg": [0.0],
            "settle_s": 0.0,
        },
    )
    vps = _load("vna_position_sweep")
    wrapper, captured = _force_motor_none(vps._build_client)
    monkeypatch.setattr(vps, "_build_client", wrapper)
    args = Namespace(cfg_file=cfg_path, dummy=True)
    vps.main(transport, args)
    client = captured["client"]
    assert client.stop_client.is_set()
    assert not client.heartbeat_thd.is_alive()


def test_no_switch_observation_calls_stop_when_motor_missing(
    transport, dummy_cfg, tmp_path, monkeypatch
):
    cfg_path = _write_cfg(
        tmp_path,
        dummy_cfg,
        switch_schedule={"RFANT": 0.0},
        motor_scan={
            "az_range_deg": [-1.0, 1.0],
            "el_range_deg": [-1.0, 1.0],
            "repeat_count": 1,
            "pause_s": 0.0,
        },
    )
    nso = _load("no_switch_observation")
    wrapper, captured = _force_motor_none(nso._build_client)
    monkeypatch.setattr(nso, "_build_client", wrapper)
    args = Namespace(cfg_file=cfg_path, dummy=True)
    nso.main(transport, args)
    client = captured["client"]
    assert client.stop_client.is_set()
    assert not client.heartbeat_thd.is_alive()


# ---------------------------------------------------------------------
# run_tag publish/clear lifecycle
# ---------------------------------------------------------------------


def _vna_stream_run_tags(transport):
    """Decode every VNA stream entry's header and return its run_tag."""
    tags = []
    for _entry_id, fields in transport.r.xrange(VNA_STREAM, "-", "+"):
        hdr_raw = fields.get(b"header") or fields.get("header")
        if hdr_raw is None:
            continue
        if isinstance(hdr_raw, (bytes, bytearray)):
            hdr_raw = hdr_raw.decode()
        hdr = json.loads(hdr_raw)
        tags.append(hdr.get("run_tag"))
    return tags


def test_no_switch_observation_tags_vna_entries_and_clears_on_exit(
    transport, dummy_cfg, tmp_path
):
    """Every VNA entry written during the script carries
    ``run_tag == "no_switch_observation"``, and the tag is cleared back
    to the empty sentinel by the time main returns.
    """
    cfg_path = _write_cfg(
        tmp_path,
        dummy_cfg,
        switch_schedule={"RFANT": 0.0, "RFNOFF": 0.05, "RFNON": 0.05},
        motor_scan={
            "az_range_deg": [-1.0, 1.0],
            "el_range_deg": [-1.0, 1.0],
            "repeat_count": 1,
            "pause_s": 0.0,
        },
    )
    nso = _load("no_switch_observation")
    args = Namespace(cfg_file=cfg_path, dummy=True)
    nso.main(transport, args)

    tags = _vna_stream_run_tags(transport)
    assert tags, "expected at least one VNA entry with a header"
    assert all(t == "no_switch_observation" for t in tags), tags
    # finally-block cleared the tag.
    assert run_tag.read(transport) == {
        "run_tag": None,
        "run_started_at_unix": None,
    }


def test_vna_position_sweep_tags_vna_entries_and_clears_on_exit(
    transport, dummy_cfg, tmp_path
):
    cfg_path = _write_cfg(
        tmp_path,
        dummy_cfg,
        vna_position_sweep={
            "az_grid_deg": [0.0],
            "el_grid_deg": [0.0],
            "settle_s": 0.0,
        },
    )
    vps = _load("vna_position_sweep")
    args = Namespace(cfg_file=cfg_path, dummy=True)
    vps.main(transport, args)

    tags = _vna_stream_run_tags(transport)
    assert tags
    assert all(t == "vna_position_sweep" for t in tags), tags
    assert run_tag.read(transport) == {
        "run_tag": None,
        "run_started_at_unix": None,
    }
