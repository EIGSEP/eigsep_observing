"""Smoke tests for the migrated motor scripts.

The scripts live under ``scripts/`` (not on the package path), so
import them by file location. Each test drives the script's ``main``
callable against the dummy transport used everywhere else.
"""

import importlib.util
import threading
import time
from argparse import Namespace
from pathlib import Path

import numpy as np
import yaml

from eigsep_observing.keys import VNA_STREAM


SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load(name):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_motor_control_main_runs_short_scan(client, monkeypatch):
    """``motor_control.main`` runs a full scan to completion against
    the dummy manager. Patch the hard-coded linspace to a small grid
    so the test finishes quickly."""
    mc = _load("motor_control")
    small = np.array([-1.0, 0.0, 1.0])
    orig_linspace = mc.np.linspace
    monkeypatch.setattr(
        mc.np,
        "linspace",
        lambda *a, **kw: (
            small if a and a[0] == -180.0 else orig_linspace(*a, **kw)
        ),
    )
    args = Namespace(el_first=False, count=1, pause_s=None, sleep_s=None)
    mc.main(client.transport, args)


def test_motor_manual_helpers_exist():
    """``motor_manual`` should expose the curses frame and arg parser."""
    mm = _load("motor_manual")
    assert callable(mm._curses_main)
    assert callable(mm._parse_args)
    assert callable(mm._render)


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
        while not scan_done.is_set():
            mode = client._read_switch_mode_from_redis()
            if mode is not None:
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
