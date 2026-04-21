"""Smoke tests for the migrated motor scripts.

The scripts live under ``scripts/`` (not on the package path), so
import them by file location. Each test drives the script's ``main``
callable against the dummy transport used everywhere else.
"""

import importlib.util
from argparse import Namespace
from pathlib import Path

import numpy as np

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
