"""Smoke test for the watch_sensors bring-up script.

watch_sensors lives under scripts/ (not an importable package), so load
it by file path the same way tests/test_motor_scripts.py does.
"""

import importlib.util
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _load(name):
    path = _REPO_ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_system_current_in_default_panda_streams():
    ws = _load("watch_sensors")
    # Panda stream (not adc_stats, which is SNAP-side) -> shown by default.
    assert "system_current" in ws._PANDA_STREAMS


def test_system_current_plots_current_a_only():
    ws = _load("watch_sensors")
    # The raw ADC current_voltage is not operator-meaningful; only amps
    # are traced in --plot.
    assert ws._plot_fields_for("system_current") == ["current_a"]
