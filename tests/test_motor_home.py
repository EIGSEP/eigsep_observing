"""Tests for scripts/motor_home.py."""

import importlib.util
from pathlib import Path

import pytest

from eigsep_observing.home_ref import publish_home_ref
from eigsep_redis.testing import DummyTransport


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_DIRS = (
    _REPO_ROOT / "scripts",
    _REPO_ROOT / "src" / "eigsep_observing" / "scripts",
)


def _load(name):
    for base in _SCRIPT_DIRS:
        path = base / f"{name}.py"
        if path.exists():
            spec = importlib.util.spec_from_file_location(name, path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
    raise FileNotFoundError(f"{name}.py not found in {_SCRIPT_DIRS}")


def test_errors_without_home_ref():
    """run() raises SystemExit when no home_ref has been published."""
    motor_home = _load("motor_home")
    t = DummyTransport()
    with pytest.raises(SystemExit):
        motor_home.run(t, dry_run=True)


def test_active_driver_listed():
    """motor_home.py must be in the ACTIVE_DRIVER_SCRIPTS partition."""
    import importlib.util as _ilu

    spec = _ilu.spec_from_file_location(
        "test_obs_config_uploaders",
        _REPO_ROOT / "tests" / "test_obs_config_uploaders.py",
    )
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert "motor_home.py" in mod.ACTIVE_DRIVER_SCRIPTS


def test_override_limits_disables_enforcement(monkeypatch):
    """run(..., override_limits=True) builds MotorHomer with enforce_limits=False."""
    import eigsep_observing

    motor_home = _load("motor_home")
    t = DummyTransport()
    publish_home_ref(t, pot_az_voltage_v0=1.0, imu_el_deg_home=0.0)

    captured = {}
    orig_homer = eigsep_observing.MotorHomer

    def _spy(transport, **kw):
        captured.update(kw)
        return orig_homer(transport, **kw)

    monkeypatch.setattr(motor_home, "MotorHomer", _spy)
    monkeypatch.setattr(motor_home, "require_pico", lambda _proxy: None)
    # dry_run=True so we don't actually move; override_limits threads through
    motor_home.run(t, dry_run=True, override_limits=True)

    assert captured.get("enforce_limits") is False
