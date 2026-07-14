"""Tests for scripts/standby_manual.py logic (no REPL/TTY needed)."""

import importlib.util
from pathlib import Path

from eigsep_redis import MetadataSnapshotReader, MetadataWriter
from eigsep_redis.testing import DummyTransport

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load(name):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeProxy:
    def __init__(self, result=None):
        self.result = result
        self.calls = []

    def send_command(self, action):
        self.calls.append(action)
        return self.result


def test_read_standby_state_from_snapshot():
    mod = _load("standby_manual")
    t = DummyTransport()
    w = MetadataWriter(t)
    w.add(
        "lidar",
        {
            "sensor_name": "lidar",
            "status": "error",
            "standby": True,
            "laser_firing": 0,
        },
    )
    snap = MetadataSnapshotReader(t)
    state = mod._read_standby_state(snap, "lidar")
    assert state["standby"] is True
    assert state["laser_firing"] == 0
    assert state["status"] == "error"


def test_read_standby_state_missing_device_is_empty():
    mod = _load("standby_manual")
    snap = MetadataSnapshotReader(DummyTransport())
    assert mod._read_standby_state(snap, "imu_el") == {}


def test_do_toggle_sends_standby_and_reports():
    mod = _load("standby_manual")
    snap = MetadataSnapshotReader(DummyTransport())
    proxy = _FakeProxy(result={})
    msg = mod._do_toggle(proxy, snap, "imu_az", True)
    assert proxy.calls == ["standby"]
    assert "imu_az" in msg
    assert "standby" in msg


def test_do_toggle_resume_sends_resume():
    mod = _load("standby_manual")
    snap = MetadataSnapshotReader(DummyTransport())
    proxy = _FakeProxy(result={})
    mod._do_toggle(proxy, snap, "lidar", False)
    assert proxy.calls == ["resume"]


def test_devices_match_helper():
    mod = _load("standby_manual")
    assert mod.STANDBY_DEVICES == ("imu_el", "imu_az", "lidar")
