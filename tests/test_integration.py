"""
Integration tests for the emulator-backed pico pipeline.

Tests that sensor data flows from pico emulators (running inside an
in-process PicoManager) through picohost reader threads into
FakeRedis, and is retrievable via ``MetadataSnapshotReader.get``.
"""

import time
import pytest
import yaml

from eigsep_redis import MetadataSnapshotReader
from eigsep_redis.testing import DummyTransport

import eigsep_observing
from eigsep_observing.testing import DummyPandaClient


@pytest.fixture()
def dummy_cfg(tmp_path):
    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg


@pytest.fixture
def transport():
    return DummyTransport()


@pytest.fixture
def client(transport, dummy_cfg):
    c = DummyPandaClient(transport, cfg=dummy_cfg)
    yield c
    c.stop()


# All emulator-fed streams we expect after the embedded PicoManager starts
# (one entry per registered DummyPico*). Used both as the fixture-readiness
# wait condition and as the assertion target in test_sensor_metadata_in_redis.
_EXPECTED_SENSORS = (
    # tempctrl publishes two streams (one per Peltier channel); the
    # picohost producer fans the firmware's combined tick into
    # tempctrl_lna and tempctrl_load.
    "tempctrl_lna",
    "tempctrl_load",
    "potmon",
    "imu_el",
    "imu_az",
    "lidar",
    "rfswitch",
)


def _wait_for_sensors(transport, sensors=_EXPECTED_SENSORS, timeout=5.0):
    """Poll the metadata snapshot until every name in ``sensors`` has
    published at least once. Returns the final snapshot. Raises
    ``AssertionError`` (with the keys actually present) on timeout.

    Replaces ``time.sleep(0.5)`` — under ``pytest -n auto`` on CI the
    200 ms emulator cadence + reader-thread + Redis write chain can take
    longer than a half-second wall clock. Condition-polling decouples
    the test from how heavily the runner is loaded.
    """
    deadline = time.monotonic() + timeout
    reader = MetadataSnapshotReader(transport)
    while True:
        metadata = reader.get()
        if all(s in metadata for s in sensors):
            return metadata
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"timed out after {timeout}s waiting for sensors "
                f"{list(sensors)}; got keys: {list(metadata.keys())}"
            )
        time.sleep(0.02)


def test_picos_registered(client):
    """PicoManager should register all dummy devices in Redis."""
    available = client.transport.r.smembers("picos")
    names = {n.decode() if isinstance(n, bytes) else n for n in available}
    expected = {
        "tempctrl",
        "potmon",
        "imu_el",
        "imu_az",
        "lidar",
        "rfswitch",
        "motor",
    }
    assert names == expected


def test_sensor_metadata_in_redis(client, transport):
    """Emulators generate status that flows through redis_handler to Redis."""
    metadata = _wait_for_sensors(transport)
    for sensor in _EXPECTED_SENSORS:
        assert sensor in metadata, (
            f"Expected sensor '{sensor}' in metadata, "
            f"got keys: {list(metadata.keys())}"
        )


def test_metadata_has_expected_fields(client, transport):
    """Verify that metadata values contain the expected sensor fields."""
    metadata = _wait_for_sensors(transport)

    # Each Peltier channel has its own stream with flat per-channel
    # fields, a top-level status, and the device-wide watchdog fields
    # duplicated in by the picohost handler.
    for stream in ("tempctrl_lna", "tempctrl_load"):
        entry = metadata.get(stream, {})
        assert entry.get("sensor_name") == stream
        assert "status" in entry
        assert "T_now" in entry
        assert "drive_level" in entry
        assert "watchdog_timeout_ms" in entry

    # Check IMU (BNO085 RVC mode)
    imu = metadata.get("imu_el", {})
    assert "yaw" in imu
    assert "accel_x" in imu
    assert imu.get("sensor_name") == "imu_el"

    # Check lidar
    lidar = metadata.get("lidar", {})
    assert "distance_m" in lidar

    # Check rfswitch
    rfswitch = metadata.get("rfswitch", {})
    assert "sw_state" in rfswitch

    # Check potmon (uncalibrated — cal/angle fields are None)
    potmon = metadata.get("potmon", {})
    assert isinstance(potmon["pot_el_voltage"], float)
    assert isinstance(potmon["pot_az_voltage"], float)
    assert potmon["pot_el_cal_slope"] is None
    assert potmon["pot_el_cal_intercept"] is None
    assert potmon["pot_el_angle"] is None
    assert potmon["pot_az_cal_slope"] is None
    assert potmon["pot_az_cal_intercept"] is None
    assert potmon["pot_az_angle"] is None


def test_metadata_snapshot_single_key(client, transport):
    """metadata_snapshot.get(key) returns just that sensor's data."""
    _wait_for_sensors(transport, sensors=("lidar",))
    lidar = MetadataSnapshotReader(transport).get("lidar")
    assert isinstance(lidar, dict)
    assert "distance_m" in lidar
