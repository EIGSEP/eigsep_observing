"""
Integration tests for the emulator-backed pico pipeline.

Tests that sensor data flows from pico emulators through picohost reader
threads into FakeRedis, and is retrievable via get_live_metadata().
"""

import time
import pytest
import yaml

import eigsep_observing
from eigsep_observing.testing import DummyEigsepRedis, DummyPandaClient


@pytest.fixture()
def dummy_cfg(tmp_path):
    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg["vna_save_dir"] = str(tmp_path)
    return cfg


@pytest.fixture
def redis():
    return DummyEigsepRedis()


@pytest.fixture
def client(redis, dummy_cfg):
    c = DummyPandaClient(redis, default_cfg=dummy_cfg)
    yield c
    # disconnect all picos to stop emulator/reader threads
    for pico in c.picos.values():
        try:
            pico.disconnect()
        except Exception:
            pass


def test_picos_initialized(client):
    """DummyPandaClient should initialize picos from the dummy config."""
    # motor is skipped in init_picos, so we expect 5 picos
    expected = {"imu", "therm", "peltier", "lidar", "switch"}
    assert set(client.picos.keys()) == expected


def test_sensor_metadata_in_redis(client, redis):
    """Emulators generate status that flows through redis_handler to Redis."""
    # Wait for at least one status cadence (50ms) + reader thread processing
    time.sleep(0.5)

    metadata = redis.get_live_metadata()

    # Each emulator writes its sensor_name as the Redis metadata key.
    # RFSwitchWithImuEmulator writes both "rfswitch" and "imu_antenna".
    expected_sensors = {
        "tempctrl",  # from peltier pico
        "temp_mon",  # from therm pico
        "imu_panda",  # from imu pico (app_id=3)
        "lidar",  # from lidar pico
        "rfswitch",  # from switch pico (composite emulator)
        "imu_antenna",  # from switch pico (composite emulator)
    }
    for sensor in expected_sensors:
        assert sensor in metadata, (
            f"Expected sensor '{sensor}' in metadata, "
            f"got keys: {list(metadata.keys())}"
        )


def test_metadata_has_expected_fields(client, redis):
    """Verify that metadata values contain the expected sensor fields."""
    time.sleep(0.5)

    metadata = redis.get_live_metadata()

    # Check motor-like fields from tempctrl
    tempctrl = metadata.get("tempctrl", {})
    assert "A_T_now" in tempctrl
    assert "B_T_now" in tempctrl
    assert tempctrl.get("sensor_name") == "tempctrl"

    # Check IMU fields
    imu = metadata.get("imu_panda", {})
    assert "quat_i" in imu
    assert "accel_x" in imu
    assert imu.get("sensor_name") == "imu_panda"

    # Check lidar
    lidar = metadata.get("lidar", {})
    assert "distance_m" in lidar

    # Check rfswitch
    rfswitch = metadata.get("rfswitch", {})
    assert "sw_state" in rfswitch


def test_get_live_metadata_single_key(client, redis):
    """get_live_metadata with a single key returns just that sensor's data."""
    time.sleep(0.5)
    lidar = redis.get_live_metadata("lidar")
    assert isinstance(lidar, dict)
    assert "distance_m" in lidar
