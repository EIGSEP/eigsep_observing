"""
Integration tests for the emulator-backed pico pipeline.

Tests that sensor data flows from pico emulators (running inside an
in-process PicoManager) through picohost reader threads into
FakeRedis, and is retrievable via get_live_metadata().
"""

import time
import pytest
import yaml

import eigsep_observing
from eigsep_observing.testing import DummyEigsepObsRedis, DummyPandaClient


@pytest.fixture()
def dummy_cfg(tmp_path):
    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg["vna_save_dir"] = str(tmp_path)
    return cfg


@pytest.fixture
def redis():
    return DummyEigsepObsRedis()


@pytest.fixture
def client(redis, dummy_cfg):
    c = DummyPandaClient(redis, default_cfg=dummy_cfg)
    yield c
    c.stop()


def test_picos_registered(client):
    """PicoManager should register all dummy devices in Redis."""
    available = client.redis.r.smembers("picos")
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


def test_sensor_metadata_in_redis(client, redis):
    """Emulators generate status that flows through redis_handler to Redis."""
    time.sleep(0.5)

    metadata = redis.get_live_metadata()

    expected_sensors = {
        "tempctrl",
        "potmon",
        "imu_el",
        "imu_az",
        "lidar",
        "rfswitch",
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

    # Check tempctrl LNA/LOAD channels
    tempctrl = metadata.get("tempctrl", {})
    assert "LNA_T_now" in tempctrl
    assert "LOAD_T_now" in tempctrl
    assert tempctrl.get("sensor_name") == "tempctrl"

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


def test_get_live_metadata_single_key(client, redis):
    """get_live_metadata with a single key returns just that sensor's data."""
    time.sleep(0.5)
    lidar = redis.get_live_metadata("lidar")
    assert isinstance(lidar, dict)
    assert "distance_m" in lidar
