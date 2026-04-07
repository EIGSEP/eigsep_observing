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
    # motor is skipped in init_picos; the dummy config has six picos
    # left after that (tempctrl, potmon, imu_el, imu_az, lidar, rfswitch).
    expected = {
        "tempctrl",
        "potmon",
        "imu_el",
        "imu_az",
        "lidar",
        "rfswitch",
    }
    assert set(client.picos.keys()) == expected


def test_sensor_metadata_in_redis(client, redis):
    """Emulators generate status that flows through redis_handler to Redis."""
    # Wait for at least one status cadence (50ms) + reader thread processing
    time.sleep(0.5)

    metadata = redis.get_live_metadata()

    # Each emulator writes its sensor_name as the Redis metadata key. The
    # two IMU picos use distinct app_id-based names (imu_el / imu_az) per
    # picohost 1.0.0.
    expected_sensors = {
        "tempctrl",  # from tempctrl pico (PicoPeltier class)
        "potmon",  # from potmon pico
        "imu_el",  # from imu_el pico (app_id=3)
        "imu_az",  # from imu_az pico (app_id=6)
        "lidar",  # from lidar pico
        "rfswitch",  # from rfswitch pico
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

    # Check IMU (BNO085 RVC mode in picohost 1.0.0)
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

    # Check potmon (post-_pot_redis_handler shape: voltages always
    # present, cal/angle fields are None for an uncalibrated stream)
    potmon = metadata.get("potmon", {})
    assert "pot_el_voltage" in potmon
    assert "pot_az_voltage" in potmon
    assert "pot_el_cal_slope" in potmon
    assert "pot_el_angle" in potmon


def test_get_live_metadata_single_key(client, redis):
    """get_live_metadata with a single key returns just that sensor's data."""
    time.sleep(0.5)
    lidar = redis.get_live_metadata("lidar")
    assert isinstance(lidar, dict)
    assert "distance_m" in lidar
