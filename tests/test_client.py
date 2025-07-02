from concurrent.futures import ThreadPoolExecutor
import pytest
import threading
import time

from cmt_vna.testing import DummyVNA
from eigsep_corr.config import load_config
from switch_network.testing import DummySwitchNetwork

import eigsep_observing
from eigsep_observing import PandaClient
from eigsep_observing.testing import DummyEigsepRedis, DummySensor


# use DummySwitchNetwork to simulate connection
@pytest.fixture(autouse=True)
def dummies(monkeypatch):
    monkeypatch.setattr(
        "eigsep_observing.client.SwitchNetwork",
        DummySwitchNetwork,
    )
    monkeypatch.setattr("eigsep_observing.client.VNA", DummyVNA)
    monkeypatch.setattr(
        "eigsep_observing.client.sensors.SENSOR_CLASSES",
        {"dummy_sensor": DummySensor},
    )


@pytest.fixture(scope="module")
def module_tmpdir(tmp_path_factory):
    """
    Create a temporary directory for the module scope.
    This will be used to store VNA files and other temporary data.
    """
    return tmp_path_factory.mktemp("module_tmpdir")


@pytest.fixture
def redis():
    return DummyEigsepRedis()


@pytest.fixture
def client(redis, module_tmpdir):
    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    dummy_cfg = load_config(path, compute_inttime=False)
    dummy_cfg["vna_save_dir"] = str(module_tmpdir)
    return PandaClient(redis, default_cfg=dummy_cfg)


def test_client(client):
    # client is initialized with redis commands
    assert client.redis.client_heartbeat_check()  # check heartbeat
    assert isinstance(client.switch_nw, DummySwitchNetwork)
    assert client.switch_nw.ser.is_open  # check if the serial port is open
    assert "dummy_sensor" in client.sensors
    sensor, sensor_thd = client.sensors["dummy_sensor"]
    assert isinstance(sensor, DummySensor)
    assert sensor.name == "dummy_sensor"
    assert sensor_thd.is_alive()
    # vna, XXX add more tests for vna
    assert isinstance(client.vna, DummyVNA)


def test_add_sensor(caplog, monkeypatch, client):
    caplog.set_level("DEBUG")
    # add invalid sensor
    sensor_classes = eigsep_observing.client.sensors.SENSOR_CLASSES
    with pytest.raises(KeyError):
        sensor_classes["invalid_sensor"]
    # so it should not be added
    client.add_sensor("invalid_sensor", "/dev/invalid_sensor", 1.0)
    # only dummy sensor should be present
    assert len(client.sensors) == 1
    assert "dummy_sensor" in client.sensors
    rec = caplog.records[-1]
    assert "Unknown sensor name: invalid_sensor" in rec.getMessage()

    sensor, sensor_thd = client.sensors["dummy_sensor"]
    assert isinstance(sensor, DummySensor)
    assert sensor.name == "dummy_sensor"
    assert isinstance(sensor_thd, threading.Thread)
    assert sensor_thd.is_alive()

    # add the same sensor again, should not raise an error but log warning
    client.add_sensor("dummy_sensor", "/dev/dummy_sensor", 0.1)
    assert len(client.sensors) == 1  # still only one sensor
    rec = caplog.records[-1]
    assert "Sensor dummy_sensor already added" in rec.getMessage()


def test_read_ctrl_switch(client):
    """
    Test read_ctrl with a switch network.
    """
    # manually add redis to switch network; not supported by DummySwitchNetwork
    client.switch_nw.redis = client.redis
    # make sure the switching updates redis
    mode = "RFANT"
    # send a switch command, should work with DummySwitchNetwork
    switch_cmd = f"switch:{mode}"
    # read_ctrl is blocking and will process the command in a thread
    with ThreadPoolExecutor() as ex:
        future = ex.submit(
            client.redis.read_ctrl
        )  # call redis.read_ctrl directly
        time.sleep(0.1)  # small delay to ensure read starts
        client.redis.send_ctrl(switch_cmd)  # send after read started
        cmd, kwargs = future.result(timeout=5)  # wait for the result
    # verify the command was read correctly
    assert cmd == switch_cmd
    # now test that client.read_ctrl() processes the command correctly
    with ThreadPoolExecutor() as ex:
        future = ex.submit(client.read_ctrl)  # client processes the command
        time.sleep(0.1)  # small delay to ensure read starts
        client.redis.send_ctrl(switch_cmd)  # send another command
        future.result(timeout=5)  # wait for processing to complete
    # check that switch was actually processed
    obs_mode = client.redis.get_live_metadata(keys="obs_mode")
    assert obs_mode == mode


def test_read_ctrl_VNA(client, module_tmpdir):
    """
    Test read_ctrl with VNA commands.
    """
    # manually add redis to switch network; not supported by DummySwitchNetwork
    client.switch_nw.redis = client.redis

    # Test that VNA commands work correctly
    mode = "ant"
    vna_cmd = f"vna:{mode}"

    # First test that redis.read_ctrl() can read VNA commands
    with ThreadPoolExecutor() as ex:
        future = ex.submit(client.redis.read_ctrl)
        time.sleep(0.1)  # ensure read starts
        client.redis.send_ctrl(vna_cmd)
        cmd, kwargs = future.result(timeout=5)

    # verify the command was read correctly
    assert cmd == vna_cmd
    assert kwargs == {}

    # Now test that client.read_ctrl() processes VNA commands correctly
    with ThreadPoolExecutor() as ex:
        future = ex.submit(client.read_ctrl)
        time.sleep(0.1)  # ensure read starts
        client.redis.send_ctrl(vna_cmd)
        future.result(timeout=10)  # VNA operations might take longer

    # Verify VNA was initialized and used
    assert client.vna is not None
    assert isinstance(client.vna, DummyVNA)
