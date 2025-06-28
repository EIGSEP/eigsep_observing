import json
from pathlib import Path
import pytest
import tempfile
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
    monkeypatch.setattr(
        "eigsep_observing.client.VNA", DummyVNA
    )
    monkeypatch.setattr(
        "eigsep_observing.client.sensors.SENSOR_CLASSES",
        {"dummy_sensor": DummySensor},
    )


class DummyPandaClient(PandaClient):
    pass


@pytest.fixture
def redis():
    return DummyEigsepRedis()


@pytest.fixture
def client(redis):
    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    dummy_cfg = load_config(path, compute_inttime=False)
    return DummyPandaClient(redis, default_cfg=dummy_cfg)


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
    client.add_sensor("invalid_sensor", "/dev/invalid_sensor")
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
    client.add_sensor("dummy_sensor", "/dev/dummy_sensor")
    assert len(client.sensors) == 1  # still only one sensor
    rec = caplog.records[-1]
    assert f"Sensor dummy_sensor already added" in rec.getMessage()


@pytest.mark.skip(reason="fix this test - need to reset redis first")
def test_read_ctrl_switch(client):
    """
    Test read_ctrl with a switch network.
    """
    # manually add redis to switch network; not supported by DummySwitchNetwork
    client.switch_nw.redis = client.redis
    # make sure the switching updates redis
    mode = "RFANT"
    client.switch_nw.switch(mode)
    obs_mode = client.redis.get_live_metadata(keys="obs_mode")
    assert obs_mode == mode

    # do it from the client, using read_ctrl  #XXX need reset redis
    assert False, "fix this test - need to reset redis first"
    thd = threading.Thread(target=client.read_ctrl, daemon=True)
    thd.start()
    # send a switch command, should work with DummySwitchNetwork
    switch_cmd = f"switch:{mode}"
    client.redis.send_ctrl(switch_cmd)
    obs_mode = client.redis.get_live_metadata(keys="obs_mode")
    assert obs_mode == mode


@pytest.mark.skip(reason="not implemented yet")
def test_read_ctrl_VNA(monkeypatch):
    monkeypatch.setattr(
        "eigsep_observing.client.SwitchNetwork", DummySwitchNetwork
    )
    monkeypatch.setattr("eigsep_observing.client.VNA", DummyVNA)
    picos = {"switch_pico": "/dev/dummy_switch"}
    client = DummyPandaClient(DummyEigsepRedisWithInit(picos=picos))
    # need to send fake server heartbeat so that the client does not stop
    client.redis.add_raw("heartbeat:server", 1)
    # manually add redis to switch network; not supported by DummySwitchNetwork
    assert client.switch_nw is not None
    client.switch_nw.redis = client.redis
    # send invalid VNA command
    thd = threading.Thread(target=client.read_ctrl, daemon=True)
    thd.start()
    # note: can't use send_ctrl here because it requires a valid command
    invalid_command = {"cmd": "vna:invalid"}
    client.redis.r.xadd("stream:ctrl", {"msg": json.dumps(invalid_command)})
    # should send a VNA error to redis but continue running
    status = client.redis.read_status()[1]
    assert status == "VNA_ERROR"
    # send a valid VNA command
    tmpdir = tempfile.mkdtemp()
    client.redis.send_ctrl("vna:ant", {"save_dir": tmpdir})
    assert client.vna_initialized is True
    assert isinstance(client.vna, DummyVNA)
    assert client.vna.save_dir == tmpdir
    # a file should have been created
    assert len(Path(tmpdir).glob("*.h5")) == 1
    # status should be VNA_COMPLETE
    status = client.redis.read_status()[1]
    assert status == "VNA_COMPLETE"
    # stop the client
    client.stop_event.set()
    thd.join(timeout=1)
    assert not thd.is_alive()
