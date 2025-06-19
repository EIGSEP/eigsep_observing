import json
from pathlib import Path
import pytest
import tempfile
import threading
import time

from cmt_vna.testing import DummyVNA
from switch_network.testing import DummySwitchNetwork

import eigsep_observing
from eigsep_observing import PandaClient
from eigsep_observing.testing import DummyEigsepRedis, DummySensor


class DummyEigsepRedisWithInit(DummyEigsepRedis):
    """
    Class that simulates a Redis client with initial commands sent from
    the server.
    """

    def __init__(self, picos={}):
        super().__init__()
        # send init command, normally sent by the server
        super().send_ctrl("init:picos", **picos)


class DummyPandaClient(PandaClient):
    """
    PandaClient with shortened timeout to speed up tests.
    """

    def read_init_commands(self, timeout=None):
        super().read_init_commands(timeout=1)  # override to shorten timeout


@pytest.fixture
def redis():
    return DummyEigsepRedisWithInit()


@pytest.fixture
def client(redis):
    return DummyPandaClient(redis)


def test_client(client):
    with pytest.raises(TimeoutError):
        DummyPandaClient(DummyEigsepRedis())  # no redis __init__ commands
    # client is initialized with redis commands
    assert client.redis.is_client_alive()  # check heartbeat
    assert client.switch_nw is None
    assert client.sensors == {}
    # vna not initialized yet
    assert client.vna_initialized is False
    # list for server hearbeat
    assert client.stop_event is None
    client.stop_event = threading.Event()
    client._listen_heartbeat()  # start heartbeat listener
    # no heartbeat sent, so stop_event will be set
    assert client.stop_event.is_set()
    # if we send a hearbeat, the stop event would not be set
    client.stop_event.clear()
    # check every 0.5 seconds if the server is alive
    listen_thd = threading.Thread(
        target=client._listen_heartbeat,
        kwargs={"cadence": 0.5},
        daemon=True,
    )
    t0 = time.time()
    client.redis.add_raw("heartbeat:server", 1, ex=3)  # alive for 3 seconds
    listen_thd.start()
    while time.time() - t0 < 3:
        assert not client.stop_event.is_set()
    # stop event should be set first check after 3 seconds
    time.sleep(0.5)  # wait for heartbeat check
    assert client.stop_event.is_set()


def test_read_init(monkeypatch):
    """
    Test that the client can read initialization commands from Redis.
    This includes setting up the switch network and sensors.
    """
    picos = {
        "dummy_sensor": "/dev/dummy_sensor",
        "switch_pico": "/dev/dummy_switch",
    }
    # normal SwitchNetwork can't connect to the pico, so it raises ValueError
    with pytest.raises(ValueError):
        DummyPandaClient(DummyEigsepRedisWithInit(picos=picos))
    # use DummySwitchNetwork to simulate connection
    monkeypatch.setattr(
        "eigsep_observing.client.SwitchNetwork",
        DummySwitchNetwork,
    )
    client = DummyPandaClient(DummyEigsepRedisWithInit(picos=picos))
    assert isinstance(client.switch_nw, DummySwitchNetwork)
    assert client.switch_nw.ser.is_open  # check if the serial port is open
    # the read_init_commands also tries to instantiate sensors
    # but DummySensor is not available in SENSOR_CLASSES so this should
    # fail silently (we don't want a missing sensor to crash the client)
    assert client.sensors == {}  # no sensors instantiated yet

    # make sure the DummySensor is available in SENSOR_CLASSES
    monkeypatch.setattr(
        "eigsep_observing.client.sensors.SENSOR_CLASSES",
        {"dummy_sensor": DummySensor},
    )
    client = DummyPandaClient(DummyEigsepRedisWithInit(picos=picos))
    # instantiation of PandaClient calls add_sensor
    assert "dummy_sensor" in client.sensors
    sensor, sensor_thd = client.sensors["dummy_sensor"]
    assert isinstance(sensor, DummySensor)
    assert sensor.name == "dummy_sensor"
    assert sensor_thd.is_alive()


def test_add_sensor(caplog, monkeypatch, client):
    caplog.set_level("DEBUG")
    # our sensor is not a valid sensor
    sensor_classes = eigsep_observing.client.sensors.SENSOR_CLASSES
    with pytest.raises(KeyError):
        sensor_classes["dummy_sensor"]
    # so it should not be added
    client.add_sensor("dummy_sensor", "/dev/dummy_sensor")
    assert client.sensors == {}
    rec = caplog.records[-1]
    assert "Unknown sensor name: dummy_sensor" in rec.getMessage()

    # add a valid sensor, but that we can't connect to
    name = "therm"  # thermistor
    assert name in sensor_classes
    client.add_sensor(name, "/dev/dummy_sensor")
    assert client.sensors == {}
    rec = caplog.records[-1]
    assert f"Failed to initialize sensor {name}" in rec.getMessage()

    # add a valid sensor that we can connect to
    monkeypatch.setattr(
        "eigsep_observing.client.sensors.SENSOR_CLASSES",
        {"dummy_sensor": DummySensor},
    )
    name = "dummy_sensor"
    client.add_sensor(name, "/dev/dummy_sensor")
    assert name in client.sensors
    assert len(client.sensors) == 1
    sensor, sensor_thd = client.sensors["dummy_sensor"]
    assert isinstance(sensor, DummySensor)
    assert sensor.name == "dummy_sensor"
    assert isinstance(sensor_thd, threading.Thread)
    assert not sensor_thd.is_alive()  # not automatically started

    # add the same sensor again, should not raise an error but log warning
    client.add_sensor(name, "/dev/dummy_sensor")
    assert len(client.sensors) == 1  # still only one sensor
    rec = caplog.records[-1]
    assert f"Sensor {name} already added" in rec.getMessage()


def test_read_ctrl_no_switch(client):
    # need to send fake server heartbeat so that the client does not stop
    client.redis.add_raw("heartbeat:server", 1)
    thd = threading.Thread(target=client.read_ctrl, daemon=True)
    thd.start()
    # send a switch command, will fail because no switch network
    client.redis.send_ctrl("switch:RFANT")
    # raises RunTimeError in thread, which ends the thread
    thd.join(timeout=1)
    assert not thd.is_alive()  # thread should have stopped


@pytest.mark.skip(reason="fix this test - need to reset redis first")
def test_read_ctrl_switch(monkeypatch):
    """
    Test read_ctrl with a switch network.
    """
    monkeypatch.setattr(
        "eigsep_observing.client.SwitchNetwork",
        DummySwitchNetwork,
    )
    picos = {"switch_pico": "/dev/dummy_switch"}
    client = DummyPandaClient(DummyEigsepRedisWithInit(picos=picos))
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
