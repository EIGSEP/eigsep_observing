from pathlib import Path
import pytest
import tempfile
import threading
import time

# from cmt_vna.tests import DummyVNA  # XXX
from eigsep_observing import PandaClient

# from switch_network.tests import DummySwitchNetwork  # XXX

from .test_redis import DummyEigsepRedis
from .test_sensors import DummySensor


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


@pytest.mark.skip(reason="DummySwitchNetwork not implemented yet")  # XXX
def test_read_init(monkeypatch):
    # make sure the DummySensor is available in SENSOR_CLASSES
    monkeypatch.setattr(
        "eigsep_observing.client.sensors.SENSOR_CLASSES",  # XXX
        {"dummy_sensor": DummySensor},
    )
    picos = {
        "dummy_sensor": "/dev/dummy_sensor",
        "switch_pico": "/dev/dummy_switch",
    }
    # can't instantiate PandaClient if SwitchNetwork is not available
    with pytest.raises(ValueError):
        DummyPandaClient(DummyEigsepRedisWithInit(picos=picos))
    # make it work with a dummy SwitchNetwork
    monkeypatch.setattr(
        "eigsep_observing.client.SwitchNetwork",
        DummySwitchNetwork,
    )
    client = DummyPandaClient(DummyEigsepRedisWithInit(picos=picos))
    assert isinstance(client.switch_nw, DummySwitchNetwork)
    # XXX implement __eq__ in switch_network
    assert client.switch_nw == DummySwitchNetwork(picos["switch_pico"])
    # instantiation of PandaClient calls add_sensor
    assert "dummy_sensor" in client.sensors
    sensor_thd = client.sensors["dummy_sensor"]
    assert sensor_thd.is_alive()


@pytest.mark.skip(reason="DummyVNA not implemented yet")  # XXX
def test_read_ctrl(monkeypatch, client):
    monkeypatch.setattr("eigsep_observing.client.VNA", DummyVNA)
    # need to send fake server heartbeat so that the client does not stop
    client.redis.add_raw("heartbeat:server", 1)
    thd = threading.Thread(target=client.read_ctrl, daemon=True)
    thd.start()
    # send a switch command, will fail because no switch network
    client.redis.send_ctrl("switch:RFANT")
    # raises RunTimeError in thread, which ends the thread
    thd.join(timeout=1)
    assert not thd.is_alive()  # thread should have stopped
    # send invalid VNA command
    thd.start()
    client.redis.send_ctrl("vna:invalid")
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


@pytest.mark.skip(reason="DummySwitchNetwork not implemented yet")  # XXX
def test_read_ctrl_switch(monkeypatch):
    # test read_ctrl with a switch network
    monkeypatch.setattr(
        "eigsep_observing.client.SwitchNetwork",
        DummySwitchNetwork,
    )
    picos = {"switch_pico": "/dev/dummy_switch"}
    client = DummyPandaClient(DummyEigsepRedisWithInit(picos=picos))
    thd = threading.Thread(target=client.read_ctrl, daemon=True)
    thd.start()
    # send a switch command, should work with DummySwitchNetwork
    client.redis.send_ctrl("switch:RFANT")
    obs_mode = client.redis.get_live_metadata(keys="obs_mode")
    assert obs_mode == "RFANT"
