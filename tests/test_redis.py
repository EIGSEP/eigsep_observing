from datetime import datetime, timezone
import numpy as np
import pytest
import time

import fakeredis
from eigsep_observing import EigsepRedis

from .utils import compare_dicts, generate_data

# mock redis connection using fakeredis
redis = fakeredis.FakeRedis(decode_responses=False)


class DummyEigsepRedis(EigsepRedis):
    def __init__(self, redis, maxlen=600):
        self.r = redis
        self.maxlen = maxlen
        self.ctrl_streams = {
            "stream:status": "0-0",
            "stream:ctrl": "0-0",
        }


@pytest.fixture
def server():
    return DummyEigsepRedis(redis)


@pytest.fixture
def client():
    return DummyEigsepRedis(redis)


def test_metadata(server, client):
    assert server.data_streams == {}  # initially empty
    today = datetime.now(timezone.utc).isoformat().split("T")[0]
    # increment acc_cnt
    for acc_cnt in range(10):
        client.add_metadata("acc_cnt", acc_cnt)
        if acc_cnt == 0:  # data stream should be created on first call
            assert server.data_streams == {b"acc_cnt": "0-0"}
        # live metadata should be updated
        assert server.get_live_metadata(keys="acc_cnt") == acc_cnt
        live = server.get_live_metadata()
        # can't expect the exact timestamp
        assert b"acc_cnt_ts" in live
        ts = live.pop(b"acc_cnt_ts")
        assert ts.startswith(today)
        compare_dicts(live, {b"acc_cnt": acc_cnt})
    # read the stream
    metadata = server.get_metadata(stream_keys="acc_cnt")
    compare_dicts(metadata, {b"acc_cnt": np.arange(10)})
    # after reading, the stream should be empty
    assert server.get_metadata(stream_keys="acc_cnt") == {}
    # live metadata should still be available
    assert server.get_live_metadata(keys="acc_cnt") == 9
    # multiple streams, mix of int and str
    for acc_cnt in range(10, 20):
        client.add_metadata("acc_cnt", acc_cnt)
        test_date = f"2025-06-02T16:25:{acc_cnt}.089640"
        client.add_metadata("update_date", test_date)
        live = server.get_live_metadata()
        assert b"acc_cnt_ts" in live
        ts = live.pop(b"acc_cnt_ts")
        assert ts.startswith(today)
        assert b"update_date_ts" in live
        ts = live.pop(b"update_date_ts")
        assert ts.startswith(today)
        compare_dicts({b"acc_cnt": acc_cnt, b"update_date": test_date}, live)
    # read the streams
    metadata = server.get_metadata()
    expected = {
        b"acc_cnt": np.arange(10, 20),
        b"update_date": np.array(
            [f"2025-06-02T16:25:{i}.089640" for i in range(10, 20)]
        ),
    }
    compare_dicts(metadata, expected)
    assert set(server.data_streams.keys()) == set([b"acc_cnt", b"update_date"])

    # test reset
    server.reset()
    assert client.data_streams == {}


def test_raw(server):
    # one integration from snap
    data = generate_data(ntimes=1, raw=True, reshape=False)
    data_back = {}
    for p, d in data.items():
        server.add_raw(f"data:{p}", d)
        data_back[p] = server.get_raw(f"data:{p}")
    compare_dicts(data, data_back)


def test_is_alive(server, client):
    ckey = "heartbeat:client"
    skey = "heartbeat:server"
    # initially, both should be empty
    assert server._is_alive(ckey) is False
    assert client._is_alive(skey) is False
    assert server.is_client_alive() is False
    assert client.is_server_alive() is False
    # set server alive
    server.add_raw(skey, 1, ex=1)
    assert client._is_alive(skey) is True
    assert client.is_server_alive() is True
    assert server.is_client_alive() is False
    time.sleep(1.1)  # wait for expiration
    assert client._is_alive(skey) is False
    assert client.is_server_alive() is False
    # set client alive
    client.add_raw(ckey, 1, ex=1)
    assert server._is_alive(ckey) is True
    assert server.is_client_alive() is True
    assert client.is_server_alive() is False
    time.sleep(1.1)  # wait for expiration
    assert server._is_alive(ckey) is False
    assert server.is_client_alive() is False
    assert client.is_server_alive() is False
    # turn on/off
    server.add_raw(skey, 1, ex=100)
    assert client.is_server_alive() is True
    server.add_raw(skey, 0, ex=100)  # turn off
    assert client.is_server_alive() is False
    client.add_raw(ckey, 1, ex=100)
    assert server.is_client_alive() is True
    client.add_raw(ckey, 0, ex=100)  # turn off
    assert server.is_client_alive() is False
    # test reset
    client.add_raw(ckey, 1, ex=100)
    server.add_raw(skey, 1, ex=100)
    assert server.is_client_alive() is True
    assert client.is_server_alive() is True
    server.reset()
    assert server.is_client_alive() is False
    assert client.is_server_alive() is False


def test_status(server, client):
    # initial state
    compare_dicts(
        client.ctrl_streams, {"stream:status": "0-0", "stream:ctrl": "0-0"}
    )
    # initially, no status
    assert server.read_status() == (None, None)
    # add status
    msg = "test"
    client.send_status(msg)
    eid, status = server.read_status()
    assert eid == server.ctrl_streams["stream:status"]
    assert status == msg
    # read status again
    assert server.read_status() == (None, None)
    # send many statuses
    for i in range(10):
        client.send_status(f"status {i}")
    # read all statuses
    for i in range(10):
        eid, status = server.read_status()  # read one by one
        assert eid == server.ctrl_streams["stream:status"]
        assert status == f"status {i}"
    assert server.read_status() == (None, None)  # no more statuses
    # test specific statuses
    client.send_vna_complete()
    assert server.read_status()[1] == "VNA_COMPLETE"
    client.send_vna_error()
    assert server.read_status()[1] == "VNA_ERROR"
    client.send_vna_timeout()
    assert server.read_status()[1] == "VNA_TIMEOUT"


def test_ctrl(server, client):
    # initial state
    compare_dicts(
        client.ctrl_streams, {"stream:status": "0-0", "stream:ctrl": "0-0"}
    )
    assert client.read_ctrl() == (None, None)
    # send ctrl message
    msg = "switch:RFANT"
    server.send_ctrl(msg)
    eid, cmd = client.read_ctrl()
    assert eid == client.ctrl_streams["stream:ctrl"]
    assert cmd == (msg, {})  # no kwargs
    # send invalid ctrl message
    with pytest.raises(ValueError):
        server.send_ctrl("invalid:command")
    # send message with kwargs
    msg = "vna:rec"
    # realistic kwargs
    kwargs = {"ip": "127.0.0.1", "port": 5025, "fstart": 1e6, "power_dbm": -40}
    server.send_ctrl(msg, **kwargs)
    eid, cmd = client.read_ctrl()
    assert eid == client.ctrl_streams["stream:ctrl"]
    assert cmd[0] == msg
    compare_dicts(cmd[1], kwargs)
    # send multiple ctrl messages
    messages = ["vna:ant", "vna:rec", "switch:RFANT"]
    for msg in messages:
        server.send_ctrl(msg)
    # read all ctrl messages
    for msg in messages:
        eid, cmd = client.read_ctrl()
        assert eid == client.ctrl_streams["stream:ctrl"]
        assert cmd[0] == msg
