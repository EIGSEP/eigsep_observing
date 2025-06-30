from datetime import datetime, timezone
import numpy as np
import pytest
import time

from eigsep_observing.testing.utils import compare_dicts, generate_data
from eigsep_observing.testing import DummyEigsepRedis


@pytest.fixture
def server():
    return DummyEigsepRedis()


@pytest.fixture
def client(server):
    c = DummyEigsepRedis()
    c.r = server.r
    return c


def test_metadata(server, client):
    assert server.data_streams == {}  # initially empty
    today = datetime.now(timezone.utc).isoformat().split("T")[0]
    # increment acc_cnt
    for acc_cnt in range(10):
        client.add_metadata("acc_cnt", acc_cnt)
        assert client.r.smembers("data_streams") == {b"acc_cnt"}
        assert server.r.smembers("data_streams") == {b"acc_cnt"}
        if acc_cnt == 0:  # data stream should be created on first call
            assert server.data_streams == {"acc_cnt": "$"}
        # live metadata should be updated
        assert server.get_live_metadata(keys="acc_cnt") == acc_cnt
        assert server.get_live_metadata(keys=["acc_cnt"]) == {
            "acc_cnt": acc_cnt
        }
        live = server.get_live_metadata()
        # can't expect the exact timestamp
        assert "acc_cnt_ts" in live
        ts = live.pop("acc_cnt_ts")
        assert ts.startswith(today)
        compare_dicts(live, {"acc_cnt": acc_cnt})
    # read the stream
    metadata = server.get_metadata(stream_keys="acc_cnt")
    compare_dicts(metadata, {"acc_cnt": np.arange(10)})
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
        assert "acc_cnt_ts" in live
        ts = live.pop("acc_cnt_ts")
        assert ts.startswith(today)
        assert "update_date_ts" in live
        ts = live.pop("update_date_ts")
        assert ts.startswith(today)
        compare_dicts({"acc_cnt": acc_cnt, "update_date": test_date}, live)
    # read the streams
    metadata = server.get_metadata()
    expected = {
        "acc_cnt": np.arange(10, 20),
        "update_date": np.array(
            [f"2025-06-02T16:25:{i}.089640" for i in range(10, 20)]
        ),
    }
    compare_dicts(metadata, expected)
    assert set(server.data_streams.keys()) == set(["acc_cnt", "update_date"])

    # test typeerror in live_metadata
    with pytest.raises(TypeError):
        server.get_live_metadata(keys=[1])  # keys must be str or list of str
    # test reset
    server.reset()
    assert server.data_streams == {}


def test_raw(server):
    # one integration from snap
    data = generate_data(ntimes=1, raw=True, reshape=False)
    data_back = {}
    for p, d in data.items():
        server.add_raw(f"data:{p}", d)
        data_back[p] = server.get_raw(f"data:{p}")
    compare_dicts(data, data_back)


def test_is_alive(server, client):
    # initially, both should be empty
    assert server.client_heartbeat_check() is False
    # set client alive (server checks client heartbeat)
    client.client_heartbeat_set(ex=1, alive=True)
    assert server.client_heartbeat_check() is True
    time.sleep(1.1)  # wait for expiration
    assert server.client_heartbeat_check() is False
    # turn on/off
    client.client_heartbeat_set(ex=100, alive=True)
    assert server.client_heartbeat_check() is True
    client.client_heartbeat_set(ex=100, alive=False)  # turn off
    assert server.client_heartbeat_check() is False
    # test reset
    client.client_heartbeat_set(ex=100, alive=True)
    assert server.client_heartbeat_check() is True
    server.reset()
    assert server.client_heartbeat_check() is False


def test_status(server, client):
    # initial state - the properties are now separate
    assert client.status_stream == {"stream:status": "$"}
    assert client.ctrl_stream == {"stream:ctrl": "$"}
    # initially, no status (will now block, so change test approach)
    # We'll test by sending first then reading
    msg = "test"
    client.send_status(status=msg)
    level, status = server.read_status()
    assert status == msg
    assert level == 20  # logging.INFO
    # send many statuses
    for i in range(10):
        client.send_status(status=f"status {i}")
    # read all statuses
    for i in range(10):
        level, status = server.read_status()  # read one by one
        assert status == f"status {i}"
    # test specific statuses
    client.send_status(status="VNA_COMPLETE")
    assert server.read_status()[1] == "VNA_COMPLETE"
    client.send_status(status="VNA_ERROR")
    assert server.read_status()[1] == "VNA_ERROR"
    client.send_status(status="VNA_TIMEOUT")
    assert server.read_status()[1] == "VNA_TIMEOUT"


def test_ctrl(server, client):
    # initial state - the properties are now separate
    assert client.status_stream == {"stream:status": "$"}
    assert client.ctrl_stream == {"stream:ctrl": "$"}
    # send ctrl message
    msg = "switch:RFANT"
    server.send_ctrl(msg)
    cmd, kwargs = client.read_ctrl()
    assert cmd == msg
    assert kwargs == {}  # no kwargs
    # send invalid ctrl message
    with pytest.raises(ValueError):
        server.send_ctrl("invalid:command")
    # send message with kwargs
    msg = "vna:rec"
    # realistic kwargs
    kwargs_dict = {
        "ip": "127.0.0.1",
        "port": 5025,
        "fstart": 1e6,
        "power_dbm": -40,
    }
    server.send_ctrl(msg, **kwargs_dict)
    cmd, kwargs = client.read_ctrl()
    assert cmd == msg
    compare_dicts(kwargs, kwargs_dict)
    # send multiple ctrl messages
    messages = ["vna:ant", "vna:rec", "switch:RFANT"]
    for msg in messages:
        server.send_ctrl(msg)
    # read all ctrl messages
    for msg in messages:
        cmd, kwargs = client.read_ctrl()
        assert cmd == msg
