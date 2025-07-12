from datetime import datetime, timezone
import pytest
import time
from concurrent.futures import ThreadPoolExecutor

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

    # Test live metadata functionality - this is the primary use case
    for acc_cnt in range(10):
        client.add_metadata("acc_cnt", acc_cnt)
        assert client.r.smembers("data_streams") == {b"stream:acc_cnt"}
        assert server.r.smembers("data_streams") == {b"stream:acc_cnt"}
        if acc_cnt == 0:  # data stream should be created on first call
            assert server.data_streams == {"stream:acc_cnt": "$"}
        # live metadata should be updated
        assert server.get_live_metadata(keys="acc_cnt") == acc_cnt
        assert server.get_live_metadata(keys=["acc_cnt"]) == {
            "acc_cnt": acc_cnt
        }
        live = server.get_live_metadata()
        # can't expect the exact timestamp - live metadata uses string keys
        assert "acc_cnt_ts" in live
        ts = live.pop("acc_cnt_ts")
        assert ts.startswith(today)
        compare_dicts(live, {"acc_cnt": acc_cnt})

    # Test stream reading behavior - with current API, reads start from $
    # which means only new messages after stream is established
    metadata = server.get_metadata(stream_keys="acc_cnt")
    assert metadata == {}  # No new messages since stream starts at $

    # Test multiple streams
    test_date = "2025-06-02T16:25:15.089640"
    client.add_metadata("update_date", test_date)
    live = server.get_live_metadata()
    assert "acc_cnt_ts" in live
    assert "update_date_ts" in live
    assert "update_date" in live
    assert set(server.data_streams.keys()) == {"stream:acc_cnt", "stream:update_date"}

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
    # Test heartbeat functionality with current API
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

    # Test blocking reads using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Start reading in background thread (will block until message arrives)
        read_future = executor.submit(server.read_status)

        # Give the read thread a moment to start
        time.sleep(0.1)

        # Send status message
        msg = "test"
        client.send_status(status=msg)

        # Get the result from the read
        level, status = read_future.result(timeout=2.0)
        assert status == msg
        assert level == 20  # logging.INFO

    # Send many statuses and read them
    messages = [f"status {i}" for i in range(5)]
    for msg in messages:
        client.send_status(status=msg)

    # Read all statuses
    for expected_msg in messages:
        level, status = server.read_status()
        assert status == expected_msg
        assert level == 20

    # test specific statuses
    client.send_status(status="VNA_COMPLETE")
    level, status = server.read_status()
    assert status == "VNA_COMPLETE"

    client.send_status(status="VNA_ERROR")
    level, status = server.read_status()
    assert status == "VNA_ERROR"

    client.send_status(status="VNA_TIMEOUT")
    level, status = server.read_status()
    assert status == "VNA_TIMEOUT"


def test_ctrl(server, client):
    # initial state - the properties are now separate
    assert client.status_stream == {"stream:status": "$"}
    assert client.ctrl_stream == {"stream:ctrl": "$"}

    # Test blocking reads using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Start reading in background thread (will block until message arrives)
        read_future = executor.submit(client.read_ctrl)

        # Give the read thread a moment to start
        time.sleep(0.1)

        # Send ctrl message
        msg = "switch:RFANT"
        server.send_ctrl(msg)

        # Get the result from the read
        cmd, kwargs = read_future.result(timeout=2.0)
        assert cmd == msg
        assert kwargs == {}  # no kwargs

    # send invalid ctrl message
    with pytest.raises(ValueError):
        server.send_ctrl("invalid:command")

    # send message with kwargs using threading
    with ThreadPoolExecutor(max_workers=2) as executor:
        read_future = executor.submit(client.read_ctrl)
        time.sleep(0.1)

        msg = "vna:rec"
        kwargs_dict = {
            "ip": "127.0.0.1",
            "port": 5025,
            "fstart": 1e6,
            "power_dbm": -40,
        }
        server.send_ctrl(msg, **kwargs_dict)

        cmd, kwargs = read_future.result(timeout=2.0)
        assert cmd == msg
        compare_dicts(kwargs, kwargs_dict)

    # send multiple ctrl messages
    messages = ["vna:ant", "vna:rec", "switch:RFANT"]
    for msg in messages:
        server.send_ctrl(msg)

    # read all ctrl messages
    for expected_msg in messages:
        cmd, kwargs = client.read_ctrl()
        assert cmd == expected_msg
