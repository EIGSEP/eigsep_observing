from datetime import datetime, timezone
import numpy as np
import pytest
import time
from concurrent.futures import ThreadPoolExecutor

from eigsep_observing.testing.utils import compare_dicts, generate_data
from eigsep_observing.testing import DummyEigsepObsRedis
from eigsep_redis.testing import DummyEigsepRedis


@pytest.fixture
def server():
    return DummyEigsepRedis()


@pytest.fixture
def client(server):
    c = DummyEigsepRedis()
    c.r = server.r
    return c


@pytest.fixture
def obs_server():
    return DummyEigsepObsRedis()


@pytest.fixture
def obs_client(obs_server):
    c = DummyEigsepObsRedis()
    c.r = obs_server.r
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
            assert "stream:acc_cnt" in server.data_streams
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
    assert set(server.data_streams.keys()) == {
        "stream:acc_cnt",
        "stream:update_date",
    }

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


def test_int32_redis_round_trip(obs_server, obs_client):
    """Int32 data survives add_corr_data → read_corr_data bit-for-bit.

    Mirrors the production pattern: consumer (server) blocks on
    read_corr_data *before* the producer (client) writes, matching
    the EigObserver ↔ EigsepFpga interaction.
    """
    data = generate_data(ntimes=1, reshape=False)
    # Convert one time-step to bytes (the wire format)
    raw = {p: d[0].tobytes() for p, d in data.items()}
    dtype = ">i4"
    pairs = list(data.keys())
    # Seed corr_sync_time so read_corr_data can retrieve it.
    # Must be a dict with "sync_time_unix" — matches fpga.py's format.
    obs_client.add_metadata(
        "corr_sync_time", {"sync_time_unix": 1748732903.42}
    )
    # In production, the FPGA is already running when the observer
    # starts reading — stream:corr exists and has at least one entry.
    # Seed the stream so read_corr_data doesn't bail on the
    # "no stream" guard, mirroring the production startup order.
    obs_client.add_corr_data(raw, cnt=0, dtype=dtype)
    # Consumer blocks first (like EigObserver), producer writes after
    # (like EigsepFpga) — same pattern as test_status.
    with ThreadPoolExecutor(max_workers=1) as executor:
        read_future = executor.submit(
            obs_server.read_corr_data, pairs=pairs, unpack=True
        )
        time.sleep(0.1)  # let consumer block
        obs_client.add_corr_data(raw, cnt=42, dtype=dtype)
        acc_cnt, _, read_data = read_future.result(timeout=5.0)
    assert acc_cnt == 42
    for p in pairs:
        original = np.frombuffer(raw[p], dtype=dtype)
        read_back = read_data[p]
        assert read_back.dtype == np.dtype(dtype), (
            f"pair '{p}': expected {dtype}, got {read_back.dtype}"
        )
        np.testing.assert_array_equal(read_back, original)


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
    # initial state
    assert client.status_stream == {"stream:status": "$"}

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
