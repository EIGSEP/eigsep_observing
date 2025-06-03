from datetime import datetime, UTC
import numpy as np
import pytest

import fakeredis
from eigsep_observing import EigsepRedis

from .utils import compare_dicts, generate_data


class DummyEigsepRedis(EigsepRedis):
    def __init__(self, host="localhost", port=6379, maxlen=600):
        self.r = fakeredis.FakeRedis(decode_responses=False)
        self.maxlen = maxlen
        self.ctrl_streams = {
            "stream:status": "0-0",
            "stream:ctrl": "0-0",
        }


@pytest.fixture
def eigsep_redis():
    return DummyEigsepRedis()


def test_metadata(eigsep_redis):
    assert eigsep_redis.data_streams == {}  # initially empty
    today = datetime.now(UTC).isoformat().split("T")[0]  # for timestamps
    # increment acc_cnt
    for acc_cnt in range(10):
        eigsep_redis.add_metadata("acc_cnt", acc_cnt)
        if acc_cnt == 0:  # data stream should be created on first call
            assert eigsep_redis.data_streams == {b"acc_cnt": "0-0"}
        # live metadata should be updated
        assert eigsep_redis.get_live_metadata(keys="acc_cnt") == acc_cnt
        live = eigsep_redis.get_live_metadata()
        # can't expect the exact timestamp
        assert b"acc_cnt_ts" in live
        ts = live.pop(b"acc_cnt_ts")
        assert ts.startswith(today)
        compare_dicts(live, {b"acc_cnt": acc_cnt})
    # read the stream
    metadata = eigsep_redis.get_metadata(stream_keys="acc_cnt")
    compare_dicts(metadata, {b"acc_cnt": np.arange(10)})
    # after reading, the stream should be empty
    assert eigsep_redis.get_metadata(stream_keys="acc_cnt") == {}
    # live metadata should still be available
    assert eigsep_redis.get_live_metadata(keys="acc_cnt") == 9
    # multiple streams, mix of int and str
    for acc_cnt in range(10, 20):
        eigsep_redis.add_metadata("acc_cnt", acc_cnt)
        test_date = f"2025-06-02T16:25:{acc_cnt}.089640"
        eigsep_redis.add_metadata("update_date", test_date)
        live = eigsep_redis.get_live_metadata()
        assert b"acc_cnt_ts" in live
        ts = live.pop(b"acc_cnt_ts")
        assert ts.startswith(today)
        assert b"update_date_ts" in live
        ts = live.pop(b"update_date_ts")
        assert ts.startswith(today)
        compare_dicts({b"acc_cnt": acc_cnt, b"update_date": test_date}, live)
    # read the streams
    metadata = eigsep_redis.get_metadata()
    expected = {
        b"acc_cnt": np.arange(10, 20),
        b"update_date": np.array(
            [f"2025-06-02T16:25:{i}.089640" for i in range(10, 20)]
        ),
    }
    compare_dicts(metadata, expected)
    assert set(eigsep_redis.data_streams.keys()) == set(
        [b"acc_cnt", b"update_date"]
    )

    # test reset
    eigsep_redis.reset()
    assert eigsep_redis.data_streams == {}


def test_raw(eigsep_redis):
    # one integration from snap
    data = generate_data(ntimes=1, raw=True, reshape=False)
    data_back = {}
    for p, d in data.items():
        eigsep_redis.add_raw(f"data:{p}", d)
        data_back[p] = eigsep_redis.get_raw(f"data:{p}")
    compare_dicts(data, data_back)
