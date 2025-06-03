from datetime import datetime, UTC
import numpy as np
import pytest

import fakeredis
from eigsep_observing import EigsepRedis

from .utils import compare_dicts


class DummyEigsepRedis(EigsepRedis):
    def __init__(self, host="localhost", port=6379, maxlen=600):
        self.r = fakeredis.FakeRedis(decode_responses=True)
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
            assert eigsep_redis.data_streams == {"acc_cnt": "0-0"}
        # live metadata should be updated
        assert eigsep_redis.get_live_metadata(key="acc_cnt") == acc_cnt
        live = eigsep_redis.get_live_metadata()
        # can't expect the exact timestamp
        assert "acc_cnt_ts" in live
        ts = live.pop("acc_cnt_ts")
        assert ts.startswith(today)
        compare_dicts(live, {"acc_cnt": acc_cnt})
    # read the stream
    metadata = eigsep_redis.get_metadata(stream_key="acc_cnt")
    compare_dicts(metadata, {"acc_cnt": np.arange(10)})
    # after reading, the stream should be empty
    assert eigsep_redis.get_metadata(stream_key="acc_cnt") == {}
    # live metadata should still be available
    assert eigsep_redis.get_live_metadata(key="acc_cnt") == 9
    # multiple streams, mix of int and str
    for acc_cnt in range(10, 20):
        eigsep_redis.add_metadata("acc_cnt", acc_cnt)
        test_date = f"2025-06-02T16:25:{acc_cnt}.089640"
        eigsep_redis.add_metadata("update_date", test_date)
        live = eigsep_redis.get_live_metadata()
        assert "acc_cnt_ts" in live
        ts = live.pop("acc_cnt_ts")
        assert ts.startswith(today)
        assert "update_date_ts" in live
        ts = live.pop("update_date_ts")
        assert ts.startswith(today)
        compare_dicts({"acc_cnt": acc_cnt, "update_date": test_date}, live)
    # read the streams
    metadata = eigsep_redis.get_metadata()
    expected = {
        "acc_cnt": np.arange(10, 20),
        "update_date": np.array(
            [f"2025-06-02T16:25:{i}.089640" for i in range(10, 20)]
        ),
    }
    compare_dicts(metadata, expected)
