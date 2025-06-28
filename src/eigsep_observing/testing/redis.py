from collections import defaultdict
import json
import fakeredis
from .. import EigsepRedis


class DummyEigsepRedis(EigsepRedis):
    def __init__(self, host="localhost", port=6379):
        self.r = fakeredis.FakeRedis(decode_responses=False)
        self._last_read_ids = defaultdict(lambda: "$")

    def send_vna_data(self, data, cal_data=None, header=None, metadata=None):
        """
        Dummy implementation of send_vna_data for testing.

        Simply adds the stream to data_streams without raising
        NotImplementedError.
        """
        self.r.sadd("data_streams", "stream:vna")

    def read_ctrl(self):
        """
        Non-blocking version of read_ctrl for testing.

        Uses block=1 (1ms timeout) instead of block=0 (indefinite blocking)
        to prevent tests from hanging.
        """
        # Use minimal timeout instead of indefinite blocking
        msg = self.r.xread(self.ctrl_stream, count=1, block=1)
        if not msg or not msg[0][1]:
            raise TypeError("No message available")
        # msg is stream_name, entries
        entries = msg[0][1]
        entry_id, dat = entries[0]  # since count=1, it's a list of 1
        # update stream id
        self._last_read_ids["stream:ctrl"] = (
            entry_id  # update the stream id
        )
        # dat is a dict with key msg
        raw = dat.get(b"msg")
        decoded = json.loads(raw)
        # msg is a dict with keys cmd and kwargs
        cmd = decoded.get("cmd")
        kwargs = decoded.get("kwargs", {})
        return (cmd, kwargs)
