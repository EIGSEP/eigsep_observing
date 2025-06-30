from collections import defaultdict

import fakeredis

from .. import EigsepRedis


class DummyEigsepRedis(EigsepRedis):
    def __init__(self, host="localhost", port=6379):
        self.r = fakeredis.FakeRedis(decode_responses=False)
        self._last_read_ids = defaultdict(lambda: "$")
        # Enable all command types for testing
        self.r.sadd("ctrl_commands", "ctrl", "switch", "VNA")

    def send_vna_data(self, data, cal_data=None, header=None, metadata=None):
        """
        Dummy implementation of send_vna_data for testing.

        Simply adds the stream to data_streams without raising
        NotImplementedError.
        """
        self.r.sadd("data_streams", "stream:vna")
