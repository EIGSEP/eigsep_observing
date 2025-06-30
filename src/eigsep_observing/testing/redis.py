from collections import defaultdict

import fakeredis

from .. import EigsepRedis


class DummyEigsepRedis(EigsepRedis):
    def __init__(self, host="localhost", port=6379):
        # Initialize parent class attributes manually for testing
        import logging
        import threading

        self.logger = logging.getLogger(__name__)
        self.retry_on_timeout = True
        self._stream_lock = threading.RLock()
        self._last_read_ids = defaultdict(lambda: "$")

        # Use fakeredis instead of real Redis
        self.r = fakeredis.FakeRedis(decode_responses=False)
        # Enable all command types for testing
        self.r.sadd("ctrl_commands", "ctrl", "switch", "VNA")

    def send_vna_data(self, data, cal_data=None, header=None, metadata=None):
        """
        Dummy implementation of send_vna_data for testing.

        Simply adds the stream to data_streams without raising
        NotImplementedError.
        """
        self.r.sadd("data_streams", "stream:vna")
