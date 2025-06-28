from collections import defaultdict
import fakeredis
from .. import EigsepRedis


class DummyEigsepRedis(EigsepRedis):
    def __init__(self, host="localhost", port=6379):
        self.r = fakeredis.FakeRedis(decode_responses=False)
        self._last_read_ids = defaultdict(lambda: "$")
