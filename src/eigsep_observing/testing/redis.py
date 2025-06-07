import fakeredis

from eigsep_observing import EigsepRedis


class DummyEigsepRedis(EigsepRedis):
    def __init__(self, redis=None, maxlen=600):
        if redis is None:
            redis = fakeredis.FakeRedis()
        self.r = redis
        self.maxlen = maxlen
        self.ctrl_streams = {
            "stream:status": "0-0",
            "stream:ctrl": "0-0",
        }
