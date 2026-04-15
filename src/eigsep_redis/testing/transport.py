import fakeredis

from ..transport import Transport


class DummyTransport(Transport):
    """In-process ``Transport`` backed by fakeredis for tests."""

    def _make_redis(self, host, port):
        return fakeredis.FakeRedis(decode_responses=False)
