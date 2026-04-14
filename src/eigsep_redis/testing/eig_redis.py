import fakeredis

from ..eig_redis import EigsepRedis


class DummyEigsepRedis(EigsepRedis):
    def _make_redis(self, *args, **kwargs):
        """
        Create a fake Redis instance for testing purposes. Overrides
        the parent class method.
        """
        return fakeredis.FakeRedis(decode_responses=False)
