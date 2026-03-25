import fakeredis

from .. import EigsepRedis


class DummyEigsepRedis(EigsepRedis):
    def _make_redis(self, *args, **kwargs):
        """
        Create a fake Redis instance for testing purposes. Overrides
        the parent class method.
        """
        r = fakeredis.FakeRedis(decode_responses=False)
        return r

    def send_vna_data(self, data, cal_data=None, header=None, metadata=None):
        """
        Dummy implementation of send_vna_data for testing.

        Simply adds the stream to data_streams without raising
        NotImplementedError.
        """
        self.r.sadd("data_streams", "stream:vna")
