from eigsep_redis.testing import DummyTransport

from ..eig_redis import EigsepObsRedis


class DummyEigsepObsRedis(EigsepObsRedis):
    transport_cls = DummyTransport
