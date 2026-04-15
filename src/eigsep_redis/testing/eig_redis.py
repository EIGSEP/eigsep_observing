from ..eig_redis import EigsepRedis
from .transport import DummyTransport


class DummyEigsepRedis(EigsepRedis):
    transport_cls = DummyTransport
