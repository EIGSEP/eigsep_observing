from eigsep_corr.testing import DummyEigsepFpga as CorrDummyEigsepFpga

from .. import EigsepFpga
from .eig_redis import DummyEigsepRedis


class DummyEigsepFpga(EigsepFpga, CorrDummyEigsepFpga):
    """
    DummyEigsepFpga class that inherits from eigsep_observing.EigsepFpga
    and eigsep_corr.DummyEigsepFpga.

    Replaces the plain FakeRedis from CorrDummyEigsepFpga with a
    DummyEigsepRedis so that EigsepRedis-specific methods are
    available.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        redis_cfg = self.cfg.get("redis", {})
        host = redis_cfg.get("host", "localhost")
        port = redis_cfg.get("port", 6379)
        self.redis = DummyEigsepRedis(host=host, port=port)
