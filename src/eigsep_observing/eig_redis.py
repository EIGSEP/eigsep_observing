import logging

from eigsep_redis import EigsepRedis

from .corr import CorrConfigStore, CorrReader, CorrWriter
from .vna import VnaReader, VnaWriter

logger = logging.getLogger(__name__)


class EigsepObsRedis(EigsepRedis):
    """
    Observing-side Redis client. Adds correlator and VNA data paths on
    top of the generic ``eigsep_redis.EigsepRedis`` bus primitives.

    The base class (``EigsepRedis``) provides the shared transport,
    metadata, status, heartbeat, and config surfaces — everything
    picohost and other external producers need. The observing-specific
    attributes added here are:

    - ``corr_config`` — :class:`CorrConfigStore` (upload/get SNAP
      config and corr header).
    - ``corr`` — :class:`CorrWriter`; producer-side corr stream.
    - ``corr_reader`` — :class:`CorrReader`; consumer-side corr stream.
    - ``vna`` — :class:`VnaWriter`; producer-side VNA stream.
    - ``vna_reader`` — :class:`VnaReader`; consumer-side VNA stream.
    """

    def __init__(self, host="localhost", port=6379):
        super().__init__(host=host, port=port)
        self.corr_config = CorrConfigStore(self.transport)
        self.corr = CorrWriter(self.transport)
        self.corr_reader = CorrReader(self.transport)
        self.vna = VnaWriter(self.transport)
        self.vna_reader = VnaReader(self.transport)
