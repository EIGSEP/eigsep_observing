import logging
import warnings

from .metadata import MetadataWriter
from .transport import Transport

logger = logging.getLogger(__name__)


class EigsepRedis:
    """Picohost-compatibility shim hosting the deprecated ``add_metadata``.

    In-tree consumers build their own per-bus surfaces from a
    :class:`Transport` directly. This class exists solely so picohost
    (still out-of-tree) can keep calling ``redis.add_metadata(...)``
    until it migrates to ``MetadataWriter.add``. It will be retired in
    a follow-up PR once that migration lands — see
    ``TODO(monorepo)`` below.
    """

    transport_cls = Transport

    def __init__(self, host="localhost", port=6379, transport=None):
        if transport is None:
            transport = self.transport_cls(host, port)
        self.transport = transport
        self.metadata = MetadataWriter(self.transport)

    @property
    def r(self):
        """Raw redis client. Exposed so picohost's ``PicoManager._redis``
        can extract the underlying client without depending on internal
        ``.transport`` structure."""
        return self.transport.r

    def add_metadata(self, key, value):
        """Deprecated shim. Use ``redis.metadata.add(key, value)``.

        TODO(monorepo): delete this class once picohost migrates to
        ``MetadataWriter.add``.
        """
        warnings.warn(
            "EigsepRedis.add_metadata is deprecated; use "
            "redis.metadata.add(key, value). This shim will be removed "
            "once picohost joins the monorepo.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.metadata.add(key, value)
