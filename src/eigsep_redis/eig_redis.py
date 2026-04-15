import logging
import warnings

from .config import ConfigStore
from .heartbeat import HeartbeatReader, HeartbeatWriter
from .metadata import (
    MetadataSnapshotReader,
    MetadataStreamReader,
    MetadataWriter,
)
from .status import StatusReader, StatusWriter
from .transport import Transport

logger = logging.getLogger(__name__)


class EigsepRedis:
    transport_cls = Transport

    def __init__(self, host="localhost", port=6379):
        """
        Initialize the EigsepRedis client.

        Parameters
        ----------
        host : str
        port : int

        """
        self.transport = self.transport_cls(host, port)
        self.metadata = MetadataWriter(self.transport)
        self.metadata_snapshot = MetadataSnapshotReader(self.transport)
        self.metadata_stream = MetadataStreamReader(self.transport)
        self.status = StatusWriter(self.transport)
        self.status_reader = StatusReader(self.transport)
        self.heartbeat = HeartbeatWriter(self.transport)
        self.heartbeat_reader = HeartbeatReader(self.transport)
        self.config = ConfigStore(self.transport)

    # ---- forwarded attributes (state lives on the transport) ----

    @property
    def r(self):
        return self.transport.r

    @property
    def logger(self):
        return self.transport.logger

    @property
    def host(self):
        return self.transport.host

    @property
    def port(self):
        return self.transport.port

    @property
    def _stream_lock(self):
        return self.transport._stream_lock

    @property
    def _last_read_ids(self):
        return self.transport._last_read_ids

    # ---- thin forwarders for transport helpers ----

    def _get_last_read_id(self, stream):
        return self.transport._get_last_read_id(stream)

    def _set_last_read_id(self, stream, read_id):
        return self.transport._set_last_read_id(stream, read_id)

    def _streams_from_set(self, set_name):
        return self.transport._streams_from_set(set_name)

    def reset(self):
        return self.transport.reset()

    def add_raw(self, key, value, ex=None):
        return self.transport.add_raw(key, value, ex=ex)

    def get_raw(self, key):
        return self.transport.get_raw(key)

    def _upload_dict(self, d, key):
        return self.transport._upload_dict(d, key)

    def is_connected(self):
        return self.transport.is_connected()

    def close(self):
        return self.transport.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ---- stream indices (property view into Redis sets) ----

    @property
    def data_streams(self):
        """
        Dictionary of all data streams (metadata, corr, vna, ...). The
        keys are the stream names and the values are the last entry id
        read from the stream. If no entry has been read, the value is
        '$', indicating that the read starts from the newest message
        delivered by the stream.

        Returns
        -------
        d : dict

        """
        return self._streams_from_set("data_streams")

    # ------------------- metadata -----------------

    def add_metadata(self, key, value):
        """
        Deprecated shim for picohost only. Prefer ``self.metadata.add``.

        TODO(monorepo): delete once picohost joins the monorepo and its
        producers have migrated to ``MetadataWriter.add``.
        """
        warnings.warn(
            "EigsepRedis.add_metadata is deprecated; use "
            "redis.metadata.add(key, value). This shim will be removed "
            "once picohost joins the monorepo.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.metadata.add(key, value)
