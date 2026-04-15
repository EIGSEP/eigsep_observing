from datetime import datetime, timezone
import json
import logging
import threading

import redis
import redis.exceptions

logger = logging.getLogger(__name__)


class Transport:
    """
    Shared Redis transport: connection, last-read-id bookkeeping,
    raw K/V, and lifecycle.

    Owns nothing bus-specific. Writer and reader classes are
    constructed with a ``Transport`` and share the connection and
    last-read-id state through it. Subclass and override
    ``_make_redis`` to swap the underlying client (e.g. fakeredis
    for testing).
    """

    def __init__(self, host="localhost", port=6379):
        self.logger = logger
        self.host = host
        self.port = port
        self._stream_lock = threading.RLock()
        self._last_read_ids = {}
        self.r = self._make_redis(host, port)

    def _make_redis(self, host, port):
        try:
            r = redis.Redis(
                host=host,
                port=port,
                decode_responses=False,
                socket_timeout=None,
                socket_connect_timeout=None,
                retry_on_timeout=False,
            )
            r.ping()
            self.logger.info(f"Connected to Redis at {host}:{port}")
        except redis.exceptions.ConnectionError as e:
            self.logger.error(
                f"Failed to connect to Redis at {host}:{port}: {e}"
            )
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to Redis: {e}")
            raise
        return r

    def _get_last_read_id(self, stream):
        with self._stream_lock:
            if stream in self._last_read_ids:
                return self._last_read_ids[stream]
            try:
                return self.r.xinfo_stream(stream)["last-generated-id"]
            except redis.exceptions.ResponseError:
                return "$"

    def _set_last_read_id(self, stream, read_id):
        with self._stream_lock:
            self._last_read_ids[stream] = read_id

    def _streams_from_set(self, set_name):
        """
        Build a ``{stream_name: last_read_id}`` dict from a Redis set
        of stream names. Missing entries default to the last generated
        ID if available, otherwise falls back to '$' (newest after read).
        """
        members = self.r.smembers(set_name)
        with self._stream_lock:
            d = {}
            for s in members:
                key = s.decode()
                if key in self._last_read_ids:
                    d[key] = self._last_read_ids[key]
                    continue
                try:
                    d[key] = self.r.xinfo_stream(key)["last-generated-id"]
                except redis.exceptions.ResponseError:
                    d[key] = "$"
            return d

    def reset(self):
        """Flush the whole Redis DB and reset last-read-id state."""
        self.r.flushdb()
        with self._stream_lock:
            self._last_read_ids.clear()

    def add_raw(self, key, value, ex=None):
        return self.r.set(key, value, ex=ex)

    def get_raw(self, key):
        return self.r.get(key)

    def _upload_dict(self, d, key):
        """Serialize ``d`` as JSON (with ``upload_time`` injected) under ``key``."""
        d = d.copy()
        d["upload_time"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        )
        self.add_raw(key, json.dumps(d).encode("utf-8"))

    def is_connected(self):
        try:
            return self.r.ping()
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ):
            return False

    def close(self):
        try:
            if hasattr(self.r, "close"):
                self.r.close()
            self.logger.info("Redis connection closed")
        except Exception as e:
            self.logger.warning(f"Error closing Redis connection: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
