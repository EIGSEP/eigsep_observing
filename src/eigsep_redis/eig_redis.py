from datetime import datetime, timezone
import json
import logging
import threading
import yaml

import redis
import redis.exceptions

logger = logging.getLogger(__name__)


class EigsepRedis:
    maxlen = {"status": 5, "data": 5000}

    def __init__(self, host="localhost", port=6379):
        """
        Initialize the EigsepRedis client.

        Parameters
        ----------
        host : str
        port : int

        """
        self.logger = logger
        self._stream_lock = threading.RLock()
        self._last_read_ids = {}
        self._last_read_ids["stream:status"] = "$"
        self.logger.info(f"{self._last_read_ids=}")
        self.r = self._make_redis(host, port)
        self.host = host
        self.port = port

    def _make_redis(self, host, port):
        """
        Create a Redis connection with error handling.

        Parameters
        ----------
        host : str
        port : int

        Returns
        -------
        r : redis.Redis
            Redis client instance

        Raises
        ------
        redis.exceptions.ConnectionError
            If connection to Redis fails

        """
        try:
            r = redis.Redis(
                host=host,
                port=port,
                decode_responses=False,
                socket_timeout=None,
                socket_connect_timeout=None,
                retry_on_timeout=False,
            )
            # Test connection
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
        """
        Thread-safe getter for last read ID.

        Parameters
        ----------
        stream : str
            Stream name

        Returns
        -------
        str
            Last read ID for the stream
        """
        with self._stream_lock:
            try:
                last = self._last_read_ids[stream]
            except KeyError:
                last = self.r.xinfo_stream(stream)["last-generated-id"]
            return last

    def _set_last_read_id(self, stream, read_id):
        """
        Thread-safe setter for last read ID.

        Parameters
        ----------
        stream : str
            Stream name
        read_id : str
            New read ID
        """
        with self._stream_lock:
            self._last_read_ids[stream] = read_id

    def reset(self):
        """
        Reset the EigsepRedis client by clearing all data streams and
        resetting the last read ids.

        """
        self.r.flushdb()
        with self._stream_lock:
            self._last_read_ids.clear()
            self._last_read_ids["stream:status"] = "$"

    def _streams_from_set(self, set_name):
        """
        Build a ``{stream_name: last_read_id}`` dict from a Redis set of
        stream names. If no entry has been read, the value is '$',
        indicating the read should start from the newest message.
        """
        members = self.r.smembers(set_name)
        with self._stream_lock:
            d = {}
            for s in members:
                try:
                    last = self._last_read_ids[s.decode()]
                except KeyError:
                    try:
                        last = self.r.xinfo_stream(s.decode())[
                            "last-generated-id"
                        ]
                    except KeyError:
                        # default to "$" for newly created streams
                        last = "$"
                d[s.decode()] = last
            return d

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

    @property
    def metadata_streams(self):
        """
        Dictionary of metadata streams only (i.e. streams registered by
        ``add_metadata``, excluding corr/vna raw-data streams). Same
        shape as ``data_streams``.

        Returns
        -------
        d : dict

        """
        return self._streams_from_set("metadata_streams")

    @property
    def status_stream(self):
        return {"stream:status": self._get_last_read_id("stream:status")}

    # ------------------- configs -------------------

    def add_raw(self, key, value, ex=None):
        """
        Update redis database with raw data in bytes.

        Parameters
        ----------
        key : str
            Data key.
        value : bytes
            Data value.
        ex : int
            Optional expiration time in seconds. If provided, the key will
            expire after this time.

        """
        return self.r.set(key, value, ex=ex)

    def get_raw(self, key):
        """
        Get raw bytes from Redis.

        Parameters
        ----------
        key : str
            Data key.

        """
        return self.r.get(key)

    def _upload_dict(self, d, key):
        """
        Helper function for uploading dictionaries to Redis.

        Parameters
        ----------
        d : dict
        key : str
            Redis key under which the configuration will be stored.

        """
        d = d.copy()
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        d["upload_time"] = now
        self.add_raw(key, json.dumps(d).encode("utf-8"))

    def upload_config(self, config, from_file=True):
        """
        Upload the Eigsep configuration to Redis.

        Parameters
        ----------
        config : str or dict
            Path to the configuration file if `from_file` is True, or a
            dictionary containing the configuration data if `from_file`
            is False.
        from_file : bool

        """
        if from_file:
            with open(config, "r") as f:
                config = yaml.safe_load(f)
        self._upload_dict(config, "config")

    def get_config(self):
        """
        Get the configuration file from Redis. This is used to retrieve
        the yaml configuration file for the Eigsep system.

        Returns
        -------
        config : dict
            Dictionary containing the configuration data.

        Raises
        ------
        ValueError
            If no configuration is found in Redis.

        """
        raw = self.get_raw("config")
        if raw is None:
            raise ValueError("No configuration found in Redis.")
        return json.loads(raw)

    # ------------------- metadata -----------------

    def add_metadata(self, key, value):
        """
        Add metadata to Redis. This both streams values and adds the current
        value to a metadata hash.

        Parameters
        ----------
        key : str
            Metadata key.
        value : JSON serializable object
            Metadata value.

        Raises
        ------
        TypeError
            If key is not a string.
        ValueError
            If key is empty or contains invalid characters.
        """
        # Validate inputs
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if not key or not key.strip():
            raise ValueError("key cannot be empty or whitespace only")
        if ":" in key:
            raise ValueError(
                "key cannot contain ':' character (reserved for Redis)"
            )

        try:
            payload = json.dumps(value).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise ValueError(f"value is not JSON serializable: {e}")

        # hash (for live updates)
        self.r.hset("metadata", key, payload)
        ts = datetime.now(timezone.utc).isoformat()  # XXX unreliable
        self.r.hset("metadata", f"{key}_ts", json.dumps(ts).encode("utf-8"))
        # stream (for file metadata)
        self.r.xadd(
            f"stream:{key}",
            {"value": payload},
            maxlen=self.maxlen["data"],
            approximate=True,
        )
        # add the stream to both the general data streams set and the
        # metadata-only set (the latter is what get_metadata() defaults
        # to, so raw-data streams like stream:corr / stream:vna never
        # end up there)
        self.r.sadd("data_streams", f"stream:{key}")
        self.r.sadd("metadata_streams", f"stream:{key}")

    def get_live_metadata(self, keys=None):
        """
        Get live metadata from Redis, i.e. the current values stored
        in the metadata hash.

        This is the **snapshot** path. It is used by ``PandaClient``
        when packaging VNA measurements (client.py): a VNA reading
        is point-in-time, taken at ~1/hour cadence, and the right
        metadata semantics are "what was the latest sensor reading
        at the moment the VNA was triggered." Use this for any
        consumer that wants a current snapshot rather than a
        cadence-matched average. **Do not use this for the corr
        loop** — corr metadata is averaged over the integration via
        ``get_metadata`` instead (see that method's docstring).

        Caveat: ``get_live_metadata`` reads the latest hash values
        with no freshness check. If a sensor stopped updating an
        hour ago, you silently get the stale value. The VNA file
        header includes ``metadata_snapshot_unix`` (set by
        PandaClient) to let downstream detect this at inspection
        time, but there is no runtime warning today.

        Parameters
        ----------
        keys : str or list of str
            Metadata key(s). If None, return all metadata.

        Returns
        -------
        m : dict or any
            Dictionary of metadata. If keys is None, return all metadata.
            If keys is a string, return the value for that key.
            If keys is a list, return a dictionary with the requested keys.

        Raises
        ------
        TypeError
            If keys is not a string or a list of strings.

        """
        if keys is not None and not isinstance(keys, (str, list)):
            raise TypeError("Keys must be a string or a list of strings.")
        if isinstance(keys, list) and not all(
            isinstance(k, str) for k in keys
        ):
            raise TypeError("All keys in the list must be strings.")
        m = {}
        metadata = self.r.hgetall("metadata")
        for k, v in metadata.items():
            m[k.decode("utf-8")] = json.loads(v)
        if keys is None:
            return m
        elif isinstance(keys, str):
            return m[keys]
        else:
            filtered_m = {}
            for k in keys:
                filtered_m[k] = m[k]
            return filtered_m

    def get_metadata(self, stream_keys=None):
        """
        Get all metadata from redis stream for file writing.

        This is the **streaming** path. It is used by
        ``EigObserver.record_corr_data`` per integration: corr
        spectra are an *integration* over a sub-second window, and
        the right metadata semantics are "average all sensor
        readings that happened during this integration." Each call
        drains the stream since the last call (advances a per-stream
        position pointer) and returns a list of dicts that the
        caller averages down to one entry per spectrum via
        ``io.avg_metadata``. **Do not use this for VNA** — VNA wants
        a point-in-time snapshot, not a drain since the previous
        VNA an hour ago (see ``get_live_metadata``).

        Because ``get_metadata`` advances the position pointer,
        only one consumer per ``EigsepRedis`` instance can call it
        per stream — otherwise consumers race for stream entries.
        Today only the corr loop calls it.

        Parameters
        ----------
        stream_keys : str or list of str
            Redis stream key. If a list, return the requested streams.
            If None, return all registered metadata streams.

        Returns
        -------
        redis_hdr : dict
            Dictionary of stream data. Each key is a stream name, and the
            value is a list of data values.

        Notes
        -----
        This grabs updated metadata from the Redis streams. If a stream
        has not been updated, it will not be included in the output.

        With ``stream_keys=None`` this reads only streams registered
        via ``add_metadata`` (tracked in the Redis ``metadata_streams``
        set), so raw-data streams like ``stream:corr`` / ``stream:vna``
        — whose payloads are not JSON — are excluded by construction.

        """
        if stream_keys is None:
            streams = self.metadata_streams
        else:
            if isinstance(stream_keys, str):
                stream_keys = [stream_keys]
            streams = {
                k: self.data_streams[k]
                for k in stream_keys
                if k in self.data_streams
            }

        # non-blocking read: correlator loop runs at ~4 Hz, so we
        # must not stall here. Picos push at 200 ms, so data will
        # accumulate between calls and be averaged by the caller.
        # Note: block=None (omit) = return immediately; block=0 =
        # block forever.
        redis_hdr = {}
        if not streams:  # no streams to read
            return redis_hdr
        resp = self.r.xread(streams)
        for stream, dat in resp:
            stream = stream.decode()  # decode stream name
            out = []
            # stream is a list of tuples (id, data)
            for eid, d in dat:
                value = json.loads(d[b"value"])
                out.append(value)
                # update the stream id
                self._set_last_read_id(stream, eid)
            redis_hdr[stream] = out
        return redis_hdr

    # ------------------- heartbeat and status -----------------

    def client_heartbeat_set(self, ex=None, alive=True):
        """
        Set the client heartbeat key in Redis. This is used to keep
        track of whether the client is alive or not.

        Parameters
        ----------
        ex : int
            Expiration time in seconds.
        alive : bool
            If True, set the key to 1, otherwise set it to 0 (not alive).

        """
        self.add_raw("heartbeat:client", int(alive), ex=ex)

    def client_heartbeat_check(self):
        """
        Check if client is alive by checking the heartbeat.

        Returns
        -------
        alive : bool
            True if client is alive, False otherwise.

        """
        raw = self.get_raw("heartbeat:client")
        if raw is None:
            return False
        return int(raw) == 1

    def send_status(self, level=logging.INFO, status=None):
        """
        Publish status message to Redis. Used by client.

        Parameters
        ----------
        level : int
            Log level.
        status : str
            Status message.

        """
        self.r.xadd(
            "stream:status",
            {"level": level, "status": status},
            maxlen=self.maxlen["status"],
        )

    def read_status(self, timeout=None):
        """
        Read status stream from Redis. Used by server.

        Parameters
        ----------
        timeout : int, optional
            Timeout in seconds for blocking read. If None, blocks indefinitely.

        Returns
        -------
        level : int
            Log level of the status message.
        status : str
            Status message. If None, no message was received.

        """
        # blocking read with optional timeout
        block_time = 0 if timeout is None else int(timeout * 1000)
        msg = self.r.xread(self.status_stream, count=1, block=block_time)
        if not msg:
            return None, None
        entry_id, status_dict = msg[0][1][0]
        self._set_last_read_id(
            "stream:status", entry_id
        )  # update the stream id
        status = status_dict.get(b"status").decode("utf-8")
        raw_level = status_dict.get(b"level")
        if raw_level is None:
            level = logging.INFO  # default to info
        else:
            level = int(raw_level.decode("utf-8"))
        return level, status

    def is_connected(self):
        """
        Check if Redis connection is active.

        Returns
        -------
        bool
            True if connected, False otherwise
        """
        try:
            return self.r.ping()
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ):
            return False

    def close(self):
        """
        Close the Redis connection and clean up resources.
        """
        try:
            if hasattr(self.r, "close"):
                self.r.close()
            self.logger.info("Redis connection closed")
        except Exception as e:
            self.logger.warning(f"Error closing Redis connection: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
