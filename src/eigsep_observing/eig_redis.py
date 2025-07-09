from collections import defaultdict
from datetime import datetime, timezone
import json
import logging
import numpy as np
import threading

from eigsep_corr.config import load_config
import redis
import redis.exceptions

logger = logging.getLogger(__name__)


class EigsepRedis:

    maxlen = {"ctrl": 10, "status": 10, "data": 10000}
    ctrl_stream_name = "stream:ctrl"

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
        self._last_read_ids = defaultdict(lambda: "$")
        self.r = self._make_redis(host, port)

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
            return self._last_read_ids[stream]

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
            self._last_read_ids = defaultdict(lambda: "$")

    @property
    def data_streams(self):
        """
        Dictionary of data streams. The keys are the stream names and
        the values are the last entry id read from the stream. If no
        entry has been read, the value is '$', indicating that the read
        start from newest message delivered by the stream.

        Returns
        -------
        d : dict

        """
        members = self.r.smembers("data_streams")
        with self._stream_lock:
            return {
                s.decode(): self._last_read_ids[s.decode()] for s in members
            }

    @property
    def ctrl_stream(self):
        key = self.ctrl_stream_name
        return {key: self._get_last_read_id(key)}

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
        d["upload_time"] = datetime.now(timezone.utc).isoformat()
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
            config = load_config(config, compute_inttime=False)
        self._upload_dict(config, "config")

    def upload_corr_config(self, config, from_file=False):
        """
        Upload the SNAP configuration to Redis. This is the
        configuration parameters used for programming the SNAP.

        Parameters
        ----------
        config : str or dict
            Path to the configuration file if `from_file` is True, or a
            dictionary containing the configuration data if `from_file`
            is False.
        from_file : bool

        """
        if from_file:
            config = load_config(config, compute_inttime=True)
        self._upload_dict(config, "corr_config")

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

    def get_corr_config(self):
        """
        Get the SNAP configuration file from Redis. These are the
        configuration parameters used for programming the SNAP.

        Returns
        -------
        config : dict
            Dictionary containing the SNAP configuration data.

        Raises
        ------
        ValueError
            If no configuration is found in Redis.

        """
        raw = self.get_raw("corr_config")
        if raw is None:
            raise ValueError("No SNAP configuration found in Redis.")
        return json.loads(raw)

    def upload_corr_header(self, header):
        """
        Upload correlation data header, from `fpga.header` attribute.

        Parameters
        ----------
        header : dict

        """
        self._upload_dict(header, "corr_header")

    def get_corr_header(self):
        """
        Get the correlation data header from Redis.

        Returns
        -------
        header : dict
            Dictionary containing the correlation data header.

        Raises
        ------
        ValueError
            If no correlation header is found in Redis.

        """
        raw = self.get_raw("corr_header")
        if raw is None:
            raise ValueError("No correlation header found in Redis.")
        return json.loads(raw)

    # ---------- correlation data and s11 measurements ----------

    def add_corr_data(self, data, cnt, dtype=">i4"):
        """
        Upload raw correlation data to Redis.

        Parameters
        ----------
        data : dict
            Dictionary holding correlation data. Keys are correlation
            pairs and values are bytes. See the `read_data` method of
            EigsepFpga in eigsep_corr.fpga for the expected format.
        cnt : int
            Accumulation count, read from SNAP.
        dtype : str
            Data type of the correlation data. Default is '>i4'
            (big-endian 32-bit integer). This is used for unpacking the
            data on the consumer side.

        Raises
        ------
        TypeError
            If data is not a dictionary or cnt is not an integer.
        ValueError
            If data is empty or contains invalid correlation pairs.
        """
        # Validate inputs
        if not isinstance(data, dict):
            raise TypeError("data must be a dictionary")
        if not isinstance(cnt, int):
            raise TypeError("cnt must be an integer")
        if not data:
            raise ValueError("data dictionary cannot be empty")
        if cnt < 0:
            raise ValueError("cnt must be non-negative")

        # Validate correlation pairs and data
        for pair, d in data.items():
            if not isinstance(pair, str):
                raise TypeError(
                    f"Correlation pair key must be string, got {type(pair)}"
                )
            if not isinstance(d, (bytes, bytearray)):
                pair_type = type(d)
                raise TypeError(
                    f"Correlation data must be bytes, got {pair_type} "
                    f"for pair {pair}"
                )

        redis_data = {p.encode("utf-8"): d for p, d in data.items()}
        # add pairs to the set of correlation pairs
        self.r.sadd("corr_pairs", *redis_data.keys())
        # add acc_cnt and dtype to the dict
        redis_data["acc_cnt"] = str(cnt).encode("utf-8")
        redis_data["dtype"] = dtype.encode("utf-8")
        # add the data to the stream
        self.r.xadd(
            "stream:corr",
            redis_data,
            maxlen=self.maxlen["data"],
            approximate=True,
        )
        self.r.sadd("data_streams", "stream:corr")

    def read_corr_data(self, pairs=None, timeout=10, unpack=True):
        """
        Read raw correlation data from Redis and optionally unpack
        from bytes. This is a blocking read, so it will wait until
        data is available.

        Parameters
        ----------
        pairs : list of str
            List of correlation pairs to read. If None, read all pairs.
        timeout : int
            Timeout in seconds for blocking read.
        unpack : bool
            If True, unpack the data from bytes to numpy arrays.
            If False, return the raw bytes.

        Returns
        -------
        acc_cnt : int
            Accumulation count, read from the correlation data.
        sync_time : float
            Synchronization time, when `acc_cnt` is 0.
        data : dict
            If `unpack` is True, return a dictionary with keys as
            correlation pairs and values as numpy arrays of complex
            numbers. If `unpack` is False, values are the raw bytes.

        Raises
        ------
        TimeoutError
            If no data is received within the timeout period.

        Notes
        -----
        The length of the data arrays is the product of the 'nchan' and
        'acc_bins' parameters in the SNAP configuration. The 'nchan'
        parameter is the number of frequency channels, while 'acc_bins'
        is the number of spectra read at each integration step.

        """
        if pairs is None:
            pairs = self.r.smembers("corr_pairs")
        last_id = self.data_streams["stream:corr"]
        out = self.r.xread(
            {"stream:corr": last_id},
            count=1,
            block=int(timeout * 1000),
        )
        if not out:
            raise TimeoutError("No correlation data received within timeout.")
        eid, fields = out[0][1][0]
        self._set_last_read_id("stream:corr", eid)  # update last read id
        acc_cnt = int(fields.pop(b"acc_cnt").decode())
        dtype = fields.pop(b"dtype").decode()
        data = {}
        for k, v in fields.items():
            if unpack:
                # unpack the bytes to numpy array of complex numbers
                arr = np.frombuffer(v, dtype=dtype)
            else:
                # return raw bytes
                arr = v
            data[k.decode()] = arr
        sync_time = self.get_live_metadata(keys="corr_sync_time")[
            "sync_time_unix"
        ]
        return acc_cnt, sync_time, data

    def send_vna_data(self, data, cal_data=None, header=None, metadata=None):
        """
        Send VNA data to Redis using a stream. Used by client.

        Parameters
        ----------
        data : dict
            Dictionary holding VNA data. Keys are measurement modes and
            values are arrays of complex numbers.
        cal_data : dict
            Dictionary holding calibration data. Keys are calibration
            modes and values are arrays of complex numbers.
        header : dict
            VNA configuration. To be placed in file header.
        metadata : dict
            Live sensor metadata. To be placed in file header.
        """
        self.r.sadd("data_streams", "stream:vna")
        raise NotImplementedError

    def read_vna_data(self, timeout=0):
        """
        Blocking read of VNA data stream from Redis. Used by server.

        Parameters
        ----------
        timeout : int
            Timeout in seconds for blocking read. Set to 0 to block
            indefinitely.

        Returns
        -------
        data : dict
            Dictionary holding VNA data. Keys are measurement modes and
            values are arrays of complex numbers. If None, no message was
            received.
        cal_data : dict
            Dictionary holding calibration data. Keys are calibration
            modes and values are arrays of complex numbers.
        header : dict
            VNA configuration. To be placed in file header.
        metadata : dict
            Live sensor metadata. To be placed in file header.

        Raises
        ------
        TimeoutError
            If no data is received within the timeout period.

        Notes
        -----
        This is a blocking read with a timeout of ``timeout`` seconds.

        """
        raise NotImplementedError("This method is not implemented yet.")

    # ------------------- metadata -----------------

    def add_metadata(self, key, value):
        """
        Add metadata to Redis. This both streams values and adds the current
        value to a metadata hash.

        Parameters
        ----------
        key : str
            Metadata key.
        value : bytes or JSON serializable object
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
            if isinstance(value, (bytes, bytearray)):
                payload = value
            else:
                payload = json.dumps(value).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise ValueError(f"value is not JSON serializable: {e}")

        # hash (for live updates)
        self.r.hset("metadata", key, payload)
        ts = datetime.now(timezone.utc).isoformat()  # XXX unreliable
        self.r.hset("metadata", f"{key}_ts", json.dumps(ts).encode("utf-8"))
        # stream (for file metadata)
        self.r.xadd(
            key,
            {"value": payload},
            maxlen=self.maxlen["data"],
            approximate=True,
        )
        # add the stream to the data streams if not already present
        self.r.sadd("data_streams", key)

    def get_live_metadata(self, keys=None):
        """
        Get live metadata from Redis, i.e. the current values stored
        in the metadata hash.

        Parameters
        ----------
        keys : str or list of str
            Metadata key(s). If None, return all metadata.

        Returns
        -------
        m : dict
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

        Parameters
        ----------
        stream_key : str or list of str
            Redis stream key. If a list, return the requested streams.
            If None, return all streams.

        Returns
        -------
        redis_hdr : dict
            Dictionary of stream data. Each key is a stream name, and the
            value is a list of data values.

        Notes
        -----
        This grabs updated metadata from the Redis streams. If a stream
        has not been updated, it will not be included in the output.

        """
        if stream_keys is None:
            streams = self.data_streams
        else:
            if isinstance(stream_keys, str):
                stream_keys = [stream_keys]
            streams = {
                k: self.data_streams[k]
                for k in stream_keys
                if k in self.data_streams
            }

        # non-blocking read
        resp = self.r.xread(streams)
        redis_hdr = {}
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

    # ------------------- control commands -----------------

    @property
    def ctrl_commands(self):
        """
        Return allowed control commands. See also the property
        ``all_commands`` which returns the same information in a flat list.

        Returns
        -------
        commands : dict
            Dictonary of commands. Key is the command type. Values are
            the allowed commands for that type.

        """
        # ctrl command is always valid
        self.r.sadd("ctrl_commands", "ctrl")
        commands = {
            "ctrl": ["ctrl:reprogram"],  # reset the panda config
            "switch": [
                # s11 measurements
                "switch:VNAO",  # open cal standard
                "switch:VNAS",  # short cal standard
                "switch:VNAL",  # load cal standard
                "switch:VNAANT",  # antenna
                "switch:VNANON",  # noise source on
                "switch:VNANOFF",  # noise source off
                "switch:VNARF",  # receiver
                # snap observing
                "switch:RFNON",  # noise source on
                "switch:RFNOFF",  # noise source off
                "switch:RFLOAD",  # load
                "switch:RFANT",  # antenna
            ],
            "VNA": [
                "vna:ant",  # antenna
                "vna:rec",  # receiver
            ],
        }
        return {
            k: v
            for k, v in commands.items()
            if self.r.sismember("ctrl_commands", k.encode("utf-8"))
        }

    @property
    def all_commands(self):
        """
        Return all allowed commands.

        Returns
        -------
        commands : list
            List of all allowed commands.

        """
        commands = []
        for v in self.ctrl_commands.values():
            commands.extend(v)
        return commands

    @property
    def switch_commands(self):
        """
        Return allowed RF switch commands.

        Returns
        -------
        commands : list
            List of allowed RF switch commands.

        """
        if "switch" not in self.ctrl_commands:
            return []
        return self.ctrl_commands["switch"]

    @property
    def vna_commands(self):
        """
        Return allowed VNA commands.

        Returns
        -------
        commands : list
            List of allowed VNA commands.

        """
        if "VNA" not in self.ctrl_commands:
            return []
        return self.ctrl_commands["VNA"]

    def send_ctrl(self, cmd, **kwargs):
        """
        Stream control message to Redis. Used by server.

        Parameters
        ----------
        cmd : str
            Command to execute.
        kwargs : dict
            Additional arguments for the command.

        Raises
        ------
        TypeError
            If cmd is not a string.
        ValueError
            If the command is not in the list of allowed commands as defined
            by the property ``control_commands''.

        """
        # Validate command
        if not isinstance(cmd, str):
            raise TypeError("cmd must be a string")
        if not cmd or not cmd.strip():
            raise ValueError("cmd cannot be empty or whitespace only")

        if cmd not in self.all_commands:
            valid_cmds = self.all_commands
            raise ValueError(
                f"Command {cmd} not allowed. Valid commands: {valid_cmds}"
            )

        # Validate kwargs are JSON serializable
        try:
            if kwargs:
                json.dumps(kwargs)
        except (TypeError, ValueError) as e:
            raise ValueError(f"kwargs are not JSON serializable: {e}")

        payload = {"cmd": cmd}
        if kwargs:
            payload["kwargs"] = kwargs
        self.r.xadd(
            self.ctrl_stream_name,
            {"msg": json.dumps(payload)},
            maxlen=self.maxlen["ctrl"],
        )

    def read_ctrl(self, timeout=None):
        """
        Read control stream from Redis. Used on client (Panda).

        Parameters
        ----------
        timeout : int, optional
            Timeout in seconds for blocking read. If None, blocks indefinitely.

        Returns
        -------
        cmd : tuple of (str, dict)
            Len 2 tuple with the first element being the command to execute
            and the second element being a dictionary of keyword arguments.
            If None, no message was received or the message was not
            properly formatted.

        Notes
        -----
        This is a blocking call which may wait indefinitely for a
        message if timeout is None.

        """
        # blocking read with optional timeout
        block_time = 0 if timeout is None else int(timeout * 1000)
        msg = self.r.xread(self.ctrl_stream, count=1, block=block_time)
        if not msg:
            return None, {}
        # msg is stream_name, entries
        entries = msg[0][1]
        entry_id, dat = entries[0]  # since count=1, it's a list of 1
        # update the stream id
        self._set_last_read_id(self.ctrl_stream_name, entry_id)
        # dat is a dict with key msg
        raw = dat.get(b"msg")
        decoded = json.loads(raw)
        # msg is a dict with keys cmd and kwargs
        cmd = decoded.get("cmd")
        kwargs = decoded.get("kwargs", {})
        return (cmd, kwargs)

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
        Publish status message to Redis. Used by client..

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
