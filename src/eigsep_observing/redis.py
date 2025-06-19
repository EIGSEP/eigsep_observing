from datetime import datetime, timezone
import json
import numpy as np
import redis


class EigsepRedis:

    def __init__(self, host="localhost", port=6379, maxlen=10000):
        """
        Initialize the EigsepRedis client.

        Parameters
        ----------
        host : str
            Redis server hostname.
        port : int
            Redis server port.
        maxlen : int
            Maximum length of Redis streams.

        """
        self.r = redis.Redis(host=host, port=port, decode_responses=False)
        self.maxlen = maxlen
        self.ctrl_streams = {
            "stream:status": "0-0",  # status stream
            "stream:ctrl": "0-0",  # control stream
        }

    def reset(self):
        """
        Reset the Redis client. This clears all data streams and control
        streams. It also resests the last read entry ids for the streams.

        Notes
        -----
        This is a destructive operation and will remove all data from Redis.
        Use with caution!

        """
        self.r.flushdb()
        self.ctrl_streams = {
            "stream:status": "0-0",
            "stream:ctrl": "0-0",
        }

    @property
    def data_streams(self):
        """
        Dictionary of data streams. The keys are the stream names and
        the values are the last entry id read from the stream.

        Returns
        -------
        data_streams : dict

        """
        stream_names = self.r.smembers("data_stream_list")
        stream_ids = self.r.hgetall("data_streams")
        data_streams = {}
        for stream in stream_names:
            # get the last entry id for the stream
            last_id = stream_ids.get(stream, "0-0")
            data_streams[stream] = last_id
        return data_streams

    @property
    def ctrl_commands(self):
        """
        Return allowed control commands. See also the property
        ``all_commands'' which returns the same information in a flat list.

        Returns
        -------
        commands : dict
            Dictonary of commands. Key is the command type. Values are
            the allowed commands for that type.

        """
        commands = {
            "switch": [
                # s11 measurements
                "switch:VNAO",  # open cal standard
                "switch:VNAS",  # short cal standard
                "switch:VNAL",  # load cal standard
                "switch:VNAANT",  # antenna
                "switch:VNAN",  # noise source
                "switch:VNARF",  # receiver
                # snap observing
                "switch:RFN",  # noise source
                "switch:RFLOAD",  # load
                "switch:RFANT",  # antenna
            ],
            "VNA": [
                "vna:ant",  # antenna
                "vna:rec",  # receiver
            ],
        }
        return commands

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
        for key in self.r.smembers("ctrl_commands"):
            key = key.decode("utf-8")
            if key in self.ctrl_commands:
                commands.extend(self.ctrl_commands[key])
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
        return self.ctrl_commands["VNA"]

    def add_metadata(self, key, value):
        """
        Add metadata to Redis. This both streams values and adds the current
        value to a metadata hash.

        Parameters
        ----------
        key : str
            Metadata key.
        value : any
            Metadata value.

        """
        if isinstance(value, (bytes, bytearray)):
            payload = value
        else:
            payload = json.dumps(value).encode("utf-8")
        # hash (for live updates)
        self.r.hset("metadata", key, payload)
        ts = datetime.now(timezone.utc).isoformat()
        self.r.hset("metadata", f"{key}_ts", json.dumps(ts).encode("utf-8"))
        # stream (for file metadata)
        self.r.xadd(
            key,
            {"value": payload},
            maxlen=self.maxlen,
            approximate=True,
        )
        # add the stream to the data streams if not already present
        self.r.sadd("data_stream_list", key)

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
        for k, v in self.r.hgetall("metadata").items():
            m[k] = json.loads(v)
        if keys is None:
            return m
        elif isinstance(keys, str):
            return m[keys.encode("utf-8")]
        else:
            filtered_m = {}
            for k in keys:
                filtered_m[k] = m[k.encode("utf-8")]
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

        """
        if stream_keys is None:
            streams = self.data_streams
        else:
            if isinstance(stream_keys, str):
                stream_keys = [stream_keys]
            stream_keys = [k.encode("utf-8") for k in stream_keys]
            streams = {
                k: self.data_streams[k]
                for k in stream_keys
                if k in self.data_streams
            }

        resp = self.r.xread(streams)  # non-blocking read
        redis_hdr = {}
        for stream, dat in resp:
            out = []
            # stream is a list of tuples (id, data)
            for eid, d in dat:
                value = json.loads(d[b"value"])
                out.append(value)
                # update the stream id
                self.r.hset("data_streams", stream, eid)
            redis_hdr[stream] = np.array(out)
        return redis_hdr

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

    def client_heartbeat_set(self, ex, alive=True):
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

    def send_status(self, status, err=False):
        """
        Publish status message to Redis. Used by client. If the status
        message is an error, it will be prefixed with "ERROR: " and
        raise an error in the server upon reading it.

        Parameters
        ----------
        status : str
            Status message.

        """
        if err:
            status = f"ERROR: {status}"
        self.r.xadd("stream:status", {"status": status}, maxlen=self.maxlen)

    def read_status(self):
        """
        Read status stream from Redis. Used by server.

        Returns
        -------
        entry_id : str
            Redis stream entry id. If None, no message was received.
        status : str
            Status message. If None, no message was received.

        Raises
        ------
        RuntimeError
            If the status message indicates an error.

        """
        # non-blocking read
        msg = self.r.xread(
            {"stream:status": self.ctrl_streams["stream:status"]},
            count=1,
        )
        if not msg:
            return None, None
        entry_id, status_dict = msg[0][1][0]
        self.ctrl_streams["stream:status"] = entry_id  # update the stream id
        status = status_dict.get(b"status").decode("utf-8")
        if status.startswith("ERROR: "):
            raise RuntimeError("Client ERROR: " + status[len("ERROR: ") :])
        return entry_id, status

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
        raise NotImplementedError

    def read_vna_data(self, timeout=120):
        """
        Read VNA data stream from Redis. Used by server.

        Parameters
        ----------
        timeout : int
            Timeout in seconds for blocking read.

        Returns
        -------
        entry_id : str
            Redis stream entry id. If None, no message was received.
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

        Notes
        -----
        This is a blocking read with a timeout of ``timeout`` seconds.
        """
        raise NotImplementedError("This method is not implemented yet.")

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
        ValueError
            If the command is not in the list of allowed commands as defined
            by the property ``control_commands''.

        """
        if cmd not in self.all_commands:
            raise ValueError(f"Command {cmd} not allowed.")
        payload = {"cmd": cmd}
        if kwargs:
            payload["kwargs"] = kwargs
        self.r.xadd("stream:ctrl", {"msg": json.dumps(payload)})

    def read_ctrl(self):
        """
        Read control stream from Redis. Used on client (Panda).

        Returns
        -------
        entry_id : str
            Redis stream entry id. If None, no message was received.
        cmd : tuple of (str, dict)
            Len 2 tuple with the first element being the command to execute
            and the second element being a dictionary of keyword arguments.
            If None, no message was received or the message was not
            properly formatted.

        Notes
        -----
        This is a non-blocking call so it will not wait for a message.
        No message is received if entry_id is None. If the message is not
        properly formatted, cmd is None.

        """
        # this is non-blocking
        msg = self.r.xread(
            {"stream:ctrl": self.ctrl_streams["stream:ctrl"]}, count=1
        )
        if not msg:
            return None, None
        # msg is stream_name, entries
        entries = msg[0][1]
        entry_id, dat = entries[0]  # since count=1, it's a list of 1
        self.ctrl_streams["stream:ctrl"] = entry_id  # update the stream id
        # dat is a dict with key msg
        raw = dat.get(b"msg")
        try:
            decoded = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return entry_id, None
        # msg is a dict with keys cmd and kwargs
        cmd = decoded.get("cmd")
        kwargs = decoded.get("kwargs", {})
        return entry_id, (cmd, kwargs)
