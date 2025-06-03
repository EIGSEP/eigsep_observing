from datetime import datetime, timezone
import json
import numpy as np
import redis


class EigsepRedis:

    def __init__(self, host="localhost", port=6379, maxlen=600):
        """
        Default EigsepRedis client.

        Parameters
        ----------
        host : str
            Redis server hostname.
        port : int
            Redis server port.
        maxlen : int
            Maximum length of Redis streams. We really only need the streams
            to contain data for one file, so 600 is very conservative. One
            file is about 1 minute of data, 600 therefore allows for 10
            updates per second.

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
            "init": ["init:picos"],  # set pico device names
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
        for cmd_list in self.ctrl_commands.values():
            commands.extend(cmd_list)
        return commands

    @property
    def init_commands(self):
        """
        Return allowed initialization commands.

        Returns
        -------
        commands : list
            List of allowed initialization commands.

        """
        return self.ctrl_commands["init"]

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
        m = {}
        for k, v in self.r.hgetall("metadata").items():
            m[k] = json.loads(v)
        if keys is None:
            return m
        elif isinstance(keys, str):
            return m[keys.encode("utf-8")]
        elif isinstance(keys, list):
            return {k: m[k.encode("utf-8")] for k in keys if k in m}
        else:
            raise TypeError("Keys must be a string or a list of strings.")

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

    def _is_alive(self, key):
        """
        Check for heartbeat.

        Returns
        -------
        alive : bool
            True if heartbeat is received, False otherwise.

        """
        raw = self.get_raw(key)
        if raw is None:
            return False
        alive_int = int(raw)
        alive = bool(alive_int)
        return alive

    def is_client_alive(self):
        """
        Check if client is alive by checking the heartbeat.

        Returns
        -------
        alive : bool
            True if client is alive, False otherwise.

        """
        return self._is_alive("heartbeat:client")

    def is_server_alive(self):
        """
        Check if server is alive by checking the heartbeat.

        Returns
        -------
        alive : bool
            True if server is alive, False otherwise.

        """
        return self._is_alive("heartbeat:server")

    def send_status(self, status):
        """
        Publish status message to Redis. Used by client.

        Status must be a string, either ``VNA_COMPLETE'', ``VNA_ERROR'' or
        ``VNA_TIMEOUT''. Timeout is only used in case of timeout (obviously),
        other errors get flagged as ``VNA_ERROR''.

        Parameters
        ----------
        status : str
            Status message.

        """
        self.r.xadd("stream:status", {"status": status}, maxlen=self.maxlen)

    def send_vna_complete(self):
        """
        Send VNA complete status to Redis. Used by client.

        This is a convenience method that sends the ``VNA_COMPLETE'' status
        message.

        """
        self.send_status("VNA_COMPLETE")

    def send_vna_error(self):
        """
        Send VNA error status to Redis. Used by client.

        This is a convenience method that sends the ``VNA_ERROR'' status
        message.

        """
        self.send_status("VNA_ERROR")

    def send_vna_timeout(self):
        """
        Send VNA timeout status to Redis. Used by client.

        This is a convenience method that sends the ``VNA_TIMEOUT'' status
        message.

        """
        self.send_status("VNA_TIMEOUT")

    def read_status(self):
        """
        Read status stream from Redis. Used by server.

        Returns
        -------
        entry_id : str
            Redis stream entry id. If None, no message was received.
        status : str
            Status message. If None, no message was received.

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
        return entry_id, status

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
        entries = msg[1]
        entry_id, dat = entries[0]  # since count=1, it's a list of 1
        self.ctrl_streams["stream:ctrl"] = entry_id  # update the stream id
        # dat is a dict with key msg
        raw = dat.get("msg")
        try:
            msg = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return entry_id, None
        # msg is a dict with keys cmd and kwargs
        cmd = msg.get("cmd")
        kwargs = msg.get("kwargs")
        return entry_id, (cmd, kwargs)
