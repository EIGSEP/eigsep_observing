import datetime
import json
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
        self.r = redis.Redis(host=host, port=port, decode_responses=True)
        self.maxlen = maxlen
        self.streams = {}  # stream name, val is last id read
        self.streams["stream:status"] = "0-0"
        self.streams["stream:ctrl"] = "0-0"

    @property
    def ctrl_commands(self):
        """
        Return allowed control commnands.

        Returns
        -------
        commands : dict
            Dictonary of commands. Key is the command type: "switch" for
            controlling RF switches or "VNA" for initiating VNA observations.
            Values is a list of allowed commands for that type.

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
        ts = datetime.utcnow().isoformat()
        self.r.hset("metadata", f"{key}_ts", ts)
        # stream (for file header)
        stream_key = f"stream:{key}"
        self.r.xadd(
            stream_key,
            {"value": payload},
            maxlen=self.maxlen,
            approximate=True,
        )
        if stream_key not in self.streams:
            self.streams[stream_key] = "0-0"  # start of stream

    def get_live_metadata(self, key=None):
        """
        Get metadata from Redis.

        Parameters
        ----------
        key : str
            Metadata key. If None, return all metadata.

        """
        m = {}
        for k, v in self.r.hgetall("metadata").items():
            m[k] = json.loads(v)
        if key is None:
            return m
        else:
            return m[key]

    def get_header(self, stream_key=None):
        """
        Populate file header from redis stream.

        Parameters
        ----------
        stream_key : str
            Redis stream key. If None, return all streams.

        Returns
        -------
        redis_hdr : dict
            Dictionary of stream data. Each key is a stream name, and the
            value is a list of data values.

        """
        # XXX
        # this runs on the Pi, so self.streams is NOT updated!!
        # need to know what streams exist!
        # XXX also need to INIT the stream-id dict with 0-0
        # XXX also need to be non-blocking in the way that if a sensor
        # stop sending data we just put empty values in the header:w
        if stream_key is None:
            streams = self.streams
        else:
            streams = {stream_key: self.streams[stream_key]}

        resp = self.r.xread(streams, block=0)
        redis_hdr = {}
        for stream, dat in resp:
            out = []
            # stream is a list of tuples (id, data)
            for eid, d in dat:
                value = json.loads(d["value"])
                out.append(value)
                # update the stream id
                self.streams[stream] = eid
            redis_hdr[stream] = out
        return redis_hdr

    def add_raw(self, key, value):
        """
        Update redis database with raw data in bytes.

        Parameters
        ----------
        key : str
            Data key.
        value : bytes
            Data value.

        """
        return self.r.set(key, value)

    def get_raw(self, key):
        """
        Get raw bytes from Redis.

        Parameters
        ----------
        key : str
            Data key.

        """
        return self.r.get(key, encoding=None)

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
        msg = self.r.xread(
            {"stream:status": self.streams["stream:status"]}, block=0, count=1
        )
        if not msg:
            return None, None
        entry_id, status_dict = msg[1][0]  # since count=1, it's a list of 1
        self.streams["stream:status"] = entry_id  # update the stream id
        status = status_dict.get("status")
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
        if cmd not in self.switch_commands and cmd not in self.vna_commands:
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
            {"stream:ctrl": self.streams["stream:ctrl"]}, block=0, count=1
        )
        if not msg:
            return None, None
        # msg is stream_name, entries
        entries = msg[1]
        entry_id, dat = entries[0]  # since count=1, it's a list of 1
        self.streams["stream:ctrl"] = entry_id  # update the stream id
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
