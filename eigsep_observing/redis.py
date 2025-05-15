from datetime import datetime, UTC
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
