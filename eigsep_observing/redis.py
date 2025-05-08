from datetime import datetime, UTC
import json
import redis


class EigsepRedis:

    def __init__(self, host="localhost", port=6379):
        """
        Default EigsepRedis client.

        Parameters
        ----------
        host : str
            Redis server hostname.
        port : int
            Redis server port.

        """
        self.r = redis.Redis(host=host, port=port, decode_responses=True)

    def add_metadata(self, key, value):
        """
        Add metadata to Redis. Automatically adds a timestamp.

        Parameters
        ----------
        key : str
            Metadata key.
        value : any
            Metadata value.

        """
        d = {"value": value, "ts": datetime.now(tz=UTC).isoformat()}
        return self.r.hset("metadata", key, json.dumps(d))

    def get_metadata(self, key=None):
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

    def add_raw(self, key, value):
        """
        Update redis database with raw data, str, or scalars.

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
        Get raw data from Redis.

        Parameters
        ----------
        key : str
            Data key.

        """
        return self.r.get(key, encoding=None)
