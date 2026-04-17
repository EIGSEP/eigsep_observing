from datetime import datetime, timezone
import json
import logging

from .keys import DATA_STREAMS_SET, METADATA_HASH, METADATA_STREAMS_SET

logger = logging.getLogger(__name__)


class MetadataWriter:
    """
    Publish sensor metadata to Redis.

    Each call to ``add`` writes to the live snapshot hash **and** the
    per-key stream as a single logical operation, and registers the
    stream in the metadata-only and generic stream-index sets. The two
    destinations back the two readers (snapshot vs streaming) — splitting
    them would let the readers drift out of sync.

    ``maxlen`` is a dead-reader failsafe, not a working buffer:
    ``MetadataStreamReader.drain`` pulls everything since its last call
    on every corr integration, so the stream sits near-empty in normal
    operation. Sized for ~5 min of tolerance at the 5 Hz producer
    cadence (picohost ``STATUS_CADENCE_MS = 200``) — past that window
    sensor data is stale and the observation is already compromised.
    """

    maxlen = 1500

    def __init__(self, transport):
        self.transport = transport

    def add(self, key, value):
        """
        Publish ``value`` under ``key``.

        Parameters
        ----------
        key : str
            Metadata key. Must be a non-empty string without ':'.
        value : JSON-serializable object

        Raises
        ------
        TypeError
            If ``key`` is not a string.
        ValueError
            If ``key`` is empty/whitespace, contains ':', or ``value``
            is not JSON-serializable.
        """
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

        r = self.transport.r
        # hash (snapshot path)
        r.hset(METADATA_HASH, key, payload)
        ts = datetime.now(timezone.utc).isoformat()
        r.hset(METADATA_HASH, f"{key}_ts", json.dumps(ts).encode("utf-8"))
        # stream (drain path for corr-cadence averaging)
        r.xadd(
            f"stream:{key}",
            {"value": payload},
            maxlen=self.maxlen,
            approximate=True,
        )
        # register the stream so the metadata-only default for
        # MetadataStreamReader.drain() excludes raw-data streams like
        # stream:corr / stream:vna by construction
        r.sadd(DATA_STREAMS_SET, f"stream:{key}")
        r.sadd(METADATA_STREAMS_SET, f"stream:{key}")


class MetadataSnapshotReader:
    """
    Snapshot-semantic metadata reader.

    Reads the latest values written by :class:`MetadataWriter` from
    the live hash. No position pointer, no draining — each call sees
    the same "latest" until the writer publishes something new. Used
    by VNA (point-in-time capture) and any other consumer that wants
    the current sensor state. **Do not use for the corr loop** — see
    :class:`MetadataStreamReader` for cadence-matched averaging.

    Caveat: no freshness check. If a sensor has stopped updating, a
    stale value is silently returned. Callers that care should record
    a snapshot timestamp alongside the read (see PandaClient's
    ``metadata_snapshot_unix`` header field).
    """

    def __init__(self, transport):
        self.transport = transport

    def get(self, keys=None):
        """
        Fetch metadata from the live hash.

        Parameters
        ----------
        keys : str or list of str or None
            If ``None``, return the full metadata dict. If a string,
            return the single value at that key. If a list, return a
            dict restricted to those keys.

        Raises
        ------
        TypeError
            If ``keys`` is not ``None``, a string, or a list of strings.
        """
        if keys is not None and not isinstance(keys, (str, list)):
            raise TypeError("Keys must be a string or a list of strings.")
        if isinstance(keys, list) and not all(
            isinstance(k, str) for k in keys
        ):
            raise TypeError("All keys in the list must be strings.")
        raw = self.transport.r.hgetall(METADATA_HASH)
        m = {k.decode("utf-8"): json.loads(v) for k, v in raw.items()}
        if keys is None:
            return m
        if isinstance(keys, str):
            return m[keys]
        return {k: m[k] for k in keys}


class MetadataStreamReader:
    """
    Streaming-semantic metadata reader.

    Each call to ``drain`` advances the per-stream position pointer
    and returns every value pushed since the previous call. Used by
    ``EigObserver.record_corr_data`` to average sensor readings over
    an integration window.

    Because the pointer advances, only one consumer per ``Transport``
    may call ``drain`` per stream — otherwise consumers race for
    entries. Today only the corr loop calls it.

    The default ``drain()`` (no arguments) reads only streams
    registered in ``metadata_streams``, so raw-data streams like
    ``stream:corr`` / ``stream:vna`` — whose payloads are not JSON —
    are excluded by construction.
    """

    def __init__(self, transport):
        self.transport = transport

    @property
    def streams(self):
        """``{stream_name: last_read_id}`` over registered metadata streams."""
        return self.transport._streams_from_set(METADATA_STREAMS_SET)

    def drain(self, stream_keys=None):
        """
        Drain metadata streams since the last call.

        Parameters
        ----------
        stream_keys : str or list of str or None
            If ``None``, drain every registered metadata stream. If
            given, drain only the listed streams (skipping any that
            aren't registered).

        Returns
        -------
        dict
            ``{stream_name: [value, ...]}`` for each stream that had
            entries since the last call. Streams with no new entries
            are omitted.
        """
        if stream_keys is None:
            streams = self.streams
        else:
            if isinstance(stream_keys, str):
                stream_keys = [stream_keys]
            all_streams = self.transport._streams_from_set(
                METADATA_STREAMS_SET
            )
            streams = {
                k: all_streams[k] for k in stream_keys if k in all_streams
            }

        # non-blocking read: correlator loop runs at ~4 Hz, so we
        # must not stall here. Picos push at 200 ms, so data will
        # accumulate between calls and be averaged by the caller.
        out = {}
        if not streams:
            return out
        resp = self.transport.r.xread(streams)
        for stream, dat in resp:
            stream = stream.decode()
            values = []
            for eid, d in dat:
                values.append(json.loads(d[b"value"]))
                self.transport._set_last_read_id(stream, eid)
            out[stream] = values
        return out
