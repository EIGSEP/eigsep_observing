from datetime import datetime, timezone
import json
import logging
import time

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

    Freshness: every :meth:`get` compares each requested key against
    its panda-side ``{key}_ts`` (written by :class:`MetadataWriter`)
    and logs ``WARNING`` for any key older than :attr:`max_age_s`.
    The stale value is still returned — staleness is informational,
    so consumers that want the "last known" reading are unaffected,
    but a dead sensor no longer passes silently. Set
    :attr:`max_age_s` to ``float("inf")`` to disable warnings.
    Keys whose ``_ts`` is missing or unparseable are skipped (no
    warning), so pre-timestamp entries don't trigger false positives.
    """

    # Producer cadence is ~200 ms (picohost STATUS_CADENCE_MS = 200),
    # so 30 s is ~150× the expected interval — transient blips don't
    # warn, but a truly dead sensor does on the next snapshot read.
    max_age_s = 30.0

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
        self._warn_if_stale(m, keys)
        if keys is None:
            return m
        if isinstance(keys, str):
            return m[keys]
        return {k: m[k] for k in keys}

    def _warn_if_stale(self, m, keys):
        """Log WARNING for any requested data key older than
        :attr:`max_age_s`. ``_ts`` bookkeeping keys are never checked
        themselves; a missing or unparseable ``_ts`` is treated as
        "freshness unknown" and skipped."""
        if self.max_age_s == float("inf"):
            return
        if keys is None:
            candidate_keys = [k for k in m if not k.endswith("_ts")]
        elif isinstance(keys, str):
            candidate_keys = [keys]
        else:
            candidate_keys = [k for k in keys if not k.endswith("_ts")]
        if not candidate_keys:
            return
        now = datetime.now(timezone.utc)
        for key in candidate_keys:
            ts_str = m.get(f"{key}_ts")
            if not isinstance(ts_str, str):
                continue
            try:
                ts = datetime.fromisoformat(ts_str)
            except ValueError:
                continue
            age = (now - ts).total_seconds()
            if age > self.max_age_s:
                logger.warning(
                    "metadata snapshot key %r is stale: last update "
                    "%.1fs ago (threshold %.1fs). Sensor may have "
                    "stopped publishing; returning cached value.",
                    key,
                    age,
                    self.max_age_s,
                )


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

    Freshness: an empty drain on a registered stream is the only
    in-band signal of producer silence (the saved file just shows
    ``None`` gaps in that sensor's column). After each ``drain``,
    any stream that returned zero entries has its panda-side
    ``{key}_ts`` looked up in :data:`METADATA_HASH` and compared
    against the current time; older than :attr:`max_age_s` logs a
    ``WARNING`` on ``eigsep_redis.metadata``. Warnings are throttled
    per-stream by :attr:`warn_interval_s` so a chronically dead
    sensor doesn't spam at the corr cadence (~4 Hz). A stream that
    returned entries this drain is fresh by definition and is not
    checked. Set :attr:`max_age_s` to ``float("inf")`` to disable.
    """

    # Mirrors MetadataSnapshotReader.max_age_s: ~150x the 200 ms
    # producer cadence, so transient pico blips don't warn but a
    # truly silent sensor does.
    max_age_s = 30.0
    # The corr loop calls drain at ~4 Hz; without throttling, a dead
    # sensor would emit ~14k warnings/hour. 60 s matches the
    # invariant-disagreement throttle in io.py.
    warn_interval_s = 60.0

    def __init__(self, transport):
        self.transport = transport
        self._last_warn_monotonic = {}

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
        silent = [s for s in streams if s not in out]
        if silent:
            self._warn_if_silent_stale(silent)
        return out

    def _warn_if_silent_stale(self, silent_streams):
        """For each registered stream that returned no entries this
        drain, peek at its panda-side ``{key}_ts`` in the snapshot
        hash and warn if older than :attr:`max_age_s`. Warnings are
        throttled per-stream by :attr:`warn_interval_s`."""
        if self.max_age_s == float("inf"):
            return
        now_mono = time.monotonic()
        now_utc = datetime.now(timezone.utc)
        r = self.transport.r
        for stream in silent_streams:
            last = self._last_warn_monotonic.get(stream)
            if (
                last is not None
                and now_mono - last < self.warn_interval_s
            ):
                continue
            # ``stream`` is ``stream:{key}``; the matching hash field
            # is ``{key}_ts``. Anything that doesn't follow the
            # writer's naming is treated as "freshness unknown".
            if ":" not in stream:
                continue
            key = stream.split(":", 1)[1]
            ts_raw = r.hget(METADATA_HASH, f"{key}_ts")
            if ts_raw is None:
                continue
            try:
                ts_str = json.loads(ts_raw)
                ts = datetime.fromisoformat(ts_str)
            except (ValueError, TypeError):
                continue
            age = (now_utc - ts).total_seconds()
            if age > self.max_age_s:
                logger.warning(
                    "metadata stream %r drained empty and is stale: "
                    "last update %.1fs ago (threshold %.1fs). Sensor "
                    "may have stopped publishing; integration row "
                    "will have None gaps for this sensor.",
                    stream,
                    age,
                    self.max_age_s,
                )
                self._last_warn_monotonic[stream] = now_mono
