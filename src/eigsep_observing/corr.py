import json
import logging
import time

import numpy as np

from eigsep_redis.keys import DATA_STREAMS_SET

from .keys import (
    CORR_CONFIG_KEY,
    CORR_HEADER_KEY,
    CORR_PAIRS_SET,
    CORR_STREAM,
)
from .utils import load_config

logger = logging.getLogger(__name__)

_UNSYNCED_LOG_THROTTLE_S = 60.0
_last_unsynced_log = [0.0]


def _log_unsynced_drop(cnt):
    """Throttled ERROR log for a pre-sync corr integration drop.

    Logs at most once per ``_UNSYNCED_LOG_THROTTLE_S`` seconds. The
    throttle state is module-level so it survives across calls; tests
    that need to exercise the throttle can reset
    ``_last_unsynced_log[0]``. This message intentionally avoids
    hard-coding a specific ``sync_time`` value because callers may drop
    integrations for any falsy or missing ``sync_time``.
    """
    now = time.time()
    if now - _last_unsynced_log[0] < _UNSYNCED_LOG_THROTTLE_S:
        return
    _last_unsynced_log[0] = now
    logger.error(
        f"SNAP not synchronized (sync_time is falsy or missing); "
        f"dropping corr integration cnt={cnt}. Data without a sync "
        f"anchor has no valid timestamps."
    )


class CorrConfigStore:
    """
    Persistent store for the SNAP correlator config and corr header.

    Both are single-key JSON blobs keyed off ``corr_config`` and
    ``corr_header``. The header carries per-sync invariants
    (``sync_time``, integration time, RF chain state) and is uploaded
    by the producer when SNAP is synchronized.
    """

    def __init__(self, transport):
        self.transport = transport

    def upload(self, config, from_file=False):
        """
        Upload the SNAP configuration.

        Parameters
        ----------
        config : str or dict
            Path to a YAML file if ``from_file`` is True, else a dict.
            When loading from a file, ``integration_time`` is computed
            and injected via :func:`utils.load_config` — the corr
            config contract requires it, which is why this path differs
            from :class:`ConfigStore.upload`.
        from_file : bool
        """
        if from_file:
            config = load_config(config)
        self.transport._upload_dict(config, CORR_CONFIG_KEY)

    def get(self):
        """
        Return the SNAP configuration.

        Raises
        ------
        ValueError
            If no configuration is present.
        """
        raw = self.transport.get_raw(CORR_CONFIG_KEY)
        if raw is None:
            raise ValueError("No SNAP configuration found in Redis.")
        return json.loads(raw)

    def upload_header(self, header):
        """Upload the correlator header (from ``EigsepFpga.header``).

        Stamps ``header_upload_unix`` at publication time so downstream
        (file headers, offline inspection) can see when the producer
        last re-published. A stale timestamp relative to ``sync_time``
        means state on the SNAP changed without the producer
        re-publishing — a contract violation worth investigating, not a
        runtime failure.
        """
        stamped = {**header, "header_upload_unix": time.time()}
        self.transport._upload_dict(stamped, CORR_HEADER_KEY)

    def get_header(self):
        """
        Return the correlator header.

        Raises
        ------
        ValueError
            If no header is present.
        """
        raw = self.transport.get_raw(CORR_HEADER_KEY)
        if raw is None:
            raise ValueError("No correlation header found in Redis.")
        return json.loads(raw)


class CorrWriter:
    """
    Publish raw correlator spectra onto the corr stream.

    The wire format matches ``EigsepFpga.read_data``: per-pair bytes
    plus ``acc_cnt`` and ``dtype`` sidecars so the reader can
    ``np.frombuffer`` without a separate type registry.
    """

    maxlen = 5000

    def __init__(self, transport):
        self.transport = transport

    def add(self, data, cnt, sync_time, dtype=">i4"):
        """
        Publish one integration.

        Parameters
        ----------
        data : dict[str, bytes]
            Keys are correlation pair names, values are raw bytes.
        cnt : int
            Accumulation count from SNAP.
        sync_time : float or int
            Unix wallclock of SNAP synchronization. ``0`` (or any
            falsy value) means the SNAP is not synchronized yet and
            the integration is dropped, because without a sync anchor
            downstream cannot compute valid timestamps from
            ``acc_cnt``. Pre-sync drops are logged at ERROR
            (throttled) — dropped data would be unusable, and it's
            better to fail loud on a real sync problem than to let
            days of untimestamped integrations accumulate.
        dtype : str
            NumPy dtype string for unpacking downstream.
        """
        if not sync_time:
            _log_unsynced_drop(cnt)
            return
        redis_data = {p.encode("utf-8"): d for p, d in data.items()}
        r = self.transport.r
        r.sadd(CORR_PAIRS_SET, *redis_data.keys())
        redis_data["acc_cnt"] = str(cnt).encode("utf-8")
        redis_data["dtype"] = dtype.encode("utf-8")
        r.xadd(
            CORR_STREAM,
            redis_data,
            maxlen=self.maxlen,
            approximate=True,
        )
        r.sadd(DATA_STREAMS_SET, CORR_STREAM)


class CorrReader:
    """
    Consume raw correlator spectra from the corr stream.

    Pure corr-stream reader — does no cross-bus fetch. Sync_time and
    other per-sync invariants live on the corr header; callers that
    need them should read the header separately
    (:class:`CorrConfigStore.get_header`).
    """

    def __init__(self, transport):
        self.transport = transport

    def seek(self, position):
        """
        Reset the read position. Used by offline tools (e.g. the
        linearity sweep) that want to start from a specific entry
        rather than from '$' / the last read.
        """
        self.transport._set_last_read_id(CORR_STREAM, position)

    def read(self, pairs=None, timeout=10, unpack=True):
        """
        Blocking read of one corr entry.

        Parameters
        ----------
        pairs : list of str or None
            Pairs to include; ``None`` means read all registered pairs.
        timeout : int
            Timeout in seconds for the blocking XREAD.
        unpack : bool
            If True, return numpy arrays of ``dtype``; else raw bytes.

        Returns
        -------
        (acc_cnt, data) : tuple
            ``(int, {pair: np.ndarray | bytes})``. Returns
            ``(None, {})`` if no corr stream exists yet.

        Raises
        ------
        TimeoutError
            If no entry arrives within ``timeout``.
        """
        r = self.transport.r
        if not r.sismember(DATA_STREAMS_SET, CORR_STREAM):
            self.transport.logger.warning(
                "No correlation data stream found. "
                "Please ensure the SNAP is running and sending data."
            )
            return None, {}
        if pairs is None:
            pairs = r.smembers(CORR_PAIRS_SET)
        pairs = {p.encode() if isinstance(p, str) else p for p in pairs}
        last_id = self.transport._streams_from_set(DATA_STREAMS_SET)[
            CORR_STREAM
        ]
        out = r.xread(
            {CORR_STREAM: last_id},
            count=1,
            block=int(timeout * 1000),
        )
        if not out:
            raise TimeoutError("No correlation data received within timeout.")
        eid, fields = out[0][1][0]
        self.transport._set_last_read_id(CORR_STREAM, eid)
        acc_cnt = int(fields.pop(b"acc_cnt").decode())
        dtype = fields.pop(b"dtype").decode()
        data = {}
        for k, v in fields.items():
            if k not in pairs:
                continue
            if unpack:
                data[k.decode()] = np.frombuffer(v, dtype=dtype)
            else:
                data[k.decode()] = v
        return acc_cnt, data
