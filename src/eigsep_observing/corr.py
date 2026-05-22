import json
import logging
import time

import numpy as np

from eigsep_redis import SingleStreamReader, SingleStreamWriter
from eigsep_redis.keys import DATA_STREAMS_SET

from .keys import (
    CORR_CONFIG_KEY,
    CORR_HEADER_KEY,
    CORR_PAIRS_SET,
    CORR_STREAM,
)

logger = logging.getLogger(__name__)

_UNSYNCED_LOG_THROTTLE_S = 60.0
_last_unsynced_log = [0.0]

_GAP_WARN_THROTTLE_S = 60.0


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

    def upload(self, config):
        """
        Upload the SNAP configuration.

        YAML file loading is the caller's responsibility — the corr
        contract requires ``integration_time`` to be injected
        (``utils.load_config`` does this), and keeping that step at
        the entry point avoids coupling this store to the observing
        utils.

        Parameters
        ----------
        config : dict
        """
        self.transport.upload_dict(config, CORR_CONFIG_KEY)

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
        self.transport.upload_dict(stamped, CORR_HEADER_KEY)

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


class CorrWriter(SingleStreamWriter):
    """
    Publish raw correlator spectra onto the corr stream.

    The wire format matches ``EigsepFpga.read_data``: per-pair bytes
    plus ``acc_cnt`` and ``dtype`` sidecars so the reader can
    ``np.frombuffer`` without a separate type registry.

    ``maxlen`` is a dead-reader failsafe: in normal operation the
    observer loop reads each integration as it arrives, so the stream
    sits at depth ~1. Sized for ~10 min of tolerance at the 4 Hz
    target integration rate (``corr_acc_len = 0x04000000``); at ~144
    KB/entry for the current 12-pair × 1024-channel layout this caps
    the worst-case Redis footprint around 360 MB.
    """

    stream = CORR_STREAM
    maxlen = 2500

    def _encode(self, data, cnt, dtype=">i4"):
        redis_data = {p.encode("utf-8"): d for p, d in data.items()}
        redis_data["acc_cnt"] = str(cnt).encode("utf-8")
        redis_data["dtype"] = dtype.encode("utf-8")
        return redis_data

    def publish(self, data, cnt, dtype=">i4"):
        """
        XADD one integration plus the side-set bookkeeping. The
        per-pair keys are registered in ``CORR_PAIRS_SET`` *before*
        the xadd so that ``CorrReader.read(pairs=None)`` can resolve
        the active pair list via ``smembers``.
        """
        pair_keys = [p.encode("utf-8") for p in data.keys()]
        self.transport.r.sadd(CORR_PAIRS_SET, *pair_keys)
        super().publish(data, cnt, dtype=dtype)

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
        self.publish(data, cnt, dtype=dtype)


class CorrReader(SingleStreamReader):
    """
    Consume raw correlator spectra from the corr stream.

    Pure corr-stream reader — does no cross-bus fetch. Sync_time and
    other per-sync invariants live on the corr header; callers that
    need them should read the header separately
    (:class:`CorrConfigStore.get_header`).

    Tracks ``acc_cnt`` across consecutive ``read`` calls and emits a
    throttled WARNING if it jumps by more than one — the only online
    signal that integrations were lost, whether because the stream was
    trimmed (reader fell behind ``CorrWriter.maxlen``) or the producer
    dropped entries. A backwards jump (``acc_cnt`` decreasing) is a
    SNAP re-sync and silently resets the tracker; the observer already
    rolls to a new file on ``sync_time`` change. ``seek`` also resets
    the tracker so offline tools replaying from a prior ID don't
    trigger false alarms.
    """

    stream = CORR_STREAM
    absent_warning = (
        "No correlation data stream found. "
        "Please ensure the SNAP is running and sending data."
    )

    def __init__(self, transport):
        super().__init__(transport)
        self._prev_acc_cnt = None
        self._last_gap_warn_monotonic = 0.0
        # Per-call decode context — set by read() before delegating
        # to super().read(), consumed by _decode(). Single-threaded
        # per reader (one corr loop), so the stash is safe.
        self._read_pairs = None
        self._read_unpack = True

    def _absent_sentinel(self):
        return None, {}

    def seek(self, position):
        """
        Reset the read position. Used by offline tools (e.g. the
        linearity sweep) that want to start from a specific entry
        rather than from '$' / the last read.
        """
        self.transport.set_last_read_id(CORR_STREAM, position)
        self._prev_acc_cnt = None

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
        if pairs is None:
            # Resolve from the producer-registered set. The base
            # read()'s membership check will short-circuit to
            # _absent_sentinel() if CORR_STREAM isn't registered
            # yet, so an empty smembers here is harmless.
            r = self.transport.r
            if r.sismember(DATA_STREAMS_SET, CORR_STREAM):
                pairs = r.smembers(CORR_PAIRS_SET)
        if pairs is not None:
            pairs = {p.encode() if isinstance(p, str) else p for p in pairs}
        self._read_pairs = pairs
        self._read_unpack = unpack
        return super().read(timeout=timeout)

    def _decode(self, entry_id, fields):
        acc_cnt = int(fields.pop(b"acc_cnt").decode())
        if self._prev_acc_cnt is not None:
            gap = acc_cnt - self._prev_acc_cnt
            if gap > 1:
                now = time.monotonic()
                if now - self._last_gap_warn_monotonic >= _GAP_WARN_THROTTLE_S:
                    self._last_gap_warn_monotonic = now
                    logger.warning(
                        "Corr stream gap: acc_cnt jumped %d -> %d "
                        "(%d missed integrations). Stream was likely "
                        "trimmed because the reader fell behind "
                        "maxlen=%d, or the producer dropped "
                        "integrations.",
                        self._prev_acc_cnt,
                        acc_cnt,
                        gap - 1,
                        CorrWriter.maxlen,
                    )
        self._prev_acc_cnt = acc_cnt
        dtype = fields.pop(b"dtype").decode()
        pairs = self._read_pairs or set()
        unpack = self._read_unpack
        data = {}
        for k, v in fields.items():
            if k not in pairs:
                continue
            if unpack:
                data[k.decode()] = np.frombuffer(v, dtype=dtype)
            else:
                data[k.decode()] = v
        return acc_cnt, data
