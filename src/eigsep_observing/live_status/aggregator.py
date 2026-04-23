"""Background aggregator for the live-status dashboard.

Owns two :class:`eigsep_redis.Transport` instances (SNAP + panda) and
the per-bus readers the dashboard needs. Each transport is drained by
its own thread so their stream-position bookkeeping never crosses.
Running the dashboard alongside a live ``EigObserver`` is safe as long
as each consumer has its own ``Transport`` (the shared hazard is the
per-stream ``last_read_id``, not the Redis server).

Design notes
------------

- **Role surface.** This class instantiates only *reader* / *store*
  surfaces. It holds no ``Writer`` of any kind. Enforced by
  ``tests/test_redis.py::test_consumer_role_surfaces_are_structural``.

- **Blocking reads use finite timeouts.** Redis ``XREAD block=0`` means
  "block forever"; a drain thread that parked inside Redis would ignore
  ``stop_event.set()`` and never join on shutdown. Every blocking reader
  call here passes a finite timeout and loops on the stop event.

- **Threshold recompute.** When ``corr_header["integration_time"]``
  changes (re-sync), :meth:`_recompute_thresholds` rebuilds
  :attr:`thresholds` so cadence and file-heartbeat bands track the
  current run.

- **Exception policy.** A transient reader exception (network blip,
  stream not yet registered, JSON decode hiccup) is caught per-call so
  one bad tick doesn't kill the thread; it's logged at ERROR (per
  CLAUDE.md "safety nets must log loudly") and the next tick tries
  again. The fatal path is ``stop_event.set()`` only.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Any, Optional

import numpy as np
from redis.exceptions import RedisError

from eigsep_redis.heartbeat import HeartbeatReader
from eigsep_redis.metadata import (
    MetadataSnapshotReader,
    MetadataStreamReader,
)
from eigsep_redis.status import StatusReader

from ..adc import AdcSnapshotReader
from ..corr import CorrConfigStore, CorrReader
from ..file_heartbeat import read as read_file_heartbeat
from ..io import reshape_data
from ..utils import calc_freqs_dfreq
from .signals import SIGNAL_REGISTRY
from .thresholds import Thresholds

logger = logging.getLogger(__name__)


STATUS_LOG_MAXLEN = 500
# MetadataWriter registers its streams as "stream:{key}" in the
# METADATA_STREAMS_SET; the drain() output is keyed by the same
# full name.
_ADC_STATS_KEY = "adc_stats"
_ADC_STATS_STREAM = f"stream:{_ADC_STATS_KEY}"


@dataclass
class StateSnapshot:
    """Everything the Flask routes read, populated by the two drain
    threads. Intentionally flat-ish so Flask handlers can JSON-serialize
    after a shallow projection.

    Fields that are not yet populated stay at their defaults (``None``
    or empty container). The front-end treats missing data as
    "unknown" rather than assuming defaults.
    """

    # Corr (SNAP side)
    corr_acc_cnt: Optional[int] = None
    corr_last_unix: Optional[float] = None
    corr_cadence_s: Optional[float] = None
    corr_pairs: dict[str, np.ndarray] = field(default_factory=dict)
    corr_freqs: Optional[np.ndarray] = None
    corr_header: Optional[dict] = None
    corr_config: Optional[dict] = None

    # ADC stats stream (SNAP side, one entry per corr integration).
    adc_stats_latest: Optional[dict] = None
    adc_stats_last_unix: Optional[float] = None

    # ADC snapshot stream (SNAP side, ~1 Hz when enabled).
    adc_snapshot_data: Optional[np.ndarray] = None
    adc_snapshot_sidecar: Optional[dict] = None
    adc_snapshot_last_unix: Optional[float] = None
    adc_clip_fraction: dict[str, float] = field(default_factory=dict)

    # Panda-side pico metadata.
    metadata_latest: dict[str, dict] = field(default_factory=dict)
    metadata_last_stream_unix: dict[str, float] = field(default_factory=dict)
    metadata_snapshot: dict[str, Any] = field(default_factory=dict)

    # Wallclock at which the current rfswitch ``sw_state_name`` first
    # appeared. Distinct from ``metadata_last_stream_unix["rfswitch"]``
    # (which bumps on every producer push, ~200 ms) — this one only
    # advances when the state actually changes, so dashboard dwell-time
    # and on-schedule checks reflect physical dwell, not push cadence.
    rfswitch_state_entered_unix: Optional[float] = None

    # Panda status log (ring buffer).
    status_log: deque = field(
        default_factory=lambda: deque(maxlen=STATUS_LOG_MAXLEN)
    )

    # Panda heartbeat.
    panda_heartbeat: bool = False
    panda_heartbeat_last_check_unix: Optional[float] = None

    # File-writing heartbeat.
    file_heartbeat: dict = field(
        default_factory=lambda: {
            "newest_h5_path": None,
            "mtime_unix": None,
            "seconds_since_write": None,
        }
    )

    # Connectivity (flips to True on first successful tick per bus).
    snap_connected: bool = False
    panda_connected: bool = False
    snap_last_tick_unix: Optional[float] = None
    panda_last_tick_unix: Optional[float] = None
    snap_error: Optional[str] = None
    panda_error: Optional[str] = None


class LiveStatusAggregator:
    """Background poller + shared state for the live-status Flask app.

    Parameters
    ----------
    transport_snap
        Transport connected to the SNAP-side Redis (``rpi_ip``). Must
        not be shared with any other consumer (see module docstring).
    transport_panda
        Transport connected to the panda-side Redis (``panda_ip``).
        Must not be shared with any other consumer.
    obs_cfg
        Loaded ``obs_config.yaml``. Read for ``corr_save_dir``,
        ``corr_ntimes``, ``use_tempctrl``, ``switch_schedule``,
        ``tempctrl_settings``.
    thresholds
        Optional pre-built :class:`Thresholds`. If ``None``, one is
        built from the bundled YAML and the first corr header seen.
    snap_tick_s, panda_tick_s
        Loop cadence for the drain threads.
    read_timeout_s
        Finite timeout for blocking stream reads (``CorrReader``,
        ``AdcSnapshotReader``, ``StatusReader``). Must be > 0.
    stop_event
        External stop event; one is created if not supplied.
    """

    def __init__(
        self,
        transport_snap,
        transport_panda,
        obs_cfg: dict,
        *,
        thresholds: Optional[Thresholds] = None,
        snap_tick_s: float = 0.5,
        panda_tick_s: float = 0.5,
        read_timeout_s: float = 0.2,
        stop_event: Optional[threading.Event] = None,
    ):
        if read_timeout_s <= 0:
            raise ValueError(
                "read_timeout_s must be > 0; "
                "zero means block-forever in Redis semantics"
            )
        self.transport_snap = transport_snap
        self.transport_panda = transport_panda
        self.obs_cfg = dict(obs_cfg)
        self._snap_tick_s = snap_tick_s
        self._panda_tick_s = panda_tick_s
        self._read_timeout_s = read_timeout_s
        self._stop_event = stop_event or threading.Event()

        # SNAP-side surfaces.
        self.corr_reader = CorrReader(transport_snap)
        self.corr_config = CorrConfigStore(transport_snap)
        self.adc_snapshot_reader = AdcSnapshotReader(transport_snap)
        self.adc_metadata_stream = MetadataStreamReader(transport_snap)

        # Panda-side surfaces.
        self.metadata_stream = MetadataStreamReader(transport_panda)
        self.metadata_snapshot = MetadataSnapshotReader(transport_panda)
        self.status_reader = StatusReader(transport_panda)
        self.heartbeat_reader = HeartbeatReader(transport_panda)

        self.state = StateSnapshot()
        self._lock = threading.Lock()

        self.thresholds = thresholds
        if self.thresholds is None:
            self.thresholds = Thresholds.from_yaml(
                self.obs_cfg, corr_header=None
            )
        # Remember the integration_time we derived bands from so we
        # only recompute on change.
        self._thresholds_int_time: Optional[float] = (
            self.thresholds.corr_header.get("integration_time")
            if self.thresholds.corr_header
            else None
        )

        self._snap_thread: Optional[threading.Thread] = None
        self._panda_thread: Optional[threading.Thread] = None

    # -- lifecycle -------------------------------------------------

    def start(self) -> None:
        """Start both drain threads."""
        if self._snap_thread is not None or self._panda_thread is not None:
            raise RuntimeError("LiveStatusAggregator already started")
        self._snap_thread = threading.Thread(
            target=self._snap_loop,
            name="live-status-snap-drain",
            daemon=True,
        )
        self._panda_thread = threading.Thread(
            target=self._panda_loop,
            name="live-status-panda-drain",
            daemon=True,
        )
        self._snap_thread.start()
        self._panda_thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        """Signal both threads to exit and join with a finite timeout.

        If a thread fails to exit within ``timeout`` the reference is
        retained (and a warning is logged) so callers can observe the
        failure rather than silently assume a clean shutdown. A
        subsequent ``stop()`` will re-attempt the join.
        """
        self._stop_event.set()
        for attr in ("_snap_thread", "_panda_thread"):
            thread = getattr(self, attr)
            if thread is None:
                continue
            thread.join(timeout=timeout)
            if thread.is_alive():
                logger.warning(
                    "%s did not stop within %.1f s; handle retained",
                    thread.name,
                    timeout,
                )
            else:
                setattr(self, attr, None)

    def snapshot(self) -> StateSnapshot:
        """Return a race-free snapshot of the state for a request.

        Drain threads *replace* numpy array references and *mutate*
        dict / deque containers. A full ``deepcopy`` is overkill — the
        arrays themselves are never written to in place, so sharing
        references is safe, and only the mutable containers need a
        shallow copy to keep the returned snapshot stable while a
        Flask handler reads it.
        """
        with self._lock:
            s = self.state
            return replace(
                s,
                corr_pairs=dict(s.corr_pairs),
                adc_clip_fraction=dict(s.adc_clip_fraction),
                metadata_latest={k: v for k, v in s.metadata_latest.items()},
                metadata_last_stream_unix=dict(s.metadata_last_stream_unix),
                metadata_snapshot=dict(s.metadata_snapshot),
                status_log=deque(s.status_log, maxlen=s.status_log.maxlen),
                file_heartbeat=dict(s.file_heartbeat),
            )

    # -- SNAP drain loop -------------------------------------------

    def _snap_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._snap_tick()
            except Exception as exc:  # pragma: no cover - diagnostic
                logger.error("SNAP drain tick failed: %s", exc)
                with self._lock:
                    self.state.snap_error = str(exc)
            self._stop_event.wait(self._snap_tick_s)

    def _snap_tick(self) -> None:
        now = time.time()
        errors: list[str] = []
        any_ok = False

        # Corr config + header (pointer-free gets). ``ValueError`` from
        # the store means "header/config not in Redis yet" — that's a
        # missing-data case, not a transport failure, so it still
        # counts as a successful round-trip when tallying connectivity.
        header, ok = self._read_benign_missing(
            "corr_config.get_header",
            self.corr_config.get_header,
            errors,
        )
        any_ok = any_ok or ok
        cfg, ok = self._read_benign_missing(
            "corr_config.get", self.corr_config.get, errors
        )
        any_ok = any_ok or ok

        # Corr read (finite timeout; a bare ``TimeoutError`` from the
        # reader counts as a clean round-trip with no new integration).
        corr_result, ok = self._read_corr()
        any_ok = any_ok or ok
        if not ok:
            errors.append("corr_reader.read: transport error")

        # ADC snapshot (finite timeout).
        adc_snap, ok = self._read_adc_snapshot()
        any_ok = any_ok or ok
        if not ok:
            errors.append("adc_snapshot_reader.read: transport error")

        # ADC stats stream (non-blocking drain).
        adc_stats_drain, ok = self._read_benign_missing(
            "adc_metadata_stream.drain",
            lambda: self.adc_metadata_stream.drain(
                stream_keys=[_ADC_STATS_STREAM]
            ),
            errors,
        )
        any_ok = any_ok or ok

        # File-write heartbeat (raw K/V, published by
        # ``EigObserver.record_corr_data`` on this transport). Derived
        # fields are computed against ``now`` every tick so the
        # dashboard's ``seconds_since_write`` advances between writes.
        file_h, ok = self._read_benign_missing(
            "file_heartbeat.read",
            lambda: read_file_heartbeat(self.transport_snap, now=now),
            errors,
        )
        any_ok = any_ok or ok

        with self._lock:
            s = self.state
            s.snap_last_tick_unix = now
            if any_ok:
                s.snap_connected = True
                s.snap_error = "; ".join(errors) if errors else None
            else:
                # Every call on this tick hit a transport-level error:
                # the bus is effectively down. Flip to False and keep
                # the aggregated error for /api/health.
                s.snap_connected = False
                s.snap_error = (
                    "; ".join(errors) if errors else "no snap reads succeeded"
                )

            if header is not None:
                s.corr_header = header
                self._maybe_recompute_thresholds(header)
            if cfg is not None:
                s.corr_config = cfg
                # cache frequency axis for /api/corr
                sample_rate = cfg.get("sample_rate")
                nchan = cfg.get("nchan") or cfg.get("n_chans")
                if sample_rate and nchan:
                    freqs, _ = calc_freqs_dfreq(
                        float(sample_rate) * 1e6, int(nchan)
                    )
                    s.corr_freqs = freqs

            if corr_result is not None:
                acc_cnt, pairs_data = corr_result
                if acc_cnt is not None:
                    prev_acc = s.corr_acc_cnt
                    prev_unix = s.corr_last_unix
                    if prev_acc is not None and prev_unix is not None:
                        dacc = acc_cnt - prev_acc
                        dt = now - prev_unix
                        if dacc > 0 and dt > 0:
                            s.corr_cadence_s = dt / dacc
                    s.corr_acc_cnt = acc_cnt
                    s.corr_last_unix = now
                    s.corr_pairs = pairs_data

            if adc_snap is not None:
                data, sidecar = adc_snap
                if data is not None:
                    s.adc_snapshot_data = data
                    s.adc_snapshot_sidecar = sidecar
                    s.adc_snapshot_last_unix = now
                    s.adc_clip_fraction = self._compute_clip_fraction(
                        data, sidecar
                    )

            if adc_stats_drain:
                stream_data = adc_stats_drain.get(_ADC_STATS_STREAM)
                if stream_data:
                    s.adc_stats_latest = stream_data[-1]
                    s.adc_stats_last_unix = now

            if file_h is not None:
                s.file_heartbeat = file_h

    def _read_corr(self) -> tuple[Optional[tuple], bool]:
        """One CorrReader.read call with a finite timeout.

        Returns ``(result, ok)``. ``ok`` is True when the call
        round-tripped to Redis (even if no new data landed in the
        window — that's a ``TimeoutError`` from ``CorrReader``, not a
        connectivity failure). ``ok=False`` means a transport error
        fired. ``result`` is ``(acc_cnt, pairs_data)`` when data was
        available, else ``None``.
        """
        try:
            acc_cnt, pairs_data = self.corr_reader.read(
                timeout=self._read_timeout_s, unpack=True
            )
        except TimeoutError:
            return None, True
        except RedisError as exc:
            logger.error("corr_reader.read transport failure: %s", exc)
            return None, False
        except Exception as exc:
            logger.error("corr_reader.read failed: %s", exc)
            return None, False
        if acc_cnt is None:
            return None, True
        # Reshape even/odd average (same pattern as plot.py /
        # File._insert_sample).
        try:
            reshaped = reshape_data(pairs_data, avg_even_odd=True)
        except Exception as exc:
            logger.error("reshape_data failed: %s", exc)
            return None, True
        return (acc_cnt, reshaped), True

    def _read_adc_snapshot(self) -> tuple[Optional[tuple], bool]:
        """One AdcSnapshotReader.read call with a finite timeout.

        Returns ``(result, ok)`` — see :meth:`_read_corr` for semantics.
        """
        try:
            data, sidecar = self.adc_snapshot_reader.read(
                timeout=self._read_timeout_s
            )
        except TimeoutError:
            return None, True
        except RedisError as exc:
            logger.error("adc_snapshot_reader.read transport failure: %s", exc)
            return None, False
        except Exception as exc:
            logger.error("adc_snapshot_reader.read failed: %s", exc)
            return None, False
        return (data, sidecar), True

    @staticmethod
    def _compute_clip_fraction(
        data: np.ndarray, sidecar: Optional[dict]
    ) -> dict[str, float]:
        """Fraction of samples at int8 extremes per input.

        ``data`` shape is ``(n_antennas, 2, n_samples)`` int8 per
        ``AdcSnapshotWriter``. Keys in the result are the snap-input
        index as a string, matching the corr pair label convention.
        """
        if data is None or data.ndim != 3:
            return {}
        out: dict[str, float] = {}
        clipped = (data == 127) | (data == -128)
        # collapse the interleaved cores (axis 1) and sample axis.
        per_input = clipped.reshape(data.shape[0], -1)
        fractions = per_input.mean(axis=1)
        for inp_idx, frac in enumerate(fractions):
            out[str(inp_idx)] = float(frac)
        return out

    def _maybe_recompute_thresholds(self, header: dict) -> None:
        """Rebuild self.thresholds when integration_time changes."""
        new_int_time = header.get("integration_time")
        if new_int_time == self._thresholds_int_time:
            return
        self.thresholds = self.thresholds.with_header(header)
        self._thresholds_int_time = new_int_time

    # -- panda drain loop ------------------------------------------

    def _panda_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._panda_tick()
            except Exception as exc:  # pragma: no cover - diagnostic
                logger.error("panda drain tick failed: %s", exc)
                with self._lock:
                    self.state.panda_error = str(exc)
            self._stop_event.wait(self._panda_tick_s)

    def _panda_tick(self) -> None:
        now = time.time()
        errors: list[str] = []
        any_ok = False

        # Drain pico metadata streams (non-blocking).
        md, ok = self._read_benign_missing(
            "metadata_stream.drain", self.metadata_stream.drain, errors
        )
        any_ok = any_ok or ok

        # Snapshot hash (includes _ts keys for age computation).
        snap, ok = self._read_benign_missing(
            "metadata_snapshot.get", self.metadata_snapshot.get, errors
        )
        any_ok = any_ok or ok

        # Heartbeat.
        hb, ok = self._read_benign_missing(
            "heartbeat_reader.check", self.heartbeat_reader.check, errors
        )
        any_ok = any_ok or ok

        # Drain status stream. ``StatusReader.read`` returns
        # ``(None, None)`` on timeout; loop until it does so the ring
        # absorbs any backlog since the last tick without stalling.
        status_entries, ok = self._drain_status()
        any_ok = any_ok or ok

        with self._lock:
            s = self.state
            s.panda_last_tick_unix = now
            if any_ok:
                s.panda_connected = True
                s.panda_error = "; ".join(errors) if errors else None
            else:
                s.panda_connected = False
                s.panda_error = (
                    "; ".join(errors) if errors else "no panda reads succeeded"
                )

            if md:
                for stream, values in md.items():
                    if not values:
                        continue
                    # Strip the "stream:" prefix that MetadataWriter
                    # registers so downstream keys match the sensor
                    # name directly.
                    name = (
                        stream[len("stream:") :]
                        if stream.startswith("stream:")
                        else stream
                    )
                    new_value = values[-1]
                    if name == "rfswitch":
                        prev = s.metadata_latest.get("rfswitch") or {}
                        prev_name = (
                            prev.get("sw_state_name")
                            if isinstance(prev, dict)
                            else None
                        )
                        new_name = (
                            new_value.get("sw_state_name")
                            if isinstance(new_value, dict)
                            else None
                        )
                        if (
                            s.rfswitch_state_entered_unix is None
                            or prev_name != new_name
                        ):
                            s.rfswitch_state_entered_unix = now
                    s.metadata_latest[name] = new_value
                    s.metadata_last_stream_unix[name] = now

            if snap is not None:
                s.metadata_snapshot = snap

            if hb is not None:
                s.panda_heartbeat = bool(hb)
                s.panda_heartbeat_last_check_unix = now

            for level, msg in status_entries:
                s.status_log.append(
                    {"level": level, "msg": msg, "ts_unix": now}
                )

    def _drain_status(self) -> tuple[list[tuple[int, str]], bool]:
        """Drain StatusReader until the next call times out.

        Each read is capped at :attr:`_read_timeout_s` so shutdown is
        prompt. Returns ``(entries, ok)`` where ``ok`` is True unless a
        transport-level exception fired; a clean timeout (``level is
        None`` on the first read) is treated as a successful empty
        drain because the round-trip to Redis completed.
        """
        out: list[tuple[int, str]] = []
        # Safety bound: never loop more than N times per tick in
        # case the producer is hosing the stream.
        for _ in range(100):
            try:
                level, msg = self.status_reader.read(
                    timeout=self._read_timeout_s
                )
            except Exception as exc:
                logger.error("status_reader.read failed: %s", exc)
                return out, False
            if level is None:
                break
            out.append((level, msg))
        return out, True

    # -- error-swallowing helpers ----------------------------------

    @staticmethod
    def _read_benign_missing(label: str, fn, errors: list[str]):
        """Run ``fn()`` returning ``(value, ok)``.

        ``ok`` is True when the call round-tripped to Redis without a
        transport error. A ``ValueError`` (the convention for "key not
        present yet" in ``CorrConfigStore`` etc.) is treated as a
        successful round-trip with no value — benign startup state, not
        a connectivity problem. Any other exception is considered a
        transport-level failure: ``ok=False`` and ``errors`` is
        appended with a short description so the aggregator can surface
        it in ``snap_error`` / ``panda_error``.
        """
        try:
            return fn(), True
        except ValueError:
            logger.debug("%s returned no data", label, exc_info=True)
            return None, True
        except RedisError as exc:
            logger.error("%s transport failure: %s", label, exc)
            errors.append(f"{label}: {exc}")
            return None, False
        except Exception as exc:
            logger.error("%s failed: %s", label, exc)
            errors.append(f"{label}: {exc}")
            return None, False

    # -- role-surface introspection (used by the structural test) --

    def _role_surface_attrs(self) -> set[str]:
        """The reader/store surfaces this consumer is expected to hold.

        Used by ``test_aggregator_exposes_expected_surfaces`` to catch a
        rename or missing wire-up. It does *not* enforce "no writer"
        on its own — that invariant is enforced type-structurally by
        ``test_aggregator_holds_no_writer_attribute`` (and the role-
        surfaces block in ``tests/test_redis.py``), which iterates
        ``vars(self).values()`` and fails on any ``isinstance`` of a
        writer class regardless of attribute name.
        """
        return {
            "transport_snap",
            "transport_panda",
            "obs_cfg",
            "corr_reader",
            "corr_config",
            "adc_snapshot_reader",
            "adc_metadata_stream",
            "metadata_stream",
            "metadata_snapshot",
            "status_reader",
            "heartbeat_reader",
            "thresholds",
            "state",
        }


def _registered_signal_names() -> set[str]:
    """Re-exported for tests — the set of registered signal names."""
    return set(SIGNAL_REGISTRY)
