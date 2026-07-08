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

- **Tick-loop reads poll, they don't wait.** The drain loops already
  pace themselves on ``snap_tick_s`` / ``panda_tick_s``; a stream read
  inside a tick only needs to consume what's already in Redis, so every
  tick-loop read passes :data:`_POLL_TIMEOUT_S` (~1 ms — the smallest
  finite block this API can express; ``timeout=None`` maps to ``XREAD
  block=0``, which is "block forever" and would ignore
  ``stop_event.set()``). The one deliberate blocker is the dedicated
  VNA thread (:data:`_VNA_BLOCK_S`), which parks inside Redis between
  ~1/hour sweeps and wakes for the stop check on each timeout.

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

from eigsep_redis import ConfigStore
from eigsep_redis.heartbeat import HeartbeatReader
from eigsep_redis.metadata import (
    MetadataSnapshotReader,
    MetadataStreamReader,
)
from eigsep_redis.status import StatusReader

from ..adc import AdcSnapshotReader
from ..corr import CorrConfigStore, CorrReader
from ..corr_health import read as read_corr_health
from ..file_heartbeat import read as read_file_heartbeat
from ..host_health import read as read_host_health
from ..io import RFSWITCH_TRANSITION_WINDOW_S, reshape_data
from ..linear_range import (
    LinearRangeError,
    load_linear_range,
    validate_operating_point,
)
from ..run_tag import read as read_run_tag
from ..snap_reinit import read as read_snap_reinit
from ..utils import calc_freqs_dfreq
from ..vna import VnaReader
from ..vna_calibration import VnaCache
from .signals import SIGNAL_REGISTRY
from .snap_probe import probe_snap_fpga
from .thresholds import Thresholds

logger = logging.getLogger(__name__)


STATUS_LOG_MAXLEN = 500
# MetadataWriter registers its streams as "stream:{key}" in the
# METADATA_STREAMS_SET; the drain() output is keyed by the same
# full name.
_ADC_STATS_KEY = "adc_stats"
_ADC_STATS_STREAM = f"stream:{_ADC_STATS_KEY}"

# VnaReader.read blocks inside Redis xread for at most this long per
# call. Longer = lower CPU between sweeps, but stop_event observation
# slows correspondingly (the drain wakes for the stop check at most
# every _VNA_BLOCK_S). 1.0 s is a comfortable middle ground for a
# ~1/hour producer.
_VNA_BLOCK_S = 1.0

# Effective "don't wait" timeout for stream reads inside the tick
# loops. The reader API maps ``timeout=None`` to ``XREAD block=0``
# (block forever), so a 1 ms block is the closest expressible thing to
# a non-blocking read: it consumes entries already in Redis and
# returns immediately when a stream is quiet, keeping the tick period
# pinned to snap_tick_s / panda_tick_s.
_POLL_TIMEOUT_S = 0.001


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

    # Per-channel linear-range bounds (raw corr counts, NaN-masked)
    # from the packaged calibration product named by
    # ``corr_config["linear_range_file"]``, populated only after the
    # product's operating point validates against the live corr
    # header. ``linear_range_key`` memoizes the (file, header-upload)
    # pair so a bad product logs one ERROR per header publish, not one
    # per ~1 s tick.
    corr_linear_min: Optional[np.ndarray] = None
    corr_linear_max: Optional[np.ndarray] = None
    linear_range_key: Optional[tuple] = None

    # ADC stats stream (SNAP side, one entry per diagnostics-period
    # tick).
    adc_stats_latest: Optional[dict] = None
    adc_stats_last_unix: Optional[float] = None

    # Corr-loop health (SNAP side, raw K/V): cumulative
    # dropped-integration count and the latest read_data wall-time,
    # published by EigsepFpga's throttled diagnostics loop.
    corr_health: dict = field(
        default_factory=lambda: {
            "dropped_integrations": None,
            "readout_time_ms": None,
            "published_unix": None,
            "seconds_since_publish": None,
        }
    )

    # ADC snapshot stream (SNAP side, ~1 Hz when enabled).
    adc_snapshot_data: Optional[np.ndarray] = None
    adc_snapshot_sidecar: Optional[dict] = None
    adc_snapshot_last_unix: Optional[float] = None
    adc_clip_fraction: dict[str, float] = field(default_factory=dict)

    # Panda-side pico metadata.
    metadata_latest: dict[str, dict] = field(default_factory=dict)
    metadata_last_stream_unix: dict[str, float] = field(default_factory=dict)
    metadata_snapshot: dict[str, Any] = field(default_factory=dict)
    # Wallclock of the last *successful* snapshot-hash read. The
    # /api/metadata age computation is ``read_unix - {key}_ts`` —
    # producer age at drain time — so the dashboard's own cache
    # staleness never inflates a healthy sensor's displayed age. Does
    # not advance on failed reads: a downed panda bus freezes the
    # cached ages (last-known values) and is surfaced via
    # ``panda_connected``, not by aging every tile.
    metadata_snapshot_read_unix: Optional[float] = None

    # Wallclock at which the current rfswitch ``sw_state_name`` first
    # appeared. Distinct from ``metadata_last_stream_unix["rfswitch"]``
    # (which bumps on every producer push, ~200 ms) — this one only
    # advances when the state actually changes, so dashboard dwell-time
    # and on-schedule checks reflect physical dwell, not push cadence.
    rfswitch_state_entered_unix: Optional[float] = None

    # Most recent integration captured while the switch was settled in
    # RFNOFF / RFNON / RFAMB (dwell past
    # ``RFSWITCH_TRANSITION_WINDOW_S``). The live-status first-order
    # Y-factor calibration reads RFNON (hot) + RFAMB (cold reference);
    # RFNOFF is cached for the dashboard cross-check only. ``RFANT``
    # and ``UNKNOWN`` integrations never evict the caches, so the
    # operator's "calibrated" toggle keeps painting across long
    # antenna dwells and short transition windows.
    last_rfnoff_pairs: Optional[dict[str, np.ndarray]] = None
    last_rfnoff_unix: Optional[float] = None
    last_rfnoff_acc_cnt: Optional[int] = None
    last_rfnon_pairs: Optional[dict[str, np.ndarray]] = None
    last_rfnon_unix: Optional[float] = None
    last_rfnon_acc_cnt: Optional[int] = None
    last_rfamb_pairs: Optional[dict[str, np.ndarray]] = None
    last_rfamb_unix: Optional[float] = None
    last_rfamb_acc_cnt: Optional[int] = None

    # Most recent VNA payload, cached per-mode. The route handler
    # calibrates lazily off these (see eigsep_observing.vna_calibration)
    # so the drain thread doesn't pay the calkit cost when nobody's
    # rendering the pane. ``ant`` and ``rec`` evict independently, so
    # the operator can flip between them without losing the other view.
    last_vna_ant: Optional[VnaCache] = None
    last_vna_rec: Optional[VnaCache] = None

    # Panda status log (ring buffer).
    status_log: deque = field(
        default_factory=lambda: deque(maxlen=STATUS_LOG_MAXLEN)
    )

    # Panda heartbeat.
    panda_heartbeat: bool = False
    panda_heartbeat_last_check_unix: Optional[float] = None

    # Latest obs_config dict read from the panda's Redis ``ConfigStore``
    # (key ``"config"``). ``None`` when no panda script has ever uploaded
    # its config to this Redis. The dashboard reads ``switch_schedule``
    # from here rather than from the on-disk YAML, so a parked switch
    # with no panda running shows no countdown.
    # ``panda_config_upload_unix`` carries the ``upload_time`` field that
    # ``Transport.upload_dict`` stamps on every upload, so the dashboard
    # can show operators how old the published config is.
    panda_config_latest: Optional[dict] = None
    panda_config_upload_unix: Optional[float] = None

    # Active panda script tag (panda_observe / no_switch_observation /
    # vna_position_sweep). ``None`` means no script is currently
    # running (cleared on clean exit, or never published).
    run_tag: Optional[str] = None
    run_started_at_unix: Optional[float] = None

    # File-writing heartbeat.
    file_heartbeat: dict = field(
        default_factory=lambda: {
            "newest_h5_path": None,
            "mtime_unix": None,
            "seconds_since_write": None,
        }
    )

    # SNAP --reinit heartbeat (bumped by eigsep-fpga-init on each
    # successful supervised re-init; surfaces thermal-cycling).
    snap_reinit: dict = field(
        default_factory=lambda: {
            "count": None,
            "last_reinit_unix": None,
            "seconds_since_reinit": None,
        }
    )

    # Raspberry Pi host vitals (raw K/V, published by each pi's
    # always-on ``eigsep-host-health`` service to its *local* Redis):
    # backend pi rides the SNAP transport, panda pi rides the panda
    # transport. Same key constant on both — the transport identifies
    # the host.
    host_health_backend: dict = field(
        default_factory=lambda: {
            "hostname": None,
            "temp_c": None,
            "published_unix": None,
            "seconds_since_publish": None,
        }
    )
    host_health_panda: dict = field(
        default_factory=lambda: {
            "hostname": None,
            "temp_c": None,
            "published_unix": None,
            "seconds_since_publish": None,
        }
    )

    # Connectivity (flips to True on first successful tick per bus).
    snap_connected: bool = False
    panda_connected: bool = False
    snap_last_tick_unix: Optional[float] = None
    panda_last_tick_unix: Optional[float] = None
    snap_error: Optional[str] = None
    panda_error: Optional[str] = None

    # SNAP FPGA reachability (independent of corr stream). Probed via
    # TCP to the katcp port only when corr is stale — corr flow is a
    # stronger signal and makes the probe redundant.
    snap_fpga_reachable: Optional[bool] = None
    snap_fpga_last_probe_unix: Optional[float] = None


def corr_observing_timeout_s(state: StateSnapshot) -> float:
    """Infer a reasonable observing-idle timeout from corr cadence.

    The corr loop advances ``corr_last_unix`` once per integration, so
    a fixed 2 s threshold misreports "not observing" whenever
    ``integration_time`` is configured above ~1 s. Prefer the measured
    cadence, fall back to the header's ``integration_time``, and
    finally to a 2 s floor. Scale by 2.5× so one slipped integration
    doesn't flip the indicator.
    """
    cadence = None
    if state.corr_cadence_s is not None and state.corr_cadence_s > 0:
        cadence = float(state.corr_cadence_s)
    elif state.corr_header is not None:
        int_time = state.corr_header.get("integration_time")
        if int_time is not None:
            try:
                int_time_f = float(int_time)
            except (TypeError, ValueError):
                int_time_f = None
            if int_time_f and int_time_f > 0:
                cadence = int_time_f
    if cadence is None:
        return 2.0
    return max(2.0, cadence * 2.5)


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
        ``corr_ntimes``, ``use_tempctrl``, ``tempctrl_settings``
        — Thresholds/signal-enablement plumbing. The active
        ``switch_schedule`` comes from the panda's ``ConfigStore``
        (Redis), not from this dict, so the dashboard's "next change"
        countdown reflects what panda is actually running.
    thresholds
        Optional pre-built :class:`Thresholds`. If ``None``, one is
        built from the bundled YAML and the first corr header seen.
    snap_tick_s, panda_tick_s
        Loop cadence for the drain threads. Stream reads inside a tick
        are ~1 ms polls (see :data:`_POLL_TIMEOUT_S`), so the tick
        cadence is the single knob controlling how stale the cached
        state can be.
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
        snap_tick_s: float = 0.25,
        panda_tick_s: float = 0.25,
        snap_fpga_probe_interval_s: float = 5.0,
        snap_fpga_host_override: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
    ):
        self.transport_snap = transport_snap
        self.transport_panda = transport_panda
        self.obs_cfg = dict(obs_cfg)
        self._snap_tick_s = snap_tick_s
        self._panda_tick_s = panda_tick_s
        self._snap_fpga_probe_interval_s = snap_fpga_probe_interval_s
        self._snap_fpga_host_override = snap_fpga_host_override
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
        self.vna_reader = VnaReader(transport_panda)
        self.panda_config_store = ConfigStore(transport_panda)

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
        self._vna_thread: Optional[threading.Thread] = None

    # -- lifecycle -------------------------------------------------

    def start(self) -> None:
        """Start the drain threads."""
        if (
            self._snap_thread is not None
            or self._panda_thread is not None
            or self._vna_thread is not None
        ):
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
        # VNA writes are ~1/hour. A dedicated thread blocks inside
        # Redis xread for ``_VNA_BLOCK_S`` between checks of the stop
        # event, so a triggered measurement surfaces in the dashboard
        # within ~1 s of arrival without polling cost between sweeps.
        self._vna_thread = threading.Thread(
            target=self._vna_loop,
            name="live-status-vna-drain",
            daemon=True,
        )
        self._snap_thread.start()
        self._panda_thread.start()
        self._vna_thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        """Signal both threads to exit and join with a finite timeout.

        If a thread fails to exit within ``timeout`` the reference is
        retained (and a warning is logged) so callers can observe the
        failure rather than silently assume a clean shutdown. A
        subsequent ``stop()`` will re-attempt the join.
        """
        self._stop_event.set()
        for attr in ("_snap_thread", "_panda_thread", "_vna_thread"):
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
                snap_reinit=dict(s.snap_reinit),
                corr_health=dict(s.corr_health),
                host_health_backend=dict(s.host_health_backend),
                host_health_panda=dict(s.host_health_panda),
                last_rfnoff_pairs=(
                    dict(s.last_rfnoff_pairs)
                    if s.last_rfnoff_pairs is not None
                    else None
                ),
                last_rfnon_pairs=(
                    dict(s.last_rfnon_pairs)
                    if s.last_rfnon_pairs is not None
                    else None
                ),
                last_rfamb_pairs=(
                    dict(s.last_rfamb_pairs)
                    if s.last_rfamb_pairs is not None
                    else None
                ),
                snap_fpga_reachable=s.snap_fpga_reachable,
                snap_fpga_last_probe_unix=s.snap_fpga_last_probe_unix,
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
        with self._lock:
            corr_unix_pre_tick = self.state.corr_last_unix
            corr_cfg_pre_tick = self.state.corr_config

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

        # Corr-loop health (raw K/V, published by ``EigsepFpga``'s
        # throttled diagnostics thread). Same shape contract as the
        # file heartbeat: missing key resolves to the empty-sentinel
        # dict.
        corr_h, ok = self._read_benign_missing(
            "corr_health.read",
            lambda: read_corr_health(self.transport_snap, now=now),
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

        # SNAP --reinit heartbeat (raw K/V, published by the
        # ``eigsep-fpga-init`` console script after each supervised
        # --reinit).
        # Same shape contract as the file heartbeat: missing key
        # resolves to the empty-sentinel dict.
        reinit_h, ok = self._read_benign_missing(
            "snap_reinit.read",
            lambda: read_snap_reinit(self.transport_snap, now=now),
            errors,
        )
        any_ok = any_ok or ok

        # Backend pi vitals (raw K/V, published by the always-on
        # ``eigsep-host-health`` service to the backend pi's local
        # Redis). Same shape contract as the file heartbeat: missing
        # key resolves to the empty-sentinel dict.
        host_h, ok = self._read_benign_missing(
            "host_health.read",
            lambda: read_host_health(self.transport_snap, now=now),
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
            if header is not None or cfg is not None:
                self._maybe_load_linear_range(s)

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
                    self._maybe_cache_onoff(s, pairs_data, acc_cnt, now)

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

            if corr_h is not None:
                s.corr_health = corr_h

            if file_h is not None:
                s.file_heartbeat = file_h

            if reinit_h is not None:
                s.snap_reinit = reinit_h

            if host_h is not None:
                s.host_health_backend = host_h

        self._maybe_probe_snap_fpga(now, corr_unix_pre_tick, corr_cfg_pre_tick)

    def _maybe_probe_snap_fpga(
        self,
        now: float,
        corr_unix_pre_tick: Optional[float],
        corr_cfg_pre_tick: Optional[dict],
    ) -> None:
        """Probe the SNAP FPGA if corr is stale + interval elapsed.

        Parameters are snapshotted by ``_snap_tick`` *before* the main
        lock block so test pre-sets survive the Redis read. In
        production the values are from the previous tick, which is
        fine — the probe is rate-limited to once per
        ``snap_fpga_probe_interval_s`` anyway.

        Skips the probe entirely when ``corr_unix_pre_tick`` is fresh —
        the corr stream itself is a stronger liveness signal. Also
        skips when no ``snap_ip`` is configured anywhere (corr config
        not yet read AND no CLI override), leaving
        ``snap_fpga_reachable`` at ``None`` so the tile shows
        "unknown" rather than a false negative.
        """
        with self._lock:
            last_probe = self.state.snap_fpga_last_probe_unix
            timeout_s = corr_observing_timeout_s(self.state)

        # Don't probe — corr stream is proving liveness.
        if corr_unix_pre_tick is not None:
            if (now - corr_unix_pre_tick) < timeout_s:
                return

        # Rate-limit the probe.
        if (
            last_probe is not None
            and (now - last_probe) < self._snap_fpga_probe_interval_s
        ):
            return

        # Pick the host: corr_config first, CLI override second.
        snap_ip = None
        if corr_cfg_pre_tick is not None:
            snap_ip = corr_cfg_pre_tick.get("snap_ip")
        if not snap_ip:
            snap_ip = self._snap_fpga_host_override
        if not snap_ip:
            return  # state.snap_fpga_reachable stays None → "unknown"

        result = probe_snap_fpga(snap_ip)
        with self._lock:
            self.state.snap_fpga_reachable = result
            self.state.snap_fpga_last_probe_unix = now

    def _read_corr(self) -> tuple[Optional[tuple], bool]:
        """Drain the corr stream to the latest entry; reshape the tail.

        ``CorrReader.read`` consumes one stream entry per call, and
        the producer publishes at the integration cadence (~4 Hz at
        the default ``corr_acc_len``) — comparable to the drain's
        ``1/snap_tick_s``. Reading one entry per tick would therefore
        fall behind whenever the producer outpaces the drain, which
        surfaces as a growing display lag — the legacy
        ``live_plotter`` polls faster than the producer
        (FuncAnimation at 20 Hz) and therefore never lags. Drain to
        the tail so the plot always reflects the most recent
        integration; intermediate entries are discarded because the
        dashboard renders current state, not history.

        Returns ``(result, ok)``. ``ok`` is True when every call
        round-tripped to Redis (a ``TimeoutError`` from ``CorrReader``
        is the natural drain-exit and counts as success). ``ok=False``
        means a transport error fired during the drain. ``result`` is
        ``(acc_cnt, pairs_data)`` for the *latest* drained entry, or
        ``None`` if the tick saw no new entries.
        """
        last_acc_cnt: Optional[int] = None
        last_pairs: Optional[dict] = None
        # Poll, don't wait: the drain only consumes entries already in
        # Redis at this moment and never blocks waiting for the
        # producer's next push (which would let a fast producer starve
        # _snap_tick from ever returning, and a quiet one inflate the
        # tick period). A new integration is picked up at worst one
        # snap_tick_s later.
        while True:
            try:
                acc_cnt, pairs_data = self.corr_reader.read(
                    timeout=_POLL_TIMEOUT_S, unpack=True
                )
            except TimeoutError:
                break
            except RedisError as exc:
                logger.error("corr_reader.read transport failure: %s", exc)
                return None, False
            except Exception as exc:
                logger.error("corr_reader.read failed: %s", exc)
                return None, False
            if acc_cnt is None:
                break
            last_acc_cnt = acc_cnt
            last_pairs = pairs_data
        if last_acc_cnt is None:
            return None, True
        # Data layout follows the firmware version the producer stamped
        # on the corr header/config (acc_bins 1 for v2.4 single-spectrum,
        # 2 for legacy even/odd).
        corr_header = self.state.corr_header or {}
        corr_config = self.state.corr_config or {}
        acc_bins = corr_header.get("acc_bins", corr_config.get("acc_bins", 2))
        avg_even_odd = corr_header.get(
            "avg_even_odd", corr_config.get("avg_even_odd", True)
        )
        try:
            reshaped = reshape_data(
                last_pairs, acc_bins=acc_bins, avg_even_odd=avg_even_odd
            )
        except Exception as exc:
            logger.error("reshape_data failed: %s", exc)
            return None, True
        return (last_acc_cnt, reshaped), True

    def _read_adc_snapshot(self) -> tuple[Optional[tuple], bool]:
        """One AdcSnapshotReader.read poll (no waiting; ~1 Hz producer
        entries are picked up at worst one snap_tick_s after arrival).

        Returns ``(result, ok)`` — see :meth:`_read_corr` for semantics.
        """
        try:
            data, sidecar = self.adc_snapshot_reader.read(
                timeout=_POLL_TIMEOUT_S
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

    @staticmethod
    def _maybe_cache_onoff(
        s: StateSnapshot,
        pairs_data: dict[str, np.ndarray],
        acc_cnt: int,
        now: float,
    ) -> None:
        """Cache the freshly-received integration as an RFNOFF, RFNON,
        or RFAMB reference if the switch has been settled in that
        state past the physical transition window.

        ``RFANT`` and ``UNKNOWN`` (and any state still inside the
        transition window) are no-ops so the operator's first-order
        cal keeps using the most recent valid on/off pair.
        """
        rf = s.metadata_latest.get("rfswitch") or {}
        name = rf.get("sw_state_name") if isinstance(rf, dict) else None
        if name not in ("RFNOFF", "RFNON", "RFAMB"):
            return
        entered = s.rfswitch_state_entered_unix
        if entered is None or (now - entered) < RFSWITCH_TRANSITION_WINDOW_S:
            return
        if name == "RFNOFF":
            s.last_rfnoff_pairs = pairs_data
            s.last_rfnoff_unix = now
            s.last_rfnoff_acc_cnt = acc_cnt
        elif name == "RFNON":
            s.last_rfnon_pairs = pairs_data
            s.last_rfnon_unix = now
            s.last_rfnon_acc_cnt = acc_cnt
        else:  # RFAMB
            s.last_rfamb_pairs = pairs_data
            s.last_rfamb_unix = now
            s.last_rfamb_acc_cnt = acc_cnt

    def _maybe_recompute_thresholds(self, header: dict) -> None:
        """Rebuild self.thresholds when integration_time changes."""
        new_int_time = header.get("integration_time")
        if new_int_time == self._thresholds_int_time:
            return
        self.thresholds = self.thresholds.with_header(header)
        self._thresholds_int_time = new_int_time

    def _maybe_load_linear_range(self, s: StateSnapshot) -> None:
        """Populate the per-channel linear-range bounds on ``s``.

        Called under ``self._lock`` from ``_snap_tick`` whenever the
        corr config or header was (re)read. Loads the packaged product
        named by ``corr_config["linear_range_file"]`` and validates its
        operating point against the live corr header; a failed load or
        mismatch clears the bounds. Memoized on the (file,
        ``header_upload_unix``) pair so a chronic failure logs one
        ERROR per header publish instead of one per tick (the tick
        re-reads config/header every ~1 s).
        """
        cfg = s.corr_config or {}
        lr_file = cfg.get("linear_range_file")
        header = s.corr_header
        if not lr_file or header is None:
            s.corr_linear_min = None
            s.corr_linear_max = None
            s.linear_range_key = None
            return
        key = (lr_file, header.get("header_upload_unix"))
        if key == s.linear_range_key:
            return
        s.linear_range_key = key
        s.corr_linear_min = None
        s.corr_linear_max = None
        try:
            product = load_linear_range(lr_file)
        except LinearRangeError as e:
            logger.error(
                "Linear-range contract violation: %s. Dashed bounds "
                "will be missing from the corr plot. Fix "
                "'linear_range_file' in corr_config or regenerate "
                "the product.",
                e,
            )
            return
        mismatches = validate_operating_point(product["header"], header)
        if mismatches:
            logger.error(
                "Linear-range operating-point mismatch for %r: %s. "
                "Dashed bounds will be missing from the corr plot. "
                "Re-measure the product at this operating point or "
                "fix corr_config.",
                lr_file,
                "; ".join(mismatches),
            )
            return
        s.corr_linear_min = product["linear_min"]
        s.corr_linear_max = product["linear_max"]

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

        # Active panda-script run_tag (raw K/V, published by the
        # panda entry-points on startup, cleared on clean exit). The
        # empty-sentinel dict is the steady-state for "no script
        # running"; a transport error is treated like the other
        # benign-missing K/V reads on this loop.
        rt, ok = self._read_benign_missing(
            "run_tag.read",
            lambda: read_run_tag(self.transport_panda),
            errors,
        )
        any_ok = any_ok or ok

        # Panda-side obs_config (key ``"config"``). ConfigStore.get
        # raises ValueError when the key is missing — handled as a
        # benign no-data result by _read_benign_missing.
        panda_cfg, ok = self._read_benign_missing(
            "panda_config_store.get",
            self.panda_config_store.get,
            errors,
        )
        any_ok = any_ok or ok

        # Panda pi vitals (raw K/V, published by the always-on
        # ``eigsep-host-health`` service to the panda pi's local
        # Redis — same key constant as the backend pi's, disambiguated
        # by transport).
        host_h, ok = self._read_benign_missing(
            "host_health.read",
            lambda: read_host_health(self.transport_panda, now=now),
            errors,
        )
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
                        # Only latch on a real observed transition. On
                        # the first metadata arrival (``prev_name is
                        # None``) we have no idea when the switch
                        # actually entered its current state — could be
                        # seconds ago, could be hours. Leaving
                        # ``entered_unix`` at None propagates that
                        # "we don't know" to the dashboard, which then
                        # shows N/A for dwell + countdown instead of
                        # claiming a freshly-entered state.
                        if prev_name is not None and prev_name != new_name:
                            s.rfswitch_state_entered_unix = now
                    s.metadata_latest[name] = new_value
                    s.metadata_last_stream_unix[name] = now

            if snap is not None:
                s.metadata_snapshot = snap
                s.metadata_snapshot_read_unix = now

            if hb is not None:
                s.panda_heartbeat = bool(hb)
                s.panda_heartbeat_last_check_unix = now

            for level, msg in status_entries:
                s.status_log.append(
                    {"level": level, "msg": msg, "ts_unix": now}
                )

            if rt is not None:
                s.run_tag = rt.get("run_tag")
                s.run_started_at_unix = rt.get("run_started_at_unix")

            if panda_cfg is not None:
                s.panda_config_latest = panda_cfg
                try:
                    s.panda_config_upload_unix = float(
                        panda_cfg["upload_time"]
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    logger.error(
                        "panda config missing/unparseable "
                        "upload_time=%r (%s); dropping field. "
                        "Producer contract: Transport.upload_dict "
                        "injects upload_time on every ConfigStore "
                        "upload as float-castable wallclock seconds.",
                        panda_cfg.get("upload_time"),
                        exc,
                    )
                    s.panda_config_upload_unix = None

            if host_h is not None:
                s.host_health_panda = host_h

    def _drain_status(self) -> tuple[list[tuple[int, str]], bool]:
        """Drain StatusReader until the next poll comes back empty.

        Each read is a :data:`_POLL_TIMEOUT_S` poll — the loop-exit
        read must not wait for the producer, or every steady-state
        tick (status stream quiet) carries the full timeout as a
        hidden tail-block on the tick period. Returns ``(entries,
        ok)`` where ``ok`` is True unless a transport-level exception
        fired; an empty poll (``level is None`` on the first read) is
        treated as a successful empty drain because the round-trip to
        Redis completed.
        """
        out: list[tuple[int, str]] = []
        # Safety bound: never loop more than N times per tick in
        # case the producer is hosing the stream.
        for _ in range(100):
            try:
                level, msg = self.status_reader.read(timeout=_POLL_TIMEOUT_S)
            except Exception as exc:
                logger.error("status_reader.read failed: %s", exc)
                return out, False
            if level is None:
                break
            out.append((level, msg))
        return out, True

    # -- VNA drain loop --------------------------------------------

    def _vna_loop(self) -> None:
        """Block on the VNA stream; cache by mode when an entry arrives.

        VNA cadence is ~1/hour, so this thread spends almost all its
        wallclock parked inside ``VnaReader.read`` and only does work
        when ``PandaClient.measure_s11`` actually publishes.
        """
        while not self._stop_event.is_set():
            try:
                self._vna_tick()
            except Exception as exc:  # pragma: no cover - diagnostic
                logger.error("VNA drain tick failed: %s", exc)
                # No connectivity flag for VNA: it shares the panda
                # transport, so panda_connected already covers the
                # underlying bus. Sleep on the stop event to avoid a
                # tight retry loop on a persistent error.
                self._stop_event.wait(self._panda_tick_s)

    def _vna_tick(self) -> None:
        """Single drain step: one blocking read, one cache write."""
        try:
            data, header, _metadata = self.vna_reader.read(
                timeout=_VNA_BLOCK_S
            )
        except TimeoutError:
            return
        except RedisError as exc:
            logger.error("vna_reader.read transport failure: %s", exc)
            self._stop_event.wait(self._panda_tick_s)
            return
        if data is None:
            # VnaReader.read returns (None, None, None) when the stream
            # hasn't been registered yet (no producer has written).
            # That's normal startup; back off briefly to avoid spinning.
            self._stop_event.wait(self._panda_tick_s)
            return

        # The producer's contract pins ``header['mode']`` to exactly
        # "ant" or "rec" (PandaClient.measure_s11). Anything else is a
        # producer bug or a stream cross-talk; log loudly and drop the
        # entry rather than caching it under a guessed slot.
        mode = (header or {}).get("mode")
        if mode not in ("ant", "rec"):
            logger.error(
                "VNA payload arrived with unexpected mode=%r; "
                "dropping. Producer contract: header['mode'] must be "
                "'ant' or 'rec'.",
                mode,
            )
            return

        cache = self._build_vna_cache(data, header)
        if cache is None:
            return
        with self._lock:
            if mode == "ant":
                self.state.last_vna_ant = cache
            else:
                self.state.last_vna_rec = cache

    @staticmethod
    def _build_vna_cache(
        data: dict[str, np.ndarray], header: dict
    ) -> Optional[VnaCache]:
        """Project a raw VNA stream entry into a :class:`VnaCache`.

        Caller must have already validated ``header['mode']`` is
        ``"ant"`` or ``"rec"``. Returns ``None`` (and logs at ERROR)
        on any further contract violation — missing data keys, missing
        ``freqs`` — so the corr / metadata panes keep painting even
        when the VNA producer publishes garbage.
        """
        dut_key = header["mode"]
        try:
            raw_s11 = data[dut_key]
            cal_o = data["cal:VNAO"]
            cal_s = data["cal:VNAS"]
            cal_l = data["cal:VNAL"]
        except KeyError as exc:
            logger.error(
                "VNA payload missing required key %s; dropping entry. "
                "Producer contract: data must include %r and "
                "cal:VNAO/VNAS/VNAL.",
                exc,
                dut_key,
            )
            return None
        freqs_raw = header.get("freqs")
        if freqs_raw is None:
            logger.error(
                "VNA header missing 'freqs'; dropping entry. Producer "
                "contract: header must include the frequency axis."
            )
            return None
        freqs = np.asarray(freqs_raw, dtype=float)
        try:
            metadata_snapshot_unix = (
                float(header["metadata_snapshot_unix"])
                if "metadata_snapshot_unix" in header
                else None
            )
        except (TypeError, ValueError) as exc:
            logger.error(
                "VNA header has unparseable metadata_snapshot_unix=%r "
                "(%s); dropping field. Producer contract: when present, "
                "the value must be a float-castable wallclock seconds.",
                header.get("metadata_snapshot_unix"),
                exc,
            )
            metadata_snapshot_unix = None
        return VnaCache(
            freqs=freqs,
            raw_s11=np.asarray(raw_s11),
            cal_o=np.asarray(cal_o),
            cal_s=np.asarray(cal_s),
            cal_l=np.asarray(cal_l),
            received_unix=time.time(),
            metadata_snapshot_unix=metadata_snapshot_unix,
        )

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
            "vna_reader",
            "panda_config_store",
            "thresholds",
            "state",
        }


def _registered_signal_names() -> set[str]:
    """Re-exported for tests — the set of registered signal names."""
    return set(SIGNAL_REGISTRY)
