import logging
import threading
import time

import redis.exceptions
from eigsep_redis import (
    ConfigStore,
    HeartbeatReader,
    MetadataStreamReader,
    StatusReader,
)

from . import io, imu_calibration, obs_config_owner, run_tag
from .corr import CorrConfigStore, CorrReader
from .file_heartbeat import publish as publish_file_heartbeat
from .status_log_handler import PANDA_RELAY_LOGGER, StatusStreamHandler
from .vna import VnaReader

logger = logging.getLogger(__name__)


# Global throttle for the panda-down log spam (corr-side drain-failure
# ERROR and VNA-side wait WARNINGs) while panda is down: matches the
# invariant-disagreement throttle in io.py and the stream-staleness
# throttle in eigsep_redis.MetadataStreamReader.
_DRAIN_WARN_INTERVAL_S = 60.0


def _tick_liveness_deadline(deadline, liveness_timeout, reason):
    """Advance the SNAP-liveness deadline; log loudly when it expires.

    The SNAP-side producer has its own systemd-managed restart loop
    (``deploy/systemd/eigsep-observe.service``); the consumer's job
    here is to keep waiting and surface the silence in the log
    instead of suiciding. Crash-recovery visibility lives on the
    live-status dashboard via ``snap_reinit`` and ``file_heartbeat``,
    not via this consumer process exiting.

    Parameters
    ----------
    deadline : float or None
        Current deadline (``time.monotonic()`` seconds), or ``None``
        if no failure has been seen since the last successful write.
    liveness_timeout : float
        Tolerated duration without a complete corr row, in seconds.
        Each crossing logs an ``ERROR`` and re-arms for another
        ``liveness_timeout`` interval — so a permanently-silent SNAP
        produces a periodic reminder, not a one-shot warning that
        scrolls off.
    reason : str
        What triggered this tick; surfaced in the log line.

    Returns
    -------
    float
        Updated deadline. Set to ``monotonic() + liveness_timeout`` on
        the first failure since the last clear; re-armed by the same
        amount each time the deadline elapses; otherwise unchanged.
    """
    now = time.monotonic()
    if deadline is None:
        return now + liveness_timeout
    if now > deadline:
        logger.error(
            f"SNAP has not produced a complete corr row for "
            f"{liveness_timeout}s: {reason}. Continuing to wait — "
            "supervisor handles SNAP recovery."
        )
        return now + liveness_timeout
    return deadline


class EigObserver:
    def __init__(self, transport_snap, transport_panda):
        """
        Main controll class and filewriter for Eigsep observing.
        Provides methods to:
         - remotely control hardware in the EIGSEP box, including
           motors, VNA, and RF switches,
         - read correlator data from the SNAP,
         - read S11 measurements from the VNA,
         - read metadata from sensors connected to the LattePanda,
         - write data to files.

        Parameters
        ----------
        transport_snap : eigsep_redis.Transport
            The Redis transport for the Rasperry Pi controlling the
            SNAP correlator. SNAP is required — the corr thread is the
            writer's reason to exist; ``scripts/observe.py`` fails fast
            if the SNAP transport cannot be built. The observer builds
            only the consumer-side corr surfaces from it
            (``corr_config``, ``corr_reader``).
        transport_panda : eigsep_redis.Transport
            The Redis transport for the LattePanda server. In production
            this is built with ``lazy=True`` by ``scripts/observe.py`` so
            construction always succeeds even when the panda is
            unreachable. The observer builds the consumer-side panda
            surfaces (``config``, ``metadata_stream``, ``status_reader``,
            ``heartbeat_reader``, ``vna_reader``) unconditionally, and
            every panda-touching call site catches ``ConnectionError``
            and falls back to empty/sentinel data. A panda that comes
            online mid-run is therefore picked up implicitly on the
            next read — no observer restart required.

        Notes
        -----
        Both transports are required. Tests that want a "panda is
        down" shape can pass an unseeded
        :class:`eigsep_redis.testing.DummyTransport` (no heartbeat key
        → ``panda_connected`` reads as ``False``); tests that want a
        truly dead transport can pass a non-fakeredis ``Transport``
        with ``lazy=True`` pointed at an unreachable host.

        """
        self.logger = logger

        self.transport_snap = transport_snap
        self.transport_panda = transport_panda

        self.corr_config = CorrConfigStore(transport_snap)
        self.corr_reader = CorrReader(transport_snap)
        self.corr_cfg = self.corr_config.get()
        # SNAP-side diagnostic surface: drains ``adc_stats`` on every
        # corr integration and feeds the file via the same averaging
        # path as panda sensors.
        self.adc_metadata_stream = MetadataStreamReader(transport_snap)

        self.config = ConfigStore(transport_panda)
        self.metadata_stream = MetadataStreamReader(transport_panda)
        self.status_reader = StatusReader(transport_panda)
        self.heartbeat_reader = HeartbeatReader(transport_panda)
        self.vna_reader = VnaReader(transport_panda)
        self._status_log_handler = StatusStreamHandler(transport_panda)
        logging.getLogger("eigsep_observing").addHandler(
            self._status_log_handler
        )
        # Dedicated child logger for re-emitting panda status
        # messages so the StatusStreamHandler can skip them; see
        # status_log_handler.PANDA_RELAY_LOGGER.
        self._panda_relay_logger = logging.getLogger(PANDA_RELAY_LOGGER)

        self.stop_event = threading.Event()  # main stop event

        # start a status thread
        self.logger.info("Starting status thread.")
        self.status_thread = threading.Thread(
            target=self.status_logger, daemon=True
        )
        self.status_thread.start()

    def close(self):
        """Stop background threads and detach the status-stream handler.

        Production has one observer per process and shuts down via
        ``scripts/observe.py``'s finally block. Tests build many
        observers and must call ``close()`` on teardown so the
        ``StatusStreamHandler`` does not leak across tests and mirror
        the next test's errors into a stale transport.
        """
        self.stop_event.set()
        self.status_thread.join(timeout=1)
        try:
            self._status_log_handler.close()
        finally:
            logging.getLogger("eigsep_observing").removeHandler(
                self._status_log_handler
            )
            self._status_log_handler = None

    @property
    def snap_connected(self):
        """
        Check if the SNAP Redis connection is established.
        """
        return self.transport_snap is not None

    @property
    def panda_connected(self):
        """Check whether the LattePanda Redis connection is alive.

        Returns ``False`` when the panda is unreachable — either it was
        down at observer startup (the lazy transport built without a
        ping) or a previously-good connection has dropped. Redis client
        operations on a dead socket raise ``ConnectionError``, which we
        treat as "not connected" rather than letting it crash the corr
        loop. Corr data is sacred; a panda failure must not block a
        SNAP-side write.
        """
        try:
            return self.heartbeat_reader.check()
        except redis.exceptions.ConnectionError:
            return False

    def status_logger(self):
        """
        Log status messages from the LattePanda Redis server.
        """
        while not self.panda_connected:
            self.logger.debug("Status thread waiting for Panda connection.")
            if self.stop_event.wait(1):
                return
        self.logger.info("Status thread started. Logging Panda status.")

        while not self.stop_event.is_set():
            t0_status = time.time()
            while not self.panda_connected:
                # print every 10 seconds
                if time.time() - t0_status > 10:
                    self.logger.warning("Panda disconnected")
                    t0_status = time.time()
                if self.stop_event.wait(1):  # wait 1s before checking again
                    return
            self.logger.debug("Panda connected.")
            try:
                level, status = self.status_reader.read(timeout=0.1)
            except redis.exceptions.ConnectionError:
                # Treat the drop as a disconnect and loop back to the
                # outer `panda_connected` wait. The next successful
                # read picks up where the producer is now.
                continue
            if status is not None:
                self._panda_relay_logger.log(level, status)

    def _with_header_overlays(self, header):
        """Return a copy of ``header`` with panda-side overlay fields merged in.

        Adds ``run_tag`` / ``run_started_at_unix`` (from
        :mod:`eigsep_observing.run_tag`),
        ``obs_config_owner`` / ``obs_config_owner_uploaded_unix`` (from
        :mod:`eigsep_observing.obs_config_owner`), ``obs_config``
        (from :class:`eigsep_redis.ConfigStore`), and
        ``imu_calibration`` / ``imu_calibration_upload_unix`` (from
        :mod:`eigsep_observing.imu_calibration`) so each corr file
        records the active panda script, the last script to upload the
        config, the panda-side config snapshot at file-open time, and
        the IMU calibration blob at the moment the file was opened.

        Downstream trust check: ``obs_config_owner != "UNKNOWN"`` means
        someone legitimately uploaded the cfg at some point;
        ``run_tag == obs_config_owner`` means that same script is the
        active driver right now.

        All overlay reads are defensive: a missing or malformed
        run_tag, a missing obs_config_owner, a missing obs_config, a
        missing IMU calibration, or a transient transport failure all
        resolve to ``"UNKNOWN"`` / ``0.0`` / ``{}`` rather than
        raising. Sentinels: ``imu_calibration = {}`` when no blob is
        stored or the panda is unreachable;
        ``imu_calibration_upload_unix = 0.0`` likewise. Corr data is
        sacred — overlay enrichment must never block a corr file from
        being written. Sentinel values (rather than dropping the keys)
        guarantee every post-PR file carries the field and let
        downstream distinguish "no producer info" from "old file
        without this field."
        """
        out = dict(header)
        # All three reads are defensive: run_tag.read and
        # obs_config_owner.read_owner already log+swallow and return
        # the empty sentinel; the explicit try/except below covers
        # ConfigStore.get raising ValueError (no config uploaded yet)
        # or ConnectionError (panda unreachable).
        tag = run_tag.read(self.transport_panda)
        owner = obs_config_owner.read_owner(self.transport_panda)
        try:
            obs_cfg = self.config.get()
        except Exception as exc:
            self.logger.error("obs_config overlay read failed: %s", exc)
            obs_cfg = {}
        out["run_tag"] = (
            tag["run_tag"] if tag["run_tag"] is not None else "UNKNOWN"
        )
        out["run_started_at_unix"] = (
            tag["run_started_at_unix"]
            if tag["run_started_at_unix"] is not None
            else 0.0
        )
        out["obs_config_owner"] = (
            owner["owner"] if owner["owner"] is not None else "UNKNOWN"
        )
        out["obs_config_owner_uploaded_unix"] = (
            owner["uploaded_at_unix"]
            if owner["uploaded_at_unix"] is not None
            else 0.0
        )
        out["obs_config"] = obs_cfg
        cal = imu_calibration.read_calibration(self.transport_panda)
        out["imu_calibration"] = cal
        out["imu_calibration_upload_unix"] = imu_calibration.upload_unix(cal)
        return out

    def record_corr_data(
        self, save_dir, ntimes=240, timeout=20, liveness_timeout=300
    ):
        """
        Read data from the SNAP correlator via Redis and write it to
        file.

        Parameters
        ----------
        save_dir : str or Path
            Directory to save the correlator data files.
        ntimes : int
            Number of spectra per file.
        timeout : int
            The time in seconds to wait for data from the correlator.
        liveness_timeout : float
            Tolerated duration, in seconds, with no complete corr row
            (valid header with non-zero ``sync_time`` *and* an entry
            on the corr stream) before the watchdog logs an ``ERROR``.
            The watchdog only logs — it does not raise — because the
            SNAP-side producer is supervised by systemd and recovers
            on its own. The consumer keeps waiting; the log is for
            operator visibility, not flow control. Each crossing
            re-arms the deadline so a permanently-silent SNAP keeps
            producing periodic reminders.

        Notes
        -----
        Every failure mode that means "SNAP is not producing a
        complete corr row right now" shares a single watchdog:

        - ``ValueError`` from ``get_header`` with no cached header
        - ``sync_time == 0`` on a fetched header
        - ``TimeoutError`` from ``corr_reader.read``
        - ``(None, {})`` from ``corr_reader.read`` (stream absent)

        The deadline starts on the first such failure and is cleared
        when ``file.add_data`` completes. Crossing it logs an
        ``ERROR`` and re-arms; it does not exit. The consumer only
        cares *whether* a row arrived, not *why* it did not —
        duck-typing on "did I get a complete row?" is more
        trustworthy than classifying header vs. stream failures,
        because ``get_header`` reads a persistent Redis hash and a
        stale header can survive a dead SNAP.

        A ``ValueError`` from ``get_header`` *with* a cached header
        is treated as a transient metadata-path blip: the cached
        header is used, the loop proceeds to read/write the next row,
        and the deadline is cleared on the next successful write.
        Corr data on the stream has valid timestamps; the blip must
        not block corr writes ("corr data is sacred").

        A valid ``sync_time`` that differs from the cached one is
        treated as a mid-run SNAP re-sync: the current file is closed
        and a new one is opened with the new anchor. This is a
        legitimate state change, not a failure, so the deadline is
        untouched.
        """
        pairs = self.corr_cfg["pairs"]
        t_int = self.corr_cfg["integration_time"]
        file_time = ntimes * t_int
        self.logger.info(
            "Reading correlator data from SNAP. "
            f"Integration time: {t_int} s, "
            f"File time: {file_time} s"
        )

        on_write = (
            (
                lambda path, mtime_unix: publish_file_heartbeat(
                    self.transport_snap, path, mtime_unix
                )
            )
            if self.transport_snap is not None
            else None
        )
        file = io.File(
            save_dir, pairs, ntimes, self.corr_cfg, on_write=on_write
        )
        cached_header = None
        cached_sync_time = None
        last_write_deadline = None
        # Track metadata-stream drops so we can skip stream positions to
        # the current tail on reconnect via metadata_stream.skip_to_latest().
        # This tracks the metadata pipeline's reachability, NOT the
        # autonomous driver's heartbeat: the picos publish metadata via
        # the always-on pico-manager service, so a manual session (with
        # panda_observe stopped) still drains live sensor metadata.
        metadata_stream_down = False
        last_drain_warn_monotonic = 0.0
        try:
            while not self.stop_event.is_set():
                if file.counter == 0:
                    try:
                        header = self.corr_config.get_header()
                    except ValueError as e:
                        if cached_header is not None:
                            self.logger.warning(
                                f"Error reading header from SNAP: {e}. "
                                "Using cached corr header."
                            )
                            file.set_header(
                                header=self._with_header_overlays(
                                    cached_header
                                )
                            )
                        else:
                            last_write_deadline = _tick_liveness_deadline(
                                last_write_deadline,
                                liveness_timeout,
                                f"header fetch failed: {e}",
                            )
                            self.logger.error(
                                f"Error reading header from SNAP: {e}. "
                                "Waiting for a valid header."
                            )
                            if self.stop_event.wait(1):
                                return
                            continue
                    else:
                        new_sync_time = header.get("sync_time")
                        if not new_sync_time:
                            last_write_deadline = _tick_liveness_deadline(
                                last_write_deadline,
                                liveness_timeout,
                                "sync_time=0 (SNAP not synchronized)",
                            )
                            self.logger.error(
                                "No sync_time in corr header. Cannot "
                                "derive accurate timestamps. Waiting "
                                "for SNAP to synchronize."
                            )
                            if self.stop_event.wait(1):
                                return
                            continue
                        if (
                            cached_sync_time is not None
                            and new_sync_time != cached_sync_time
                        ):
                            self.logger.warning(
                                f"SNAP re-synchronized from "
                                f"{cached_sync_time} to {new_sync_time}; "
                                "rolling to new file."
                            )
                            file.close()
                            file = io.File(
                                save_dir,
                                pairs,
                                ntimes,
                                self.corr_cfg,
                                on_write=on_write,
                            )
                        cached_header = header
                        cached_sync_time = new_sync_time
                        file.set_header(
                            header=self._with_header_overlays(header)
                        )
                try:
                    acc_cnt, data = self.corr_reader.read(
                        pairs=pairs, timeout=timeout, unpack=True
                    )
                except TimeoutError:
                    last_write_deadline = _tick_liveness_deadline(
                        last_write_deadline,
                        liveness_timeout,
                        f"no corr entry within {timeout}s",
                    )
                    self.logger.error(
                        f"No correlation data received within {timeout}s. "
                        "Waiting for SNAP to produce data."
                    )
                    continue
                if acc_cnt is None:
                    last_write_deadline = _tick_liveness_deadline(
                        last_write_deadline,
                        liveness_timeout,
                        "corr stream does not exist yet",
                    )
                    if self.stop_event.wait(1):
                        return
                    continue
                self.logger.info(f"{acc_cnt=}")
                # Drain the metadata sidecar whenever the panda Redis is
                # reachable, independent of the autonomous driver's
                # heartbeat. The picos publish via the always-on
                # pico-manager service, so a manual session (panda_observe
                # stopped, heartbeat gone) still has live sensor metadata.
                # ConnectionError is the real "panda unreachable" signal;
                # corr stays sacred (metadata=None).
                if metadata_stream_down:
                    # On reconnect, jump every metadata stream past the
                    # outage backlog so the next drain picks up the
                    # producer's "now". Keeps the metadata cadence aligned
                    # with corr integrations instead of smearing
                    # historical readings into the current row.
                    try:
                        self.metadata_stream.skip_to_latest()
                    except redis.exceptions.ConnectionError:
                        # Bounced again between the skip attempt and the
                        # drain; treat as still-down and try next
                        # iteration.
                        metadata = {}
                        adc = self.adc_metadata_stream.drain()
                        if adc:
                            metadata.update(adc)
                        if not metadata:
                            metadata = None
                        file.add_data(
                            acc_cnt,
                            cached_sync_time,
                            data,
                            metadata=metadata,
                        )
                        last_write_deadline = None
                        continue
                try:
                    metadata = self.metadata_stream.drain()
                except redis.exceptions.ConnectionError as exc:
                    # Safety net for the corr-is-sacred contract:
                    # ERROR (not WARNING) per CLAUDE.md, throttled
                    # to one emit per ``_DRAIN_WARN_INTERVAL_S``
                    # window so a long outage doesn't flood the log.
                    metadata_stream_down = True
                    now_mono = time.monotonic()
                    if (
                        now_mono - last_drain_warn_monotonic
                        >= _DRAIN_WARN_INTERVAL_S
                    ):
                        self.logger.error(
                            "Panda metadata drain failed: %s. "
                            "Continuing corr writes with empty "
                            "metadata until panda is back.",
                            exc,
                        )
                        last_drain_warn_monotonic = now_mono
                    metadata = {}
                else:
                    # Only signal "reconnected" once the drain actually
                    # succeeds. If skip_to_latest() passes but drain
                    # immediately raises, the INFO would otherwise fire
                    # at ~4 Hz for the length of the flap. Tying it to
                    # drain success makes the INFO mean "metadata
                    # pipeline is back."
                    if metadata_stream_down:
                        self.logger.info(
                            "Panda metadata pipeline back; stream "
                            "positions reset to current tails."
                        )
                        metadata_stream_down = False
                # adc_stats lives on the SNAP transport,
                # merge into the same metadata dict
                adc = self.adc_metadata_stream.drain()
                if adc:
                    metadata.update(adc)
                if not metadata:
                    metadata = None
                file.add_data(
                    acc_cnt, cached_sync_time, data, metadata=metadata
                )
                last_write_deadline = None
        finally:
            file.close()

    def record_vna_data(self, save_dir, timeout=60):
        """
        Read VNA data from the LattePanda Redis server and write it to
        file.

        Parameters
        ----------
        save_dir : str or Path
            Directory to save the VNA data files.
        timeout : int
            Timeout in seconds for each blocking read. The loop retries
            after each timeout, allowing it to check for stop events
            and panda reconnection.

        """
        # The three "panda is down" branches below share one throttle
        # using ``_DRAIN_WARN_INTERVAL_S`` (60 s) — the wait loop above
        # otherwise iterates at 1 Hz, which would flood the log for the
        # full outage. The timer is reset on every panda-up iteration so
        # a fresh outage always emits immediately, matching the
        # ``record_corr_data`` drain-warn cadence.
        last_warn_monotonic = 0.0

        def _throttled_warn(msg):
            nonlocal last_warn_monotonic
            now_mono = time.monotonic()
            if now_mono - last_warn_monotonic >= _DRAIN_WARN_INTERVAL_S:
                self.logger.warning(msg)
                last_warn_monotonic = now_mono

        while not self.panda_connected:
            _throttled_warn(
                "Waiting for LattePanda Redis connection to be established."
            )
            # wait(1) returns True when stop is requested
            if self.stop_event.wait(1):
                return
        while not self.stop_event.is_set():
            # Panda can disconnect mid-operation after the initial
            # wait above; check here to avoid a full timeout cycle.
            if not self.panda_connected:
                _throttled_warn("Panda disconnected, waiting.")
                if self.stop_event.wait(1):
                    return
                continue
            # Panda is up — arm the throttle so the next disconnect
            # (if any) emits immediately instead of being swallowed by
            # the previous outage's window.
            last_warn_monotonic = 0.0
            try:
                data, header, metadata = self.vna_reader.read(timeout=timeout)
            except TimeoutError:
                continue
            except redis.exceptions.ConnectionError:
                # Panda dropped between the `panda_connected` gate
                # above and the read (or mid-xread). Treat the same
                # as the disconnect-wait branch: log, back off, loop
                # back to the gate. Without this, the VNA thread
                # would crash and stay dead until observer restart,
                # contradicting the "opportunistic panda" guarantee
                # and the module docstring claim that the loop "idles
                # on an empty VNA stream" when the panda is gone.
                _throttled_warn(
                    "VNA read failed: panda disconnected, waiting."
                )
                if self.stop_event.wait(1):
                    return
                continue
            if data is None:
                self.logger.warning("No VNA data available. Waiting.")
                if self.stop_event.wait(1):
                    return
                continue
            io.write_s11_file(
                data,
                header,
                metadata=metadata,
                save_dir=save_dir,
            )
            self.logger.info(f"Wrote VNA data to {save_dir}.")
