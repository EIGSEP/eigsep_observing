import logging
import threading
import time
from contextlib import contextmanager

import yaml

from cmt_vna import VNA
from eigsep_redis import (
    ConfigStore,
    HeartbeatWriter,
    MetadataSnapshotReader,
    StatusWriter,
)
from picohost.base import PicoRFSwitch
from picohost.proxy import PicoProxy

from .io import _validate_vna_s11_data, _validate_vna_s11_header
from .motor_scanner import MotorScanner
from .tempctrl_client import TempCtrlClient
from .utils import get_config_path
from .vna import VnaWriter

logger = logging.getLogger(__name__)
default_cfg_file = get_config_path("obs_config.yaml")
with open(default_cfg_file, "r") as f:
    default_cfg = yaml.safe_load(f)

# Valid RF switch state names, sourced from the firmware-side class so
# that a pico firmware change flows through automatically.
VALID_SWITCH_STATES = set(PicoRFSwitch.path_str)


class PandaClient:
    """
    Client class that runs on the computer in the suspended box.

    Reads sensor data published to Redis by PicoManager and sends
    control commands (e.g. RF switching) via PicoManager's Redis
    command stream. Does **not** hold serial connections — all pico
    communication is mediated by the PicoManager service.

    Parameters
    ----------
    transport : eigsep_redis.Transport
        Shared Redis transport. The client builds only the per-bus
        writer/reader surfaces it actually uses (config, metadata
        snapshot, status, heartbeat, VNA producer) — not the full
        observer-side bus. Wrong-role access (e.g. ``corr_reader``) is
        an ``AttributeError`` rather than a runtime foot-gun.
    default_cfg : dict
        Default configuration to use if no config is found in Redis.
    """

    def __init__(self, transport, default_cfg=default_cfg):
        self.logger = logger
        self.transport = transport
        self.config = ConfigStore(transport)
        self.metadata_snapshot = MetadataSnapshotReader(transport)
        self.status = StatusWriter(transport)
        self.heartbeat = HeartbeatWriter(transport)
        self.vna_writer = VnaWriter(transport)
        self.stop_client = threading.Event()
        cfg = self._get_cfg()
        if cfg is None:
            self.logger.warning(
                "No configuration found in Redis, using default config."
            )
            self.config.upload(default_cfg)
            cfg = self._get_cfg()
        self.cfg = cfg

        # RF switch proxy is a thin Redis-key facade — no hardware
        # contact, so construction cannot fail. PicoManager owns the
        # real serial link and publishes its device list into the
        # "picos" Redis set; we just log it for startup observability.
        self.sw_proxy = PicoProxy(
            "rfswitch", self.transport, source="panda_client"
        )
        self.switch_lock = threading.Lock()
        available = self.transport.r.smembers("picos")
        if available:
            names = sorted(
                n.decode() if isinstance(n, bytes) else n for n in available
            )
            self.logger.info(f"PicoManager devices: {names}")
        else:
            self.logger.warning("No pico devices registered by PicoManager.")

        # Boot-time invariant: wake up in RFANT. RFANT is the
        # all-switches-off state, so it's the physically safe default;
        # anything else must be an explicit switch-out by switch_loop
        # or switch_session. Side benefit: forces PicoManager to
        # publish ``sw_state_name`` immediately so downstream readers
        # have a truth to read from the first iteration. Best-effort —
        # if the rfswitch pico is unreachable at boot, log loudly and
        # continue; the Python client can't enforce the hardware
        # default on its own.
        if not self._safe_switch("RFANT"):
            self._error_with_status(
                "Boot-time RFANT initialization failed; rfswitch state "
                "is unknown."
            )

        if self.cfg.get("use_vna", False):
            self.init_VNA()
        else:
            self.vna = None
            self.logger.info("VNA not initialized")

        if self.cfg.get("use_motor", False):
            self.init_motor_scanner()
        else:
            self.motor_scanner = None
            self.logger.info("Motor scanner not initialized")

        if self.cfg.get("use_tempctrl", False):
            self.init_tempctrl()
        else:
            self.tempctrl = None
            self.logger.info("Tempctrl not initialized")

        self.heartbeat_thd = threading.Thread(
            target=self._send_heartbeat,
            kwargs={"ex": 60},
            daemon=True,
        )
        self.heartbeat_thd.start()

    def _log_with_status(self, msg, level):
        """Log locally at ``level`` and push to the Redis status stream.

        Panda-side ``self.logger`` writes only to a local
        ``RotatingFileHandler``; the ground observer sees the message
        only if it's also pushed through ``self.status``, which
        ``EigObserver.status_logger`` re-emits ground-side. Use the
        ``_warn_with_status`` / ``_error_with_status`` wrappers for
        operator-visible events (contract violations, config errors,
        hardware fault detection) — not for steady-state DEBUG/INFO
        telemetry, because the status stream is bounded to the last
        ``StatusWriter.maxlen`` entries.
        """
        self.logger.log(level, msg)
        self.status.send(msg, level=level)

    def _warn_with_status(self, msg):
        self._log_with_status(msg, logging.WARNING)

    def _error_with_status(self, msg):
        """ERROR-level sibling of ``_warn_with_status``.

        Per CLAUDE.md, safety nets around non-corr processing that
        swallow an exception or recover from a broken invariant must
        log at ERROR so the upstream fault is visible and actionable.
        """
        self._log_with_status(msg, logging.ERROR)

    def _get_cfg(self):
        """
        Try to get the current configuration from Redis. If it fails,
        return None.

        Returns
        -------
        cfg : dict or None
            The configuration dictionary if available, otherwise None.

        """
        try:
            cfg = self.config.get()
        except ValueError:
            return None  # no config in Redis
        upload_time = cfg["upload_time"]
        # upload_time is Unix seconds (Transport.upload_dict); render
        # for the operator log without changing the on-the-wire format.
        upload_str = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", time.gmtime(upload_time)
        )
        self.logger.info(f"Using config from Redis, updated at {upload_str}.")
        return cfg

    def _switch(self, state):
        """Route an RF switch command through PicoManager; raise on failure.

        Wired as ``switch_fn`` for ``cmt_vna.VNA`` (see :meth:`init_VNA`).
        Matches the eigsep-vna 1.3 ``switch_fn`` contract: raise on any
        failure, return is ignored. A switch failure during an S11
        measurement propagates out of ``measure_*`` and aborts the run
        instead of contaminating it.

        Raises
        ------
        RuntimeError
            Firmware/manager error, or device not registered with
            PicoManager (``None`` from the proxy).
        TimeoutError
            Proxy timeout waiting for the firmware response.
        """
        result = self.sw_proxy.send_command("switch", state=state)
        if result is None:
            raise RuntimeError(
                f"RF switch to {state} failed: rfswitch device not "
                f"registered with PicoManager."
            )

    def _safe_switch(self, state):
        """Bool-returning wrapper around :meth:`_switch` for observing loops.

        Returns ``True`` on confirmed success, ``False`` on any
        failure. Never raises: observing loops depend on this to stay
        up across transient Pico faults so corr data keeps flowing
        while a switch hiccup is logged for the operator. Not wired
        as ``switch_fn`` — cmt_vna gets the raising :meth:`_switch`
        directly so a mid-measurement switch failure aborts cleanly.
        """
        try:
            self._switch(state)
        except (RuntimeError, TimeoutError) as exc:
            self.logger.warning(
                f"RF switch to {state} failed: {type(exc).__name__}: {exc}"
            )
            return False
        return True

    def _read_switch_mode_from_redis(self):
        """Return the RF switch mode string PicoManager last published.

        Reads ``sw_state_name`` from the rfswitch metadata snapshot —
        picohost v3's rfswitch redis handler publishes the
        human-readable name alongside the raw ``sw_state`` int, so no
        reverse-mapping is needed here. Returns ``None`` if the
        rfswitch hasn't published yet or if the firmware could not map
        the raw state to a known mode (mid-switch, manual override),
        in which case ``sw_state_name`` is ``None``. The caller
        decides the fallback. PicoManager's published state is the
        single source of truth; the panda holds no shadow that could
        drift across a restart on either side.
        """
        try:
            snap = self.metadata_snapshot.get("rfswitch")
        except KeyError:
            return None
        return snap.get("sw_state_name")

    @contextmanager
    def switch_session(self):
        """Context manager for interactive / scripted RF switch use.

        Acquires :attr:`switch_lock` (pausing ``switch_loop`` and
        ``vna_loop`` for the duration of the block), yields a callable
        ``sw(mode) -> bool`` that routes through :meth:`_safe_switch`, and
        restores the mode that was active on entry when the block
        exits. Matches the common "switch, measure, switch back" REPL
        pattern without manual bookkeeping.

        Behavior:

        * The callable warns and returns ``False`` if the underlying
          switch call fails; returns ``True`` on success.
        * Auto-restore fires only if ``sw`` was actually called inside
          the block, so a "just pause the loops" block leaves hardware
          alone on exit.
        * If the rfswitch hasn't published any state on entry (the
          session starts with an unknown mode), restore is skipped
          with a warning — auto-guessing RFANT would be surprising.
        * A failed restore logs a warning; the lock is released either
          way, so a stuck switch doesn't wedge the session.

        Examples
        --------
        >>> with panda.switch_session() as sw:
        ...     sw("RFLOAD")
        ...     take_measurement()
        # rfswitch auto-restored to the mode that was active on entry
        """
        with self.switch_lock:
            prev_mode = self._read_switch_mode_from_redis()
            switched = False

            def sw(mode):
                nonlocal switched
                if mode not in VALID_SWITCH_STATES:
                    self.logger.warning(
                        f"Invalid switch mode {mode}; valid modes are "
                        f"{VALID_SWITCH_STATES}"
                    )
                    return False
                if not self._safe_switch(mode):
                    self.logger.warning(f"Failed to switch to {mode}")
                    return False
                switched = True
                return True

            try:
                yield sw
            finally:
                if switched:
                    if prev_mode is None:
                        self.logger.warning(
                            "switch_session: entry mode unknown "
                            "(rfswitch had not published); skipping "
                            "auto-restore."
                        )
                    elif not self._safe_switch(prev_mode):
                        self.logger.warning(
                            f"switch_session: failed to restore to {prev_mode}"
                        )

    def _send_heartbeat(self, ex=60):
        """
        Send a heartbeat message to the Redis server to indicate that the
        client is alive and running.

        Parameters
        ----------
        ex : float
            The expiration time for the heartbeat in seconds.

        """
        while not self.stop_client.is_set():
            self.heartbeat.set(ex=ex, alive=True)
            self.stop_client.wait(1.0)
        self.heartbeat.set(alive=False)

    def stop(self, timeout=5.0):
        """
        Signal all client loops to stop and wait for the heartbeat
        thread to emit its ``alive=False`` farewell.

        Idempotent — safe to call more than once. Caller-managed
        threads (``switch_loop``, ``vna_loop``) observe ``stop_client``
        and must be joined separately.
        """
        self.stop_client.set()
        if self.heartbeat_thd.is_alive():
            self.heartbeat_thd.join(timeout=timeout)
            if self.heartbeat_thd.is_alive():
                self.logger.warning(
                    f"Heartbeat thread did not exit within {timeout}s."
                )

    def init_VNA(self):
        """
        Initialize the VNA instance using the configuration from Redis.

        Notes
        -----
        Called by the constructor of the client. Can be called again
        to reinitialize the VNA if the configuration changes.

        """
        self.logger.info("INIT VNA")
        self.vna = VNA(
            ip=self.cfg["vna_ip"],
            port=self.cfg["vna_port"],
            timeout=self.cfg["vna_timeout"],
            save_dir=self.cfg["vna_save_dir"],
            switch_fn=self._switch,
        )
        kwargs = self.cfg["vna_settings"].copy()
        kwargs["power_dBm"] = kwargs["power_dBm"]["ant"]
        self.logger.info(f"vna kwargs: {kwargs}")
        self.vna.setup(**kwargs)
        self.logger.info("VNA initialized")

    def init_motor_scanner(self):
        """
        Build a :class:`MotorScanner` from the config.

        ``motor_scanner_kwargs`` (optional) forwards to the
        :class:`MotorScanner` constructor (per-axis step delays, poll
        interval, stall timeout). Absent/empty → use scanner defaults.

        Notes
        -----
        Called by the constructor when ``use_motor`` is true. Safe to
        call again if the config changes; no hardware contact, so
        construction cannot fail.
        """
        self.motor_scanner = None
        raw_kwargs = self.cfg.get("motor_scanner_kwargs")
        kwargs = raw_kwargs or {}
        if not isinstance(kwargs, dict):
            self.logger.warning(
                "Invalid motor_scanner_kwargs config; expected dict, "
                f"got {type(raw_kwargs).__name__}. Motor scanner disabled."
            )
            return
        try:
            self.motor_scanner = MotorScanner(self.transport, **kwargs)
        except TypeError as err:
            self.logger.warning(
                "Invalid motor_scanner_kwargs for MotorScanner: "
                f"{err}. Motor scanner disabled."
            )
            return
        self.logger.info(f"Motor scanner initialized (kwargs={kwargs})")

    def init_tempctrl(self):
        """
        Build a :class:`TempCtrlClient` from the config.

        ``tempctrl_settings`` (yaml, nested per-channel) forwards to the
        :class:`TempCtrlClient` constructor and is re-applied every
        iteration of :meth:`tempctrl_loop`. Absent/empty → no settings
        are pushed by :meth:`TempCtrlClient.apply_settings` (firmware
        keeps whatever it had).

        Notes
        -----
        Called by the constructor when ``use_tempctrl`` is true. Safe
        to call again if the config changes; no hardware contact, so
        construction cannot fail for config reasons — only an
        obviously-wrong (non-dict) settings block disables the client.
        """
        self.tempctrl = None
        raw_settings = self.cfg.get("tempctrl_settings")
        settings = raw_settings or {}
        if not isinstance(settings, dict):
            self.logger.warning(
                "Invalid tempctrl_settings config; expected dict, "
                f"got {type(raw_settings).__name__}. Tempctrl disabled."
            )
            return
        self.tempctrl = TempCtrlClient(self.transport, settings=settings)
        self.logger.info(f"Tempctrl initialized (settings={settings})")

    def switch_loop(self):
        """
        Use the RF switches to switch between sky, load, and noise
        source measurements according to the switch schedule.

        Notes
        -----
        The majority of the observing time is spent on sky
        measurements. Therefore, S11 measurements are only allowed
        to interrupt the sky measurements, and not the load or
        noise source measurements. That is, we release the switch
        lock immediately after switching to sky.

        """
        schedule = self.cfg.get("switch_schedule", None)
        if schedule is None:
            self.logger.warning(
                "No switch schedule found in config. Cannot execute "
                "switching commands."
            )
            return
        elif not schedule:
            self.logger.warning(
                "Empty switch schedule found in config. Cannot execute "
                "switching commands."
            )
            return
        elif any(k not in VALID_SWITCH_STATES for k in schedule):
            self.logger.warning(
                "Invalid switch keys found in schedule. Cannot execute "
                "switching commands. Schedule keys must be in: "
                f"{sorted(VALID_SWITCH_STATES)}."
            )
            return
        # Validate wait_time values and drop zero-wait modes into a
        # local schedule — do not mutate self.cfg["switch_schedule"].
        active_schedule = {}
        for mode, wait_time in schedule.items():
            if not isinstance(wait_time, (int, float)) or wait_time < 0:
                self.logger.warning(
                    f"Invalid wait_time for mode {mode}: {wait_time}. "
                    "All wait_time values must be positive numbers."
                )
                return
            elif wait_time == 0:
                self.logger.info(
                    f"Zero wait_time for mode {mode}: skipping this mode."
                )
                continue
            active_schedule[mode] = wait_time
        while not self.stop_client.is_set():
            for mode, wait_time in active_schedule.items():
                # RFANT (sky) releases the switch lock during the wait
                # so an S11 measurement can interrupt; other modes
                # (load, noise) hold the lock for the full wait.
                hold_lock_during_wait = mode != "RFANT"
                with self.switch_lock:
                    self.logger.info(f"Switching to {mode} measurements")
                    if not self._safe_switch(mode):
                        self._warn_with_status(f"Failed to switch to {mode}")
                    if hold_lock_during_wait and self._wait_or_stop(wait_time):
                        return
                if not hold_lock_during_wait and self._wait_or_stop(wait_time):
                    return

    def _wait_or_stop(self, wait_time):
        """Sleep for ``wait_time`` or until ``stop_client`` fires.

        Returns True if stop was requested (caller should unwind its
        loop), False otherwise.
        """
        if self.stop_client.wait(wait_time):
            self.logger.info("Switching stopped by event")
            return True
        return False

    def measure_s11(self, mode):
        """
        Measure S11 with the VNA and stream the results to Redis.

        Parameters
        ----------
        mode : str
            The mode of operation, either 'ant' for antenna or 'rec'
            for receiver.

        Raises
        ------
        ValueError
            If the mode is not 'ant' or 'rec'.
        RuntimeError
            If the VNA is not initialized.

        Notes
        -----
        This function does all the switching needed for the VNA
        measurement, including to OSL calibrators. The VNA internally
        invokes the ``switch_fn`` callable wired in ``init_VNA``, which
        routes through PicoManager.

        """
        if mode not in ["ant", "rec"]:
            raise ValueError(
                f"Unknown VNA mode: {mode}. Must be 'ant' or 'rec'."
            )
        if self.vna is None:
            raise RuntimeError(
                "VNA not initialized. Cannot execute VNA commands."
            )

        self.vna.power_dBm = self.cfg["vna_settings"]["power_dBm"][mode]
        osl_s11 = self.vna.measure_OSL()
        if mode == "ant":
            self.logger.info("Measuring antenna, noise, load S11")
            s11 = self.vna.measure_ant(measure_noise=True, measure_load=True)
        else:  # mode is rec
            self.logger.info("Measuring receiver S11")
            s11 = self.vna.measure_rec()
        # s11 is a dict with keys ant & noise, or rec
        for k, v in osl_s11.items():
            s11[f"cal:{k}"] = v  # add OSL calibration data

        header = self.vna.header
        header["mode"] = mode
        header["metadata_snapshot_unix"] = time.time()
        metadata = self.metadata_snapshot.get()

        # Producer self-check against the VNA S11 contract (see
        # io.VNA_S11_HEADER_SCHEMA). Loud but non-blocking: never
        # raises, always publishes, so corr/VNA data flow is
        # uninterrupted when the producer disagrees with its own
        # contract.
        violations = _validate_vna_s11_header(header) + _validate_vna_s11_data(
            s11, mode
        )
        if violations:
            self._warn_with_status(
                f"VNA S11 producer contract violation (mode={mode!r}): "
                + "; ".join(violations)
            )

        self.vna_writer.add(s11, header=header, metadata=metadata)
        self.logger.info("Vna data added to redis")

    def vna_loop(self):
        """
        Observe with VNA and write data to files.
        """
        if self.vna is None:
            self._warn_with_status(
                "VNA not initialized. Cannot execute VNA commands."
            )
            return
        while not self.stop_client.is_set():
            with self.switch_lock:
                prev_mode = self._read_switch_mode_from_redis()
                if prev_mode is None:
                    self._warn_with_status(
                        "rfswitch state unavailable in Redis; defaulting "
                        "post-VNA switch-back to RFANT."
                    )
                    prev_mode = "RFANT"
                target_mode = prev_mode
                try:
                    for mode in ["ant", "rec"]:
                        self.logger.info(f"Measuring S11 of {mode} with VNA")
                        self.measure_s11(mode)
                except Exception as exc:
                    # Any exception from measure_s11 (``_switch`` raising
                    # mid-OSL under the eigsep-vna 1.3 contract, VNA
                    # instrument TimeoutError, Redis write failure, ...)
                    # leaves the switch at whatever state cmt_vna last
                    # drove it to. Default the recovery target to RFANT
                    # rather than prev_mode — we've lost the
                    # "known-good state" invariant and RFANT is the
                    # physically safe fallback. The next switch_loop
                    # iteration will re-assert the configured mode.
                    self._error_with_status(
                        f"VNA cycle aborted "
                        f"({type(exc).__name__}: {exc}); "
                        "recovering rfswitch to RFANT."
                    )
                    target_mode = "RFANT"
                self.logger.info(
                    f"Switching rfswitch to {target_mode} "
                    f"(previous mode: {prev_mode})"
                )
                if not self._safe_switch(target_mode):
                    self._warn_with_status(
                        f"Failed to switch back to {target_mode}"
                    )
            self.stop_client.wait(self.cfg["vna_interval"])

    def motor_loop(self):
        """Periodic az/el beam scans, sibling of :meth:`vna_loop`.

        Runs a full ``MotorScanner.scan`` every ``motor_interval``
        seconds with ``motor_scan`` kwargs (``az_range_deg``,
        ``el_range_deg``, ``el_first``, ``repeat_count``, ``pause_s``,
        ``sleep_between``). Runs concurrently with ``switch_loop`` and
        ``vna_loop``; does **not** acquire ``switch_lock``. This is the
        steady-state "do a pointing survey every N hours" mode, and it
        deliberately lets switching/VNA continue uninterrupted while
        the motors move.

        Failure handling. ``MotorScanner.scan`` already halts the
        motors in its own ``finally`` block, so by the time a
        ``TimeoutError``/``RuntimeError`` reaches us the motors are
        stationary — but they may be stuck at an arbitrary grid point
        rather than at home. The failure path:

        1. Log the abort at ERROR on both channels.
        2. Attempt an inline ``home()`` to park the motors at
           ``(0, 0)`` so a subsequent power loss (batteries dying
           before the next scan) doesn't leave the rig in a random
           position that's annoying to lower.
        3. If the inline park succeeded, return to normal cadence —
           the next full scan fires at ``motor_interval``.
        4. If the inline park also failed, enter **recovery mode**:
           subsequent iterations retry ``home()`` only (no full scan)
           at ``motor_failure_retry_s`` cadence. Running a full grid
           every failure_retry_s would churn the motors and flood the
           bounded status stream during a persistent fault; once
           parked we're safe, so there's no benefit to re-running the
           scan fast. Recovery-mode park failures log at INFO locally
           only (no status push) so the status ring keeps the
           original actionable ERROR.
        5. When recovery-mode ``home()`` finally succeeds, the loop
           exits recovery and waits ``motor_interval`` before the
           next real scan.

        After a successful scan, the cadence is ``motor_interval``.

        Follow-ups (deferred; separate top-level scripts when we know
        what we want):

        * Beam mapping: scan with rfswitch pinned to RFANT for clean
          per-position corr (needs ``switch_lock`` + switch-loop pause).
        * VNA-at-positions: lockstep motor move / VNA measure for S11
          vs pointing comparisons.
        * Motion/switch sync: suppress switching while motors are
          moving if hardware interference proves to be a concern.
        """
        if self.motor_scanner is None:
            self._warn_with_status(
                "Motor scanner not initialized. Cannot execute motor scans."
            )
            return
        interval = self.cfg.get("motor_interval")
        if not isinstance(interval, (int, float)) or interval <= 0:
            self._warn_with_status(
                f"Invalid motor_interval ({interval!r}); motor_loop will "
                "not run."
            )
            return
        failure_retry_s = self.cfg.get("motor_failure_retry_s", 60)
        if (
            not isinstance(failure_retry_s, (int, float))
            or failure_retry_s <= 0
        ):
            self._warn_with_status(
                f"Invalid motor_failure_retry_s ({failure_retry_s!r}); "
                f"falling back to motor_interval ({interval}s) for "
                "post-failure retries."
            )
            failure_retry_s = interval
        scan_kwargs = self.cfg.get("motor_scan")
        if scan_kwargs is None:
            scan_kwargs = {}
        elif not isinstance(scan_kwargs, dict):
            self._warn_with_status(
                f"Invalid motor_scan ({scan_kwargs!r}); motor_loop will "
                "not run."
            )
            return

        # Push delay config once at loop entry. A failure here (motor
        # pico unreachable) is warned but not fatal — the scan call
        # below re-surfaces the same fault and drives the retry loop.
        try:
            self.motor_scanner.set_delay()
        except (RuntimeError, TimeoutError) as exc:
            self._warn_with_status(
                f"Motor set_delay failed at loop start "
                f"({type(exc).__name__}: {exc}); scans will retry."
            )

        needs_park = False
        while not self.stop_client.is_set():
            if needs_park:
                # Recovery mode: retry ``home()`` only. Do NOT run a
                # full scan — see docstring step 4.
                try:
                    self.motor_scanner.home()
                    needs_park = False
                    self._log_with_status(
                        "Motors parked at home after prior scan failure.",
                        logging.INFO,
                    )
                    wait_s = interval
                except (TimeoutError, RuntimeError) as exc:
                    # Log locally only so repeated failures during a
                    # persistent fault don't flood the bounded status
                    # stream. Operator already saw the original ERROR
                    # pair explaining that we'd retry.
                    self.logger.info(
                        f"Park retry still failing "
                        f"({type(exc).__name__}: {exc}); "
                        f"retrying in {failure_retry_s}s."
                    )
                    wait_s = failure_retry_s
            else:
                try:
                    self.motor_scanner.scan(
                        stop_event=self.stop_client, **scan_kwargs
                    )
                    wait_s = interval
                except (TimeoutError, RuntimeError) as exc:
                    self._error_with_status(
                        f"Motor scan aborted ({type(exc).__name__}: {exc})."
                    )
                    try:
                        self.motor_scanner.home()
                        self.logger.info(
                            "Motors parked at home after scan failure."
                        )
                        wait_s = interval
                    except (TimeoutError, RuntimeError) as park_exc:
                        self._error_with_status(
                            f"Post-failure home() also failed "
                            f"({type(park_exc).__name__}: {park_exc}); "
                            "motors in unknown position — retrying "
                            f"park every {failure_retry_s}s (no full "
                            "scans until parked)."
                        )
                        needs_park = True
                        wait_s = failure_retry_s
            if self.stop_client.wait(wait_s):
                return

    def tempctrl_loop(self):
        """Seed firmware config once, then poll health forever.

        The first iteration calls ``apply_settings`` to push the
        yaml-configured watchdog / clamps / setpoints / enable flags to
        the tempctrl pico. Once that succeeds the loop switches to
        health-check-only mode: every ``tempctrl_interval`` seconds it
        inspects the metadata snapshot for operator-actionable faults:

        * firmware watchdog tripped (channels disabled by firmware),
        * a channel's ``status == "error"`` (thermistor read failed),
        * drive saturated at the clamp while the channel is still far
          from its target (peltier can't keep up — fan failure,
          thermal-interface degradation, setpoint outside achievable
          range).

        Reboot recovery is owned by picohost:
        :class:`picohost.base.PicoPeltier` caches the last config each
        setter pushed and replays it from ``on_reconnect``. On EIGSEP
        hardware every firmware reset path (hard watchdog, brownout,
        picotool re-flash via BOOTSEL) drops USB CDC, so the replay
        fires on the serial-link reconnect without the panda needing a
        periodic re-apply as insurance.

        If the initial ``apply_settings`` fails (proxy/manager transient
        — ``RuntimeError`` / ``TimeoutError``) it is retried on the
        same cadence until it succeeds, then the loop locks into
        health-check-only mode. A persistent command-delivery outage
        just means warnings at the loop cadence, not quadratic-blowup
        retries.
        """
        if self.tempctrl is None:
            self._warn_with_status(
                "Tempctrl not initialized. Cannot run tempctrl_loop."
            )
            return
        interval = self.cfg.get("tempctrl_interval")
        if not isinstance(interval, (int, float)) or interval <= 0:
            self._warn_with_status(
                f"Invalid tempctrl_interval ({interval!r}); tempctrl_loop "
                "will not run."
            )
            return

        applied = False
        while not self.stop_client.is_set():
            if not applied:
                try:
                    self.tempctrl.apply_settings()
                    applied = True
                except (RuntimeError, TimeoutError) as exc:
                    self._warn_with_status(
                        f"Tempctrl apply_settings failed "
                        f"({type(exc).__name__}: {exc}); will retry in "
                        f"{interval}s."
                    )
            if applied:
                status = self.tempctrl.get_status()
                if status is not None:
                    self._tempctrl_health_check(status)
            if self.stop_client.wait(interval):
                return

    def _tempctrl_health_check(self, status):
        """Emit operator-visible warnings for tempctrl fault states.

        Called once per :meth:`tempctrl_loop` iteration on the latest
        metadata snapshot. Three warn conditions:

        1. ``watchdog_tripped`` — firmware disabled both channels
           because panda-side commands stopped arriving. The next
           ``apply_settings`` will re-arm once the link recovers, but
           the operator needs to see why the peltiers went dark.
        2. Per-channel ``status == "error"`` — thermistor read failed
           on that side. Control is disabled by firmware for the
           affected channel until the sensor recovers.
        3. Per-channel drive saturated at the clamp while
           ``|T_now - T_target| > 1°C`` — the peltier is trying its
           hardest and still losing ground, i.e. a fan or
           thermal-interface fault, or a setpoint outside the rig's
           thermal capability. Uses a ±0.02 slack on the clamp check
           because ``drive_level`` is a floating-point duty cycle and
           we don't want to false-fire on the last bit of rounding.
        """
        if status.get("watchdog_tripped"):
            self._warn_with_status(
                "Tempctrl firmware watchdog tripped; channels disabled."
            )
        for ch in ("LNA", "LOAD"):
            if status.get(f"{ch}_status") == "error":
                self._warn_with_status(
                    f"Tempctrl {ch} thermistor in error state; "
                    "firmware has disabled that channel."
                )
                continue
            drive = status.get(f"{ch}_drive_level")
            clamp = status.get(f"{ch}_clamp")
            t_now = status.get(f"{ch}_T_now")
            t_target = status.get(f"{ch}_T_target")
            if (
                drive is not None
                and clamp is not None
                and t_now is not None
                and t_target is not None
                and abs(drive) >= abs(clamp) - 0.02
                and abs(t_now - t_target) > 1.0
            ):
                self._warn_with_status(
                    f"Tempctrl {ch} drive saturated at clamp "
                    f"({drive:.2f}/{clamp:.2f}) with T_now={t_now:.2f}°C "
                    f"vs target={t_target:.2f}°C; peltier cannot keep up."
                )
