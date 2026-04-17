import logging
import threading
import time
import yaml

from cmt_vna import VNA
from picohost.base import PicoRFSwitch
from picohost.proxy import PicoProxy

from .io import _validate_vna_s11_data, _validate_vna_s11_header
from .utils import get_config_path

logger = logging.getLogger(__name__)
default_cfg_file = get_config_path("obs_config.yaml")
with open(default_cfg_file, "r") as f:
    default_cfg = yaml.safe_load(f)

# Valid RF switch state names, sourced from the firmware-side class so
# that a pico firmware change flows through automatically.
VALID_SWITCH_STATES = set(PicoRFSwitch.path_str)

# Inverse of PicoRFSwitch.path_str: {sw_state_int: mode_name}. Used to
# map the rfswitch's published `sw_state` back to a mode string when
# reading the current switch state from PicoManager's Redis snapshot.
_SW_INT_TO_MODE = {
    PicoRFSwitch.rbin(v): k for k, v in PicoRFSwitch.path_str.items()
}


class PandaClient:
    """
    Client class that runs on the computer in the suspended box.

    Reads sensor data published to Redis by PicoManager and sends
    control commands (e.g. RF switching) via PicoManager's Redis
    command stream. Does **not** hold serial connections — all pico
    communication is mediated by the PicoManager service.

    Parameters
    ----------
    redis : EigsepObsRedis
        The Redis server object to push data to and read commands
        from.
    default_cfg : dict
        Default configuration to use if no config is found in Redis.
    """

    def __init__(self, redis, default_cfg=default_cfg):
        self.logger = logger
        self.redis = redis
        self.stop_client = threading.Event()
        cfg = self._get_cfg()
        if cfg is None:
            self.logger.warning(
                "No configuration found in Redis, using default config."
            )
            self.redis.config.upload(default_cfg)
            cfg = self._get_cfg()
        self.cfg = cfg

        self.peltier = None

        # RF switch proxy is a thin Redis-key facade — no hardware
        # contact, so construction cannot fail. PicoManager owns the
        # real serial link and publishes its device list into the
        # "picos" Redis set; we just log it for startup observability.
        self.sw_proxy = PicoProxy(
            "rfswitch", self.redis.r, source="panda_client"
        )
        self.switch_lock = threading.Lock()
        available = self.redis.r.smembers("picos")
        if available:
            names = sorted(
                n.decode() if isinstance(n, bytes) else n for n in available
            )
            self.logger.info(f"PicoManager devices: {names}")
        else:
            self.logger.warning("No pico devices registered by PicoManager.")

        if self.cfg.get("use_vna", False):
            self.init_VNA()
        else:
            self.vna = None
            self.logger.info("VNA not initialized")

        self.heartbeat_thd = threading.Thread(
            target=self._send_heartbeat,
            kwargs={"ex": 60},
            daemon=True,
        )
        self.heartbeat_thd.start()

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
            cfg = self.redis.config.get()
        except ValueError:
            return None  # no config in Redis
        upload_time = cfg["upload_time"]
        self.logger.info(f"Using config from Redis, updated at {upload_time}.")
        return cfg

    def _switch_to(self, state):
        """Route an RF switch command through PicoManager.

        Returns the manager's response dict on success, or ``None`` if
        PicoManager has not registered the rfswitch device (no-op). The
        caller treats falsy as "switch failed".
        """
        return self.sw_proxy.send_command("switch", state=state)

    def _read_switch_mode_from_redis(self):
        """Return the RF switch mode string PicoManager last published.

        Reads ``sw_state`` from the rfswitch metadata snapshot and maps
        it back to a mode name via :data:`_SW_INT_TO_MODE`. Returns
        ``None`` if the rfswitch hasn't published yet or the published
        ``sw_state`` doesn't match a known mode — the caller decides
        the fallback. PicoManager's published state is the single
        source of truth; the panda holds no shadow that could drift
        across a restart on either side.
        """
        try:
            snap = self.redis.metadata_snapshot.get("rfswitch")
        except KeyError:
            return None
        sw_state = snap.get("sw_state")
        if sw_state is None:
            return None
        return _SW_INT_TO_MODE.get(sw_state)

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
            self.redis.heartbeat.set(ex=ex, alive=True)
            self.stop_client.wait(1.0)
        self.redis.heartbeat.set(alive=False)

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
            switch_fn=self._switch_to,
        )
        kwargs = self.cfg["vna_settings"].copy()
        kwargs["power_dBm"] = kwargs["power_dBm"]["ant"]
        self.logger.info(f"vna kwargs: {kwargs}")
        self.vna.setup(**kwargs)
        self.logger.info("VNA initialized")

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
                    if not self._switch_to(mode):
                        self.logger.warning(f"Failed to switch to {mode}")
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
        metadata = self.redis.metadata_snapshot.get()

        # Producer self-check against the VNA S11 contract (see
        # io.VNA_S11_HEADER_SCHEMA). Loud but non-blocking: never
        # raises, always publishes, so corr/VNA data flow is
        # uninterrupted when the producer disagrees with its own
        # contract. Emit on two channels because panda-side
        # ``self.logger.warning`` writes only to the local rotating
        # file — the operator on the ground sees nothing unless we
        # also push through the Redis status stream, which
        # ``EigObserver.status_logger`` re-emits ground-side. See
        # project_status_stream_log_bridge memory.
        violations = _validate_vna_s11_header(header) + _validate_vna_s11_data(
            s11, mode
        )
        if violations:
            msg = (
                f"VNA S11 producer contract violation (mode={mode!r}): "
                + "; ".join(violations)
            )
            self.logger.warning(msg)
            self.redis.status.send(msg, level=logging.WARNING)

        self.redis.vna.add(s11, header=header, metadata=metadata)
        self.logger.info("Vna data added to redis")

    def vna_loop(self):
        """
        Observe with VNA and write data to files.
        """
        if self.vna is None:
            self.logger.warning(
                "VNA not initialized. Cannot execute VNA commands."
            )
            return
        while not self.stop_client.is_set():
            with self.switch_lock:
                prev_mode = self._read_switch_mode_from_redis()
                if prev_mode is None:
                    self.logger.warning(
                        "rfswitch state unavailable in Redis; defaulting "
                        "post-VNA switch-back to RFANT."
                    )
                    prev_mode = "RFANT"
                for mode in ["ant", "rec"]:
                    self.logger.info(f"Measuring S11 of {mode} with VNA")
                    self.measure_s11(mode)
                self.logger.info(
                    f"Switching back to previous mode: {prev_mode}"
                )
                if not self._switch_to(prev_mode):
                    self.logger.warning(
                        f"Failed to switch back to {prev_mode}"
                    )
            self.stop_client.wait(self.cfg["vna_interval"])
