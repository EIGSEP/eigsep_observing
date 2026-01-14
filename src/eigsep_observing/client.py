import json
import logging
import threading
import yaml

from cmt_vna import VNA
import picohost

from .utils import get_config_path

logger = logging.getLogger(__name__)
default_cfg_file = get_config_path("obs_config.yaml")
with open(default_cfg_file, "r") as f:
    default_cfg = yaml.safe_load(f)


class PandaClient:

    PICO_CLASSES = {
        "imu": picohost.PicoIMU,
        "therm": picohost.PicoDevice,
        "peltier": picohost.PicoPeltier,
        "lidar": picohost.PicoDevice,
        "switch": picohost.PicoRFSwitch,
        #        "motor": picohost.PicoMotor,
    }

    def __init__(self, redis, default_cfg=default_cfg):
        """
        Client class that runs on the computer in the suspended box.
        This pulls data from connected sensors and pushes it to the
        Redis server. Moreover, it listens to control commands from the
        main computer on the ground, executes them, and reports the
        results back to the Redis.

        Parameters
        ----------
        redis : EigsepRedis
            The Redis server object to push data to and read commands
            from.

        """
        self.logger = logger
        self.redis = redis
        self.serial_timeout = 5  # serial port timeout in seconds
        self.stop_client = threading.Event()  # flag to stop the client
        cfg = self._get_cfg()  # get the current config from Redis
        # cfg = None
        if cfg is None:
            self.logger.warning(
                "No configuration found in Redis, using default config."
            )
            cfg = default_cfg.copy()
        # add pico info
        try:
            fname = cfg["pico_config_file"]
            apps = cfg["pico_app_mapping"]
            pico_cfg = self.get_pico_config(fname, app_mapping=apps)
        except Exception as e:
            self.logger.warning(
                f"Failed to read pico config file: {e}. "
                "Running without picos."
            )
            pico_cfg = {}
        # add pico config to the cfg
        cfg["picos"] = pico_cfg
        self.logger.info(f"pico config: {pico_cfg}")
        # upload config to Redis
        self.redis.upload_config(cfg, from_file=False)
        self.cfg = cfg

        # initialize the picos and VNA
        # self.motor = None
        self.peltier = None
        self._switch_nw = None
        self.switch_lock = None
        self._initialize()  # initialize the client

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
            cfg = self.redis.get_config()
        except ValueError:
            return None  # no config in Redis
        upload_time = cfg["upload_time"]
        self.logger.info(f"Using config from Redis, updated at {upload_time}.")
        return cfg

    @property
    def switch_nw(self):
        return self._switch_nw

    @switch_nw.setter
    def switch_nw(self, value):
        self._switch_nw = value
        self.redis.r.sadd("ctrl_commands", "switch")
        self.switch_lock = threading.Lock()

    def get_pico_config(self, fname, app_mapping):
        """
        Read pico configuration from the config file. This is used to
        update `cfg` and the configuration in Redis.

        Parameters
        ----------
        fname : str or Path
            The name of the pico configuration file to read from.
        app_mapping : dict
            Mapping of Pico app_id to name.

        Returns
        -------
        pico_cfg : dict
            The pico configuration dictionary read from the file. Keys
            are pico names, values are serial ports.

        """
        with open(fname, "r") as f:
            cfg = json.load(f)  # list of dicts
        pico_cfg = {}
        for dev in cfg:
            try:
                app_id = str(dev["app_id"])
                name = app_mapping[app_id]
            except KeyError:
                self.logger.warning(
                    f"Skipping pico with unknown or missing app_id, {dev}"
                )
                continue  # skip unknown app_ids
            pico_cfg[name] = dev["port"]
        return pico_cfg

    def _initialize(self):
        self.stop_client.clear()  # reset the stop flag
        self.init_picos()  # initialize picos
        if self.switch_nw is None:
            self.logger.info("no switches, no vna")
            self.vna = None
        elif self.cfg["use_vna"]:
            self.init_VNA()
        else:
            self.vna = None

        # start heartbeat thread, telling others that we are alive
        self.heartbeat_thd = threading.Thread(
            target=self._send_heartbeat,
            kwargs={"ex": 60},
            daemon=True,
        )
        self.heartbeat_thd.start()

    def reprogram(self, force=False):
        """
        Reprogram the client by stopping all threads and reinitializing
        the client. This is useful when the configuration changes.

        Parameters
        ----------
        force : bool
            If True, reprogram even if the config appears to be the
            unchanged.

        Notes
        -----
        This method interrupts all running threads, use with caution.

        """
        cfg = self.redis.get_config()
        if force or cfg != self.cfg:
            self.cfg = cfg  # update the config
            self.logger.info("Client reprogrammed.")
            return
        if not force and cfg == self.cfg:  # not force
            msg = "Configuration unchanged, skipping reprogram."
            self.logger.info(msg)
            self.redis.send_status(level=logging.INFO, status=msg)

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
            self.redis.client_heartbeat_set(ex=ex, alive=True)
            self.stop_client.wait(1.0)  # update faster than expiration
        # if we reach here, the client should stop running
        self.redis.client_heartbeat_set(alive=False)

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
            switch_network=self.switch_nw,
        )
        kwargs = self.cfg["vna_settings"].copy()
        kwargs["power_dBm"] = kwargs["power_dBm"]["ant"]
        self.logger.info(f"vna kwargs: {kwargs}")
        self.vna.setup(**kwargs)
        self.redis.r.sadd("ctrl_commands", "VNA")
        self.logger.info("VNA initialized")

    def init_picos(self):
        """
        Initialize pico readings based on the configuration in Redis.

        Notes
        -----
        Called by the constructor of the client. This method can be
        called again to reinitialize the picos if the configuration
        changes.

        """
        self.picos = {}  # pico name : pico instance
        try:
            pico_cfg = self.cfg["picos"].copy()  # name: serial port mapping
        except KeyError:
            self.logger.warning(
                "No sensor config provided, no sensors will be initialized."
            )
            return

        for name, port in pico_cfg.items():
            if name == "motor":
                self.logger.warning("Skipping motor init in client")
                continue
            self.logger.info(f"Adding sensor {name}.")
            # instantiate the pico class
            try:
                cls = self.PICO_CLASSES[name]
            except KeyError:
                self.logger.warning(
                    f"Unknown pico class {name}. "
                    "Must be in picos.PICO_CLASSES."
                )
                continue

            try:
                p = cls(
                    port,
                    timeout=self.serial_timeout,
                    name=name,
                    eig_redis=self.redis,
                )
                if p.is_connected:
                    self.picos[name] = p
                    self.redis.r.sadd("picos", name)
            except Exception as e:
                self.logger.warning(f"Failed to initialize pico {name}: {e}")
                continue  # Skip picos that fail to initialize

        if not self.picos:
            self.logger.warning("Running without pico threads.")
            return

        # create reference to switch_nw, motor, peltier if they exist
        self.switch_nw = self.picos.get("switch", None)
        # self.motor = self.picos.get("motor", None)
        self.peltier = self.picos.get("peltier", None)

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
        if self.switch_nw is None:
            self.logger.warning(
                "Switch network not initialized. Cannot execute "
                "switching commands."
            )
            return
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
        elif any(k not in self.switch_nw.path_str for k in schedule):
            self.logger.warning(
                "Invalid switch keys found in schedule. Cannot execute "
                "switching commands. Schedule keys must be in: "
                f"{list(self.switch_nw.path_str.keys())}."
            )
            return
        # Validate that all wait_time values are positive numbers
        for mode, wait_time in schedule.items:
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
                schedule.pop(mode)
        while not self.stop_client.is_set():
            for mode, wait_time in schedule.items():
                if mode == "RFANT":
                    with self.switch_lock:
                        self.logger.info(f"Switching to {mode} measurements")
                        self.switch_nw.switch(mode)
                    # release the lock during sky wait time
                    if self.stop_client.wait(wait_time):
                        self.logger.info("Switching stopped by event")
                        return
                else:
                    with self.switch_lock:
                        self.logger.info(f"Switching to {mode} measurements")
                        self.switch_nw.switch(mode)
                        if self.stop_client.wait(wait_time):  # wait with stop
                            self.logger.info("Switching stopped by event")
                            return

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
            If the switch network is not initialized or the VNA is not
            initialized.

        Notes
        -----
        This function does all the switching needed for the VNA
        measurement, including to OSL calibrators. There's no option to
        skip the calibration.

        """
        if mode not in ["ant", "rec"]:
            raise ValueError(
                f"Unknown VNA mode: {mode}. Must be 'ant' or 'rec'."
            )
        if self.switch_nw is None:
            raise RuntimeError(
                "Switch network not initialized. Cannot execute "
                "VNA commands."
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
        metadata = self.redis.get_live_metadata()
        self.redis.add_vna_data(s11, header=header, metadata=metadata)
        self.logger.info("Vna data added to redis")

    def vna_loop(self):
        """
        Observe with VNA and write data to files.
        """
        while self.vna is None:
            self.logger.warning(
                "VNA not initialized. Cannot execute VNA commands."
            )
            threading.Event().wait(5)  # wait for VNA to be initialized
        while not self.stop_client.is_set():
            with self.switch_lock:
                try:
                    sw_status = self.redis.get_live_metadata(keys="rfswitch")
                except Exception as e:
                    self.logger.warning(
                        f"Failed to get switch status from Redis: {e}. "
                    )
                    sw_status = {}
                # default to RFANT if not found
                prev_mode = sw_status.get("sw_state", "RFANT")
                for mode in ["ant", "rec"]:
                    self.logger.info(f"Measuring S11 of {mode} with VNA")
                    self.measure_s11(mode)
                # restore previous mode
                self.logger.info(
                    f"Switching back to previous mode: {prev_mode}"
                )
                self.switch_nw.switch(prev_mode)
            # wait for the next iteration
            self.stop_client.wait(self.cfg["vna_interval"])

    # XXX
    def rotate_motors(self):
        raise NotImplementedError

    def read_ctrl(self):
        """
        Read control commands from Redis.

        Notes
        -----
        This method should be called in a loop to continuously
        monitor for commands. If an exception occurs while reading
        or executing the command, it is logged and an error message
        is sent back to Redis. This prevents users from connecting
        to the client and crashing it with invalid commands.

        """
        try:
            msg = self.redis.read_ctrl(timeout=0.1)
        except TypeError as e:
            err = f"Error reading control command: {e}"
            self.logger.error(err)
            self.redis.send_status(level=logging.ERROR, status=err)
            return

        if msg is None:
            return

        cmd, kwargs = msg
        if cmd is None:
            return  # no command to execute

        self.logger.info(f"Received control message: {msg}")

        if cmd == "ctrl:reprogram":
            self.logger.warning("Reprogramming client.")
            self.reprogram(**kwargs)
            self.redis.send_status(
                level=logging.INFO, status="Client reprogrammed."
            )
        elif cmd in self.redis.switch_commands:
            if self.switch_nw is None:
                err = (
                    "Switch network not initialized. Cannot execute "
                    "switch commands."
                )
                self.logger.error(err)
                self.redis.send_status(level=logging.ERROR, status=err)
                return
            mode = cmd.split(":")[1]
            with self.switch_lock:
                self.logger.info(f"Switching to {mode} measurements")
                self.switch_nw.switch(mode)
        elif cmd in self.redis.vna_commands:
            mode = cmd.split(":")[1]
            with self.switch_lock:
                try:
                    self.measure_s11(mode)
                except (ValueError, RuntimeError) as e:
                    err = f"Error executing VNA command {cmd}: {e}"
                    self.logger.error(err)
                    self.redis.send_status(level=logging.ERROR, status=err)
        else:
            err = f"Unknown command: {msg=}, {cmd=}, {kwargs=}. "
            self.logger.error(err)
            self.redis.send_status(level=logging.ERROR, status=err)

    def ctrl_loop(self):
        """
        Control loop that reads commands from Redis and executes them.
        This method runs in a separate thread.

        """
        while not self.stop_client.is_set():
            self.read_ctrl()
