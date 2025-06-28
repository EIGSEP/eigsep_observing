import logging
import threading

from eigsep_corr.config import load_config
from cmt_vna import VNA
from switch_network import SwitchNetwork

from . import sensors
from .utils import get_config_path

logger = logging.getLogger(__name__)
default_cfg_file = get_config_path("obs_config.yaml")
default_cfg = load_config(default_cfg_file, compute_inttime=False)


class PandaClient:

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
        try:
            self.cfg = self.redis.get_config()
            upload_time = self.cfg["upload_time"]
            self.logger.info(
                f"Using config from Redis, updated at {upload_time}."
            )
        except ValueError:
            self.logger.warning(
                "Missing configuration in Redis, using default."
            )
            self.cfg = default_cfg
            self.redis.upload_config(self.cfg, from_file=False)
        self._initialize()  # initialize the client

    def _initialize(self):
        self.stop_client.clear()  # reset the stop flag
        self.init_switch_network()
        if self.cfg["use_vna"] and self.switch_nw is not None:
            self.init_VNA()
        else:
            self.vna = None
        self.init_sensors()

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
        if not force:  # check if the config has changed
            if cfg == self.cfg:
                msg = "Configuration unchanged, skipping reprogram."
                self.logger.info(msg)
                self.redis.send_status(level=logging.INFO, status=msg)
                return
            self.logger.info("Configuration changed, reprogramming client.")
            self.cfg = cfg

        self.stop_client.set()  # stop all threads
        # wait for all threads to finish
        self.heartbeat_thd.join()
        for sensor, thd in self.sensors.values():
            thd.join()
        self._initialize()  # reinitialize the client

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
            self.stop_client.wait(ex / 2)  # update faster than expiration
        # if we reach here, the client should stop running
        self.redis.client_heartbeat_set(alive=False)

    def init_switch_network(self):
        """
        Initialize the switch network, using the serial port and GPIO
        settings from the Redis configuration.

        Notes
        -----
        This methods overrides the attribute ``switch_nw`` with the
        initialized SwitchNetwork instance. If no cofiguration is
        provided or there is an error, ``switch_nw`` will remain None.

        """
        switch_pico = self.cfg.get("switch_pico", None)
        if switch_pico is None:
            self.logger.warning(
                "No switch pico provided in configuration. "
                "Switch network will not be initialized."
            )
            return
        try:
            self.switch_nw = SwitchNetwork(
                serport=switch_pico,
                logger=self.logger,
                redis=self.redis,
            )
        except ValueError as e:
            err = (
                f"Failed to initialize switch network: {e}. "
                "Check the serial port and GPIO settings."
            )
            self.logger.error(err)
            self.redis.send_status(level=logging.ERROR, status=err)
            self.switch_nw = None
        self.logger.info(f"Switch network initialized with {switch_pico}.")
        self.redis.r.sadd("ctrl_commands", "switch")

    def init_VNA(self):
        """
        Initialize the VNA instance using the configuration from Redis.
        """
        self.vna = VNA(
            ip=self.cfg["vna_ip"],
            port=self.cfg["vna_port"],
            timeout=self.cfg["vna_timeout"],
            save_dir=self.cfg["vna_save_dir"],
            switch_network=self.switch_nw,
        )
        self.redis.r.sadd("ctrl_commands", "VNA")

    def init_sensors(self):
        """
        Initialize sensors based on the configuration in Redis. This
        creates sensor instances and starts threads to read data from
        the sensors. It also adds a list of sensor names to redis
        under the key 'sensors'.
        """
        self.sensors = {}  # key: sensor name, value: (sensor, thread)
        sensor_picos = self.cfg.get("sensor_picos", {})
        if not sensor_picos:
            self.logger.warning(
                "No sensor picos provided in configuration. "
                "No sensors will be initialized."
            )
            return
        for sensor_name, sensor_pico in sensor_picos.items():
            self.logger.info(
                f"Adding sensor {sensor_name} with pico {sensor_pico}."
            )
            self.add_sensor(sensor_name, sensor_pico)

        if self.sensors:
            self.logger.info(f"Starting {len(self.sensors)} sensor threads.")
            for sensor, thd in self.sensors.values():
                thd.start()
                self.redis.r.sadd("sensors", sensor.name)

    def add_sensor(self, sensor_name, sensor_pico, sleep_time=1):
        """
        Add a sensor to the client. Spawns a thread that reads data
        from the sensor and pushes to redis.

        Parameters
        ----------
        sensor_name : str
            Name of the sensor. Must be in sensors.SENSOR_CLASSES.
        sensor_pico : str
            Serial port of the pico that controls the sensor.
        sleep_time : float
            The time to sleep between reads from the sensor. Default is 1
            second.

        """
        try:
            sensor_cls = sensors.SENSOR_CLASSES[sensor_name]
        except KeyError:
            self.logger.warning(
                f"Unknown sensor name: {sensor_name}. "
                "Must be in sensors.SENSOR_CLASSES."
            )
            return
        try:
            sensor = sensor_cls(
                sensor_name, sensor_pico, timeout=self.serial_timeout
            )
        except RuntimeError as e:
            self.logger.error(
                f"Failed to initialize sensor {sensor_name}: {e}. "
                "Check the serial port and GPIO settings."
            )
            return
        if sensor.name in self.sensors:
            self.logger.warning(f"Sensor {sensor.name} already added.")
            return
        thd = threading.Thread(
            target=sensor.read,
            args=(self.redis, self.stop_client),
            kwargs={"cadence": sleep_time},
            daemon=True,
        )
        self.sensors[sensor.name] = (sensor, thd)

    def measure_s11(self, mode, **kwargs):
        """
        Measure S11 with the VNA and write the results to file. The
        directory where the results are saved is set by the
        ``save_dir'' attribute of the VNA instance.

        Parameters
        ----------
        mode : str
            The mode of operation, either 'ant' for antenna or 'rec'
            for receiver.
        kwargs : dict
            Additional keyword arguments for the VNA measurement.
            Passed to the VNA setup method.

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

        setup_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        _ = self.vna.setup(**setup_kwargs)

        osl_s11 = self.vna.measure_OSL()

        if mode == "ant":
            s11 = self.vna.measure_ant(measure_noise=True)
        else:  # mode is rec
            s11 = self.vna.measure_rec()

        header = self.vna.header
        header["mode"] = mode
        metadata = self.redis.get_metadata()
        self.redis.send_vna_data(
            s11, cal_data=osl_s11, header=header, metadata=metadata
        )

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
            msg = self.redis.read_ctrl()
        except TypeError as e:
            err = f"Error reading control command: {e}"
            self.logger.error(err)
            self.redis.send_status(level=logging.ERROR, status=err)
            return

        cmd, kwargs = msg
        if cmd == "ctrl:reprogram":
            self.logger.info("Reprogramming client.")
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
            self.switch_nw.switch(mode, verify=True)
        elif cmd in self.redis.vna_commands:
            mode = cmd.split(":")[1]
            try:
                self.measure_s11(mode, **kwargs)
            except (ValueError, RuntimeError) as e:
                err = f"Error executing VNA command {cmd}: {e}"
                self.logger.error(err)
                self.redis.send_status(level=logging.ERROR, status=err)
        else:
            err = f"Unknown command: {cmd}"
            self.logger.error(err)
            self.redis.send_status(level=logging.ERROR, status=err)
