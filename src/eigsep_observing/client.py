import logging
import queue
import threading

from eigsep_corr.config import load_config
from cmt_vna import VNA
import picohost

from .utils import get_config_path

logger = logging.getLogger(__name__)
default_cfg_file = get_config_path("obs_config.yaml")
default_cfg = load_config(default_cfg_file, compute_inttime=False)


class PandaClient:

    PICO_CLASSES = {
        "imu": picohost.PicoDevice,
        "therm": picohost.PicoDevice,
        "peltier": picohost.PicoPeltier,
        "lidar": picohost.PicoDevice,
        "switch": picohost.PicoRFSwitch,
        "motor": picohost.PicoMotor,
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
        self.init_picos()  # initialize picos
        if self.switch_nw is None:
            self.vna = None
        else:
            self.redis.r.sadd("ctrl_commands", "switch")
            if self.cfg["use_vna"]:
                self.init_VNA()

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

    def metadata_pusher(self, queue):
        """
        Push metadata from the pico queue to Redis.
        This method runs in a separate thread and continuously reads
        metadata from the queue and sends it to Redis.

        Parameters
        ----------
        queue : queue.Queue
            The queue from which to read pico metadata. All picos
            should push their metadata to this queue.

        """
        while not self.stop_client.is_set():
            pico_name, metadata = queue.get()
            self.redis.add_metadata(pico_name, metadata)

    @staticmethod
    def _pico_response_handler(queue, name):
        """
        Handle responses from the pico devices and push them to the
        provided queue.

        Parameters
        ----------
        queue : queue.Queue
            The queue to which the pico data should be pushed.
        name : str
            The name of the pico device.

        Returns
        -------
        handler : callable
            A function that can be used as a response handler for the
            pico device. It takes the data from the pico and pushes it
            to the queue.

        """

        def handler(data):
            """
            Handle data from the pico device and push it to the queue.
            """
            try:
                queue.put_nowait((name, data))
            except queue.Full:
                logging.warning(
                    f"Queue is full, dropping data from {name}: {data}"
                )
                try:
                    queue.get_nowait()  # remove oldest item
                except queue.Empty:
                    pass
                queue.put_nowait((name, data))

        return handler

    def init_picos(self):
        """
        Initialize pico readings based on the configuration in Redis.
        """
        # queue for pico readings
        pico_queue = queue.Queue(maxsize=1000)
        self.metadata_thd = threading.Thread(
            target=self.metadata_pusher,
            args=(pico_queue,),
            daemon=True,
        )

        self.picos = {}  # pico name : pico instance
        try:
            pico_cfg = self.cfg["picos"]  # name: serial port mapping
        except KeyError:
            self.logger.warning(
                "No sensor config provided, no sensors will be initialized."
            )
            return

        for name, port in pico_cfg.items():
            self.logger.info(f"Adding sensor {name}.")
            # instantiate the pico class
            try:
                cls = self.PICO_CLASSES[name]
            except KeyError:
                self.logger.warning(
                    f"Unknown pico class {name}. "
                    "Must be in picos.PICO_CLASSES."
                )
            p = cls(port, timeout=self.serial_timeout)
            p.set_response_handler(
                self._pico_response_handler(pico_queue, name)
            )
            if p.connect():
                self.picos[name] = p
                self.redis.r.sadd("picos", name)

        if not self.picos:
            self.logger.warning("Running without pico threads.")
            return

        self.metadata_thd.start()
        for name, p in self.picos.items():
            self.logger.debug(f"Starting pico {name} thread.")
            p.start()

        # create reference to switch_nw, motor, peltier if they exist
        self.switch_nw = self.picos.get("switch", None)
        self.motor = self.picos.get("motor", None)
        self.peltier = self.picos.get("peltier", None)

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
            self.switch_nw.switch(mode)
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
