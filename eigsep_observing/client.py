import logging
from pathlib import Path
import threading
import time

from cmt_vna import VNA
from switch_network import SwitchNetwork

from . import io, sensors


class PandaClient:

    def __init__(self, redis, mnt_path=Path("/mnt/rpi"), logger=None):
        """
        Client class that runs on the computer in the suspended box. This
        pulls data from connected sensors and pushes it to the Redis server.
        Moreover, it listens to control commands from the main computer on
        the ground, executes them, and reports the results back to the Redis.

        Parameters
        ----------
        redis : EigsepRedis
            The Redis server object to push data to and read commands from.
        mnt_path : Path or str
            The path where the Raspberry Pi is mounted. This is used to
            save files on the Raspberry Pi.

        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
        self.logger = logger
        self.redis = redis
        self.mnt_path = Path(mnt_path).resolve()
        self.sensors = {}  # key: sensor name, value: (sensor, thread)
        self.switch_nw = None
        self.vna = None

        try:
            self.read_init_commands()
        except TimeoutError:
            self.logger.error(
                "No initialization commands received within the timeout."
                "Check Redis connection."
            )
            raise

        if self.sensors:
            self.logger.info(f"Starting {len(self.sensors)} sensor threads.")
            for sensor, thd in self.sensors.values():
                thd.start()

    def read_init_commands(self, timeout=60):
        """
        Read initialization commands from Redis. This is used to set up the
        switch network and sensors before starting the main loop.

        Parameters
        ----------
        timeout : int
            The maximum time to wait for initialization commands in seconds.
            Default is 60 seconds.

        Raises
        ------
        TimeoutError
            If no initialization commands are received within the timeout.

        """
        time_start = time.time()
        while True:
            if time.time() - time_start > timeout:
                raise TimeoutError(
                    "No initialization commands received within the timeout."
                )
            entry_id, msg = self.redis.read_ctrl()
            if entry_id is None:  # no message
                self.logger.debug("No message received. Waiting.")
                time.sleep(1)
                continue
            if msg is None:  # invalid message
                self.logger.warning("Invalid message received.")
                continue
            cmd, pico_ids = msg
            if cmd not in self.redis.init_commands:
                self.logger.warning(f"Unknown command: {cmd}")
                continue
        # pico_ids a dictionary
        switch_pico = pico_ids.pop("switch_pico", None)
        if switch_pico is not None:
            self.logger.info(
                f"Initializing switch network with pico {switch_pico}."
            )
            # uses default gpios, paths, timeout
            self.switch_nw = SwitchNetwork(
                serport=switch_pico, logger=self.logger, redis=self.redis
            )
        else:
            self.logger.warning(
                "No switch pico provided. No switch network initialized."
            )
        for sensor_name, sensor_pico in pico_ids.items():
            self.logger.info(
                f"Adding sensor {sensor_name} with pico {sensor_pico}."
            )
            self.add_sensor(sensor_name, sensor_pico)

    def add_sensor(self, sensor_name, sensor_pico, sleep_time=1):
        """
        Add a sensor to the client. Spawns a thread that reads data from the
        sensor and pushes to redis.

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
            self.logger.error(
                f"Unknown sensor name: {sensor_name}. "
                "Must be in sensors.SENSOR_CLASSES."
            )
            return
        sensor = sensor_cls(sensor_name, sensor_pico)
        if sensor.name in self.sensors:
            self.logger.warning(f"Sensor {sensor.name} already added.")
            return
        thd = threading.Thread(
            target=sensor.read,
            args=(self.redis),
            kwargs={"sleep": sleep_time},
            daemon=True,
        )
        self.sensors[sensor.name] = (sensor, thd)

    @property
    def vna_initialized(self):
        return self.vna is not None

    def measure_s11(self, mode, **kwargs):
        """
        Measure S11 with the VNA and write the results to file. The directory
        where the results are saved is set by the ``save_dir'' attribute of
        the VNA instance.

        Parameters
        ----------
        mode : str
            The mode of operation, either ``ant'' for antenna or ``rec'' for
            receiver.
        kwargs : dict
            Additional keyword arguments for the VNA measurement. Passed to
            the VNA setup method.

        Raises
        ------
        ValueError
            If the mode is not ``ant'' or ``rec''.

        Notes
        -----
        This function does all the switching needed for the VNA measurement,
        including to OSL calibrators. There's no option to skip the
        calibration.

        """
        if mode not in ["ant", "rec"]:
            raise ValueError(
                f"Unknown VNA mode: {mode}. Must be 'ant' or 'rec'."
            )
        ip = kwargs.pop("ip", None)
        port = kwargs.pop("port", None)
        timeout = kwargs.pop("timeout", None)
        save_dir = kwargs.pop("save_dir", None)
        if not self.vna_initialized:
            self.logger.info("VNA not initialized. Initializing now.")
            vna = VNA(
                ip=ip,
                port=port,
                timeout=timeout,
                save_dir=save_dir,
            )
            self.vna = vna
        setup_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        _ = self.vna.setup(**setup_kwargs)
        osl_s11 = self.vna.measure_OSL(snw=self.switch_nw)
        if mode == "ant":
            s11 = self.vna.measure_ant(measure_noise=True)
        else:  # mode is rec
            s11 = self.vna.measure_rec()
        # make sure save_dir is on the Raspberry Pi, not on the client
        panda_path = Path(self.vna.save_dir) / Path(mode)
        save_dir = io.to_remote_path(panda_path, mnt_path=self.mnt_path)
        header = self.vna.header
        header["mode"] = mode
        metadata = self.redis.get_header()
        io.write_s11_file(
            s11,
            header,
            metadata=metadata,
            cal_data=osl_s11,
            fname=None,
            save_dir=save_dir,
        )

    def read_ctrl(self):
        """
        Read control commands from Redis. Executes the
        commands and sends acknowledgements back to the Redis server.

        """
        while True:
            entry_id, msg = self.redis.read_ctrl()
            if entry_id is None:  # no message
                self.logger.debug("No message received. Waiting.")
                time.sleep(1)
                continue
            if msg is None:  # invalid message
                self.logger.warning("Invalid message received.")
                continue
            cmd, kwargs = msg
            if cmd in self.redis.switch_commands:
                if self.switch_nw is None:
                    raise RuntimeError(
                        "Switch network not initialized. Cannot execute "
                        "switch commands."
                    )
                mode = cmd.split(":")[1]
                path = self.switch_nw.paths[mode]
                self.switch_nw.switch(path)
            elif cmd in self.redis.vna_commands:
                mode = cmd.split(":")[1]
                try:
                    self.measure_s11(mode, **kwargs)
                except ValueError:
                    self.logger.warning(f"Unknown VNA mode: {mode}")
                    self.redis.send_vna_error()
                    continue
                self.redis.send_vna_complete()
            else:
                self.logger.warning(f"Unknown command: {cmd}")
                continue
