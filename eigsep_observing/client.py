import logging
import threading
import time

from . import io

class PandaClient:

    def __init__(
        self,
        redis,
        switch_nw=None,
        logger=None,
    ):
        """
        Client class that runs on the computer in the suspended box. This
        pulls data from connected sensors and pushes it to the Redis server.
        Moreover, it listens to control commands from the main computer on
        the ground, executes them, and reports the results back to the Redis.

        Parameters
        ----------
        redis : EigsepRedis
            The Redis server object to push data to and read commands from.
        switch_nw : switch_network.SwitchNetwork
            The switch network object to control the switches. Needed for
            switching during VNA measurements.

        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
        self.logger = logger
        self.redis = redis
        self.sensors = {}  # key: sensor name, value: (sensor, thread)
        self.switch_nw = switch_nw
        self.vna = None

    def add_sensor(self, sensor):
        """
        Add a sensor to the client. Spawns a thread that reads data from the
        sensor and pushes to redis.

        Parameters
        ----------
        sensor : Sensor
            The sensor to add.

        """
        if sensor.name in self.sensors:
            self.logger.warning(f"Sensor {sensor.name} already added.")
            return
        # XXX add args/kwargs if needed
        thd = threading.Thread(
            target=sensor.read, args=(self.redis), daemon=True
        )
        thd.start()
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
        osl_s11 = self.vna.measure_osl(snw=self.switch_nw)
        if mode == "ant":
            s11 = self.vna.measure_ant(measure_noise=True)
        else:  # mode is rec
            s11 = self.vna.measure_rec()
        # XXX need to save to file here, osl_s11 and s11 are dictionaries
        # something like:
        # fname: save_dir + mode + date + time + ".s11"
        # header containing vna metadata, vna.metadata
        # XXX header needs to also pull the box orientation
        #io.write_s11_file(fname, data, header, cal_data=osl_s11)
        raise NotImplementedError("No file save yet")

    def read_ctrl(self):
        """
        Read commands that set switching and S11 observing. Executes the
        commands and sends acknowledgements back to the Redis server.

        Notes
        -----
        Commands received are strings, either containing ``switch'' or ``VNA''.
        The former indicates a switch command, the latter indicates observing
        with the VNA.

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
