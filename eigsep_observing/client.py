import logging

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
            The switch network object to control the switches.

        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
        self.logger = logger
        self.redis = redis
        self.sensors = {}  # key: sensor name, value: (sensor, thread)
        self.switch_nw = switch_nw

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
                mode = cmd.split(":")[1]
                path = self.switch_nw.paths[mode]
                self.switch_nw.switch(path, update_redis=True)
            # XXX this need to also do all switching for VNA (in the elif)
            elif cmd in self.redis.vna_commands:
                mode = cmd.split(":")[1]
                if mode == "ant":
                    # XXX calibrate osl, measure ant/noise at 0 dBm
                    # basically just get the flags for the vna script
                    # flags = some stuff
                elif mode == "rec":
                    # XXX calibrate osl, measure rec at 0 dBm
                    # falgs = some stuff
                else:
                    self.logger.warning(f"Unknown VNA mode: {mode}")
                    continue
                # XXX
                # subrpocess.run(VNA SCRIPT, flags)
                # make sure it's blocking
                # XXX send VNA COMPLETED to redis
                # also where to put the data? XXX
            else:
                self.logger.warning(f"Unknown command: {cmd}")
                continue



