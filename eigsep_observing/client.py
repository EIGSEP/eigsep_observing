import logging
from . import EigsepRedis

class PandaClient:

    def __init__(self, host_ip="10.10.10.10", port=None, logger=None):
        """
        Client class that runs on the computer in the suspended box. This
        pulls data from connected sensors and pushes it to the Redis server.
        Moreover, it listens to control commands from the main computer on
        the ground, executes them, and reports the results back to the Redis.

        Parameters
        ----------
        host_ip : str
            The IP address of the Redis server.
        port : int
            Port number for Redis server. Only specified if not the default.

        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
        self.logger = logger
        redis_kwargs = {"host": host_ip}
        if port is not None:
            redis_kwargs["port"] = port
        self.redis = EigsepRedis(**redis_kwargs)
        self.sensors = {}  # key: sensor name, value: (sensor, thread)

    def add_sensor(self, sensor):
        """
        Add a sensor to the client. Spwans a thread that reads data from the
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
