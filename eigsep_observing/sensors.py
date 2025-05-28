import numpy as np
import time

# serial port names for the pico sensors
PICO_IDS = {
    "imu_az": "/dev/pico_imu_az",
    "imu_el": "/dev/pico_imu_el",
    "therm_load": "/dev/pico_therm_load",
    "therm_lna": "/dev/pico_therm_lna",
    "therm_vna_load": "/dev/pico_therm_vna_load",
    "peltier": "/dev/pico_peltier",
    "lidar": "/dev/pico_lidar",
    "switches": "/dev/pico_switch",
}


class Sensor:

    def __init__(self, name, serial_port):
        """
        Initialize the Sensor class.

        Parameters
        ----------
        name : str
            Name of the sensor.
        serial_port : str
            Serial port name for the sensor.

        """
        self.name = name
        self.serial_port = serial_port

    def grab_data(self):
        """
        Read data from the sensor. This method should be implemented by
        subclasses.

        Returns
        -------
        data : dict
            Dictionary containing the sensor data.

        """
        raise NotImplementedError("Subclasses should implement this method.")

    def read(self, redis, sleep=0):
        """
        Read sensor data and push it to Redis. Note that subclasses should
        implement the method ``grab_data`` to read the data from the sensor.

        Parameters
        ----------
        redis : EigsepRedis
            Redis client instance.
        sleep : float
            Sleep time between reads in seconds.

        """
        while True:
            data = self.grab_data()
            redis.add_metadata(self.name, data)
            time.sleep(sleep)
