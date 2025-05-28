import time


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


class ImuSensor(Sensor):

    def grab_data(self):
        return


class ThermSensor(Sensor):

    def grab_data(self):
        return


class PeltierSensor(Sensor):

    def grab_data(self):
        return


class LidarSensor(Sensor):

    def grab_data(self):
        return


SENSOR_CLASSES = {
    "imu_az": ImuSensor,
    "imu_el": ImuSensor,
    "therm_load": ThermSensor,
    "therm_lna": ThermSensor,
    "therm_vna_load": ThermSensor,
    "peltier": PeltierSensor,
    "lidar": LidarSensor,
}
