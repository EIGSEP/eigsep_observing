from queue import Queue
from threading import Thread
import time

import serial


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

        Raises
        ------
        ValueError
            If the serial port cannot be opened.

        """
        self.name = name
        self.queue = Queue()
        try:
            self.serial = serial.Serial(
                port=serial_port, baudrate=115200, timeout=10
            )
        except serial.SerialException as e:
            raise ValueError(
                f"Could not open serial port {serial_port}: {e}"
            ) from e

    def grab_data(self):
        """
        Read data from the sensor and put to queue. This method should
        be implemented by subclasses.

        """
        raise NotImplementedError("Subclasses should implement this method.")

    def read(self, redis, sleep=0):
        """
        Read sensor data from queue and push it to Redis.

        Parameters
        ----------
        redis : EigsepRedis
            Redis client instance.
        sleep : float
            Sleep time between reads in seconds.

        """
        thd = Thread(target=self.grab_data, daemon=True)
        thd.start()
        while True:
            if self.queue.empty():
                continue
            data = self.queue.get()
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
