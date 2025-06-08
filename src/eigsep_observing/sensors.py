from abc import ABC, abstractmethod
from queue import Queue
from threading import Thread
import time

import eigsep_sensors as eig_sensors


class Sensor(ABC):
    """
    Base class for sensors. This class provides a template for
    reading data from sensors and pushing it redis so it can be
    saved as metadata. Subclasses should implement the
    `from_sensor` method to read data from the specific sensor.

    """

    @abstractmethod
    def __init__(self, name, port, timeout=10, **kwargs):
        """
        Initialize the Sensor class. This method should be implemented
        by subclasses to initialize the sensor.

        Parameters
        ----------
        name : str
            Name of the sensor.
        port : str
            Serial port to which the sensor is connected.
        timeout : float
            Timeout for serial communication in seconds.
        **kwargs : dict
            Additional keyword arguments for sensor-specific initialization.

        """
        self.name = name
        self.queue = Queue()

    @abstractmethod
    def from_sensor(self):
        """
        Read data from the sensor. This method must be implemented by
        subclasses.

        Returns
        -------
        str
            JSON string representing the sensor data.

        Notes
        -----
        This method is expected to do one single read from the sensor.
        It should be non-blocking and return immediately after reading
        the data.

        """

    def _queue_data(self, cadence):
        """
        Read data from the sensor and put it into the queue.
        This method runs in a separate thread and should be called
        by the `read' method.

        Parameters
        ----------
        cadence : int
            Number of seconds between reads.

        Notes
        -----
        This method is expected to run continuously and should not
        block. It should read data from the sensor and put it into
        the queue.

        """
        while True:
            data = self.from_sensor()
            if data is not None:
                self.queue.put(data)
            time.sleep(cadence)

    def read(self, redis, cadence=5):
        """
        Read sensor data from queue and push it to Redis.

        Parameters
        ----------
        redis : EigsepRedis
            Redis client instance.
        cadence : float
            Sleep time between reads in seconds.

        """
        sleep_time = cadence / 2  # to avoid busy waiting
        thd = Thread(target=self._queue_data, args=(cadence,), daemon=True)
        thd.start()
        while True:
            if self.queue.empty():
                continue
            data = self.queue.get()
            redis.add_metadata(self.name, data)
            time.sleep(sleep_time)


class ImuSensor(Sensor):

    def __init__(self, name, port, timeout=10):
        pass

    def from_sensor(self):
        return


class ThermSensor(Sensor):

    def __init__(self, name, port, timeout=10):
        """
        Initialize the ThermSensor class. This is a subclass of Sensor
        and adds an instance of the Thermistor class from
        eigsep_sensors.

        Parameters
        ----------
        name : str
            Name of the sensor.
        port : str
            Serial port to which the thermistor is connected.
        timeout : float
            Timeout for serial communication in seconds.

        """
        super().__init__(name, port, timeout=timeout)
        self.thermistor = eig_sensors.Thermistor(port, timeout=timeout)

    def from_sensor(self):
        """
        Read temperature from the thermistor.

        Returns
        -------
        str
            JSON string representing the temperature data. The JSON
            string is a dictionary where keys are ADC pin numbers and
            values are the associated temperatures in degrees Celsius.

        """
        return json.dumps(self.thermistor.read_temperature())


class PeltierSensor(Sensor):

    def __init__(self, name, port, timeout=10):
        pass

    def from_sensor(self):
        return


class LidarSensor(Sensor):

    def __init__(self, name, port, timeout=10):
        pass

    def from_sensor(self):
        return


SENSOR_CLASSES = {
    "imu_az": ImuSensor,
    "imu_el": ImuSensor,
    "therm": ThermSensor,
    "peltier": PeltierSensor,
    "lidar": LidarSensor,
}
