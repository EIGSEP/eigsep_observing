from abc import ABC, abstractmethod
import json
import serial
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
        by subclasses to initialize the sensor. Note that subclasses
        should raise a RuntimeError if there is an issue connecting
        to the sensor on the specified port.

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
        It should blocking.

        """

    def read(self, redis, stop_event):
        """
        Read sensor data and push it to Redis.

        Parameters
        ----------
        redis : EigsepRedis
            Redis client instance.
        stop_event : threading.Event
            Event to signal when to stop reading data.
        
        """
        while not stop_event.is_set():
            data = self.from_sensor()  # blocking call
            redis.add_metadata(self.name, data)

class ImuSensor(Sensor):

    def __init__(self, name, port, timeout=10):
        """
        Initialize the ImuSensor class. This is a subclass of Sensor
        and adds an instance of the IMU_BN0085 class from
        eigsep_sensors.

        Parameters
        ----------
        name : str
            Name of the sensor.
        port : str
            Serial port to which the IMU is connected.
        timeout : float
            Timeout for serial communication in seconds.

        Raises
        -------
        RuntimeError
            If there is an issue connecting to the IMU on the specified
            port.

        """
        super().__init__(name, port, timeout=timeout)
        try:
            self.imu = eig_sensors.IMU_BNO085(port, timeout=timeout)
        except serial.SerialException as e:
            raise RuntimeError(
                f"Failed to connect to IMU on port {port}: {e}"
            ) from e

    def from_sensor(self):  # XXX what keys are in the JSON?
        """
        Read data from the IMU.

        Returns
        -------
        str
            JSON string representing the IMU data. The JSON string is a
            dictionary with keys ???.

        """
        return json.dumps(self.imu.read_imu())


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

        Raises
        -------
        RuntimeError
            If there is an issue connecting to the thermistor on the
            specified port.

        """
        super().__init__(name, port, timeout=timeout)
        try:
            self.thermistor = eig_sensors.Thermistor(port, timeout=timeout)
        except serial.SerialException as e:
            raise RuntimeError(
                f"Failed to connect to thermistor on port {port}: {e}"
            ) from e

    def from_sensor(self):
        """
        Read temperature from the thermistor. This is blocking, as
        expected by the base class.

        Returns
        -------
        str
            JSON string representing the temperature data. The JSON
            string is a dictionary where keys are ADC pin numbers and
            values are the associated temperatures in degrees Celsius.

        """
        d = self.thermistor.read_temperature()
        if d is None:
            payload = {"data": None, "status": "TIMEOUT"}
        else:
            payload = {"data": d, "status": "OK"}
        return json.dumps(payload)


class PeltierSensor(Sensor):

    def __init__(self, name, port, timeout=10):
        super().__init__(name, port, timeout=timeout)

    def from_sensor(self):
        return


class LidarSensor(Sensor):

    def __init__(self, name, port, timeout=10):
        super().__init__(name, port, timeout=timeout)

    def from_sensor(self):
        return


SENSOR_CLASSES = {
    "imu_az": ImuSensor,
    "imu_el": ImuSensor,
    "therm": ThermSensor,
    "peltier": PeltierSensor,
    "lidar": LidarSensor,
}
