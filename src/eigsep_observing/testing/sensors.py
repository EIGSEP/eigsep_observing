from queue import Queue

from .. import sensors


# XXX need to mock up sensor to run w/o pico
class DummySensor(sensors.Sensor):

    def __init__(self, name="dummy_sensor", serial_port="/dev/dummy_sensor"):
        self.name = name
        self.serial_port = serial_port
        self.queue = Queue()
        if self.name not in sensors.SENSOR_CLASSES:
            sensors.SENSOR_CLASSES[self.name] = DummySensor
