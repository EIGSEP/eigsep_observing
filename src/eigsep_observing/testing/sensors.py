import json

from .. import sensors


class DummySensor(sensors.Sensor):

    def __init__(
        self, name="dummy_sensor", port="/dev/dummy_sensor", timeout=1
    ):
        super().__init__(name, port, timeout=timeout)
        self._data = 0  # fake data
        if self.name not in sensors.SENSOR_CLASSES:
            sensors.SENSOR_CLASSES[self.name] = DummySensor

    def from_sensor(self):
        self._data += 1
        return json.dumps(f"data: {self._data}")
