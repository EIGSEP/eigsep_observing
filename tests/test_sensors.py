import json
import pytest

from eigsep_observing import sensors
from eigsep_observing.testing import DummyEigsepRedis, DummySensor


@pytest.fixture
def dummy_sensor():
    return DummySensor()


@pytest.fixture
def redis():
    return DummyEigsepRedis()


def test_init():
    name = "dummy_sensor"
    port = "/dev/dummy_sensor"
    with pytest.raises(TypeError):  # can't init abstract class
        sensors.Sensor(name, port)
    dummy = DummySensor(name=name, port=port)
    assert dummy.name == name

# make a subclass of Sensor that does not implement from_sensor
class NoFromSensor(sensors.Sensor):
    def __init__(self, name="", port=""):
        super().__init__(name, port)


def test_no_from_sensor():
    with pytest.raises(TypeError):
        NoFromSensor()


@pytest.mark.skip(reason="Not implemented yet")
def test_read():
    pass


@pytest.mark.skip(reason="Not implemented yet")
def test_from_sensor():
    pass
