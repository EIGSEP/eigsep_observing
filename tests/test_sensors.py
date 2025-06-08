import json
import pytest
from threading import Thread

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
    # init also creates a queue
    assert dummy.queue is not None
    assert dummy.queue.empty()


# make a subclass of Sensor that does not implement from_sensor
class NoFromSensor(sensors.Sensor):
    def __init__(self, name="", port=""):
        super().__init__(name, port)


def test_no_from_sensor():
    with pytest.raises(TypeError):
        NoFromSensor()


def test_queue_data(dummy_sensor):
    cadence = 0.1  # seconds
    thd = Thread(target=dummy_sensor._queue_data, args=(cadence,), daemon=True)
    thd.start()
    for i in range(10):
        data = dummy_sensor.queue.get(timeout=1.0)
        assert json.loads(data) == f"data: {i+1}"


@pytest.mark.skip(reason="Not implemented yet")
def test_read():
    pass


@pytest.mark.skip(reason="Not implemented yet")
def test_from_sensor():
    pass
