import numpy as np
import pytest
from queue import Queue
from threading import Thread

from eigsep_observing import sensors

from .test_redis import DummyEigsepRedis

pytestmark = pytest.mark.skip(reason="Sensor class not implemented yet")


# XXX need to mock up sensor to run w/o pico
class DummySensor(sensors.Sensor):

    def __init__(self, name="dummy_sensor", serial_port="/dev/dummy_sensor"):
        self.name = name
        self.serial_port = serial_port
        self.queue = Queue()
        if self.name not in sensors.SENSOR_CLASSES:
            sensors.SENSOR_CLASSES[self.name] = DummySensor


@pytest.fixture
def redis():
    return DummyEigsepRedis()


def test_base_class(redis):
    name = "dummy_sensor"
    serial_port = "/dev/dummy_sensor"
    s = DummySensor(name=name, serial_port=serial_port)
    # __init__
    assert s.name == name
    assert s.serial_port == serial_port
    assert s.queue is not None
    assert s.queue.empty()
    # invalid port
    with pytest.raises(ValueError):
        sensors.Sensor(name, "/dev/invalid_port")
    # grab_data, not implemented
    with pytest.raises(NotImplementedError):
        s.grab_data()


# XXX need a way to test without connecting picos
@pytest.mark.parametrize("sensor_name", sensors.SENSOR_CLASSES)
def test_grab_data(sensor_name):
    sensor_class = sensors.SENSOR_CLASSES[sensor_name]
    sensor = sensor_class(sensor_name, "/dev/test_sensor")
    thd = Thread(target=sensor.grab_data, daemon=True)
    thd.start()
    data = sensor.queue.get(timeout=1)
    # XXX do some comparisons


@pytest.mark.parametrize("sensor_name", sensors.SENSOR_CLASSES)
def test_read(sensor_name, redis):
    sensor_class = sensors.SENSOR_CLASSES[sensor_name]
    sensor = sensor_class(sensor_name, "/dev/test_sensor")
    thd = Thread(target=sensor.read, args=(redis,), daemon=True)
    thd.start()
    # compare data in redis with data in queue
    data = sensor.queue.get(timeout=5)  # XXX get one reading only
    redis_hdr = redis.get_metadata(stream_keys=sensor.name)
    assert len(redis_hdr) == 1  # only one sensor at a time
    redis_data = redis_hdr.values()[0]
    np.testing.assert_array_equal(data, redis_data)
