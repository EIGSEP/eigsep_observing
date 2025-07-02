import json
import threading
import time
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


def test_read(dummy_sensor, redis):
    """Test the sensor read method."""
    
    # Create a stop event
    stop_event = threading.Event()
    
    # Start reading in a separate thread with default cadence
    read_thread = threading.Thread(
        target=dummy_sensor.read, 
        args=(redis, stop_event)
    )
    read_thread.start()
    
    # Let it run for a short time
    time.sleep(0.1)
    
    # Stop the reading
    stop_event.set()
    read_thread.join(timeout=1)
    
    # Check that data was added to Redis
    # The dummy sensor should have added metadata through Redis
    # Check using Redis hash commands
    metadata_keys = redis.r.hkeys("metadata")
    assert len(metadata_keys) > 0
    assert b"dummy_sensor" in metadata_keys
    
    # Get the metadata value
    metadata_value = redis.r.hget("metadata", "dummy_sensor")
    assert metadata_value is not None
    
    # Check that data was streamed
    # The stream key is just the sensor name
    stream_entries = redis.r.xrange("dummy_sensor")
    assert len(stream_entries) > 0
    
    # Each stream entry should contain the sensor data
    for entry_id, fields in stream_entries:
        assert b"value" in fields
        # Data should be dict from dummy sensor
        dat_dict = json.loads(fields[b"value"])
        assert isinstance(dat_dict, dict)
        assert "data" in dat_dict
        assert "status" in dat_dict
        assert dat_dict["status"] == "OK"
        assert "cadence" in dat_dict
        assert isinstance(dat_dict["cadence"], float)


def test_from_sensor(dummy_sensor):
    """Test the from_sensor method."""
    expected_keys = {"data", "status"}
    for i in range(5):
        data = dummy_sensor.from_sensor()
        assert isinstance(data, dict)
        assert set(data.keys()) == expected_keys
        assert data["status"] == "OK"
        assert data["data"] == i + 1  # Should increment each call

def test_read_with_cadence(dummy_sensor, redis):
    """Test the sensor read method with custom cadence."""
    # Create a stop event
    stop_event = threading.Event()
    
    # Use a short cadence for testing
    cadence = 0.05  # 50ms
    
    # Start reading in a separate thread with custom cadence
    read_thread = threading.Thread(
        target=dummy_sensor.read, 
        args=(redis, stop_event),
        kwargs={"cadence": cadence}
    )
    
    start_time = time.time()
    read_thread.start()
    
    # Let it run for enough time to get multiple readings
    time.sleep(0.3)  # Should get ~6 readings with 50ms cadence
    
    # Stop the reading
    stop_event.set()
    read_thread.join(timeout=1)
    
    # Check that multiple data points were added
    stream_entries = redis.r.xrange("dummy_sensor")
    
    # With 300ms runtime and 50ms cadence, we should have at least 5 entries
    # (allowing for some timing variance)
    assert len(stream_entries) >= 5
    
    # Verify the timing between entries is roughly the cadence
    # Note: This is approximate due to thread scheduling
    timestamps = []
    for entry_id, fields in stream_entries:
        # Extract timestamp from entry_id (format: timestamp-sequence)
        timestamp = int(entry_id.decode().split('-')[0])
        timestamps.append(timestamp)
    
    # Check that readings are spaced appropriately
    if len(timestamps) > 1:
        intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
        avg_interval = sum(intervals) / len(intervals)
        # Allow for some variance in timing (50ms cadence = ~50ms intervals)
        assert 40 < avg_interval < 100  # milliseconds
