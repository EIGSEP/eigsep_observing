import pytest
import serial
import time
import threading
from unittest.mock import Mock, patch, MagicMock

from eigsep_observing.sensors import (
    Sensor, 
    ImuSensor, 
    ThermSensor,
    PeltierSensor,
    LidarSensor
)
from eigsep_observing.testing import DummyEigsepRedis
from eigsep_observing.testing.sensors import DummySensor


@pytest.fixture
def mock_redis():
    """Mock Redis connection for sensor tests."""
    redis = DummyEigsepRedis()
    redis.add_sensor_data = Mock()
    return redis


class TestSensorBaseClass:
    """Test the base Sensor class error handling."""

    def test_sensor_abstract_from_sensor(self, mock_redis):
        """Test that base Sensor class raises NotImplementedError for from_sensor."""
        sensor = Sensor("test_sensor", mock_redis)
        
        with pytest.raises(NotImplementedError):
            sensor.from_sensor()

    def test_sensor_queue_data_redis_error(self, mock_redis):
        """Test queue_data when Redis operation fails."""
        mock_redis.add_sensor_data.side_effect = Exception("Redis error")
        
        sensor = Sensor("test_sensor", mock_redis)
        
        # Should handle Redis errors gracefully
        sensor.queue_data({"test": "data"})
        
        # Error should be logged but not raised
        mock_redis.add_sensor_data.assert_called_once()

    def test_sensor_read_continuous_error_recovery(self, mock_redis):
        """Test continuous reading with error recovery."""
        sensor = DummySensor("test_sensor", mock_redis)
        sensor.from_sensor = Mock()
        
        # First call fails, second succeeds, third triggers stop
        sensor.from_sensor.side_effect = [
            Exception("Sensor error"),
            {"data": "success"},
            Exception("Stop reading")  # Will trigger stop after success
        ]
        
        # Start reading in thread
        stop_event = threading.Event()
        sensor_thread = threading.Thread(
            target=sensor.read,
            args=(stop_event,),
            kwargs={"interval": 0.01}
        )
        sensor_thread.start()
        
        # Let it run briefly then stop
        time.sleep(0.05)
        stop_event.set()
        sensor_thread.join(timeout=1)
        
        # Should have attempted multiple reads despite first error
        assert sensor.from_sensor.call_count >= 2

    def test_sensor_read_stop_event(self, mock_redis):
        """Test that sensor reading respects stop event."""
        sensor = DummySensor("test_sensor", mock_redis)
        sensor.from_sensor = Mock(return_value={"data": "test"})
        
        stop_event = threading.Event()
        stop_event.set()  # Set immediately
        
        # Should exit immediately due to stop event
        sensor.read(stop_event, interval=0.01)
        
        # Should not have called from_sensor
        sensor.from_sensor.assert_not_called()


class TestImuSensorErrors:
    """Test IMU sensor error handling."""

    @patch('serial.Serial')
    def test_imu_sensor_serial_exception_init(self, mock_serial_class, mock_redis):
        """Test IMU sensor initialization with serial exception."""
        mock_serial_class.side_effect = serial.SerialException("Port not found")
        
        with pytest.raises(serial.SerialException):
            ImuSensor("imu_sensor", mock_redis, pico="/dev/ttyUSB0")

    @patch('serial.Serial')
    def test_imu_sensor_permission_error_init(self, mock_serial_class, mock_redis):
        """Test IMU sensor initialization with permission error."""
        mock_serial_class.side_effect = PermissionError("Permission denied")
        
        with pytest.raises(PermissionError):
            ImuSensor("imu_sensor", mock_redis, pico="/dev/ttyUSB0")

    @patch('serial.Serial')
    def test_imu_sensor_from_sensor_timeout(self, mock_serial_class, mock_redis):
        """Test IMU sensor data reading timeout."""
        mock_serial = Mock()
        mock_serial.readline.return_value = b""  # Empty response
        mock_serial_class.return_value = mock_serial
        
        sensor = ImuSensor("imu_sensor", mock_redis, pico="/dev/ttyUSB0")
        
        # Should handle empty/timeout responses gracefully
        data = sensor.from_sensor()
        assert data is None or isinstance(data, dict)

    @patch('serial.Serial')
    def test_imu_sensor_from_sensor_malformed_data(self, mock_serial_class, mock_redis):
        """Test IMU sensor with malformed data."""
        mock_serial = Mock()
        mock_serial.readline.return_value = b"malformed_data\n"
        mock_serial_class.return_value = mock_serial
        
        sensor = ImuSensor("imu_sensor", mock_redis, pico="/dev/ttyUSB0")
        
        with patch('json.loads', side_effect=ValueError("Invalid JSON")):
            data = sensor.from_sensor()
            # Should handle JSON parsing errors gracefully
            assert data is None

    @patch('serial.Serial')
    def test_imu_sensor_serial_communication_error(self, mock_serial_class, mock_redis):
        """Test IMU sensor serial communication error during reading."""
        mock_serial = Mock()
        mock_serial.readline.side_effect = serial.SerialException("Communication error")
        mock_serial_class.return_value = mock_serial
        
        sensor = ImuSensor("imu_sensor", mock_redis, pico="/dev/ttyUSB0")
        
        with pytest.raises(serial.SerialException):
            sensor.from_sensor()


class TestThermSensorErrors:
    """Test thermistor sensor error handling."""

    @patch('serial.Serial')
    def test_therm_sensor_serial_exception_init(self, mock_serial_class, mock_redis):
        """Test thermistor sensor initialization with serial exception."""
        mock_serial_class.side_effect = serial.SerialException("Device not found")
        
        with pytest.raises(serial.SerialException):
            ThermSensor("therm_sensor", mock_redis, pico="/dev/ttyUSB1")

    @patch('serial.Serial')
    def test_therm_sensor_from_sensor_communication_error(self, mock_serial_class, mock_redis):
        """Test thermistor sensor communication error."""
        mock_serial = Mock()
        mock_serial.readline.side_effect = OSError("Device disconnected")
        mock_serial_class.return_value = mock_serial
        
        sensor = ThermSensor("therm_sensor", mock_redis, pico="/dev/ttyUSB1")
        
        with pytest.raises(OSError):
            sensor.from_sensor()

    @patch('serial.Serial')
    def test_therm_sensor_from_sensor_invalid_data(self, mock_serial_class, mock_redis):
        """Test thermistor sensor with invalid temperature data."""
        mock_serial = Mock()
        mock_serial.readline.return_value = b"invalid_temp_reading\n"
        mock_serial_class.return_value = mock_serial
        
        sensor = ThermSensor("therm_sensor", mock_redis, pico="/dev/ttyUSB1")
        
        # Should handle parsing errors gracefully
        data = sensor.from_sensor()
        assert data is None or isinstance(data, dict)

    @patch('serial.Serial')
    def test_therm_sensor_timeout_recovery(self, mock_serial_class, mock_redis):
        """Test thermistor sensor timeout and recovery."""
        mock_serial = Mock()
        # First call times out, second succeeds
        mock_serial.readline.side_effect = [
            b"",  # Timeout/empty
            b'{"temperature": 25.5}\n'  # Valid data
        ]
        mock_serial_class.return_value = mock_serial
        
        sensor = ThermSensor("therm_sensor", mock_redis, pico="/dev/ttyUSB1")
        
        # First call should handle timeout
        data1 = sensor.from_sensor()
        
        # Second call should succeed
        with patch('json.loads', return_value={"temperature": 25.5}):
            data2 = sensor.from_sensor()
            assert data2 == {"temperature": 25.5}


class TestPeltierSensorStub:
    """Test Peltier sensor stub implementation."""

    def test_peltier_sensor_not_implemented(self, mock_redis):
        """Test that Peltier sensor raises NotImplementedError."""
        sensor = PeltierSensor("peltier_sensor", mock_redis)
        
        with pytest.raises(NotImplementedError):
            sensor.from_sensor()


class TestLidarSensorStub:
    """Test Lidar sensor stub implementation."""

    def test_lidar_sensor_not_implemented(self, mock_redis):
        """Test that Lidar sensor raises NotImplementedError."""
        sensor = LidarSensor("lidar_sensor", mock_redis)
        
        with pytest.raises(NotImplementedError):
            sensor.from_sensor()


class TestSensorThreadSafety:
    """Test sensor thread safety and concurrent access."""

    def test_sensor_concurrent_queue_data(self, mock_redis):
        """Test concurrent queue_data calls."""
        sensor = DummySensor("test_sensor", mock_redis)
        
        def queue_test_data(data_id):
            for i in range(10):
                sensor.queue_data({"id": data_id, "value": i})
        
        # Start multiple threads queuing data
        threads = []
        for thread_id in range(3):
            thread = threading.Thread(target=queue_test_data, args=(thread_id,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=1)
        
        # Should have received all data without errors
        assert mock_redis.add_sensor_data.call_count == 30

    def test_sensor_read_thread_interruption(self, mock_redis):
        """Test sensor reading thread interruption and cleanup."""
        sensor = DummySensor("test_sensor", mock_redis)
        sensor.from_sensor = Mock(side_effect=lambda: time.sleep(0.01) or {"data": "test"})
        
        stop_event = threading.Event()
        sensor_thread = threading.Thread(
            target=sensor.read,
            args=(stop_event,),
            kwargs={"interval": 0.005}
        )
        
        sensor_thread.start()
        
        # Let it run briefly
        time.sleep(0.02)
        
        # Interrupt the thread
        stop_event.set()
        sensor_thread.join(timeout=1)
        
        # Thread should have stopped cleanly
        assert not sensor_thread.is_alive()

    def test_sensor_redis_error_resilience(self, mock_redis):
        """Test sensor resilience to intermittent Redis errors."""
        sensor = DummySensor("test_sensor", mock_redis)
        
        # Simulate intermittent Redis failures
        mock_redis.add_sensor_data.side_effect = [
            Exception("Redis error 1"),
            None,  # Success
            Exception("Redis error 2"),
            None,  # Success
        ]
        
        # Queue multiple data points
        for i in range(4):
            sensor.queue_data({"value": i})
        
        # Should have attempted all queue operations despite errors
        assert mock_redis.add_sensor_data.call_count == 4


class TestSensorInitializationEdgeCases:
    """Test sensor initialization edge cases."""

    def test_sensor_empty_name(self, mock_redis):
        """Test sensor with empty name."""
        with pytest.raises(ValueError):
            Sensor("", mock_redis)

    def test_sensor_none_redis(self):
        """Test sensor with None Redis connection."""
        with pytest.raises(TypeError):
            Sensor("test_sensor", None)

    @patch('serial.Serial')
    def test_imu_sensor_invalid_pico_path(self, mock_serial_class, mock_redis):
        """Test IMU sensor with invalid pico path."""
        mock_serial_class.side_effect = FileNotFoundError("Device not found")
        
        with pytest.raises(FileNotFoundError):
            ImuSensor("imu_sensor", mock_redis, pico="/invalid/path")

    @patch('serial.Serial')
    def test_therm_sensor_device_busy(self, mock_serial_class, mock_redis):
        """Test thermistor sensor when device is busy."""
        mock_serial_class.side_effect = serial.SerialException("Device or resource busy")
        
        with pytest.raises(serial.SerialException, match="Device or resource busy"):
            ThermSensor("therm_sensor", mock_redis, pico="/dev/ttyUSB0")


class TestSensorDataValidation:
    """Test sensor data validation and formatting."""

    def test_sensor_queue_data_none(self, mock_redis):
        """Test queue_data with None data."""
        sensor = DummySensor("test_sensor", mock_redis)
        
        # Should handle None data gracefully
        sensor.queue_data(None)
        
        # Should not call Redis with None data
        mock_redis.add_sensor_data.assert_not_called()

    def test_sensor_queue_data_empty_dict(self, mock_redis):
        """Test queue_data with empty dictionary."""
        sensor = DummySensor("test_sensor", mock_redis)
        
        sensor.queue_data({})
        
        # Should call Redis even with empty dict (might be valid sensor state)
        mock_redis.add_sensor_data.assert_called_once_with("test_sensor", {})

    @patch('serial.Serial')
    def test_imu_sensor_data_format_validation(self, mock_serial_class, mock_redis):
        """Test IMU sensor data format validation."""
        mock_serial = Mock()
        
        # Test various data formats
        test_cases = [
            b'{"accel": {"x": 1.0, "y": 2.0, "z": 3.0}}\n',  # Valid
            b'{"invalid": "format"}\n',  # Missing expected fields
            b'{}\n',  # Empty but valid JSON
            b'not_json\n',  # Invalid JSON
        ]
        
        mock_serial.readline.side_effect = test_cases
        mock_serial_class.return_value = mock_serial
        
        sensor = ImuSensor("imu_sensor", mock_redis, pico="/dev/ttyUSB0")
        
        # Test each case
        for i in range(len(test_cases)):
            try:
                data = sensor.from_sensor()
                # Should return dict or None, never raise for data format issues
                assert data is None or isinstance(data, dict)
            except Exception as e:
                # Only serial/communication errors should bubble up
                assert isinstance(e, (serial.SerialException, OSError))


class TestSensorResourceManagement:
    """Test sensor resource management and cleanup."""

    @patch('serial.Serial')
    def test_imu_sensor_context_manager(self, mock_serial_class, mock_redis):
        """Test IMU sensor as context manager."""
        mock_serial = Mock()
        mock_serial_class.return_value = mock_serial
        
        with ImuSensor("imu_sensor", mock_redis, pico="/dev/ttyUSB0") as sensor:
            assert sensor.ser == mock_serial
        
        # Should close serial connection on exit
        mock_serial.close.assert_called_once()

    @patch('serial.Serial')
    def test_therm_sensor_explicit_cleanup(self, mock_serial_class, mock_redis):
        """Test thermistor sensor explicit cleanup."""
        mock_serial = Mock()
        mock_serial_class.return_value = mock_serial
        
        sensor = ThermSensor("therm_sensor", mock_redis, pico="/dev/ttyUSB1")
        
        # Manually close
        sensor.close()
        
        mock_serial.close.assert_called_once()

    @patch('serial.Serial')
    def test_sensor_cleanup_error_handling(self, mock_serial_class, mock_redis):
        """Test sensor cleanup error handling."""
        mock_serial = Mock()
        mock_serial.close.side_effect = Exception("Cleanup error")
        mock_serial_class.return_value = mock_serial
        
        sensor = ImuSensor("imu_sensor", mock_redis, pico="/dev/ttyUSB0")
        
        # Should handle cleanup errors gracefully
        try:
            sensor.close()
        except Exception:
            pytest.fail("Sensor cleanup should handle errors gracefully")