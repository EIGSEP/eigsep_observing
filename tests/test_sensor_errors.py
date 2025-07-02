import pytest
import serial
import time
import threading
from unittest.mock import Mock, patch

from eigsep_observing.sensors import (
    Sensor,
    ImuSensor,
    ThermSensor,
    PeltierSensor,
    LidarSensor,
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
        """Test that base Sensor class cannot be instantiated due to
        abstract methods."""
        # Sensor is an abstract class and cannot be instantiated directly
        with pytest.raises(
            TypeError, match="Can't instantiate abstract class"
        ):
            Sensor("test_sensor", "/dev/test", timeout=1)

    def test_sensor_queue_data_redis_error(self, mock_redis):
        """Test read when Redis operation fails during read."""
        mock_redis.add_metadata = Mock(side_effect=Exception("Redis error"))

        sensor = DummySensor("test_sensor", "/dev/test", timeout=1)
        sensor.from_sensor = Mock(return_value='{"test": "data"}')

        # Run read briefly with a stop event
        stop_event = threading.Event()
        thread = threading.Thread(
            target=sensor.read,
            args=(mock_redis, stop_event),
        )
        thread.start()
        time.sleep(0.05)
        stop_event.set()
        thread.join(timeout=1)

        # Should have attempted to add metadata despite Redis errors
        assert mock_redis.add_metadata.called

    def test_sensor_read_from_sensor_exception(self, mock_redis):
        """Test that exceptions in from_sensor terminate the
        read loop."""
        sensor = DummySensor("test_sensor", "/dev/test", timeout=1)
        sensor.from_sensor = Mock(side_effect=Exception("Sensor error"))
        mock_redis.add_metadata = Mock()

        # Start reading in thread
        stop_event = threading.Event()
        sensor_thread = threading.Thread(
            target=sensor.read,
            args=(mock_redis, stop_event),
        )
        sensor_thread.start()

        # Let it run briefly then stop
        time.sleep(0.05)
        stop_event.set()
        sensor_thread.join(timeout=1)

        # Should have attempted to read from sensor at least once
        # Exception in from_sensor will terminate the read loop
        assert sensor.from_sensor.call_count >= 1
        # Redis should not have been called due to exception
        mock_redis.add_metadata.assert_not_called()

    def test_sensor_read_stop_event(self, mock_redis):
        """Test that sensor reading respects stop event."""
        sensor = DummySensor("test_sensor", "/dev/test", timeout=1)
        sensor.from_sensor = Mock(return_value='{"data": "test"}')
        mock_redis.add_metadata = Mock()

        stop_event = threading.Event()
        stop_event.set()  # Set immediately

        # Should exit immediately due to stop event
        sensor.read(mock_redis, stop_event)

        # The read loop should exit immediately without calling from_sensor
        sensor.from_sensor.assert_not_called()
        # Redis should not be called because the loop exits immediately
        mock_redis.add_metadata.assert_not_called()


class TestImuSensorErrors:
    """Test IMU sensor error handling."""

    @patch("eigsep_sensors.IMU_BNO085")
    def test_imu_sensor_serial_exception_init(
        self, mock_imu_class, mock_redis
    ):
        """Test IMU sensor initialization with serial exception."""
        mock_imu_class.side_effect = serial.SerialException("Port not found")

        with pytest.raises(RuntimeError, match="Failed to connect to IMU"):
            ImuSensor("imu_sensor", "/dev/ttyUSB0", timeout=10)

    @patch("eigsep_sensors.IMU_BNO085")
    def test_imu_sensor_permission_error_init(
        self, mock_imu_class, mock_redis
    ):
        """Test IMU sensor initialization with permission error."""
        mock_imu_class.side_effect = PermissionError("Permission denied")

        with pytest.raises(PermissionError, match="Permission denied"):
            ImuSensor("imu_sensor", "/dev/ttyUSB0", timeout=10)

    @patch("eigsep_sensors.IMU_BNO085")
    def test_imu_sensor_from_sensor_timeout(self, mock_imu_class, mock_redis):
        """Test IMU sensor data reading timeout."""
        mock_imu = Mock()
        mock_imu.read_imu.return_value = None  # Timeout/empty response
        mock_imu_class.return_value = mock_imu

        sensor = ImuSensor("imu_sensor", "/dev/ttyUSB0", timeout=10)

        # Should handle empty/timeout responses gracefully
        data = sensor.from_sensor()
        assert data == "null"  # JSON null for None

    @patch("eigsep_sensors.IMU_BNO085")
    def test_imu_sensor_from_sensor_malformed_data(
        self, mock_imu_class, mock_redis
    ):
        """Test IMU sensor with malformed data."""
        mock_imu = Mock()
        mock_imu.read_imu.return_value = "malformed_data"  # Invalid data
        mock_imu_class.return_value = mock_imu

        sensor = ImuSensor("imu_sensor", "/dev/ttyUSB0", timeout=10)

        # Should handle malformed data by JSON serializing it
        data = sensor.from_sensor()
        assert data == '"malformed_data"'  # JSON string

    @patch("eigsep_sensors.IMU_BNO085")
    def test_imu_sensor_serial_communication_error(
        self, mock_imu_class, mock_redis
    ):
        """Test IMU sensor communication error during reading."""
        mock_imu = Mock()
        mock_imu.read_imu.side_effect = OSError("Communication error")
        mock_imu_class.return_value = mock_imu

        sensor = ImuSensor("imu_sensor", "/dev/ttyUSB0", timeout=10)

        with pytest.raises(OSError):
            sensor.from_sensor()


class TestThermSensorErrors:
    """Test thermistor sensor error handling."""

    @patch("eigsep_sensors.Thermistor")
    def test_therm_sensor_serial_exception_init(
        self, mock_thermistor_class, mock_redis
    ):
        """Test thermistor sensor initialization with serial exception."""
        mock_thermistor_class.side_effect = serial.SerialException(
            "Device not found"
        )

        with pytest.raises(
            RuntimeError, match="Failed to connect to thermistor"
        ):
            ThermSensor("therm_sensor", "/dev/ttyUSB1", timeout=10)

    @patch("eigsep_sensors.Thermistor")
    def test_therm_sensor_from_sensor_communication_error(
        self, mock_thermistor_class, mock_redis
    ):
        """Test thermistor sensor communication error."""
        mock_thermistor = Mock()
        mock_thermistor.read_temperature.side_effect = OSError(
            "Device disconnected"
        )
        mock_thermistor_class.return_value = mock_thermistor

        sensor = ThermSensor("therm_sensor", "/dev/ttyUSB1", timeout=10)

        with pytest.raises(OSError):
            sensor.from_sensor()

    @patch("eigsep_sensors.Thermistor")
    def test_therm_sensor_from_sensor_invalid_data(
        self, mock_thermistor_class, mock_redis
    ):
        """Test thermistor sensor with invalid temperature data."""
        mock_thermistor = Mock()
        mock_thermistor.read_temperature.return_value = "invalid_temp_reading"
        mock_thermistor_class.return_value = mock_thermistor

        sensor = ThermSensor("therm_sensor", "/dev/ttyUSB1", timeout=10)

        # Should handle invalid data gracefully (JSON serialization of string)
        data = sensor.from_sensor()
        assert isinstance(data, str)  # JSON string

    @patch("eigsep_sensors.Thermistor")
    def test_therm_sensor_timeout_recovery(
        self, mock_thermistor_class, mock_redis
    ):
        """Test thermistor sensor timeout and recovery."""
        mock_thermistor = Mock()
        # First call times out, second succeeds
        mock_thermistor.read_temperature.side_effect = [
            None,  # Timeout/empty
            {"temperature": 25.5},  # Valid data
        ]
        mock_thermistor_class.return_value = mock_thermistor

        sensor = ThermSensor("therm_sensor", "/dev/ttyUSB1", timeout=10)

        # First call should handle timeout
        data1 = sensor.from_sensor()
        assert '{"data": null, "status": "TIMEOUT"}' == data1

        # Second call should succeed
        data2 = sensor.from_sensor()
        assert '{"data": {"temperature": 25.5}, "status": "OK"}' == data2


class TestPeltierSensorStub:
    """Test Peltier sensor stub implementation."""

    def test_peltier_sensor_not_implemented(self, mock_redis):
        """Test that Peltier sensor returns None (not implemented)."""
        sensor = PeltierSensor("peltier_sensor", "/dev/peltier", timeout=10)

        # Peltier sensor from_sensor returns None (not NotImplementedError)
        result = sensor.from_sensor()
        assert result is None


class TestLidarSensorStub:
    """Test Lidar sensor stub implementation."""

    def test_lidar_sensor_not_implemented(self, mock_redis):
        """Test that Lidar sensor returns None (not implemented)."""
        sensor = LidarSensor("lidar_sensor", "/dev/lidar", timeout=10)

        # Lidar sensor from_sensor returns None (not NotImplementedError)
        result = sensor.from_sensor()
        assert result is None


class TestSensorThreadSafety:
    """Test sensor thread safety and concurrent access."""

    def test_sensor_concurrent_read_operations(self, mock_redis):
        """Test concurrent sensor read operations."""
        sensor = DummySensor("test_sensor", "/dev/test", timeout=1)
        mock_redis.add_metadata = Mock()

        def run_sensor_read(stop_event):
            try:
                sensor.read(mock_redis, stop_event)
            except Exception:
                pass  # Ignore exceptions in test threads

        # Start multiple threads with sensor reads
        stop_events = []
        threads = []
        for _ in range(3):
            stop_event = threading.Event()
            stop_events.append(stop_event)
            thread = threading.Thread(
                target=run_sensor_read, args=(stop_event,)
            )
            threads.append(thread)
            thread.start()

        # Let them run briefly
        time.sleep(0.05)

        # Stop all threads
        for stop_event in stop_events:
            stop_event.set()

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=1)

        # All threads should have completed without hanging
        for thread in threads:
            assert not thread.is_alive()

    def test_sensor_read_thread_interruption(self, mock_redis):
        """Test sensor reading thread interruption and cleanup."""
        sensor = DummySensor("test_sensor", "/dev/test", timeout=1)
        sensor.from_sensor = Mock(
            side_effect=lambda: time.sleep(0.01) or {"data": "test"}
        )

        stop_event = threading.Event()
        sensor_thread = threading.Thread(
            target=sensor.read,
            args=(mock_redis, stop_event),
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
        sensor = DummySensor("test_sensor", "/dev/test", timeout=1)

        # Simulate intermittent Redis failures during sensor reads
        mock_redis.add_metadata = Mock(
            side_effect=[
                Exception("Redis error 1"),
                None,  # Success
                Exception("Redis error 2"),
                None,  # Success
            ]
        )

        # Run sensor read briefly to trigger Redis calls
        stop_event = threading.Event()
        thread = threading.Thread(
            target=sensor.read,
            args=(mock_redis, stop_event),
        )
        thread.start()
        time.sleep(0.05)
        stop_event.set()
        thread.join(timeout=1)

        # Should have attempted metadata operations despite errors
        assert mock_redis.add_metadata.called


class TestSensorInitializationEdgeCases:
    """Test sensor initialization edge cases."""

    def test_sensor_empty_name(self, mock_redis):
        """Test sensor with empty name."""
        with pytest.raises(
            TypeError, match="Can't instantiate abstract class"
        ):
            Sensor("", "/dev/test", timeout=1)

    def test_sensor_none_redis(self):
        """Test sensor abstract class instantiation."""
        with pytest.raises(
            TypeError, match="Can't instantiate abstract class"
        ):
            Sensor("test_sensor", "/dev/test", timeout=1)

    @patch("eigsep_sensors.IMU_BNO085")
    def test_imu_sensor_invalid_pico_path(self, mock_imu_class, mock_redis):
        """Test IMU sensor with invalid port path."""
        mock_imu_class.side_effect = FileNotFoundError("Device not found")

        with pytest.raises(FileNotFoundError, match="Device not found"):
            ImuSensor("imu_sensor", "/invalid/path", timeout=10)

    @patch("eigsep_sensors.IMU_BNO085")
    def test_therm_sensor_device_busy(self, mock_serial_class, mock_redis):
        """Test thermistor sensor when device is busy."""
        mock_serial_class.side_effect = serial.SerialException(
            "Device or resource busy"
        )

        with pytest.raises(
            RuntimeError, match="Failed to connect to thermistor"
        ):
            ThermSensor("therm_sensor", "/dev/ttyUSB0", timeout=10)


class TestSensorDataValidation:
    """Test sensor data validation and formatting."""

    def test_sensor_from_sensor_none_data(self, mock_redis):
        """Test sensor handling when from_sensor returns None."""
        sensor = DummySensor("test_sensor", "/dev/test", timeout=1)
        sensor.from_sensor = Mock(return_value=None)
        mock_redis.add_metadata = Mock()

        # Run sensor read briefly
        stop_event = threading.Event()
        thread = threading.Thread(
            target=sensor.read,
            args=(mock_redis, stop_event),
        )
        thread.start()
        time.sleep(0.02)
        stop_event.set()
        thread.join(timeout=1)

        # Should call from_sensor and add_metadata with None
        sensor.from_sensor.assert_called()
        # Redis should be called with None data
        mock_redis.add_metadata.assert_called()

    def test_sensor_from_sensor_empty_dict(self, mock_redis):
        """Test sensor handling when from_sensor returns empty dict."""
        sensor = DummySensor("test_sensor", "/dev/test", timeout=1)
        sensor.from_sensor = Mock(return_value="{}")
        mock_redis.add_metadata = Mock()

        # Run sensor read briefly
        stop_event = threading.Event()
        thread = threading.Thread(
            target=sensor.read,
            args=(mock_redis, stop_event),
        )
        thread.start()
        time.sleep(0.02)
        stop_event.set()
        thread.join(timeout=1)

        # Should call Redis with empty JSON string
        mock_redis.add_metadata.assert_called()

    @patch("eigsep_sensors.IMU_BNO085")
    def test_imu_sensor_data_format_validation(
        self, mock_imu_class, mock_redis
    ):
        """Test IMU sensor data format validation."""
        mock_imu = Mock()

        # Test various data formats
        test_cases = [
            {"accel": {"x": 1.0, "y": 2.0, "z": 3.0}},  # Valid
            {"invalid": "format"},  # Missing expected fields
            {},  # Empty but valid dict
            "not_dict",  # String instead of dict
        ]

        mock_imu_class.return_value = mock_imu
        sensor = ImuSensor("imu_sensor", "/dev/ttyUSB0", timeout=10)

        # Test each case
        for test_data in test_cases:
            mock_imu.read_imu.return_value = test_data
            data = sensor.from_sensor()
            # Should return JSON string, never raise for data format issues
            assert isinstance(data, str)
            # Should be valid JSON
            import json

            parsed = json.loads(data)
            assert parsed == test_data


class TestSensorResourceManagement:
    """Test sensor resource management and cleanup."""

    @patch("eigsep_sensors.IMU_BNO085")
    def test_imu_sensor_initialization(self, mock_serial_class, mock_redis):
        """Test IMU sensor initialization."""
        mock_serial = Mock()
        mock_serial_class.return_value = mock_serial

        sensor = ImuSensor("imu_sensor", "/dev/ttyUSB0", timeout=10)
        # Sensor should have been initialized with the mock serial connection
        assert hasattr(sensor, "imu")
        mock_serial_class.assert_called_once()

    @patch("eigsep_sensors.Thermistor")
    def test_therm_sensor_initialization(
        self, mock_thermistor_class, mock_redis
    ):
        """Test thermistor sensor initialization."""
        mock_thermistor = Mock()
        mock_thermistor_class.return_value = mock_thermistor

        sensor = ThermSensor("therm_sensor", "/dev/ttyUSB1", timeout=10)

        # Sensor should have been initialized with thermistor
        assert hasattr(sensor, "thermistor")
        mock_thermistor_class.assert_called_once()

    @patch("eigsep_sensors.IMU_BNO085")
    def test_sensor_from_sensor_call(self, mock_imu_class, mock_redis):
        """Test sensor from_sensor method call."""
        mock_imu = Mock()
        mock_imu.read_imu.return_value = {"test": "data"}
        mock_imu_class.return_value = mock_imu

        sensor = ImuSensor("imu_sensor", "/dev/ttyUSB0", timeout=10)

        # Should call from_sensor without errors
        result = sensor.from_sensor()
        assert result is not None
        assert isinstance(result, str)  # JSON string
        mock_imu.read_imu.assert_called_once()
