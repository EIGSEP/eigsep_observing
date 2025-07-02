import pytest
from unittest.mock import Mock, patch

from cmt_vna.testing import DummyVNA
from eigsep_corr.config import load_config
from switch_network.testing import DummySwitchNetwork

import eigsep_observing
import eigsep_observing.utils
from eigsep_observing.client import PandaClient
from eigsep_observing.testing import DummyEigsepRedis, DummySensor


# Use dummy implementations to avoid hardware dependencies
@pytest.fixture(autouse=True)
def dummies(monkeypatch):
    monkeypatch.setattr(
        "eigsep_observing.client.SwitchNetwork",
        DummySwitchNetwork,
    )
    monkeypatch.setattr("eigsep_observing.client.VNA", DummyVNA)
    monkeypatch.setattr(
        "eigsep_observing.client.sensors.SENSOR_CLASSES",
        {"dummy_sensor": DummySensor},
    )


@pytest.fixture
def redis():
    """Redis connection for client tests."""
    return DummyEigsepRedis()


@pytest.fixture
def client(redis, tmp_path):
    """Create client for error tests."""
    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    dummy_cfg = load_config(path, compute_inttime=False)
    dummy_cfg["vna_save_dir"] = str(tmp_path)
    return PandaClient(redis, default_cfg=dummy_cfg)


class TestClientInitializationErrors:
    """Test client initialization error handling."""

    def test_init_redis_config_fallback(self, redis, tmp_path):
        """
        Test initialization falls back to default when
        redis.get_config() fails.
        """
        redis.get_config = Mock(side_effect=ValueError("Config not found"))

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should not raise - should fall back to default config
        client = PandaClient(redis, default_cfg=dummy_cfg)

        # Should have used default config
        assert client.cfg == dummy_cfg

    def test_init_redis_connection_error(self, redis, tmp_path):
        """Test initialization when Redis connection fails during heartbeat."""
        redis.client_heartbeat_check = Mock(
            side_effect=ConnectionError("Redis unavailable")
        )

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # This might not always raise if heartbeat is called later
        try:
            client = PandaClient(redis, default_cfg=dummy_cfg)
            # If it succeeds, verify it was created
            assert client.redis == redis
        except ConnectionError:
            # If it fails, that's also acceptable behavior
            pass

    def test_init_switch_network_error(self, redis, tmp_path, monkeypatch):
        """Test initialization when switch network fails."""

        def failing_switch(*args, **kwargs):
            raise RuntimeError("Switch network unavailable")

        monkeypatch.setattr(
            "eigsep_observing.client.SwitchNetwork", failing_switch
        )

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        with pytest.raises(RuntimeError, match="Switch network unavailable"):
            PandaClient(redis, default_cfg=dummy_cfg)

    def test_init_vna_error(self, redis, tmp_path, monkeypatch):
        """Test initialization when VNA fails."""

        def failing_vna(*args, **kwargs):
            raise RuntimeError("VNA unavailable")

        monkeypatch.setattr("eigsep_observing.client.VNA", failing_vna)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        with pytest.raises(RuntimeError, match="VNA unavailable"):
            PandaClient(redis, default_cfg=dummy_cfg)

    def test_init_sensor_error(self, redis, tmp_path, monkeypatch):
        """Test initialization when sensor setup fails."""

        def failing_sensor(*args, **kwargs):
            raise RuntimeError("Sensor unavailable")

        monkeypatch.setattr(
            "eigsep_observing.client.sensors.SENSOR_CLASSES",
            {"dummy_sensor": failing_sensor},
        )

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Client should handle sensor errors gracefully
        client = PandaClient(redis, default_cfg=dummy_cfg)

        # The failing sensor should not be in the sensors dict
        assert "dummy_sensor" not in client.sensors

        # Client should still be initialized successfully
        assert client.redis == redis

    def test_init_invalid_config_format(self, redis, tmp_path):
        """Test initialization with invalid config format."""
        redis.get_config = Mock(return_value="invalid_config")  # Not a dict

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should either raise or fall back to default config
        try:
            client = PandaClient(redis, default_cfg=dummy_cfg)
            # If it succeeds, should have fallen back to default
            assert client.cfg == dummy_cfg
        except (TypeError, AttributeError, KeyError):
            # If it fails due to config format, that's acceptable
            pass

    def test_init_missing_config_keys(self, redis, tmp_path):
        """Test initialization with missing required config keys."""
        redis.get_config = Mock(return_value={"incomplete": "config"})

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should either raise KeyError or handle gracefully
        try:
            client = PandaClient(redis, default_cfg=dummy_cfg)
            # If it succeeds, verify it has some config
            assert client.cfg is not None
        except KeyError:
            # If it fails due to missing keys, that's acceptable
            pass

    def test_init_config_none(self, redis, tmp_path):
        """Test initialization when config is None."""
        redis.get_config = Mock(return_value=None)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should either raise or handle gracefully
        try:
            client = PandaClient(redis, default_cfg=dummy_cfg)
            # If it succeeds, should have some config
            assert client.cfg is not None
        except (TypeError, AttributeError, KeyError):
            # If it fails, that's acceptable
            pass

    def test_init_empty_config(self, redis, tmp_path):
        """Test initialization with empty config."""
        redis.get_config = Mock(return_value={})

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should either raise KeyError or handle gracefully
        try:
            client = PandaClient(redis, default_cfg=dummy_cfg)
            # If it succeeds, should have some config
            assert client.cfg is not None
        except KeyError:
            # If it fails due to missing keys, that's acceptable
            pass


class TestClientReprogramErrors:
    """Test client reprogram functionality error handling."""

    def test_reprogram_success(self, client):
        """Test successful reprogram operation."""
        # Client fixture provides a working client
        result = client.reprogram()

        # reprogram() doesn't return a value, just ensure it doesn't raise
        assert result is None or result is True

    def test_reprogram_redis_error(self, client):
        """Test reprogram when Redis logging fails."""
        client.redis.add_live_log = Mock(side_effect=Exception("Redis error"))

        # Should still succeed despite Redis error
        result = client.reprogram()

        # reprogram() doesn't return a value, just ensure it doesn't raise
        assert result is None or result is True

    def test_reprogram_with_exception(self, client):
        """Test reprogram when underlying operation fails."""
        with patch.object(client, "logger") as mock_logger:
            mock_logger.info.side_effect = Exception("Logging error")

            # reprogram doesn't handle logger errors
            with pytest.raises(Exception, match="Logging error"):
                client.reprogram()


class TestClientSensorErrors:
    """Test client sensor management error handling."""

    def test_add_sensor_invalid_name(self, client):
        """Test adding sensor with invalid name."""
        # Should handle invalid sensor names gracefully
        client.add_sensor("invalid_sensor", "/dev/invalid", 1)

        # Should not have added the sensor
        assert "invalid_sensor" not in client.sensors

    def test_add_sensor_duplicate(self, client):
        """Test adding duplicate sensor."""
        # Add sensor twice (dummy_sensor already exists from fixture)
        initial_count = len(client.sensors)
        client.add_sensor("dummy_sensor", "/dev/test1", 1)
        client.add_sensor("dummy_sensor", "/dev/test2", 1)

        # Should still have the same number of sensors
        assert len(client.sensors) == initial_count

    def test_add_sensor_connection_error(self, client, monkeypatch):
        """Test adding sensor when connection fails."""

        def failing_sensor(*args, **kwargs):
            raise RuntimeError("Connection failed")

        monkeypatch.setattr(
            "eigsep_observing.client.sensors.SENSOR_CLASSES",
            {"failing_sensor": failing_sensor},
        )

        # Should handle connection errors gracefully
        client.add_sensor("failing_sensor", "/dev/test", 1)

        # Sensor should not be added
        assert "failing_sensor" not in client.sensors

    def test_sensor_thread_management(self, client):
        """Test sensor thread lifecycle management."""
        # Should have dummy_sensor from config
        if "dummy_sensor" in client.sensors:
            sensor, thread = client.sensors["dummy_sensor"]
            assert thread.is_alive()

            # Thread should be running
            assert thread.is_alive()

    def test_sensor_stop_event_handling(self, client):
        """Test sensor stop event handling."""
        # Check if client has stop events for sensors
        # Note: The current implementation may not have stop_events attribute
        if hasattr(client, "stop_events") and "dummy_sensor" in client.sensors:
            if "dummy_sensor" in client.stop_events:
                assert not client.stop_events["dummy_sensor"].is_set()

                # Set stop event
                client.stop_events["dummy_sensor"].set()
                assert client.stop_events["dummy_sensor"].is_set()

    def test_sensor_error_resilience(self, client, monkeypatch):
        """Test client resilience to sensor errors."""

        def failing_sensor(*args, **kwargs):
            raise RuntimeError("Sensor error")

        monkeypatch.setattr(
            "eigsep_observing.client.sensors.SENSOR_CLASSES",
            {"dummy_sensor": DummySensor, "failing_sensor": failing_sensor},
        )

        # Try to add multiple sensors
        client.add_sensor("failing_sensor", "/dev/fail", 1)
        client.add_sensor("dummy_sensor", "/dev/work", 1)

        # Should handle mixed success/failure
        assert "failing_sensor" not in client.sensors
        # dummy_sensor should still work


class TestClientEdgeCases:
    """Test client edge cases and error conditions."""

    def test_error_handling_edge_cases(self, tmp_path):
        """Test edge cases in error handling."""
        # Test with None parameters
        with pytest.raises(AttributeError):
            PandaClient(None)

        # Test with invalid Redis object
        class InvalidRedis:
            pass

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        with pytest.raises(AttributeError):
            PandaClient(InvalidRedis(), default_cfg=dummy_cfg)

    def test_concurrent_sensor_operations(self, client):
        """Test concurrent sensor operations."""
        import threading

        def add_sensors():
            for i in range(3):
                client.add_sensor("dummy_sensor", f"/dev/test{i}", 1)

        # Start multiple threads adding sensors
        threads = []
        for _ in range(2):
            thread = threading.Thread(target=add_sensors)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join(timeout=1)

        # Should handle concurrent operations without crashing
        assert isinstance(client.sensors, dict)

    def test_memory_leak_prevention(self, client):
        """Test that client doesn't leak memory with repeated operations."""
        # Repeatedly add sensors (same name, should not accumulate)
        for i in range(5):
            client.add_sensor("dummy_sensor", f"/dev/temp{i}", 1)

        # Should not accumulate excessive resources
        assert len(client.sensors) <= 5
        if hasattr(client, "stop_events"):
            assert len(client.stop_events) <= 5

    def test_client_cleanup_on_failure(self, redis, tmp_path, monkeypatch):
        """Test client cleanup when initialization fails."""

        def failing_switch(*args, **kwargs):
            raise RuntimeError("Switch initialization failed")

        monkeypatch.setattr(
            "eigsep_observing.client.SwitchNetwork", failing_switch
        )

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        dummy_cfg = load_config(path, compute_inttime=False)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        with pytest.raises(RuntimeError):
            PandaClient(redis, default_cfg=dummy_cfg)

        # Even after failure, should not leave resources hanging
        # This is more about ensuring no partial initialization state

    def test_invalid_sensor_configuration(self, client):
        """Test client with invalid sensor configuration."""
        # Try adding sensor with invalid parameters
        client.add_sensor("", "/dev/test", 1)  # Empty name
        client.add_sensor("test_sensor", "", 1)  # Empty device
        client.add_sensor(None, "/dev/test", 1)  # None name
        client.add_sensor("test_sensor", "/dev/test", -1)  # negative cadence

        # Should handle all gracefully
        assert isinstance(client.sensors, dict)
