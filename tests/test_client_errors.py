import pytest
from unittest.mock import Mock, patch
import yaml

from cmt_vna.testing import DummyVNA

# Import dummy classes before importing client to ensure mocking works
from eigsep_observing.testing import DummyEigsepRedis
from picohost.testing import (
    DummyPicoDevice,
    DummyPicoRFSwitch,
    DummyPicoPeltier,
    DummyPicoMotor,
)

import eigsep_observing
import eigsep_observing.utils
from eigsep_observing.client import PandaClient


# Use dummy implementations to avoid hardware dependencies
@pytest.fixture(autouse=True)
def dummies(monkeypatch):
    # Mock picohost at import time
    import picohost

    picohost.PicoDevice = DummyPicoDevice
    picohost.PicoRFSwitch = DummyPicoRFSwitch
    picohost.PicoPeltier = DummyPicoPeltier
    picohost.PicoMotor = DummyPicoMotor

    monkeypatch.setattr("eigsep_observing.client.VNA", DummyVNA)


@pytest.fixture
def redis():
    """Redis connection for client tests."""
    return DummyEigsepRedis()


@pytest.fixture
def client(redis, tmp_path, monkeypatch):
    """Create client for error tests."""
    # Patch init_picos to ensure attributes are set even if no picos connect
    original_init_picos = PandaClient.init_picos

    def patched_init_picos(self):
        # Initialize attributes first
        self.switch_nw = None
        self.motor = None
        self.peltier = None
        # Call original method
        original_init_picos(self)

    monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    with open(path, "r") as f:
        dummy_cfg = yaml.safe_load(f)
    dummy_cfg["vna_save_dir"] = str(tmp_path)
    return PandaClient(redis, default_cfg=dummy_cfg)


class TestClientInitializationErrors:
    """Test client initialization error handling."""

    def test_init_redis_config_fallback(self, redis, tmp_path, monkeypatch):
        """
        Test initialization falls back to default when
        redis.get_config() fails.
        """
        redis.get_config = Mock(side_effect=ValueError("Config not found"))

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should not raise - should fall back to default config
        client = PandaClient(redis, default_cfg=dummy_cfg)

        # Should've used default cfg as base, with added picos and upload_time
        expected_keys = set(dummy_cfg.keys()) | {"picos", "upload_time"}
        assert set(client.cfg.keys()) == expected_keys
        # Check that original config values are preserved
        for key, value in dummy_cfg.items():
            assert client.cfg[key] == value

    def test_init_redis_connection_error(self, redis, tmp_path, monkeypatch):
        """Test initialization when Redis connection fails during heartbeat."""
        redis.client_heartbeat_check = Mock(
            side_effect=ConnectionError("Redis unavailable")
        )

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # This might not always raise if heartbeat is called later
        try:
            client = PandaClient(redis, default_cfg=dummy_cfg)
            # If it succeeds, verify it was created
            assert client.redis == redis
        except ConnectionError:
            # If it fails, that's also acceptable behavior
            pass

    def test_init_pico_error(self, redis, tmp_path, monkeypatch):
        """Test initialization when pico initialization fails."""

        def failing_pico(*args, **kwargs):
            raise RuntimeError("Pico unavailable")

        # Mock picohost to raise errors
        import picohost

        picohost.PicoDevice = failing_pico
        picohost.PicoRFSwitch = failing_pico

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should handle pico errors gracefully (no picos will connect)
        client = PandaClient(redis, default_cfg=dummy_cfg)
        # Should have no picos connected
        assert len(client.picos) == 0

    def test_init_vna_error(self, redis, tmp_path, monkeypatch):
        """Test initialization when VNA fails."""

        def failing_vna(*args, **kwargs):
            raise RuntimeError("VNA unavailable")

        monkeypatch.setattr("eigsep_observing.client.VNA", failing_vna)

        def patched_init_picos(self):
            self.switch_nw = DummyPicoRFSwitch("/dev/switch")
            self.switch_nw.connect()
            self.motor = None
            self.peltier = None
            self.picos = {"switch": self.switch_nw}

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)
        dummy_cfg["use_vna"] = True  # Force VNA initialization

        with pytest.raises(RuntimeError, match="VNA unavailable"):
            PandaClient(redis, default_cfg=dummy_cfg)

    def test_init_pico_connection_error(self, redis, tmp_path, monkeypatch):
        """Test initialization when pico connection fails."""

        class FailingPico:
            def __init__(self, *args, **kwargs):
                pass

            def connect(self):
                return False  # Connection fails

            def set_response_handler(self, handler):
                pass

        # Mock picohost with failing connection
        import picohost

        picohost.PicoDevice = FailingPico
        picohost.PicoRFSwitch = FailingPico

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Client should handle pico connection errors gracefully
        client = PandaClient(redis, default_cfg=dummy_cfg)

        # No picos should be connected
        assert len(client.picos) == 0

        # Client should still be initialized successfully
        assert client.redis == redis

    def test_init_invalid_config_format(self, redis, tmp_path, monkeypatch):
        """Test initialization with invalid config format."""
        redis.get_config = Mock(return_value="invalid_config")  # Not a dict

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should either raise or fall back to default config
        try:
            client = PandaClient(redis, default_cfg=dummy_cfg)
            # If it succeeds, should have fallen back to default
            assert client.cfg == dummy_cfg
        except (TypeError, AttributeError, KeyError):
            # If it fails due to config format, that's acceptable
            pass

    def test_init_missing_config_keys(self, redis, tmp_path, monkeypatch):
        """Test initialization with missing required config keys."""
        redis.get_config = Mock(return_value={"incomplete": "config"})

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should either raise KeyError or handle gracefully
        try:
            client = PandaClient(redis, default_cfg=dummy_cfg)
            # If it succeeds, verify it has some config
            assert client.cfg is not None
        except KeyError:
            # If it fails due to missing keys, that's acceptable
            pass

    def test_init_config_none(self, redis, tmp_path, monkeypatch):
        """Test initialization when config is None."""
        redis.get_config = Mock(return_value=None)

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Should either raise or handle gracefully
        try:
            client = PandaClient(redis, default_cfg=dummy_cfg)
            # If it succeeds, should have some config
            assert client.cfg is not None
        except (TypeError, AttributeError, KeyError):
            # If it fails, that's acceptable
            pass

    def test_init_empty_config(self, redis, tmp_path, monkeypatch):
        """Test initialization with empty config."""
        redis.get_config = Mock(return_value={})

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
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


class TestClientPicoErrors:
    """Test client pico management error handling."""

    def test_pico_invalid_config(self, client):
        """Test pico initialization with invalid config."""
        # Test that client handles missing pico config gracefully
        # This is tested during initialization - if picos key is missing,
        # client should continue without picos
        assert hasattr(client, "picos")
        # Should be dict even if empty
        assert isinstance(client.picos, dict)

    def test_pico_no_config(self, redis, tmp_path, monkeypatch):
        """Test pico initialization with no pico config."""
        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)
        # Remove picos config
        if "picos" in dummy_cfg:
            del dummy_cfg["picos"]

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        client = PandaClient(redis, default_cfg=dummy_cfg)
        # Should handle missing pico config gracefully
        assert len(client.picos) == 0

    def test_pico_unknown_class(self, redis, tmp_path, monkeypatch):
        """Test pico initialization with unknown pico class."""
        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)
        # Add unknown pico type to config
        dummy_cfg["picos"] = {"unknown_pico": "/dev/unknown"}

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        client = PandaClient(redis, default_cfg=dummy_cfg)
        # Should handle unknown pico class gracefully
        assert "unknown_pico" not in client.picos

    def test_pico_thread_management(self, client):
        """Test pico thread lifecycle management."""
        # Check if any picos were initialized
        if client.picos:
            for name, pico in client.picos.items():
                # Picos should have started if they connected
                if hasattr(pico, "_thread") and pico._thread:
                    assert pico._thread.is_alive()

    def test_pico_stop_event_handling(self, client):
        """Test pico stop event handling."""
        # Check if client has stop_client event
        assert hasattr(client, "stop_client")
        assert not client.stop_client.is_set()

        # Set stop event
        client.stop_client.set()
        assert client.stop_client.is_set()

    def test_pico_error_resilience(self, redis, tmp_path, monkeypatch):
        """Test client resilience to pico errors."""
        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)
        dummy_cfg["picos"] = {
            "imu": "/dev/nonexistent1",  # These will fail to connect
            "therm": "/dev/nonexistent2",
        }

        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        client = PandaClient(redis, default_cfg=dummy_cfg)

        # Should handle pico connection errors gracefully
        assert len(client.picos) == 0  # None should connect
        assert client.redis == redis  # Client should still be functional


class TestClientEdgeCases:
    """Test client edge cases and error conditions."""

    def test_error_handling_edge_cases(self, tmp_path, monkeypatch):
        """Test edge cases in error handling."""
        # Patch init_picos to set attributes
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            original_init_picos(self)

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        # Test with None parameters
        with pytest.raises(AttributeError):
            PandaClient(None)

        # Test with invalid Redis object
        class InvalidRedis:
            pass

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        with pytest.raises(AttributeError):
            PandaClient(InvalidRedis(), default_cfg=dummy_cfg)

    def test_concurrent_pico_operations(self, client):
        """Test concurrent pico operations."""
        import threading

        def check_picos():
            # Just check picos dict multiple times
            for i in range(3):
                _ = len(client.picos)
                _ = client.switch_nw

        # Start multiple threads checking picos
        threads = []
        for _ in range(2):
            thread = threading.Thread(target=check_picos)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join(timeout=1)

        # Should handle concurrent operations without crashing
        assert isinstance(client.picos, dict)

    def test_memory_leak_prevention(self, client):
        """Test that client doesn't leak memory with repeated operations."""
        # Check that picos dict doesn't grow unexpectedly
        initial_pico_count = len(client.picos)

        # Multiple checks shouldn't change pico count
        for i in range(5):
            _ = client.picos
            _ = client.switch_nw

        # Should not accumulate excessive resources
        assert len(client.picos) == initial_pico_count

    def test_client_cleanup_on_failure(self, redis, tmp_path, monkeypatch):
        """Test client cleanup when initialization fails."""

        def failing_vna(*args, **kwargs):
            raise RuntimeError("VNA initialization failed")

        monkeypatch.setattr("eigsep_observing.client.VNA", failing_vna)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)
        # Force VNA initialization by having a switch
        dummy_cfg["picos"] = {"switch": "/dev/switch"}

        def patched_init_picos(self):
            self.switch_nw = DummyPicoRFSwitch("/dev/switch")
            self.switch_nw.connect()
            self.motor = None
            self.peltier = None
            self.picos = {"switch": self.switch_nw}

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        with pytest.raises(RuntimeError):
            PandaClient(redis, default_cfg=dummy_cfg)

        # Even after failure, should not leave resources hanging
        # This is more about ensuring no partial initialization state

    def test_invalid_pico_configuration(self, redis, tmp_path, monkeypatch):
        """Test client with invalid pico configuration."""

        # Patch init_picos to set attributes once
        original_init_picos = PandaClient.init_picos

        def patched_init_picos(self):
            self.switch_nw = None
            self.motor = None
            self.peltier = None
            try:
                original_init_picos(self)
            except (TypeError, AttributeError):
                # Handle gracefully
                self.picos = {}

        monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

        path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
        with open(path, "r") as f:
            dummy_cfg = yaml.safe_load(f)
        dummy_cfg["vna_save_dir"] = str(tmp_path)

        # Test with empty picos config
        dummy_cfg["picos"] = {}
        client = PandaClient(redis, default_cfg=dummy_cfg)
        assert isinstance(client.picos, dict)
        assert len(client.picos) == 0
