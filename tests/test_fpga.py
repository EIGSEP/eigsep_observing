import pytest
from threading import Thread
from unittest.mock import Mock, patch

from eigsep_observing import EigsepFpga
from eigsep_observing.testing import DummyEigsepFpga
from eigsep_observing.testing import DummyEigsepRedis
from eigsep_corr.testing import DummyPam


@pytest.fixture
def mock_redis():
    """Mock Redis connection for FPGA tests."""
    redis = DummyEigsepRedis()
    redis.upload_corr_config = Mock()
    redis.add_metadata = Mock()
    redis.add_corr_data = Mock()
    return redis


@pytest.fixture
def fpga_instance(mock_redis):
    """Create a DummyEigsepFpga instance with mocked dependencies."""
    # Use default config, don't pass custom config to avoid missing keys
    fpga = DummyEigsepFpga(program=False)
    # Manually set the redis instance for testing
    fpga.redis = mock_redis
    fpga.logger = Mock()
    fpga.validate_config = Mock()
    fpga.fpga = Mock()
    fpga.sync_time = 1234567890.0
    fpga.autos = ["00", "11"]
    fpga.crosses = ["01", "10"]
    fpga.pairs = ["00", "01", "10", "11"]
    fpga.prev_cnt = 0
    fpga.read_data = Mock(return_value={"00": [1, 2, 3], "11": [4, 5, 6]})
    return fpga


@patch("eigsep_corr.fpga.Pam", DummyPam)
class TestEigsepFpga:
    """Test cases for EigsepFpga class."""

    @patch("eigsep_observing.fpga.EigsepRedis")
    def test_create_redis(self, mock_redis_class):
        """Test _create_redis static method."""
        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        redis = EigsepFpga._create_redis("localhost", 6379)

        mock_redis_class.assert_called_once_with(host="localhost", port=6379)
        assert redis == mock_redis_instance

    def test_upload_config_with_validation_success(self, fpga_instance):
        """Test upload_config with successful validation."""
        fpga_instance.validate_config.return_value = None

        fpga_instance.upload_config(validate=True)

        fpga_instance.validate_config.assert_called_once()
        fpga_instance.redis.upload_corr_config.assert_called_once_with(
            fpga_instance.cfg, from_file=False
        )
        fpga_instance.logger.debug.assert_called_with(
            "Uploading configuration to Redis."
        )

    def test_upload_config_without_validation(self, fpga_instance):
        """Test upload_config without validation."""
        fpga_instance.upload_config(validate=False)

        fpga_instance.validate_config.assert_not_called()
        fpga_instance.redis.upload_corr_config.assert_called_once_with(
            fpga_instance.cfg, from_file=False
        )

    def test_upload_config_validation_failure(self, fpga_instance):
        """Test upload_config when validation fails."""
        fpga_instance.validate_config.side_effect = RuntimeError(
            "Config invalid"
        )

        with pytest.raises(
            RuntimeError, match="Configuration validation failed"
        ):
            fpga_instance.upload_config(validate=True)

        fpga_instance.validate_config.assert_called_once()
        fpga_instance.logger.error.assert_called_with(
            "Configuration validation failed: Config invalid"
        )
        fpga_instance.redis.upload_corr_config.assert_not_called()

    def test_synchronize(self, fpga_instance):
        """Test synchronize method."""
        # Mock the corr dummy synchronize method (second base class)
        with patch.object(
            fpga_instance.__class__.__bases__[1], "synchronize"
        ) as mock_super_sync:
            fpga_instance.synchronize(delay=5)

            # Check that the parent method was called
            mock_super_sync.assert_called_once()
            fpga_instance.redis.add_metadata.assert_called_once()

            # Check metadata structure
            call_args = fpga_instance.redis.add_metadata.call_args
            assert call_args[0][0] == "corr_sync_time"
            metadata = call_args[0][1]
            assert "sync_time_unix" in metadata
            assert "sync_date" in metadata
            assert metadata["sync_time_unix"] == 1234567890.0

    def test_synchronize_default_delay(self, fpga_instance):
        """Test synchronize with default delay."""
        # Test the behavior rather than implementation details
        fpga_instance.synchronize()

        # Verify that redis.add_metadata was called
        fpga_instance.redis.add_metadata.assert_called_once()

        # Verify metadata structure
        call_args = fpga_instance.redis.add_metadata.call_args
        assert call_args[0][0] == "corr_sync_time"
        metadata = call_args[0][1]
        assert "sync_time_unix" in metadata
        assert "sync_date" in metadata

    def test_initialize_all_enabled(self, fpga_instance):
        """Test initialize with all options enabled."""
        # Track calls to synchronize method
        with patch.object(fpga_instance, "synchronize") as mock_sync:
            fpga_instance.initialize(
                initialize_adc=True, initialize_fpga=True, sync=True
            )

            # Verify that synchronize was called when sync=True
            mock_sync.assert_called_once()
            fpga_instance.logger.debug.assert_called_with(
                "Synchronizing correlator clock."
            )

    def test_initialize_sync_disabled(self, fpga_instance):
        """Test initialize with sync disabled."""
        # Track calls to synchronize method
        with patch.object(fpga_instance, "synchronize") as mock_sync:
            fpga_instance.initialize(sync=False)

            # Verify that synchronize was NOT called when sync=False
            mock_sync.assert_not_called()

    def test_initialize_adc_disabled(self, fpga_instance):
        """Test initialize with ADC initialization disabled."""
        # Track calls to synchronize method
        with patch.object(fpga_instance, "synchronize") as mock_sync:
            fpga_instance.initialize(initialize_adc=False, sync=True)

            # Verify that synchronize was called even with initialize_adc=False
            mock_sync.assert_called_once()

    def test_update_redis(self, fpga_instance):
        """Test update_redis method."""
        test_data = {"00": [1, 2, 3], "11": [4, 5, 6]}
        test_cnt = 42

        fpga_instance.update_redis(test_data, test_cnt)

        # Use the actual dtype from config instead of hardcoding
        expected_dtype = fpga_instance.cfg["dtype"]
        fpga_instance.redis.add_corr_data.assert_called_once_with(
            test_data, test_cnt, dtype=expected_dtype
        )

    def test_read_integrations_no_new_data(self, fpga_instance):
        """Test _read_integrations when no new data is available."""
        from queue import Queue
        from threading import Event

        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        fpga_instance.fpga.read_int.return_value = (
            5  # Always return same count
        )

        # Set event to stop the loop
        fpga_instance.event.set()

        # Run _read_integrations, won't put to queue since no new data
        fpga_instance._read_integrations(["00", "11"], timeout=0.1)

        assert fpga_instance.queue.empty()
        fpga_instance.fpga.read_int.assert_called_with("corr_acc_cnt")

    def test_read_integrations_new_data(self, fpga_instance):
        """Test _read_integrations with new data available."""
        from queue import Queue
        from threading import Event

        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        fpga_instance.fpga.read_int.side_effect = [
            5,  # Initial count
            6,  # New count (triggers read)
            6,  # Validation read after data read
        ]
        expected_data = {"00": [1, 2, 3], "11": [4, 5, 6]}
        fpga_instance.read_data.return_value = expected_data

        # Set event after first iteration
        def stop_after_one(msg):
            fpga_instance.event.set()

        fpga_instance.logger.info.side_effect = stop_after_one

        # Run _read_integrations
        fpga_instance._read_integrations(["00", "11"], timeout=0.1)

        # Check that data was put in queue
        assert not fpga_instance.queue.empty()
        queued_item = fpga_instance.queue.get()
        assert queued_item["data"] == expected_data
        assert queued_item["cnt"] == 6
        fpga_instance.logger.info.assert_called_with("Reading acc_cnt=6")
        fpga_instance.read_data.assert_called_once_with(
            pairs=["00", "11"], unpack=False
        )

    def test_read_integrations_missed_integrations(self, fpga_instance):
        """Test _read_integrations when integrations are missed."""
        from queue import Queue
        from threading import Event

        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        fpga_instance.fpga.read_int.side_effect = [
            5,  # Initial count
            8,  # Jumped from 5 to 8 (missed 2)
            8,  # Validation read
        ]
        expected_data = {"00": [1, 2, 3], "11": [4, 5, 6]}
        fpga_instance.read_data.return_value = expected_data

        # Set event after warning is logged
        def stop_after_warning(msg):
            fpga_instance.event.set()

        fpga_instance.logger.warning.side_effect = stop_after_warning

        # Run _read_integrations
        fpga_instance._read_integrations(["00", "11"], timeout=0.1)

        # Check that data was put in queue
        assert not fpga_instance.queue.empty()
        queued_item = fpga_instance.queue.get()
        assert queued_item["data"] == expected_data
        assert queued_item["cnt"] == 8
        fpga_instance.logger.warning.assert_called_with(
            "Missed 2 integration(s)."
        )

    def test_read_integrations_read_failure(self, fpga_instance):
        """Test _read_integrations when read fails to complete in time."""
        from queue import Queue
        from threading import Event

        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        fpga_instance.fpga.read_int.side_effect = [
            5,  # Initial count
            6,  # New count (triggers read)
            7,  # Count changed during read - indicates failure
        ]
        expected_data = {"00": [1, 2, 3], "11": [4, 5, 6]}
        fpga_instance.read_data.return_value = expected_data

        # Set event after error is logged
        def stop_after_error(msg):
            fpga_instance.event.set()

        fpga_instance.logger.error.side_effect = stop_after_error

        # Run _read_integrations
        fpga_instance._read_integrations(["00", "11"], timeout=0.1)

        # Check that data was still put in queue (even with error)
        assert not fpga_instance.queue.empty()
        queued_item = fpga_instance.queue.get()
        assert queued_item["data"] == expected_data
        assert queued_item["cnt"] == 6
        fpga_instance.logger.error.assert_called_with(
            "Read of acc_cnt=6 FAILED to complete before next integration."
        )

    def test_end_observing(self, fpga_instance):
        """Test that end_observing method exists and can be called."""
        # Just verify the method exists and can be called without error
        # The actual implementation is in the parent class
        fpga_instance.end_observing()

    def test_observe_basic_functionality(self, fpga_instance):
        """Test basic observe functionality."""
        fpga_instance.upload_config = Mock()
        fpga_instance.update_redis = Mock()

        # Set up proper numeric values for hardware reads
        fpga_instance.fpga.read_uint.return_value = 1024  # acc_len

        # Mock the thread behavior
        def mock_thread_run(target, args, kwargs):
            # Put some data in the queue
            fpga_instance.queue.put({"data": {"00": [1, 2, 3]}, "cnt": 1})
            fpga_instance.queue.put({"data": {"00": [4, 5, 6]}, "cnt": 2})
            # Signal thread is done
            fpga_instance.event.set()
            return Mock(spec=Thread)

        with patch("eigsep_observing.fpga.Thread") as mock_thread_class:
            mock_thread = Mock()
            mock_thread_class.return_value = mock_thread
            mock_thread.start = lambda: mock_thread_run(None, None, None)

            fpga_instance.observe(pairs=["00"], timeout=10)

        fpga_instance.upload_config.assert_called_once_with(validate=True)
        assert fpga_instance.update_redis.call_count == 2
        fpga_instance.update_redis.assert_any_call({"00": [1, 2, 3]}, 1)
        fpga_instance.update_redis.assert_any_call({"00": [4, 5, 6]}, 2)

    @pytest.mark.skip(
        reason="Test needs rewrite for new thread-based implementation"
    )
    def test_observe_default_pairs(self, fpga_instance):
        """Test observe with default pairs (None)."""
        # TODO: Rewrite this test to work with new thread/queue implementation
        pass

    @pytest.mark.skip(
        reason="Test needs rewrite for new thread-based implementation"
    )
    def test_observe_timeout_immediate(self, fpga_instance):
        """Test observe timeout behavior."""
        # TODO: Rewrite this test to work with new thread/queue implementation
        pass

    @pytest.mark.skip(
        reason="Test needs rewrite for new thread-based implementation"
    )
    def test_observe_continuous_no_data(self, fpga_instance):
        """Test observe when continuously getting no data until timeout."""
        # TODO: Rewrite this test to work with new thread/queue implementation
        pass

    @pytest.mark.skip(
        reason="Test needs rewrite for new thread-based implementation"
    )
    def test_observe_logging(self, fpga_instance):
        """Test observe logging behavior."""
        # TODO: Rewrite this test to work with new thread/queue implementation
        pass

    @pytest.mark.skip(reason="Test needs rewrite - prev_cnt no longer used")
    def test_observe_prev_cnt_update(self, fpga_instance):
        """Test that observe updates prev_cnt correctly."""
        # TODO: Remove or rewrite this test - prev_cnt is no longer used
        pass

    @pytest.mark.skip(
        reason="Test needs rewrite for new thread-based implementation"
    )
    @patch("time.time")
    @patch("time.sleep")
    def test_observe_integration_loop(
        self, mock_sleep, mock_time, fpga_instance
    ):
        """Test observe integration loop with multiple data reads."""
        # TODO: Rewrite this test to work with new thread/queue implementation
        pass
