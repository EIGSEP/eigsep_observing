from contextlib import contextmanager

import pytest
from unittest.mock import Mock, patch

from eigsep_observing import EigsepFpga
from eigsep_observing.testing import DummyEigsepFpga
from eigsep_observing.testing import DummyEigsepRedis
from eigsep_corr.testing import DummyPam


@contextmanager
def _patch_observe_thread(fpga, items):
    """
    Patch ``eigsep_observing.fpga.Thread`` for tests of
    ``EigsepFpga.observe()``.

    Replaces the producer thread with a mock whose ``start()`` runs
    synchronously in the calling thread, pushes ``items`` into
    ``fpga.queue``, and sets ``fpga.event``. ``observe()``'s consumer
    loop then runs in the main thread against deterministic input.

    Parameters
    ----------
    fpga : DummyEigsepFpga
        The fpga fixture under test. ``fpga.queue`` and ``fpga.event``
        are created by ``observe()`` itself, so they are accessed
        lazily inside the fake ``start``.
    items : iterable
        Items to inject. Each is either a dict
        ``{"data": ..., "cnt": ...}`` (a normal integration) or
        ``None`` (the end-of-stream sentinel that the real producer
        pushes via ``end_observing``). The helper does NOT auto-append
        a sentinel — pass one explicitly if you want the consumer to
        log ``"End of queue, processing finished."``.

    Yields
    ------
    Mock
        The patched ``Thread`` class, so tests can assert on how
        ``observe()`` constructed it (e.g. ``args``, ``kwargs``).
    """
    # observe() reads acc_len via fpga.read_uint to build self.header;
    # the real test_observe_basic_functionality used to set this inline
    # — centralize it here so every observe() test gets a sane default.
    fpga.fpga.read_uint.return_value = 1024

    with patch("eigsep_observing.fpga.Thread") as mock_thread_class:
        mock_thread = Mock()
        mock_thread_class.return_value = mock_thread

        def fake_start():
            for item in items:
                fpga.queue.put(item)
            fpga.event.set()

        mock_thread.start = fake_start
        yield mock_thread_class


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

        items = [
            {"data": {"00": [1, 2, 3]}, "cnt": 1},
            {"data": {"00": [4, 5, 6]}, "cnt": 2},
        ]
        with _patch_observe_thread(fpga_instance, items):
            fpga_instance.observe(pairs=["00"], timeout=10)

        fpga_instance.upload_config.assert_called_once_with(validate=True)
        assert fpga_instance.update_redis.call_count == 2
        fpga_instance.update_redis.assert_any_call({"00": [1, 2, 3]}, 1)
        fpga_instance.update_redis.assert_any_call({"00": [4, 5, 6]}, 2)

    def test_observe_default_pairs(self, fpga_instance):
        """observe() with no pairs arg defaults to self.pairs."""
        fpga_instance.update_redis = Mock()

        items = [{"data": {"00": [1, 2, 3]}, "cnt": 1}]
        with _patch_observe_thread(fpga_instance, items) as mock_thread_cls:
            fpga_instance.observe()  # no pairs arg → defaults to self.pairs

        # Producer thread was constructed with self.pairs
        call_kwargs = mock_thread_cls.call_args.kwargs
        assert call_kwargs["args"] == (fpga_instance.pairs,)
        assert call_kwargs["kwargs"] == {"timeout": 10}
        assert call_kwargs["target"] == fpga_instance._read_integrations

        # And the log line names self.pairs, not the literal "None"
        fpga_instance.logger.info.assert_any_call(
            f"Starting observation for pairs: {fpga_instance.pairs}."
        )
        # Data still drained normally
        fpga_instance.update_redis.assert_called_once_with(
            {"00": [1, 2, 3]}, 1
        )

    def test_observe_timeout_immediate(self, fpga_instance):
        """
        observe() exits cleanly when the producer ends without ever
        pushing data (only the None sentinel arrives).

        This subsumes the old test_observe_continuous_no_data — the
        consumer can't tell whether the producer never had data or
        timed out without it; both surface as "sentinel only". The
        producer-side no-data path is covered separately by
        test_read_integrations_no_new_data.
        """
        fpga_instance.update_redis = Mock()

        with _patch_observe_thread(fpga_instance, [None]):
            fpga_instance.observe(pairs=["00"], timeout=1)

        fpga_instance.update_redis.assert_not_called()
        fpga_instance.logger.info.assert_any_call(
            "End of queue, processing finished."
        )

    def test_observe_logging(self, fpga_instance):
        """observe() emits the expected info log lines."""
        items = [{"data": {"00": [1, 2, 3]}, "cnt": 1}, None]
        with _patch_observe_thread(fpga_instance, items):
            # Compute expected integration time *inside* the helper:
            # the helper sets fpga.read_uint.return_value, which the
            # header property needs to compute t_int.
            expected_t_int = fpga_instance.header["integration_time"]
            fpga_instance.observe(pairs=["00"], timeout=10)

        fpga_instance.logger.info.assert_any_call(
            f"Integration time is {expected_t_int} seconds."
        )
        fpga_instance.logger.info.assert_any_call(
            "Starting observation for pairs: ['00']."
        )
        fpga_instance.logger.info.assert_any_call(
            "End of queue, processing finished."
        )

    def test_observe_integration_loop(self, fpga_instance):
        """
        Consumer drains every queued integration, in order, before
        exiting on the sentinel.
        """
        fpga_instance.update_redis = Mock()

        items = [{"data": {"00": [i]}, "cnt": 10 + i} for i in range(5)] + [
            None
        ]
        with _patch_observe_thread(fpga_instance, items):
            fpga_instance.observe(pairs=["00", "11"], timeout=10)

        assert fpga_instance.update_redis.call_count == 5
        # Order matters — assert via call_args_list, not assert_any_call.
        for i, call in enumerate(fpga_instance.update_redis.call_args_list):
            assert call.args == ({"00": [i]}, 10 + i)
