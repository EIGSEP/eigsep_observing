import pytest
from unittest.mock import Mock, patch

from eigsep_observing import EigsepFpga
from eigsep_observing.testing import DummyEigsepFpga
from eigsep_observing.testing import DummyEigsepRedis


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
    fpga.prev_cnt = 0
    fpga.read_data = Mock(return_value={"00": [1, 2, 3], "11": [4, 5, 6]})
    return fpga


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
        fpga_instance.fpga.read_int.return_value = 5  # Same as prev_cnt

        data, cnt = fpga_instance._read_integrations(["00", "11"], prev_cnt=5)

        assert data is None
        assert cnt == 5
        fpga_instance.fpga.read_int.assert_called_once_with("corr_acc_cnt")

    def test_read_integrations_new_data(self, fpga_instance):
        """Test _read_integrations with new data available."""
        fpga_instance.fpga.read_int.side_effect = [
            6,
            6,
        ]  # New count, then same for validation
        expected_data = {"00": [1, 2, 3], "11": [4, 5, 6]}
        fpga_instance.read_data.return_value = expected_data

        data, cnt = fpga_instance._read_integrations(["00", "11"], prev_cnt=5)

        assert data == expected_data
        assert cnt == 6
        fpga_instance.logger.info.assert_called_with(
            "Reading acc_cnt=6 from correlator."
        )
        fpga_instance.read_data.assert_called_once_with(
            pairs=["00", "11"], unpack=False
        )

    def test_read_integrations_missed_integrations(self, fpga_instance):
        """Test _read_integrations when integrations are missed."""
        fpga_instance.fpga.read_int.side_effect = [8, 8]  # Jumped from 5 to 8
        expected_data = {"00": [1, 2, 3], "11": [4, 5, 6]}
        fpga_instance.read_data.return_value = expected_data

        data, cnt = fpga_instance._read_integrations(["00", "11"], prev_cnt=5)

        assert data == expected_data
        assert cnt == 8
        fpga_instance.logger.warning.assert_called_with(
            "Missed 2 integration(s)."
        )

    def test_read_integrations_read_failure(self, fpga_instance):
        """Test _read_integrations when read fails to complete in time."""
        fpga_instance.fpga.read_int.side_effect = [
            6,
            7,
        ]  # Count changed during read
        expected_data = {"00": [1, 2, 3], "11": [4, 5, 6]}
        fpga_instance.read_data.return_value = expected_data

        data, cnt = fpga_instance._read_integrations(["00", "11"], prev_cnt=5)

        assert data == expected_data
        assert cnt == 6
        fpga_instance.logger.error.assert_called_with(
            "Read of acc_cnt=6 FAILED to complete before next integration. "
        )

    def test_end_observing_not_implemented(self, fpga_instance):
        """Test that end_observing raises NotImplementedError."""
        with pytest.raises(
            NotImplementedError, match="Not implemented in eigsep_observing"
        ):
            fpga_instance.end_observing()

    def test_observe_basic_functionality(self, fpga_instance):
        """Test basic observe functionality."""
        # Mock upload_config and _read_integrations
        fpga_instance.upload_config = Mock()
        fpga_instance._read_integrations = Mock()
        fpga_instance.update_redis = Mock()

        # Set up proper numeric values for hardware reads
        fpga_instance.fpga.read_uint.return_value = 1024  # acc_len

        # Set up sequence: no data, then data, then stop
        fpga_instance._read_integrations.side_effect = [
            (None, 0),  # No new data first
            ({"00": [1, 2, 3]}, 1),  # New data second
        ]

        # Use a small timeout and patch time.sleep to speed up test
        with patch("time.sleep"):
            with patch("time.time") as mock_time:
                # Simulate time progression:
                # start, loop checks, data found, reset, timeout
                mock_time.side_effect = [0, 0.1, 0.2, 0.3, 15]

                with pytest.raises(
                    TimeoutError, match="Read operation timed out"
                ):
                    fpga_instance.observe(pairs=["00"], timeout=10)

        fpga_instance.upload_config.assert_called_once_with(validate=True)
        assert fpga_instance._read_integrations.call_count == 2
        fpga_instance.update_redis.assert_called_once_with(
            {"00": [1, 2, 3]}, 1
        )

    def test_observe_default_pairs(self, fpga_instance):
        """Test observe with default pairs (None)."""
        fpga_instance.upload_config = Mock()
        fpga_instance._read_integrations = Mock(
            return_value=({"00": [1, 2, 3]}, 1)
        )
        fpga_instance.update_redis = Mock()

        # Set up proper numeric values for hardware reads
        fpga_instance.fpga.read_uint.return_value = 1024  # acc_len

        with patch("time.time") as mock_time:
            # Allow one _read_integrations call before timeout
            mock_time.side_effect = [
                0,
                1,
                2,
                15,
            ]  # start, check, data read, timeout

            with pytest.raises(TimeoutError):
                fpga_instance.observe(pairs=None, timeout=10)

        # Should use all pairs (autos + crosses)
        expected_pairs = ["00", "11", "01", "10"]
        fpga_instance._read_integrations.assert_called_with(expected_pairs, 0)

    def test_observe_timeout_immediate(self, fpga_instance):
        """Test observe timeout behavior."""
        fpga_instance.upload_config = Mock()

        # Set up proper numeric values for hardware reads
        fpga_instance.fpga.read_uint.return_value = 1024  # acc_len

        with patch("time.time") as mock_time:
            # Simulate immediate timeout
            mock_time.side_effect = [0, 15]  # Start time, then past timeout

            with pytest.raises(TimeoutError, match="Read operation timed out"):
                fpga_instance.observe(timeout=10)

    def test_observe_continuous_no_data(self, fpga_instance):
        """Test observe when continuously getting no data until timeout."""
        fpga_instance.upload_config = Mock()
        fpga_instance._read_integrations = Mock(return_value=(None, 0))

        # Set up proper numeric values for hardware reads
        fpga_instance.fpga.read_uint.return_value = 1024  # acc_len

        with patch("time.sleep") as mock_sleep:
            with patch("time.time") as mock_time:
                # No data for several iterations, then timeout
                mock_time.side_effect = [0, 1, 2, 3, 15]

                with pytest.raises(TimeoutError):
                    fpga_instance.observe(timeout=10)

        # Should have called sleep between checks
        assert mock_sleep.call_count >= 1
        mock_sleep.assert_called_with(0.1)

    def test_observe_logging(self, fpga_instance):
        """Test observe logging behavior."""
        fpga_instance.upload_config = Mock()
        fpga_instance._read_integrations = Mock(return_value=(None, 0))

        # Set up proper numeric values for hardware reads
        fpga_instance.fpga.read_uint.return_value = 1024  # acc_len

        with patch("time.time") as mock_time:
            mock_time.side_effect = [0, 15]  # Immediate timeout

            with pytest.raises(TimeoutError):
                fpga_instance.observe(pairs=["00", "11"])

        # Check logging calls
        # Integration time is calculated from actual config values
        fpga_instance.logger.info.assert_any_call(
            "Integration time is 4.096e-06 seconds."
        )
        fpga_instance.logger.info.assert_any_call(
            "Starting observation for pairs: ['00', '11']."
        )

    def test_observe_prev_cnt_update(self, fpga_instance):
        """Test that observe updates prev_cnt correctly."""
        fpga_instance.upload_config = Mock()
        fpga_instance.update_redis = Mock()

        # Set up proper numeric values for hardware reads
        fpga_instance.fpga.read_uint.return_value = 1024  # acc_len

        # Simulate getting data with specific count
        fpga_instance._read_integrations = Mock(
            side_effect=[
                ({"00": [1, 2, 3]}, 5),  # First data
                (None, 5),  # No new data (timeout will trigger)
            ]
        )

        with patch("time.time") as mock_time:
            mock_time.side_effect = [
                0,
                1,
                2,
                15,
            ]  # start, check, reset timer, timeout

            with pytest.raises(TimeoutError):
                fpga_instance.observe(timeout=10)

        # Check that prev_cnt was updated
        assert fpga_instance.prev_cnt == 5

    @patch("time.time")
    @patch("time.sleep")
    def test_observe_integration_loop(
        self, mock_sleep, mock_time, fpga_instance
    ):
        """Test observe integration loop with multiple data reads."""
        fpga_instance.upload_config = Mock()
        fpga_instance.update_redis = Mock()

        # Set up proper numeric values for hardware reads
        fpga_instance.fpga.read_uint.return_value = 1024  # acc_len

        # Simulate multiple data reads before timeout
        fpga_instance._read_integrations = Mock(
            side_effect=[
                (None, 0),  # No data
                ({"00": [1, 2, 3]}, 1),  # First data
                (None, 1),  # No new data
                ({"00": [4, 5, 6]}, 2),  # Second data
                (None, 2),  # No data (will timeout)
            ]
        )

        # Time progression: start, multiple checks, timeout
        mock_time.side_effect = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 15.0]

        with pytest.raises(TimeoutError):
            fpga_instance.observe(timeout=10)

        # Verify correct number of calls
        assert fpga_instance._read_integrations.call_count == 5
        assert fpga_instance.update_redis.call_count == 2

        # Verify update_redis calls
        fpga_instance.update_redis.assert_any_call({"00": [1, 2, 3]}, 1)
        fpga_instance.update_redis.assert_any_call({"00": [4, 5, 6]}, 2)

        # Verify prev_cnt progression
        assert fpga_instance.prev_cnt == 2
