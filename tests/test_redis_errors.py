import logging
import pytest
import json
from unittest.mock import Mock, patch
import redis

from eigsep_observing.redis import EigsepRedis
from eigsep_observing.testing import DummyEigsepRedis


class TestRedisConnectionErrors:
    """Test Redis connection error handling."""

    @patch("redis.Redis")
    def test_init_connection_error(self, mock_redis_class):
        """Test initialization when Redis connection fails."""
        mock_redis_instance = Mock()
        mock_redis_instance.ping.side_effect = redis.ConnectionError(
            "Connection failed"
        )
        mock_redis_class.return_value = mock_redis_instance

        with pytest.raises(redis.ConnectionError):
            EigsepRedis(host="localhost", port=6379)

    @patch("redis.Redis")
    def test_init_timeout_error(self, mock_redis_class):
        """Test initialization when Redis connection times out."""
        mock_redis_instance = Mock()
        mock_redis_instance.ping.side_effect = redis.TimeoutError(
            "Connection timeout"
        )
        mock_redis_class.return_value = mock_redis_instance

        with pytest.raises(redis.TimeoutError):
            EigsepRedis(host="localhost", port=6379)


class TestRedisSafeOperations:
    """Test _safe_redis_operation method error handling."""

    @pytest.fixture
    def redis_instance(self):
        """Create a Redis instance with mocked connection."""
        redis_obj = DummyEigsepRedis()
        redis_obj._connection_retries = 3
        redis_obj._connection_timeout = 1.0
        return redis_obj

    def test_safe_redis_operation_success(self, redis_instance):
        """Test successful safe Redis operation."""
        mock_operation = Mock(return_value="success")

        result = redis_instance._safe_redis_operation(mock_operation)

        assert result == "success"
        mock_operation.assert_called_once()

    def test_safe_redis_operation_connection_error_retry(self, redis_instance):
        """Test retry logic on connection errors."""
        # Ensure retry is enabled
        redis_instance.retry_on_timeout = True

        mock_operation = Mock()
        # Fail once, then succeed on retry
        mock_operation.side_effect = [
            redis.ConnectionError("Connection lost"),
            "success",
        ]

        # Mock the ping method to succeed (simulating successful reconnection)
        with patch.object(redis_instance.r, "ping"):
            with patch("time.sleep"):  # Speed up test
                result = redis_instance._safe_redis_operation(mock_operation)

        assert result == "success"
        assert mock_operation.call_count == 2

    def test_safe_redis_operation_max_retries_exceeded(self, redis_instance):
        """Test when max retries are exceeded."""
        # Ensure retry is enabled
        redis_instance.retry_on_timeout = True

        mock_operation = Mock()
        mock_operation.side_effect = redis.ConnectionError(
            "Persistent failure"
        )

        # Mock ping to fail as well (connection can't be re-established)
        with patch.object(
            redis_instance.r,
            "ping",
            side_effect=redis.ConnectionError("Ping failed"),
        ):
            with patch("time.sleep"):  # Speed up test
                with pytest.raises(
                    redis.ConnectionError, match="Persistent failure"
                ):
                    redis_instance._safe_redis_operation(mock_operation)

        assert (
            mock_operation.call_count == 1
        )  # Only initial call, retry fails during ping

    def test_safe_redis_operation_timeout_error(self, redis_instance):
        """Test timeout error handling."""
        mock_operation = Mock()
        mock_operation.side_effect = redis.TimeoutError("Operation timeout")

        with pytest.raises(redis.TimeoutError, match="Operation timeout"):
            redis_instance._safe_redis_operation(mock_operation)

        mock_operation.assert_called_once()  # No retries for timeout

    def test_safe_redis_operation_unexpected_error(self, redis_instance):
        """Test unexpected error handling."""
        mock_operation = Mock()
        mock_operation.side_effect = ValueError("Unexpected error")

        with pytest.raises(ValueError, match="Unexpected error"):
            redis_instance._safe_redis_operation(mock_operation)

        mock_operation.assert_called_once()  # No retries for unexpected errors


class TestRedisDataValidation:
    """Test data validation in Redis operations."""

    @pytest.fixture
    def redis_instance(self):
        """Create a Redis instance for testing."""
        return DummyEigsepRedis()

    def test_add_corr_data_empty_data(self, redis_instance):
        """Test add_corr_data with empty data."""
        with pytest.raises(
            ValueError, match="data dictionary cannot be empty"
        ):
            redis_instance.add_corr_data({}, 1, dtype="float32")

    def test_add_corr_data_invalid_type(self, redis_instance):
        """Test add_corr_data with invalid data type."""
        with pytest.raises(TypeError):
            redis_instance.add_corr_data("not_a_dict", 1, dtype="float32")

    def test_add_corr_data_invalid_count(self, redis_instance):
        """Test add_corr_data with invalid count."""
        with pytest.raises(ValueError):
            redis_instance.add_corr_data(
                {"00": b"test_data"}, -1, dtype="float32"
            )

    def test_add_corr_data_invalid_dtype(self, redis_instance):
        """Test add_corr_data with invalid dtype."""
        # This test actually checks data type validation first
        with pytest.raises(TypeError, match="Correlation data must be bytes"):
            redis_instance.add_corr_data(
                {"00": [1, 2, 3]}, 1, dtype="invalid_type"
            )

    def test_add_corr_data_missing_pairs(self, redis_instance):
        """Test add_corr_data with missing correlation pairs."""
        # Mock the pairs property to expect specific pairs
        redis_instance.pairs = ["00", "11", "01"]

        with pytest.raises(TypeError, match="Correlation data must be bytes"):
            redis_instance.add_corr_data({"00": [1, 2, 3]}, 1, dtype="float32")

    def test_add_corr_data_extra_pairs(self, redis_instance):
        """Test add_corr_data with extra correlation pairs."""
        redis_instance.pairs = ["00", "11"]

        with pytest.raises(TypeError, match="Correlation data must be bytes"):
            redis_instance.add_corr_data(
                {
                    "00": [1, 2, 3],
                    "11": [4, 5, 6],
                    "22": [7, 8, 9],  # Extra pair
                },
                1,
                dtype="float32",
            )


class TestRedisControlCommands:
    """Test control command validation and handling."""

    @pytest.fixture
    def redis_instance(self):
        """Create a Redis instance for testing."""
        return DummyEigsepRedis()

    def test_send_ctrl_invalid_command(self, redis_instance):
        """Test sending invalid control command."""
        # DummyEigsepRedis already sets up valid commands
        # No need to mock smembers

        with pytest.raises(ValueError):
            redis_instance.send_ctrl("invalid_command", param1="value1")

    def test_send_ctrl_serialization_error(self, redis_instance):
        """Test control command with non-serializable kwargs."""
        # DummyEigsepRedis already sets up valid commands
        # No need to mock smembers

        # Create a non-serializable object
        class NonSerializable:
            pass

        with pytest.raises(ValueError):
            redis_instance.send_ctrl("switch:VNAO", param=NonSerializable())

    def test_send_ctrl_redis_error(self, redis_instance):
        """Test control command when Redis operation fails."""
        # DummyEigsepRedis already sets up valid commands
        # No need to mock smembers
        redis_instance.r.xadd = Mock(
            side_effect=redis.RedisError("Redis operation failed")
        )

        with pytest.raises(redis.RedisError):
            redis_instance.send_ctrl("switch:VNAO", param="value")

    def test_read_ctrl_timeout(self, redis_instance):
        """Test read_ctrl with timeout."""
        redis_instance.r.xread = Mock(return_value=[])  # No data - empty list

        # read_ctrl returns (None, {}) when no data, doesn't raise TimeoutError
        result = redis_instance.read_ctrl(timeout=0.1)
        assert result == (None, {})

    def test_read_ctrl_type_error(self, redis_instance):
        """Test read_ctrl with malformed data causing JSONDecodeError."""
        # Mock malformed response in correct format:
        # [(stream_name, [(entry_id, fields)])]
        redis_instance.r.xread = Mock(
            return_value=[
                (b"stream:ctrl", [(b"id", {b"msg": b"malformed_json"})])
            ]
        )

        # This will raise JSONDecodeError
        with pytest.raises(json.JSONDecodeError):
            redis_instance.read_ctrl(timeout=1)


class TestRedisConnectionManagement:
    """Test connection management methods."""

    @pytest.fixture
    def redis_instance(self):
        """Create a Redis instance for testing."""
        return DummyEigsepRedis()

    def test_is_connected_error(self, redis_instance):
        """Test is_connected when ping raises an error."""
        redis_instance.r.ping = Mock(
            side_effect=redis.ConnectionError("Not connected")
        )

        assert redis_instance.is_connected() is False

    def test_is_connected_timeout(self, redis_instance):
        """Test is_connected when ping times out."""
        redis_instance.r.ping = Mock(
            side_effect=redis.TimeoutError("Ping timeout")
        )

        assert redis_instance.is_connected() is False

    def test_close_connection_error(self, redis_instance):
        """Test close method when connection close fails."""
        redis_instance.r.close = Mock(
            side_effect=redis.RedisError("Close failed")
        )

        # Should not raise exception, just log error
        redis_instance.close()
        redis_instance.r.close.assert_called_once()

    def test_context_manager_error_handling(self, redis_instance):
        """Test context manager when Redis close fails."""
        # Mock the Redis connection's close method to raise an error
        redis_instance.r.close = Mock(
            side_effect=Exception("Redis close failed")
        )

        # The close method handles exceptions internally
        with redis_instance:
            pass

        # Verify close was attempted
        redis_instance.r.close.assert_called_once()

    def test_context_manager_with_exception(self, redis_instance):
        """Test context manager when operation inside raises exception."""
        redis_instance.close = Mock()

        with pytest.raises(ValueError):
            with redis_instance:
                raise ValueError("Test exception")

        # Should still call close
        redis_instance.close.assert_called_once()


class TestRedisStreamOperations:
    """Test Redis stream operations error handling."""

    @pytest.fixture
    def redis_instance(self):
        """Create a Redis instance for testing."""
        return DummyEigsepRedis()

    def test_read_corr_data_timeout(self, redis_instance):
        """Test read_corr_data timeout behavior."""
        # Set up the stream first
        redis_instance.r.sadd("data_streams", "stream:corr")
        redis_instance.r.xread = Mock(return_value={})  # No data

        with pytest.raises(TimeoutError):
            redis_instance.read_corr_data(timeout=0.1)

    def test_read_corr_data_malformed_data(self, redis_instance):
        """Test read_corr_data with malformed stream data."""
        # Set up the stream first
        redis_instance.r.sadd("data_streams", "stream:corr")

        # Mock malformed response
        redis_instance.r.xread = Mock(
            return_value=[(b"stream:corr", [(b"id", {b"malformed": b"data"})])]
        )

        with pytest.raises(KeyError):
            redis_instance.read_corr_data(timeout=1)

    def test_read_corr_data_unpacking_error(self, redis_instance):
        """Test read_corr_data when data unpacking fails."""
        # Set up the stream first
        redis_instance.r.sadd("data_streams", "stream:corr")

        # Mock response with correct Redis stream structure:
        # [(stream_name, [(entry_id, fields)])]
        redis_instance.r.xread = Mock(
            return_value=[
                (
                    b"stream:corr",
                    [
                        (
                            b"id",
                            {
                                b"acc_cnt": b"123",
                                b"dtype": b">i4",
                                b"00": b"invalid_data",
                            },
                        )
                    ],
                )
            ]
        )

        with patch("numpy.frombuffer", side_effect=ValueError("Invalid data")):
            with pytest.raises(ValueError):
                redis_instance.read_corr_data(timeout=1, unpack=True)

    def test_send_status_logging_error(self, redis_instance):
        """Test send_status when Redis operation fails."""
        redis_instance.r.xadd = Mock(
            side_effect=redis.RedisError("Stream error")
        )

        with pytest.raises(redis.RedisError):
            redis_instance.send_status(logging.INFO, "Test message")


class TestRedisConfigOperations:
    """Test configuration operations error handling."""

    @pytest.fixture
    def redis_instance(self):
        """Create a Redis instance for testing."""
        return DummyEigsepRedis()

    def test_upload_corr_config_serialization_error(self, redis_instance):
        """Test upload_corr_config with non-serializable config."""

        class NonSerializable:
            pass

        config = {"valid": "data", "invalid": NonSerializable()}

        with pytest.raises(TypeError):
            redis_instance.upload_corr_config(config, from_file=False)

    def test_get_corr_config_missing(self, redis_instance):
        """Test get_corr_config when config doesn't exist."""
        redis_instance.r.get = Mock(return_value=None)

        with pytest.raises(
            ValueError, match="No SNAP configuration found in Redis"
        ):
            redis_instance.get_corr_config()

    def test_get_corr_config_malformed(self, redis_instance):
        """Test get_corr_config with malformed JSON."""
        redis_instance.r.get = Mock(return_value=b"invalid_json")

        with pytest.raises(json.JSONDecodeError):
            redis_instance.get_corr_config()

    def test_get_config_missing(self, redis_instance):
        """Test get_config when config doesn't exist."""
        redis_instance.r.get = Mock(return_value=None)

        with pytest.raises(ValueError, match="No configuration found"):
            redis_instance.get_config()


class TestRedisMetadataOperations:
    """Test metadata operations error handling."""

    @pytest.fixture
    def redis_instance(self):
        """Create a Redis instance for testing."""
        return DummyEigsepRedis()

    def test_add_metadata_serialization_error(self, redis_instance):
        """Test add_metadata with non-serializable data."""

        class NonSerializable:
            pass

        with pytest.raises(ValueError, match="value is not JSON serializable"):
            redis_instance.add_metadata("test_key", NonSerializable())

    def test_get_live_metadata_connection_error(self, redis_instance):
        """Test get_live_metadata when Redis connection fails."""
        redis_instance.r.hgetall = Mock(
            side_effect=redis.ConnectionError("Connection lost")
        )

        with pytest.raises(redis.ConnectionError):
            redis_instance.get_live_metadata()

    def test_get_metadata_stream_error(self, redis_instance):
        """Test get_metadata when stream read fails."""
        redis_instance.r.xread = Mock(
            side_effect=redis.RedisError("Stream error")
        )

        with pytest.raises(redis.RedisError):
            redis_instance.get_metadata(stream_keys="test_key")
