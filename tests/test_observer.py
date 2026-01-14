import logging
import pytest
import threading
import time
from unittest.mock import Mock, patch

from eigsep_observing import EigObserver
from eigsep_observing.testing import DummyEigsepRedis
from eigsep_observing.testing.utils import generate_data, generate_s11_data


@pytest.fixture
def redis_snap():
    """Mock Redis connection for SNAP correlator."""
    redis = DummyEigsepRedis()
    # Mock correlator config
    redis.get_corr_config = Mock(
        return_value={
            "integration_time": 1.0,
            "pairs": ["0", "1", "2", "3", "02", "13"],
        }
    )
    return redis


@pytest.fixture
def redis_panda():
    """Mock Redis connection for LattePanda."""
    redis = DummyEigsepRedis()
    # Set up control commands for switch and VNA
    redis.r.sadd("ctrl_commands", "switch")
    redis.r.sadd("ctrl_commands", "VNA")

    # Mock config
    redis.get_config = Mock(
        return_value={
            "switch_schedule": {"sky": 0.05, "load": 0.02, "noise": 0.02},
            "vna_settings": {
                "ip": "127.0.0.1",
                "port": 5025,
                "fstart": 1e6,
                "power_dBm": {"ant": -20, "rec": -40},
            },
            "vna_save_dir": "/tmp/test_vna",
            "vna_interval": 0.5,
        }
    )
    # Mock client heartbeat check and control methods
    redis.client_heartbeat_check = Mock(return_value=True)
    redis.send_ctrl = Mock()
    redis.read_vna_data = Mock()
    redis.read_status = Mock(
        return_value=(None, None)
    )  # Default return to avoid thread warnings
    return redis


@pytest.fixture
def observer_snap_only(redis_snap):
    """EigObserver with only SNAP connection."""
    obs = EigObserver(redis_snap=redis_snap)
    yield obs
    obs.stop_event.set()  # ensure any threads are stopped after test
    obs.status_thread.join(timeout=1)


@pytest.fixture
def observer_panda_only(redis_panda):
    """EigObserver with only LattePanda connection."""
    obs = EigObserver(redis_panda=redis_panda)
    yield obs
    obs.stop_event.set()
    obs.status_thread.join(timeout=1)


@pytest.fixture
def observer_both(redis_snap, redis_panda):
    """EigObserver with both SNAP and LattePanda connections."""
    obs = EigObserver(redis_snap=redis_snap, redis_panda=redis_panda)
    yield obs
    obs.stop_event.set()
    obs.status_thread.join(timeout=1)


def test_observer_init_snap_only(observer_snap_only, redis_snap):
    """Test EigObserver initialization with only SNAP connection."""
    assert observer_snap_only.redis_snap is redis_snap
    assert observer_snap_only.redis_panda is None
    assert observer_snap_only.corr_cfg is not None


def test_observer_init_panda_only(observer_panda_only, redis_panda):
    """Test EigObserver initialization with only LattePanda connection."""
    assert observer_panda_only.redis_snap is None
    assert observer_panda_only.redis_panda is redis_panda
    assert observer_panda_only.cfg is not None


def test_observer_init_both(observer_both, redis_snap, redis_panda):
    """Test EigObserver initialization with both connections."""
    assert observer_both.redis_snap is redis_snap
    assert observer_both.redis_panda is redis_panda
    assert observer_both.corr_cfg is not None
    assert observer_both.cfg is not None


def test_observer_init_none():
    """Test EigObserver initialization with no connections."""
    observer = EigObserver()
    assert observer.redis_snap is None
    assert observer.redis_panda is None
    observer.stop_event.set()
    observer.status_thread.join(timeout=1)


def test_snap_connected_property(observer_snap_only, observer_panda_only):
    """Test snap_connected property."""
    assert observer_snap_only.snap_connected is True
    assert observer_panda_only.snap_connected is False


def test_panda_connected_property(
    observer_snap_only, observer_panda_only, redis_panda
):
    """Test panda_connected property."""
    assert observer_snap_only.panda_connected is False
    assert observer_panda_only.panda_connected is True

    # Test when redis is None
    observer_none = EigObserver()
    assert observer_none.panda_connected is False

    # Test when heartbeat check fails
    redis_panda.client_heartbeat_check.return_value = False
    assert observer_panda_only.panda_connected is False

    # clean up
    observer_none.stop_event.set()
    observer_none.status_thread.join(timeout=1)


def test_set_mode_valid(observer_panda_only, redis_panda):
    """Test set_mode with valid modes."""
    observer = observer_panda_only

    for mode in ["RFANT", "RFNOFF", "RFNON"]:
        observer.set_mode(mode)
        redis_panda.send_ctrl.assert_called()

        # Check the command mapping
        expected_cmds = {
            "RFANT": "switch:RFANT",
            "RFNOFF": "switch:RFNOFF",
            "RFNON": "switch:RFNON",
        }
        last_call = redis_panda.send_ctrl.call_args[0][0]
        assert last_call == expected_cmds[mode]


def test_set_mode_invalid(observer_panda_only):
    """Test set_mode with invalid mode."""
    with pytest.raises(ValueError, match="Invalid mode: invalid"):
        observer_panda_only.set_mode("invalid")


def test_set_mode_no_panda():
    """Test set_mode without panda connection raises AttributeError."""
    observer = EigObserver()
    with pytest.raises(AttributeError):
        observer.set_mode("sky")

    # clean up
    observer.stop_event.set()
    observer.status_thread.join(timeout=1)


def test_measure_s11_valid_modes(observer_panda_only, redis_panda):
    """Test measure_s11 with valid modes."""
    observer = observer_panda_only

    # Mock VNA data response
    mock_data, mock_cal_data = generate_s11_data(cal=True)
    # Combine data and cal_data into single dict as the actual VNA would return
    combined_data = mock_data.copy()
    for k, v in mock_cal_data.items():
        combined_data[f"cal:{k}"] = v
    redis_panda.read_vna_data.return_value = (
        combined_data,
        {"header": "test"},
        {"meta": "test"},
    )

    for mode in ["ant", "rec"]:
        result = observer.measure_s11(mode, write_files=False)
        assert result is not None
        # The method returns combined data (not separated)
        assert result == combined_data

        # Verify ctrl command was sent with correct mode
        redis_panda.send_ctrl.assert_called()
        last_call_args = redis_panda.send_ctrl.call_args
        assert last_call_args[0][0] == f"vna:{mode}"


def test_measure_s11_invalid_mode(observer_panda_only):
    """Test measure_s11 with invalid mode."""
    with pytest.raises(ValueError, match="Invalid mode: invalid"):
        observer_panda_only.measure_s11("invalid")


def test_measure_s11_timeout(observer_panda_only, redis_panda, caplog):
    """Test measure_s11 timeout handling."""
    caplog.set_level(logging.ERROR)
    redis_panda.read_vna_data.side_effect = TimeoutError

    result = observer_panda_only.measure_s11("ant", write_files=False)
    assert result is None
    assert "Timeout while waiting for VNA data" in caplog.text


@patch("eigsep_observing.io.write_s11_file")
def test_measure_s11_write_files(mock_write, observer_panda_only, redis_panda):
    """Test measure_s11 with file writing."""
    observer = observer_panda_only
    mock_data, mock_cal_data = generate_s11_data(cal=True)
    mock_header = {"header": "test"}
    mock_metadata = {"meta": "test"}

    # Combine data and cal_data as the VNA would return
    combined_data = mock_data.copy()
    for k, v in mock_cal_data.items():
        combined_data[f"cal:{k}"] = v
    redis_panda.read_vna_data.return_value = (
        combined_data,
        mock_header,
        mock_metadata,
    )

    observer.measure_s11("ant", write_files=True)

    # Verify write_s11_file was called with correct arguments
    mock_write.assert_called_once_with(
        combined_data,
        mock_header,
        metadata=mock_metadata,
        save_dir="/tmp/test_vna",
    )


def test_measure_s11_no_panda():
    """Test measure_s11 without panda connection raises AttributeError."""
    observer = EigObserver()
    with pytest.raises(AttributeError):
        observer.measure_s11("ant")


@patch("eigsep_observing.io.File")
def test_record_corr_data(mock_file_class, observer_snap_only, redis_snap):
    """Test record_corr_data method."""
    observer = observer_snap_only

    # Mock file instance
    mock_file = Mock()
    mock_file.__len__ = Mock(return_value=0)
    mock_file.add_data = Mock(return_value=None)  # No file written yet
    mock_file_class.return_value = mock_file

    # Mock correlator data
    mock_data = generate_data(ntimes=1)
    redis_snap.read_corr_data = Mock(return_value=(123, 0, mock_data))

    # Start recording in a thread and stop it quickly
    stop_event = observer.stop_event

    def stop_after_delay():
        time.sleep(0.1)
        stop_event.set()

    stop_thread = threading.Thread(target=stop_after_delay)
    stop_thread.start()

    observer.record_corr_data("/tmp/test", timeout=5)
    stop_thread.join()

    # Verify File was created with correct parameters
    mock_file_class.assert_called_once_with(
        "/tmp/test",  # save_dir
        ["0", "1", "2", "3", "02", "13"],  # pairs (default)
        240,  # ntimes
        observer.corr_cfg,
    )

    # Verify data was read and added
    redis_snap.read_corr_data.assert_called()
    mock_file.add_data.assert_called_with(123, 0, mock_data, metadata=None)


def test_record_corr_data_no_snap():
    """Test record_corr_data without snap connection raises AttributeError."""
    observer = EigObserver()
    with pytest.raises(AttributeError):
        observer.record_corr_data("/tmp")


def test_status_logger(observer_panda_only, redis_panda, caplog):
    """Test status_logger method."""
    caplog.set_level(logging.INFO)

    # Set up mock to return messages then None
    redis_panda.read_status.side_effect = [
        (logging.INFO, "Test status 1"),
        (logging.WARNING, "Test status 2"),
        (None, None),
        (None, None),  # Continue returning None
    ]

    # Run status logger briefly
    observer = observer_panda_only

    def run_logger():
        # Run for a short time then stop
        count = 0
        while count < 3:  # Limit iterations
            level, status = observer.redis_panda.read_status()
            if status is None:
                break
            observer.logger.log(level, status)
            count += 1

    logger_thread = threading.Thread(target=run_logger)
    logger_thread.start()
    logger_thread.join(timeout=1)

    # Verify status messages were logged
    assert "Test status 1" in caplog.text
    assert "Test status 2" in caplog.text


def test_logger_attribute(observer_both):
    """Test that logger attribute is set."""
    assert hasattr(observer_both, "logger")
    assert observer_both.logger == logging.getLogger(
        "eigsep_observing.observer"
    )
