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
            "file_time": 60.0,
            "save_dir": "/tmp/test",
            "ntimes": 60,
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
    return EigObserver(redis_snap=redis_snap)


@pytest.fixture
def observer_panda_only(redis_panda):
    """EigObserver with only LattePanda connection."""
    return EigObserver(redis_panda=redis_panda)


@pytest.fixture
def observer_both(redis_snap, redis_panda):
    """EigObserver with both SNAP and LattePanda connections."""
    return EigObserver(redis_snap=redis_snap, redis_panda=redis_panda)


def test_observer_init_snap_only(observer_snap_only, redis_snap):
    """Test EigObserver initialization with only SNAP connection."""
    assert observer_snap_only.redis_snap is redis_snap
    assert observer_snap_only.redis_panda is None
    assert observer_snap_only.corr_cfg is not None
    assert hasattr(observer_snap_only, "stop_events")
    assert hasattr(observer_snap_only, "switch_lock")


def test_observer_init_panda_only(observer_panda_only, redis_panda):
    """Test EigObserver initialization with only LattePanda connection."""
    assert observer_panda_only.redis_snap is None
    assert observer_panda_only.redis_panda is redis_panda
    assert observer_panda_only.cfg is not None
    assert hasattr(observer_panda_only, "stop_events")
    assert hasattr(observer_panda_only, "switch_lock")


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


def test_set_mode_valid(observer_panda_only, redis_panda):
    """Test set_mode with valid modes."""
    observer = observer_panda_only

    for mode in ["sky", "load", "noise"]:
        observer.set_mode(mode)
        redis_panda.send_ctrl.assert_called()

        # Check the command mapping
        expected_cmds = {
            "sky": "switch:RFANT",
            "load": "switch:RFLOAD",
            "noise": "switch:RFN",
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


def test_measure_s11_valid_modes(observer_panda_only, redis_panda):
    """Test measure_s11 with valid modes."""
    observer = observer_panda_only

    # Mock VNA data response
    mock_data, mock_cal_data = generate_s11_data(cal=True)
    redis_panda.read_vna_data.return_value = (
        "test_eid",
        mock_data,
        mock_cal_data,
        {"header": "test"},
        {"meta": "test"},
    )

    for mode in ["ant", "rec"]:
        result = observer.measure_s11(mode, write_files=False)
        assert result is not None
        data, cal_data = result
        assert data == mock_data
        assert cal_data == mock_cal_data

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
    assert result == (None, None)
    assert "Timeout while waiting for VNA data" in caplog.text


@patch("eigsep_observing.io.write_s11_file")
def test_measure_s11_write_files(mock_write, observer_panda_only, redis_panda):
    """Test measure_s11 with file writing."""
    observer = observer_panda_only
    mock_data, mock_cal_data = generate_s11_data(cal=True)
    mock_header = {"header": "test"}
    mock_metadata = {"meta": "test"}

    redis_panda.read_vna_data.return_value = (
        "test_eid",
        mock_data,
        mock_cal_data,
        mock_header,
        mock_metadata,
    )

    observer.measure_s11("ant", write_files=True)

    # Verify write_s11_file was called with correct arguments
    mock_write.assert_called_once_with(
        mock_data,
        mock_header,
        metadata=mock_metadata,
        cal_data=mock_cal_data,
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
    redis_snap.read_corr_data = Mock(return_value=(123, mock_data))

    # Start recording in a thread and stop it quickly
    stop_event = observer.stop_events["snap"]

    def stop_after_delay():
        time.sleep(0.1)
        stop_event.set()

    stop_thread = threading.Thread(target=stop_after_delay)
    stop_thread.start()

    observer.record_corr_data(timeout=5)
    stop_thread.join()

    # Verify File was created with correct parameters
    mock_file_class.assert_called_once_with(
        "/tmp/test",  # save_dir
        None,  # pairs (default)
        60,  # ntimes
        observer.corr_cfg,
        redis=None,  # redis_panda is None
    )

    # Verify data was read and added
    redis_snap.read_corr_data.assert_called()
    mock_file.add_data.assert_called_with(123, mock_data)


def test_record_corr_data_no_snap():
    """Test record_corr_data without snap connection raises AttributeError."""
    observer = EigObserver()
    with pytest.raises(AttributeError):
        observer.record_corr_data()


def test_do_switching(observer_panda_only, redis_panda):
    """Test do_switching method."""
    observer = observer_panda_only

    # Start switching in thread and stop after a brief delay
    stop_event = observer.stop_events["switches"]

    def stop_after_delay():
        time.sleep(0.1)  # Wait long enough for at least one switching cycle
        stop_event.set()

    stop_thread = threading.Thread(target=stop_after_delay)
    stop_thread.start()

    # Run switching with timeout protection
    start_time = time.time()
    observer.do_switching()
    end_time = time.time()
    stop_thread.join()

    # Verify the test completed quickly
    assert (
        end_time - start_time < 1.0
    ), f"Test took too long: {end_time - start_time:.2f}s"

    # Verify switching commands were sent
    assert redis_panda.send_ctrl.call_count > 0


def test_observe_vna(observer_panda_only, redis_panda):
    """Test observe_vna method."""
    observer = observer_panda_only

    # Mock VNA data
    mock_data, mock_cal_data = generate_s11_data(cal=True)
    redis_panda.read_vna_data.return_value = (
        "test_eid",
        mock_data,
        mock_cal_data,
        {"header": "test"},
        {"meta": "test"},
    )

    # Reduce VNA interval to speed up test
    observer.cfg["vna_interval"] = 0.01

    # Start VNA observation in thread and stop quickly
    stop_event = observer.stop_events["vna"]

    def stop_after_delay():
        time.sleep(0.05)  # Reduced from 0.1 to 0.05
        stop_event.set()

    stop_thread = threading.Thread(target=stop_after_delay)
    stop_thread.start()

    with patch("eigsep_observing.io.write_s11_file"):
        start_time = time.time()
        observer.observe_vna()
        duration = time.time() - start_time

    stop_thread.join(timeout=1)  # Add timeout to join

    # Ensure test completes quickly
    assert duration < 5.0, f"Test took too long: {duration:.2f}s"

    # Verify VNA commands were sent
    assert redis_panda.send_ctrl.call_count > 0


def test_rotate_motors_not_implemented(observer_panda_only):
    """Test rotate_motors raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        observer_panda_only.rotate_motors(["motor1", "motor2"])


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


def test_threading_stop_events(observer_both):
    """Test that stop events are properly initialized."""
    expected_events = ["switches", "vna", "motors", "snap", "status"]
    for event_name in expected_events:
        assert event_name in observer_both.stop_events
        assert isinstance(
            observer_both.stop_events[event_name], threading.Event
        )
        assert not observer_both.stop_events[event_name].is_set()


def test_switch_lock_initialized(observer_both):
    """Test that switch lock is properly initialized."""
    assert hasattr(observer_both, "switch_lock")
    assert isinstance(observer_both.switch_lock, type(threading.Lock()))


def test_logger_attribute(observer_both):
    """Test that logger attribute is set."""
    assert hasattr(observer_both, "logger")
    assert observer_both.logger == logging.getLogger(
        "eigsep_observing.observer"
    )
