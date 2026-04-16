import logging
import pytest
import threading
import time
from unittest.mock import Mock, patch

from cmt_vna.testing import DummyVNA
from picohost.testing import DummyPicoRFSwitch

from eigsep_observing import EigObserver
from eigsep_observing.testing import DummyEigsepObsRedis
from eigsep_observing.testing.utils import generate_data


@pytest.fixture
def redis_snap():
    """DummyEigsepObsRedis seeded with a correlator config."""
    redis = DummyEigsepObsRedis()
    redis.corr_config.upload_config(
        {
            "integration_time": 1.0,
            "pairs": ["0", "1", "2", "3", "02", "13"],
        },
        from_file=False,
    )
    return redis


@pytest.fixture
def redis_panda():
    """DummyEigsepObsRedis seeded with a panda config and a live heartbeat.

    The real ``ConfigStore`` / ``HeartbeatReader`` / ``StatusReader``
    run against fakeredis via ``DummyTransport``; per-test behavior is
    applied with ``patch.object`` in the tests that need it.
    """
    redis = DummyEigsepObsRedis()
    redis.config.upload(
        {
            "switch_schedule": {"sky": 0.05, "load": 0.02, "noise": 0.02},
            "vna_settings": {
                "ip": "127.0.0.1",
                "port": 5025,
                "fstart": 1e6,
                "power_dBm": {"ant": -20, "rec": -40},
            },
            "vna_save_dir": "/tmp/test_vna",
            "vna_interval": 0.5,
        },
        from_file=False,
    )
    redis.heartbeat.set(alive=True)
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
    redis_panda.heartbeat.set(alive=False)
    assert observer_panda_only.panda_connected is False

    # clean up
    observer_none.stop_event.set()
    observer_none.status_thread.join(timeout=1)


@patch("eigsep_observing.io.File")
def test_record_corr_data(mock_file_class, observer_snap_only, redis_snap):
    """Test record_corr_data method."""
    observer = observer_snap_only

    # Upload a header with sync_time so record_corr_data can proceed
    sync_time = 1713200000.0
    redis_snap.corr_config.upload_header({"sync_time": sync_time})

    # Mock file instance
    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file.add_data = Mock(return_value=None)  # No file written yet
    mock_file_class.return_value = mock_file

    # Mock correlator data
    mock_data = generate_data(ntimes=1)

    # Start recording in a thread and stop it quickly
    stop_event = observer.stop_event

    def stop_after_delay():
        time.sleep(0.1)
        stop_event.set()

    stop_thread = threading.Thread(target=stop_after_delay)
    stop_thread.start()

    with patch.object(
        redis_snap.corr_reader, "read", return_value=(123, mock_data)
    ) as mock_read:
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
    mock_read.assert_called()
    mock_file.add_data.assert_called_with(
        123, sync_time, mock_data, metadata=None
    )


@patch("eigsep_observing.io.File")
def test_record_corr_data_transient_header_blip_uses_cache(
    mock_file_class, observer_snap_only, redis_snap, caplog
):
    """Transient header-fetch ValueError with a valid cache → fall
    back to the cache and save the corr data. Non-corr failures must
    not block corr writes.
    """
    observer = observer_snap_only
    sync_time = 1713200000.0

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    mock_data = generate_data(ntimes=1)

    # First header fetch succeeds; second raises ValueError (blip).
    # After that, stop the loop.
    read_count = [0]

    def read_side_effect(*a, **kw):
        read_count[0] += 1
        if read_count[0] >= 2:
            observer.stop_event.set()
        return (100 + read_count[0], mock_data)

    good_header = {"sync_time": sync_time}
    header_calls = [good_header, ValueError("Redis blip")]

    def header_side_effect():
        val = header_calls.pop(0)
        if isinstance(val, Exception):
            raise val
        return val

    caplog.set_level(logging.WARNING, logger="eigsep_observing.observer")
    with (
        patch.object(
            redis_snap.corr_config,
            "get_header",
            side_effect=header_side_effect,
        ),
        patch.object(
            redis_snap.corr_reader, "read", side_effect=read_side_effect
        ),
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    # set_header called twice: once with fetched, once with cached.
    assert mock_file.set_header.call_count == 2
    mock_file.set_header.assert_any_call(header=good_header)
    # add_data called on both iterations — corr data is sacred, the
    # blip must not have blocked writes.
    assert mock_file.add_data.call_count >= 2
    assert "Using cached corr header" in caplog.text


@patch("eigsep_observing.io.File")
def test_record_corr_data_unsynced_watchdog_crashes(
    mock_file_class, observer_snap_only, redis_snap
):
    """sync_time=0 persistently → bounded wait → RuntimeError."""
    observer = observer_snap_only

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    redis_snap.corr_config.upload_header({"sync_time": 0})

    with pytest.raises(RuntimeError, match="SNAP never synchronized"):
        observer.record_corr_data(
            "/tmp/test", ntimes=1, timeout=5, header_wait_timeout=0.2
        )

    # No data should have been read or written — no sync anchor.
    mock_file.add_data.assert_not_called()
    # Partial-buffer flush still runs (try/finally).
    mock_file.close.assert_called()


@patch("eigsep_observing.io.File")
def test_record_corr_data_no_header_ever_crashes(
    mock_file_class, observer_snap_only, redis_snap
):
    """get_header always raises, no cache → bounded wait → RuntimeError."""
    observer = observer_snap_only

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    with (
        patch.object(
            redis_snap.corr_config,
            "get_header",
            side_effect=ValueError("no header"),
        ),
        pytest.raises(RuntimeError, match="No corr header"),
    ):
        observer.record_corr_data(
            "/tmp/test", ntimes=1, timeout=5, header_wait_timeout=0.2
        )

    mock_file.add_data.assert_not_called()
    mock_file.close.assert_called()


@patch("eigsep_observing.io.File")
def test_record_corr_data_resync_rolls_file(
    mock_file_class, observer_snap_only, redis_snap, caplog
):
    """A successful header fetch with a *different* non-zero sync_time
    than the cached one → close current file, open new one, continue.
    """
    observer = observer_snap_only

    # Each File() call returns a fresh mock so we can count
    # constructions.
    file_mocks = [Mock(), Mock(), Mock()]
    for fm in file_mocks:
        fm.counter = 0
        fm.__len__ = Mock(return_value=0)
    mock_file_class.side_effect = file_mocks

    mock_data = generate_data(ntimes=1)

    t1 = 1713200000.0
    t2 = 1713300000.0
    header_values = [{"sync_time": t1}, {"sync_time": t2}]

    def header_side_effect():
        return header_values.pop(0)

    read_count = [0]

    def read_side_effect(*a, **kw):
        read_count[0] += 1
        if read_count[0] >= 2:
            observer.stop_event.set()
        return (read_count[0], mock_data)

    caplog.set_level(logging.WARNING, logger="eigsep_observing.observer")
    with (
        patch.object(
            redis_snap.corr_config,
            "get_header",
            side_effect=header_side_effect,
        ),
        patch.object(
            redis_snap.corr_reader, "read", side_effect=read_side_effect
        ),
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    # Constructed File twice: initial, plus one roll after re-sync.
    # (Final try/finally close happens on the second one.)
    assert mock_file_class.call_count == 2
    file_mocks[0].close.assert_called()
    assert "SNAP re-synchronized" in caplog.text


def test_record_corr_data_no_snap():
    """Test record_corr_data without snap connection raises AttributeError."""
    observer = EigObserver()
    with pytest.raises(AttributeError):
        observer.record_corr_data("/tmp")


def test_status_logger(observer_panda_only, redis_panda, caplog):
    """Test status_logger method.

    The observer's status_logger thread is already running (started in
    __init__) and calling the real ``StatusReader.read`` against an
    empty fakeredis stream. Scope a ``patch.object`` to this test to
    feed it a sequence of messages, then signal the stop event once
    all messages are consumed and join the thread.
    """
    caplog.set_level(logging.INFO)

    messages = [
        (logging.INFO, "Test status 1"),
        (logging.WARNING, "Test status 2"),
    ]
    call_count = [0]

    def read_status_effect(timeout=0):
        idx = call_count[0]
        call_count[0] += 1
        if idx < len(messages):
            return messages[idx]
        observer_panda_only.stop_event.set()
        return (None, None)

    with patch.object(
        redis_panda.status_reader, "read", side_effect=read_status_effect
    ):
        observer_panda_only.status_thread.join(timeout=2)

    assert "Test status 1" in caplog.text
    assert "Test status 2" in caplog.text


def test_logger_attribute(observer_both):
    """Test that logger attribute is set."""
    assert hasattr(observer_both, "logger")
    assert observer_both.logger == logging.getLogger(
        "eigsep_observing.observer"
    )


# --- record_vna_data tests ---


@pytest.fixture
def dummy_vna():
    """DummyVNA instance for generating realistic VNA test data."""
    switch = DummyPicoRFSwitch(port="/dev/null", name="switch")
    vna = DummyVNA(switch_fn=switch.switch)
    vna.setup(fstart=1e6, fstop=250e6, npoints=10, ifbw=100, power_dBm=0)
    return vna


def _make_vna_payload(vna):
    """Generate VNA data, header, and metadata from a DummyVNA."""
    s11 = vna.measure_ant(measure_noise=True, measure_load=True)
    header = dict(vna.header)
    header["freqs"] = header["freqs"].tolist()  # JSON serializable
    header["mode"] = "ant"
    metadata = {"temp": 25.0}
    return s11, header, metadata


@patch("eigsep_observing.io.write_s11_file")
def test_record_vna_data(
    mock_write, observer_panda_only, redis_panda, dummy_vna
):
    """Test record_vna_data reads from stream and writes to file."""
    observer = observer_panda_only
    s11, header, metadata = _make_vna_payload(dummy_vna)

    # Push data into the Redis stream and reset read position so
    # the VNA reader picks it up on the first call.
    redis_panda.vna.add(s11, header=header, metadata=metadata)
    redis_panda._set_last_read_id("stream:vna", "0-0")

    def stop_after_read():
        """Wait for the write, then stop."""
        while not mock_write.called:
            time.sleep(0.01)
        observer.stop_event.set()

    stop_thread = threading.Thread(target=stop_after_read)
    stop_thread.start()

    observer.record_vna_data("/tmp/test_vna", timeout=5)
    stop_thread.join()

    mock_write.assert_called_once()
    call_args = mock_write.call_args
    assert set(call_args[0][0].keys()) == {"ant", "load", "noise"}
    assert call_args[0][1]["mode"] == "ant"
    assert call_args[1]["save_dir"] == "/tmp/test_vna"


def test_record_vna_data_timeout(observer_panda_only, redis_panda):
    """Test record_vna_data retries on timeout (no data in stream)."""
    observer = observer_panda_only

    # No data pushed — vna_reader.read returns (None, None, None) because
    # the stream doesn't exist yet. After two None responses, stop.
    call_count = [0]
    real_read = redis_panda.vna_reader.read

    def counting_read(timeout=0):
        call_count[0] += 1
        if call_count[0] >= 2:
            observer.stop_event.set()
        return real_read(timeout=timeout)

    with patch.object(
        redis_panda.vna_reader, "read", side_effect=counting_read
    ):
        observer.record_vna_data("/tmp/test_vna", timeout=1)

    assert call_count[0] >= 2


def test_record_vna_data_disconnect(observer_panda_only, redis_panda):
    """Test record_vna_data waits when panda disconnects."""
    observer = observer_panda_only

    call_count = [0]

    def heartbeat_side_effect():
        call_count[0] += 1
        if call_count[0] <= 2:
            return False  # disconnected
        return True  # reconnected

    def stop_after_delay():
        time.sleep(0.3)
        observer.stop_event.set()

    stop_thread = threading.Thread(target=stop_after_delay)
    stop_thread.start()

    with patch.object(
        redis_panda.heartbeat_reader,
        "check",
        side_effect=heartbeat_side_effect,
    ) as mock_check:
        observer.record_vna_data("/tmp/test_vna", timeout=1)
    stop_thread.join()

    # Verify heartbeat was checked multiple times
    assert mock_check.call_count >= 2


def test_record_vna_data_stop_event(observer_panda_only, redis_panda):
    """Test record_vna_data exits on stop_event during initial wait."""
    observer = observer_panda_only

    # Panda not connected — will enter the initial wait loop
    redis_panda.heartbeat.set(alive=False)

    def stop_after_delay():
        time.sleep(0.1)
        observer.stop_event.set()

    stop_thread = threading.Thread(target=stop_after_delay)
    stop_thread.start()

    observer.record_vna_data("/tmp/test_vna", timeout=1)
    stop_thread.join()
