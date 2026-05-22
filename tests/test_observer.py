import logging
import pytest
import queue
import threading
import time
from unittest.mock import Mock, patch

from cmt_vna.testing import DummyVNA
from eigsep_redis import ConfigStore, HeartbeatWriter, MetadataWriter
from eigsep_redis.status import STATUS_STREAM
from eigsep_redis.testing import DummyTransport
from eigsep_observing import EigObserver, run_tag
from eigsep_observing.corr import CorrConfigStore
from eigsep_observing.status_log_handler import (
    PANDA_RELAY_LOGGER,
    StatusStreamHandler,
)
from eigsep_observing.testing.utils import generate_data
from eigsep_observing.vna import VnaWriter


@pytest.fixture
def transport_snap():
    """DummyTransport seeded with a correlator config."""
    transport = DummyTransport()
    CorrConfigStore(transport).upload(
        {
            "integration_time": 1.0,
            "pairs": ["0", "1", "2", "3", "02", "13"],
        }
    )
    return transport


@pytest.fixture
def transport_panda():
    """DummyTransport seeded with a panda config and a live heartbeat.

    The observer builds its own ``ConfigStore`` / ``HeartbeatReader``
    / ``StatusReader`` surfaces from the transport; per-test behavior
    is applied with ``patch.object`` on those surfaces.
    """
    transport = DummyTransport()
    ConfigStore(transport).upload(
        {
            "switch_schedule": {"sky": 0.05, "load": 0.02, "noise": 0.02},
            "vna_settings": {
                "ip": "127.0.0.1",
                "port": 5025,
                "fstart": 1e6,
                "power_dBm": {"ant": -20, "rec": -40},
            },
            "vna_interval": 0.5,
        }
    )
    HeartbeatWriter(transport).set(alive=True)
    return transport


@pytest.fixture
def observer_snap_only(transport_snap):
    """EigObserver with only SNAP connection."""
    obs = EigObserver(transport_snap=transport_snap)
    yield obs
    obs.close()


@pytest.fixture
def observer_panda_only(transport_panda):
    """EigObserver with only LattePanda connection."""
    obs = EigObserver(transport_panda=transport_panda)
    yield obs
    obs.close()


@pytest.fixture
def observer_both(transport_snap, transport_panda):
    """EigObserver with both SNAP and LattePanda connections."""
    obs = EigObserver(
        transport_snap=transport_snap, transport_panda=transport_panda
    )
    yield obs
    obs.close()


def test_observer_init_snap_only(observer_snap_only, transport_snap):
    """Test EigObserver initialization with only SNAP connection."""
    assert observer_snap_only.transport_snap is transport_snap
    assert observer_snap_only.transport_panda is None
    assert observer_snap_only.corr_cfg is not None


def test_observer_init_panda_only(observer_panda_only, transport_panda):
    """Test EigObserver initialization with only LattePanda connection."""
    assert observer_panda_only.transport_snap is None
    assert observer_panda_only.transport_panda is transport_panda
    assert observer_panda_only.config is not None


def test_observer_init_both(observer_both, transport_snap, transport_panda):
    """Test EigObserver initialization with both connections."""
    assert observer_both.transport_snap is transport_snap
    assert observer_both.transport_panda is transport_panda
    assert observer_both.corr_cfg is not None
    assert observer_both.config is not None


def test_observer_init_none():
    """Test EigObserver initialization with no connections."""
    observer = EigObserver()
    assert observer.transport_snap is None
    assert observer.transport_panda is None
    observer.close()


def test_observer_init_panda_empty_config_does_not_raise():
    """Construction against an unseeded panda ConfigStore must not raise.

    The writer must start as soon as the backend boots, without waiting
    for ``panda_observe`` to upload an ``obs_config``. ``_with_header_overlays``
    is the only consumer of the panda config; its read is defensive and
    falls back to sentinels.
    """
    transport_panda = DummyTransport()
    HeartbeatWriter(transport_panda).set(alive=True)
    observer = EigObserver(transport_panda=transport_panda)
    try:
        assert observer.config is not None
        overlaid = observer._with_header_overlays({"foo": 1})
        assert overlaid["foo"] == 1
        assert overlaid["obs_config"] == {}
        assert overlaid["run_tag"] == "UNKNOWN"
        assert overlaid["run_started_at_unix"] == 0.0
        assert overlaid["obs_config_owner"] == "UNKNOWN"
        assert overlaid["obs_config_owner_uploaded_unix"] == 0.0
    finally:
        observer.close()


def test_status_handler_does_not_block_caller_on_slow_xadd(
    observer_panda_only, transport_panda
):
    """A hung ``StatusWriter.send`` must not stall the caller (corr)
    thread — the queue → listener split is the guarantee.

    Patch the listener's emitter to sleep on every ``send``; verify
    that ``logger.error`` on the caller thread returns essentially
    immediately. CLAUDE.md priority-1: corr writes must not be blocked
    by panda mirroring.
    """
    handler = observer_panda_only._status_log_handler
    block = threading.Event()
    original_send = handler._emitter._status.send

    def hanging_send(*args, **kwargs):
        block.wait(timeout=5.0)
        return original_send(*args, **kwargs)

    with patch.object(
        handler._emitter._status, "send", side_effect=hanging_send
    ):
        t0 = time.monotonic()
        logging.getLogger("eigsep_observing.observer").error("blocking-test")
        elapsed = time.monotonic() - t0
        # Listener is blocked, but the caller's emit is a queue put —
        # well under 100ms even on slow CI. Pre-refactor (sync XADD)
        # this would have been ~5s.
        assert elapsed < 0.5, elapsed
        block.set()
    handler.flush()


def test_status_handler_logs_loudly_on_send_failure(
    observer_panda_only, caplog
):
    """Listener-side ``StatusWriter.send`` failure must surface as an
    ERROR on a logger outside the ``eigsep_observing`` hierarchy.

    CLAUDE.md priority-2: safety nets are acceptable only if they log
    loudly at ERROR so the upstream contract violation is visible.
    Using a non-``eigsep_observing`` logger avoids re-queueing through
    the handler's own filter.
    """
    handler = observer_panda_only._status_log_handler
    caplog.set_level(logging.ERROR, logger="eigsep_status_handler_errors")

    with patch.object(
        handler._emitter._status,
        "send",
        side_effect=RuntimeError("xadd boom"),
    ):
        logging.getLogger("eigsep_observing.observer").error("payload-foo")
        handler.flush()

    matching = [
        rec
        for rec in caplog.records
        if rec.name == "eigsep_status_handler_errors"
        and rec.levelno == logging.ERROR
        and "failed to publish" in rec.getMessage()
        and "payload-foo" in rec.getMessage()
    ]
    assert matching, caplog.records


def test_status_handler_logs_loudly_on_queue_full(observer_panda_only, caplog):
    """Caller-side ``queue.Full`` (listener parked on a hung XADD)
    must also surface as an ERROR on the same out-of-hierarchy logger.
    """
    handler = observer_panda_only._status_log_handler
    caplog.set_level(logging.ERROR, logger="eigsep_status_handler_errors")

    with patch.object(handler.queue, "put_nowait", side_effect=queue.Full):
        logging.getLogger("eigsep_observing.observer").error("drop-me")

    matching = [
        rec
        for rec in caplog.records
        if rec.name == "eigsep_status_handler_errors"
        and rec.levelno == logging.ERROR
        and "failed to enqueue" in rec.getMessage()
    ]
    assert matching, caplog.records


def test_status_handler_filter_rejects_sibling_root(
    observer_panda_only, transport_panda
):
    """``eigsep_observing_foo`` is a sibling root, not a descendant of
    ``eigsep_observing`` — must not be mirrored. Guards against a bare
    ``startswith`` regression in the filter.
    """
    before = len(_read_status_entries(transport_panda))
    logging.getLogger("eigsep_observing_foo").error("sibling-not-mirrored")
    observer_panda_only._status_log_handler.flush()
    new_entries = _read_status_entries(transport_panda)[before:]
    assert not any("sibling-not-mirrored" in msg for _, msg in new_entries), (
        new_entries
    )


def test_close_detaches_status_stream_handler(transport_panda):
    """``close()`` must remove the StatusStreamHandler from the
    module-level logger so a subsequent observer in the same process
    (notably the next test) does not mirror records into the stale
    transport.
    """
    ground = logging.getLogger("eigsep_observing")
    before = [h for h in ground.handlers if isinstance(h, StatusStreamHandler)]

    obs = EigObserver(transport_panda=transport_panda)
    installed = [
        h for h in ground.handlers if isinstance(h, StatusStreamHandler)
    ]
    assert len(installed) == len(before) + 1

    obs.close()
    after = [h for h in ground.handlers if isinstance(h, StatusStreamHandler)]
    assert after == before
    assert obs._status_log_handler is None


def test_snap_connected_property(observer_snap_only, observer_panda_only):
    """Test snap_connected property."""
    assert observer_snap_only.snap_connected is True
    assert observer_panda_only.snap_connected is False


def test_panda_connected_property(
    observer_snap_only, observer_panda_only, transport_panda
):
    """Test panda_connected property."""
    assert observer_snap_only.panda_connected is False
    assert observer_panda_only.panda_connected is True

    # Test when redis is None
    observer_none = EigObserver()
    assert observer_none.panda_connected is False

    # Test when heartbeat check fails
    HeartbeatWriter(transport_panda).set(alive=False)
    assert observer_panda_only.panda_connected is False

    # clean up
    observer_none.close()


@patch("eigsep_observing.io.File")
def test_record_corr_data(mock_file_class, observer_snap_only, transport_snap):
    """Test record_corr_data method."""
    observer = observer_snap_only

    # Upload a header with sync_time so record_corr_data can proceed
    sync_time = 1713200000.0
    CorrConfigStore(transport_snap).upload_header({"sync_time": sync_time})

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
        observer.corr_reader, "read", return_value=(123, mock_data)
    ) as mock_read:
        observer.record_corr_data("/tmp/test", timeout=5)
    stop_thread.join()

    # Verify File was created with correct parameters. The
    # ``on_write`` kwarg is a lambda bound to the live transport so
    # the live-status dashboard can publish the file-heartbeat; any
    # callable is acceptable here — its behavior is tested in
    # ``test_record_corr_data_publishes_file_heartbeat``.
    mock_file_class.assert_called_once()
    args, kwargs = mock_file_class.call_args
    assert args == (
        "/tmp/test",
        ["0", "1", "2", "3", "02", "13"],
        240,
        observer.corr_cfg,
    )
    assert callable(kwargs["on_write"])

    # Verify data was read and added
    mock_read.assert_called()
    mock_file.add_data.assert_called_with(
        123, sync_time, mock_data, metadata=None
    )


@patch("eigsep_observing.io.File")
def test_record_corr_data_on_write_publishes_heartbeat(
    mock_file_class, observer_snap_only, transport_snap
):
    """The ``on_write`` callback wired into ``io.File`` must publish
    the file-heartbeat K/V on the SNAP transport. The dashboard runs
    on a different host and has no filesystem access to
    ``corr_save_dir``, so Redis is the only surface the writer can
    signal through."""
    from eigsep_observing.file_heartbeat import read as read_heartbeat

    observer = observer_snap_only
    CorrConfigStore(transport_snap).upload_header(
        {"sync_time": 1_713_200_000.0}
    )

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    # Stop the loop after the first add_data so the test stays tight.
    def _stop_after_add(*a, **kw):
        observer.stop_event.set()

    mock_file.add_data.side_effect = _stop_after_add
    mock_data = generate_data(ntimes=1)

    with patch.object(
        observer.corr_reader, "read", return_value=(1, mock_data)
    ):
        observer.record_corr_data("/tmp/test", timeout=1)

    # The callback the observer wired into io.File:
    on_write = mock_file_class.call_args.kwargs["on_write"]

    # Simulate the writer thread firing the callback after os.rename.
    on_write("/data/corr_20260424_120000.h5", 1_713_200_100.0)

    out = read_heartbeat(transport_snap, now=1_713_200_300.0)
    assert out["newest_h5_path"] == "/data/corr_20260424_120000.h5"
    assert out["mtime_unix"] == 1_713_200_100.0
    assert out["seconds_since_write"] == 200.0


@patch("eigsep_observing.io.File")
def test_record_corr_data_transient_header_blip_uses_cache(
    mock_file_class, observer_snap_only, caplog
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
            observer.corr_config,
            "get_header",
            side_effect=header_side_effect,
        ),
        patch.object(
            observer.corr_reader, "read", side_effect=read_side_effect
        ),
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    # set_header called twice: once with fetched, once with cached.
    # Both calls now carry the panda-side overlay fields injected by
    # _with_header_overlays — the snap-only fixture has no
    # transport_panda, so overlays resolve to sentinels.
    assert mock_file.set_header.call_count == 2
    expected_header = {
        **good_header,
        "run_tag": "UNKNOWN",
        "run_started_at_unix": 0.0,
        "obs_config_owner": "UNKNOWN",
        "obs_config_owner_uploaded_unix": 0.0,
        "obs_config": {},
    }
    mock_file.set_header.assert_any_call(header=expected_header)
    # add_data called on both iterations — corr data is sacred, the
    # blip must not have blocked writes.
    assert mock_file.add_data.call_count >= 2
    assert "Using cached corr header" in caplog.text


@patch("eigsep_observing.io.File")
def test_record_corr_data_unsynced_watchdog_logs_and_waits(
    mock_file_class, observer_snap_only, caplog
):
    """sync_time=0 persistently → ERROR log → loop keeps waiting.

    The producer is supervised by systemd; the consumer must not
    suicide on producer silence. Verify that the watchdog logs the
    persistent silence at ERROR but ``record_corr_data`` returns
    cleanly when ``stop_event`` is set, with no exception raised.
    """
    observer = observer_snap_only

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    observer.corr_config.upload_header({"sync_time": 0})

    stop_after = threading.Timer(1.5, observer.stop_event.set)
    stop_after.start()
    caplog.set_level(logging.ERROR, logger="eigsep_observing.observer")
    try:
        observer.record_corr_data(
            "/tmp/test", ntimes=1, timeout=5, liveness_timeout=0.2
        )
    finally:
        stop_after.cancel()

    # No data should have been read or written — no sync anchor.
    mock_file.add_data.assert_not_called()
    # Partial-buffer flush still runs (try/finally).
    mock_file.close.assert_called()
    # Watchdog logged the persistent silence at ERROR.
    assert any(
        "SNAP has not produced a complete corr row" in rec.message
        for rec in caplog.records
    )


@patch("eigsep_observing.io.File")
def test_record_corr_data_no_header_ever_logs_and_waits(
    mock_file_class, observer_snap_only, caplog
):
    """get_header always raises, no cache → ERROR log → keep waiting."""
    observer = observer_snap_only

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    stop_after = threading.Timer(1.5, observer.stop_event.set)
    stop_after.start()
    caplog.set_level(logging.ERROR, logger="eigsep_observing.observer")
    try:
        with patch.object(
            observer.corr_config,
            "get_header",
            side_effect=ValueError("no header"),
        ):
            observer.record_corr_data(
                "/tmp/test", ntimes=1, timeout=5, liveness_timeout=0.2
            )
    finally:
        stop_after.cancel()

    mock_file.add_data.assert_not_called()
    mock_file.close.assert_called()
    assert any(
        "SNAP has not produced a complete corr row" in rec.message
        for rec in caplog.records
    )


@patch("eigsep_observing.io.File")
def test_record_corr_data_resync_rolls_file(
    mock_file_class, observer_snap_only, caplog
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
            observer.corr_config,
            "get_header",
            side_effect=header_side_effect,
        ),
        patch.object(
            observer.corr_reader, "read", side_effect=read_side_effect
        ),
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    # Constructed File twice: initial, plus one roll after re-sync.
    # (Final try/finally close happens on the second one.)
    assert mock_file_class.call_count == 2
    file_mocks[0].close.assert_called()
    assert "SNAP re-synchronized" in caplog.text


@patch("eigsep_observing.io.File")
def test_record_corr_data_read_timeout_watchdog_logs_and_waits(
    mock_file_class, observer_snap_only, caplog
):
    """Persistent ``TimeoutError`` from ``corr_reader.read`` → ERROR
    log → keep waiting. Unified with the header-side watchdog: the
    consumer only cares whether complete rows arrive, and on
    persistent silence it logs but never raises.
    """
    observer = observer_snap_only

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    observer.corr_config.upload_header({"sync_time": 1713200000.0})

    stop_after = threading.Timer(1.5, observer.stop_event.set)
    stop_after.start()
    caplog.set_level(logging.ERROR, logger="eigsep_observing.observer")
    try:
        with patch.object(
            observer.corr_reader,
            "read",
            side_effect=TimeoutError("no data"),
        ):
            observer.record_corr_data(
                "/tmp/test", ntimes=1, timeout=0.05, liveness_timeout=0.2
            )
    finally:
        stop_after.cancel()

    mock_file.add_data.assert_not_called()
    mock_file.close.assert_called()
    assert any(
        "SNAP has not produced a complete corr row" in rec.message
        for rec in caplog.records
    )


@patch("eigsep_observing.io.File")
def test_record_corr_data_read_stream_absent_watchdog_logs_and_waits(
    mock_file_class, observer_snap_only, caplog
):
    """Persistent ``(None, {})`` from ``corr_reader.read`` (stream not
    created yet) → ERROR log → keep waiting.
    """
    observer = observer_snap_only

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    observer.corr_config.upload_header({"sync_time": 1713200000.0})

    stop_after = threading.Timer(1.5, observer.stop_event.set)
    stop_after.start()
    caplog.set_level(logging.ERROR, logger="eigsep_observing.observer")
    try:
        with patch.object(
            observer.corr_reader, "read", return_value=(None, {})
        ):
            observer.record_corr_data(
                "/tmp/test", ntimes=1, timeout=5, liveness_timeout=0.2
            )
    finally:
        stop_after.cancel()

    mock_file.add_data.assert_not_called()
    mock_file.close.assert_called()
    assert any(
        "SNAP has not produced a complete corr row" in rec.message
        for rec in caplog.records
    )


@patch("eigsep_observing.io.File")
def test_record_corr_data_read_timeout_then_success_resets_deadline(
    mock_file_class, observer_snap_only
):
    """A single read timeout followed by a successful read must not
    crash. ``file.add_data`` clears ``last_write_deadline`` so the
    next failure starts a fresh watchdog instead of inheriting the
    prior one.
    """
    observer = observer_snap_only
    sync_time = 1713200000.0
    observer.corr_config.upload_header({"sync_time": sync_time})

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    mock_data = generate_data(ntimes=1)

    calls = [0]

    def read_side_effect(*a, **kw):
        calls[0] += 1
        if calls[0] == 1:
            raise TimeoutError("transient")
        if calls[0] >= 3:
            observer.stop_event.set()
        return (100 + calls[0], mock_data)

    with patch.object(
        observer.corr_reader, "read", side_effect=read_side_effect
    ):
        observer.record_corr_data(
            "/tmp/test", ntimes=1, timeout=0.05, liveness_timeout=0.05
        )

    # At least one successful write happened; the first-call timeout
    # did not crash the loop because the follow-up write cleared it.
    assert mock_file.add_data.call_count >= 1


@patch("eigsep_observing.io.File")
def test_record_corr_data_panda_connected_drains_metadata(
    mock_file_class, observer_both, transport_panda
):
    """When the panda is connected, the metadata stream is drained and
    forwarded to ``file.add_data``; when not, ``metadata=None`` is passed
    (covered by ``test_record_corr_data``). This exercises the only
    success-path branch that touches cross-class logic.
    """
    observer = observer_both
    sync_time = 1713200000.0
    observer.corr_config.upload_header({"sync_time": sync_time})

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    mock_data = generate_data(ntimes=1)

    # Push one sensor reading onto the panda metadata stream via the
    # real MetadataWriter (runs against fakeredis in DummyTransport).
    sample = {"temp_c": 23.5, "status": "update"}
    MetadataWriter(transport_panda).add("sensor_a", sample)
    # Rewind last-read-id so drain() picks up the entry we just pushed
    # (xread skips entries at/before the last-generated-id by default).
    transport_panda.set_last_read_id("stream:sensor_a", "0-0")

    def read_side_effect(*a, **kw):
        observer.stop_event.set()
        return (123, mock_data)

    with patch.object(
        observer.corr_reader, "read", side_effect=read_side_effect
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    # add_data received the drained metadata, keyed by the stream name
    # MetadataWriter.add uses (``stream:<key>``).
    mock_file.add_data.assert_called_once()
    call_kwargs = mock_file.add_data.call_args.kwargs
    assert call_kwargs["metadata"] == {"stream:sensor_a": [sample]}


def test_record_corr_data_no_snap():
    """Test record_corr_data without snap connection raises AttributeError."""
    observer = EigObserver()
    try:
        with pytest.raises(AttributeError):
            observer.record_corr_data("/tmp")
    finally:
        observer.close()


def test_status_logger(observer_panda_only, caplog):
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
        observer_panda_only.status_reader,
        "read",
        side_effect=read_status_effect,
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
    """DummyVNA instance for generating realistic VNA test data.

    ``switch_fn`` is a no-op — cmt_vna 1.3 ignores the return value
    and only treats raised exceptions as failure. The switch network
    isn't under test here.
    """
    vna = DummyVNA(switch_fn=lambda state: None)
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
    mock_write, observer_panda_only, transport_panda, dummy_vna
):
    """Test record_vna_data reads from stream and writes to file."""
    observer = observer_panda_only
    s11, header, metadata = _make_vna_payload(dummy_vna)

    # Push data into the Redis stream and reset read position so
    # the VNA reader picks it up on the first call.
    VnaWriter(transport_panda).add(s11, header=header, metadata=metadata)
    transport_panda.set_last_read_id("stream:vna", "0-0")

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


def test_record_vna_data_stream_absent(observer_panda_only):
    """When no VNA data has ever been published, ``vna_reader.read``
    returns ``(None, None, None)`` and the loop retries after a wait.
    """
    observer = observer_panda_only

    # No data pushed — vna_reader.read returns (None, None, None) because
    # the stream doesn't exist yet. After two None responses, stop.
    call_count = [0]
    real_read = observer.vna_reader.read

    def counting_read(timeout=0):
        call_count[0] += 1
        if call_count[0] >= 2:
            observer.stop_event.set()
        return real_read(timeout=timeout)

    with patch.object(observer.vna_reader, "read", side_effect=counting_read):
        observer.record_vna_data("/tmp/test_vna", timeout=1)

    assert call_count[0] >= 2


def test_record_vna_data_timeout(observer_panda_only):
    """``TimeoutError`` from ``vna_reader.read`` is expected during idle
    windows between hourly VNA triggers — bare ``continue``, no log, no
    watchdog. The loop simply retries.
    """
    observer = observer_panda_only

    call_count = [0]

    def timeout_read(timeout=0):
        call_count[0] += 1
        if call_count[0] >= 2:
            observer.stop_event.set()
        raise TimeoutError("no VNA entry")

    with patch.object(observer.vna_reader, "read", side_effect=timeout_read):
        observer.record_vna_data("/tmp/test_vna", timeout=1)

    assert call_count[0] >= 2


def test_record_vna_data_disconnect(observer_panda_only):
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
        observer.heartbeat_reader,
        "check",
        side_effect=heartbeat_side_effect,
    ) as mock_check:
        observer.record_vna_data("/tmp/test_vna", timeout=1)
    stop_thread.join()

    # Verify heartbeat was checked multiple times
    assert mock_check.call_count >= 2


def test_record_vna_data_stop_event(observer_panda_only, transport_panda):
    """Test record_vna_data exits on stop_event during initial wait."""
    observer = observer_panda_only

    # Panda not connected — will enter the initial wait loop
    HeartbeatWriter(transport_panda).set(alive=False)

    def stop_after_delay():
        time.sleep(0.1)
        observer.stop_event.set()

    stop_thread = threading.Thread(target=stop_after_delay)
    stop_thread.start()

    observer.record_vna_data("/tmp/test_vna", timeout=1)
    stop_thread.join()


# ---------------------------------------------------------------------------
# Header overlay tests (run_tag + obs_config)
# ---------------------------------------------------------------------------


def test_with_header_overlays_no_panda_uses_sentinels(observer_snap_only):
    """No transport_panda → run_tag + owner sentinels + empty obs_config."""
    observer = observer_snap_only
    out = observer._with_header_overlays({"sync_time": 12345.0})
    assert out["sync_time"] == 12345.0
    assert out["run_tag"] == "UNKNOWN"
    assert out["run_started_at_unix"] == 0.0
    assert out["obs_config_owner"] == "UNKNOWN"
    assert out["obs_config_owner_uploaded_unix"] == 0.0
    assert out["obs_config"] == {}


def test_with_header_overlays_panda_published(observer_both, transport_panda):
    """run_tag.publish on panda transport flows into the overlay."""
    run_tag.publish(transport_panda, "panda_observe", started_unix=42.0)
    out = observer_both._with_header_overlays({"sync_time": 1.0})
    assert out["run_tag"] == "panda_observe"
    assert out["run_started_at_unix"] == 42.0
    # obs_config carries the panda fixture config (plus the
    # ConfigStore.upload_dict-injected ``upload_time`` field).
    assert out["obs_config"]["vna_interval"] == 0.5
    assert "vna_settings" in out["obs_config"]


def test_with_header_overlays_panda_no_tag_published(
    observer_both,
):
    """Panda transport present but no run_tag published → sentinels."""
    out = observer_both._with_header_overlays({"sync_time": 1.0})
    assert out["run_tag"] == "UNKNOWN"
    assert out["run_started_at_unix"] == 0.0
    # obs_config is still populated — the fixture uploaded one.
    assert "vna_settings" in out["obs_config"]


def test_with_header_overlays_owner_trusted(observer_both, transport_panda):
    """run_tag == obs_config_owner: the active driver also uploaded the cfg.

    Downstream's strongest trust check passes — the cfg block in the
    header reflects what is running right now.
    """
    from eigsep_observing import obs_config_owner

    run_tag.publish(transport_panda, "panda_observe", started_unix=10.0)
    obs_config_owner.publish_owner(
        transport_panda, "panda_observe", uploaded_at_unix=5.0
    )
    out = observer_both._with_header_overlays({"sync_time": 1.0})
    assert out["run_tag"] == "panda_observe"
    assert out["obs_config_owner"] == "panda_observe"
    assert out["obs_config_owner_uploaded_unix"] == 5.0


def test_with_header_overlays_bring_up_overlap(observer_both, transport_panda):
    """run_tag=vna_manual, owner=panda_observe: bring-up tool overlap.

    The persistent cfg was uploaded by panda_observe but a bring-up
    tool (which never touches ConfigStore.upload) is currently driving
    the panda. Downstream's necessary trust check
    (owner != "UNKNOWN") passes; the stronger run_tag == owner check
    distinguishes this case.
    """
    from eigsep_observing import obs_config_owner

    obs_config_owner.publish_owner(
        transport_panda, "panda_observe", uploaded_at_unix=5.0
    )
    run_tag.publish(transport_panda, "vna_manual", started_unix=10.0)
    out = observer_both._with_header_overlays({"sync_time": 1.0})
    assert out["run_tag"] == "vna_manual"
    assert out["obs_config_owner"] == "panda_observe"
    assert out["obs_config_owner_uploaded_unix"] == 5.0


def test_with_header_overlays_owner_unknown(observer_both, transport_panda):
    """No owner published → "UNKNOWN" sentinel; trust check fails closed."""
    run_tag.publish(transport_panda, "vna_manual", started_unix=10.0)
    out = observer_both._with_header_overlays({"sync_time": 1.0})
    assert out["run_tag"] == "vna_manual"
    assert out["obs_config_owner"] == "UNKNOWN"
    assert out["obs_config_owner_uploaded_unix"] == 0.0


def test_with_header_overlays_obs_config_failure_errors(observer_both, caplog):
    """ConfigStore.get raising → log ERROR, set obs_config={}.

    Per CLAUDE.md, narrow safety nets around non-corr processing must
    log loudly at ERROR level so the upstream contract violation is
    visible and actionable. obs_config-overlay failure is one such
    contract violation: corr data still gets written (the safety net),
    but the producer/transport problem must surface to operators.
    """
    with patch.object(
        observer_both.config, "get", side_effect=RuntimeError("redis down")
    ):
        with caplog.at_level(logging.ERROR):
            out = observer_both._with_header_overlays({"sync_time": 1.0})
    assert out["obs_config"] == {}
    matching = [
        rec
        for rec in caplog.records
        if "obs_config overlay read failed" in rec.message
    ]
    assert matching, "expected obs_config overlay failure log"
    assert all(rec.levelno == logging.ERROR for rec in matching)


def test_with_header_overlays_does_not_mutate_input(observer_snap_only):
    """Helper returns a new dict — caller's cached header is preserved."""
    cached = {"sync_time": 999.0}
    out = observer_snap_only._with_header_overlays(cached)
    assert "run_tag" not in cached
    assert "obs_config" not in cached
    assert out is not cached


@patch("eigsep_observing.io.File")
def test_record_corr_data_writes_overlays_into_header(
    mock_file_class, observer_both, transport_panda, transport_snap
):
    """End-to-end: record_corr_data merges overlays into set_header arg."""
    run_tag.publish(
        transport_panda, "no_switch_observation", started_unix=100.0
    )
    sync_time = 1713200000.0
    CorrConfigStore(transport_snap).upload_header({"sync_time": sync_time})

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    mock_data = generate_data(ntimes=1)
    read_count = [0]

    def read_side_effect(*a, **kw):
        read_count[0] += 1
        if read_count[0] >= 1:
            observer_both.stop_event.set()
        return (read_count[0], mock_data)

    with patch.object(
        observer_both.corr_reader, "read", side_effect=read_side_effect
    ):
        observer_both.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    assert mock_file.set_header.called
    (call_kwargs,) = (c.kwargs for c in mock_file.set_header.call_args_list)
    written = call_kwargs["header"]
    assert written["sync_time"] == sync_time
    assert written["run_tag"] == "no_switch_observation"
    assert written["run_started_at_unix"] == 100.0
    assert written["obs_config_owner"] == "UNKNOWN"
    assert written["obs_config_owner_uploaded_unix"] == 0.0
    assert written["obs_config"]["vna_interval"] == 0.5


def _read_status_entries(transport):
    """Return the status stream as a list of ``(level, message)``.

    Reads via ``xrange`` so it doesn't contend with the observer's
    background ``status_thread`` for the per-transport last-read-id.
    """
    raw = transport.r.xrange(STATUS_STREAM)
    out = []
    for _id, fields in raw:
        status = fields[b"status"].decode("utf-8")
        level = int(fields[b"level"].decode("utf-8"))
        out.append((level, status))
    return out


def test_status_handler_forwards_observer_error(
    observer_panda_only, transport_panda
):
    """Ground-side ERROR records are mirrored to the panda status
    stream with a logger-name prefix the dashboard can render."""
    before = len(_read_status_entries(transport_panda))
    logging.getLogger("eigsep_observing.observer").error("test-error-foo")
    # XADD happens on the QueueListener thread; drain it before read.
    observer_panda_only._status_log_handler.flush()
    new_entries = _read_status_entries(transport_panda)[before:]
    assert any(
        level == logging.ERROR
        and msg == "[eigsep_observing.observer] test-error-foo"
        for level, msg in new_entries
    ), new_entries


def test_status_handler_skips_aggregator_and_relay(
    observer_panda_only, transport_panda
):
    """Aggregator-owned and panda-relay loggers are excluded.

    Aggregator errors are visible in the operator's live_status
    terminal already. Re-emitting panda status messages via the relay
    logger must not loop back through the handler.
    """
    before = len(_read_status_entries(transport_panda))
    logging.getLogger("eigsep_observing.live_status.aggregator").error(
        "agg-error-should-not-mirror"
    )
    logging.getLogger(PANDA_RELAY_LOGGER).error(
        "relay-error-should-not-mirror"
    )
    observer_panda_only._status_log_handler.flush()
    new_entries = _read_status_entries(transport_panda)[before:]
    assert not any(
        "agg-error-should-not-mirror" in msg for _, msg in new_entries
    ), new_entries
    assert not any(
        "relay-error-should-not-mirror" in msg for _, msg in new_entries
    ), new_entries


def test_status_handler_skips_warning_and_info(
    observer_panda_only, transport_panda
):
    """Sub-ERROR levels are dropped — some WARNING sites fire at corr
    cadence and would blow ``StatusWriter.maxlen``."""
    before = len(_read_status_entries(transport_panda))
    io_logger = logging.getLogger("eigsep_observing.io")
    io_logger.warning("io-warning-should-not-mirror")
    io_logger.info("io-info-should-not-mirror")
    observer_panda_only._status_log_handler.flush()
    new_entries = _read_status_entries(transport_panda)[before:]
    assert not any(
        "io-warning-should-not-mirror" in msg for _, msg in new_entries
    ), new_entries
    assert not any(
        "io-info-should-not-mirror" in msg for _, msg in new_entries
    ), new_entries
