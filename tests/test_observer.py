import itertools
import logging
import pytest
import queue
import threading
import time
from contextlib import contextmanager
from unittest.mock import Mock, patch

from cmt_vna.testing import DummyVNA
from eigsep_redis import ConfigStore, HeartbeatWriter, MetadataWriter
from eigsep_redis.status import STATUS_STREAM
from eigsep_redis.testing import DummyTransport
from eigsep_observing import EigObserver, run_tag
from eigsep_observing._test_fixtures import IMU_CALIBRATION
from picohost.buses import ImuCalStore
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
def transport_panda_down():
    """DummyTransport with no heartbeat — ``panda_connected`` reads False.

    Drop-in for tests that previously passed ``transport_panda=None``.
    Surfaces still exist (the lazy-Transport refactor builds them
    unconditionally) so use sites that wrap reads in
    ``try/except redis.exceptions.ConnectionError`` get to exercise
    the absent-data path against a real (fakeredis-backed) transport.
    """
    return DummyTransport()


@pytest.fixture
def observer_snap_only(transport_snap, transport_panda_down):
    """EigObserver with SNAP up, panda transport present but offline."""
    obs = EigObserver(
        transport_snap=transport_snap,
        transport_panda=transport_panda_down,
    )
    yield obs
    obs.close()


@pytest.fixture
def transport_snap_only_for_panda():
    """DummyTransport seeded with a corr config — SNAP-side default for
    panda-focused tests that previously passed ``transport_snap=None``.

    Production requires SNAP to be reachable (the corr thread is the
    writer's reason to exist); the constructor now reflects that by
    making ``transport_snap`` mandatory. Tests that don't exercise the
    corr loop still need a seeded SNAP transport so
    ``CorrConfigStore(transport_snap).get()`` succeeds during observer
    construction.
    """
    t = DummyTransport()
    CorrConfigStore(t).upload(
        {
            "integration_time": 1.0,
            "pairs": ["0", "1", "2", "3", "02", "13"],
        }
    )
    return t


@pytest.fixture
def observer_panda_only(transport_snap_only_for_panda, transport_panda):
    """EigObserver with panda up; SNAP transport is present (required)
    but tests only exercise the panda surface.
    """
    obs = EigObserver(
        transport_snap=transport_snap_only_for_panda,
        transport_panda=transport_panda,
    )
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


def test_observer_init_snap_only(
    observer_snap_only, transport_snap, transport_panda_down
):
    """SNAP up, panda transport present but offline (no heartbeat).

    Verifies the post-refactor contract: both transports always exist
    on the observer, and panda surfaces are built whether or not the
    panda is reachable.
    """
    assert observer_snap_only.transport_snap is transport_snap
    assert observer_snap_only.transport_panda is transport_panda_down
    assert observer_snap_only.corr_cfg is not None
    assert observer_snap_only.config is not None
    assert observer_snap_only.panda_connected is False


def test_observer_init_panda_only(observer_panda_only, transport_panda):
    """Panda up; SNAP transport present (mandatory) but tests focus on
    the panda surface.
    """
    assert observer_panda_only.transport_panda is transport_panda
    assert observer_panda_only.config is not None
    assert observer_panda_only.panda_connected is True


def test_observer_init_both(observer_both, transport_snap, transport_panda):
    """Test EigObserver initialization with both connections."""
    assert observer_both.transport_snap is transport_snap
    assert observer_both.transport_panda is transport_panda
    assert observer_both.corr_cfg is not None
    assert observer_both.config is not None


def test_observer_init_requires_both_transports():
    """Both transports are required after the opportunistic-panda
    refactor; ``EigObserver()`` with no args must raise.

    Production builds the panda transport with ``lazy=True`` so
    construction never fails even when the panda is unreachable;
    tests that want a "panda is down" shape should pass an unseeded
    ``DummyTransport`` (the ``transport_panda_down`` fixture).
    """
    with pytest.raises(TypeError):
        EigObserver()
    with pytest.raises(TypeError):
        EigObserver(transport_snap=DummyTransport())
    with pytest.raises(TypeError):
        EigObserver(transport_panda=DummyTransport())


def test_observer_init_panda_empty_config_does_not_raise(transport_snap):
    """Construction against an unseeded panda ConfigStore must not raise.

    The writer must start as soon as the backend boots, without waiting
    for ``panda_observe`` to upload an ``obs_config``. ``_with_header_overlays``
    is the only consumer of the panda config; its read is defensive and
    falls back to sentinels.
    """
    transport_panda = DummyTransport()
    HeartbeatWriter(transport_panda).set(alive=True)
    observer = EigObserver(
        transport_snap=transport_snap, transport_panda=transport_panda
    )
    try:
        assert observer.config is not None
        overlaid = observer._with_header_overlays({"foo": 1})
        assert overlaid["foo"] == 1
        assert overlaid["obs_config"] == {}
        assert overlaid["run_tag"] == "UNKNOWN"
        assert overlaid["run_started_at_unix"] == 0.0
        assert overlaid["obs_config_owner"] == "UNKNOWN"
        assert overlaid["obs_config_owner_uploaded_unix"] == 0.0
        assert overlaid["imu_calibration"] == {}
        assert overlaid["imu_calibration_upload_unix"] == 0.0
    finally:
        observer.close()


def test_with_header_overlays_embeds_imu_calibration(transport_snap):
    transport_panda = DummyTransport()
    HeartbeatWriter(transport_panda).set(alive=True)
    ImuCalStore(transport_panda).upload(IMU_CALIBRATION)
    observer = EigObserver(
        transport_snap=transport_snap, transport_panda=transport_panda
    )
    try:
        overlaid = observer._with_header_overlays({})
        assert (
            overlaid["imu_calibration"]["imu_az"]
            == IMU_CALIBRATION["imu_az"]
        )
        assert overlaid["imu_calibration_upload_unix"] > 0.0
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


def test_close_detaches_status_stream_handler(transport_snap, transport_panda):
    """``close()`` must remove the StatusStreamHandler from the
    module-level logger so a subsequent observer in the same process
    (notably the next test) does not mirror records into the stale
    transport.
    """
    ground = logging.getLogger("eigsep_observing")
    before = [h for h in ground.handlers if isinstance(h, StatusStreamHandler)]

    obs = EigObserver(
        transport_snap=transport_snap, transport_panda=transport_panda
    )
    installed = [
        h for h in ground.handlers if isinstance(h, StatusStreamHandler)
    ]
    assert len(installed) == len(before) + 1

    obs.close()
    after = [h for h in ground.handlers if isinstance(h, StatusStreamHandler)]
    assert after == before
    assert obs._status_log_handler is None


def test_panda_connected_property(
    observer_snap_only, observer_panda_only, transport_panda
):
    """``panda_connected`` reflects heartbeat liveness, not transport
    existence (the transport is built unconditionally in lazy mode and
    always exists after construction).
    """
    # Snap-only fixture: panda transport present but no heartbeat → False.
    assert observer_snap_only.panda_connected is False
    # Panda-only fixture: heartbeat seeded by the transport_panda fixture.
    assert observer_panda_only.panda_connected is True

    # Heartbeat goes false → panda_connected goes false.
    HeartbeatWriter(transport_panda).set(alive=False)
    assert observer_panda_only.panda_connected is False


def test_panda_connected_handles_connection_error(observer_panda_only):
    """``panda_connected`` must absorb ``redis.exceptions.ConnectionError``
    from ``heartbeat_reader.check()`` and return False — the corr loop
    relies on this gate, and a panda that dies mid-run must not crash
    it. Replaces the legacy "transport is None" short-circuit, which
    no longer applies under the lazy-Transport refactor.
    """
    import redis.exceptions

    with patch.object(
        observer_panda_only.heartbeat_reader,
        "check",
        side_effect=redis.exceptions.ConnectionError("nope"),
    ):
        assert observer_panda_only.panda_connected is False


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


@contextmanager
def _deterministic_watchdog(observer):
    """Make ``record_corr_data``'s liveness watchdog fire deterministically.

    ``_tick_liveness_deadline`` arms a deadline on its first call and
    only logs once ``time.monotonic()`` passes it — so the watchdog
    needs at least two loop iterations to emit. These tests used to race
    a real ``threading.Timer(1.5, stop_event.set)`` against a real
    ``liveness_timeout`` window; on a loaded CI runner the wall-clock
    stop could land before the second tick, the ERROR never logged, and
    the ``any(...)`` assertion failed (observed flaking the 3.10 job).

    This removes every real-time dependency:

    - ``time.monotonic`` is replaced with a fake clock that advances a
      full second per call, so the deadline (a sub-second
      ``liveness_timeout``) is always exceeded by the second tick.
      ``_tick_liveness_deadline`` is the only ``monotonic`` caller on
      these watchdog paths, but the stop is driven separately (below),
      so the clock only has to be monotonically increasing — exact
      call-to-iteration alignment is not relied on.
    - ``stop_event.wait`` is made non-blocking (returns the current
      ``is_set()``), so the ``stop_event.wait(1)`` calls in the
      header / stream-absent branches don't sleep.

    The caller still drives ``stop_event`` from a counting closure on
    whichever producer method the test exercises (``read`` or
    ``get_header``), so the loop exits on its real control variable.
    """
    clock = itertools.count(1.0, 1.0)
    with (
        patch(
            "eigsep_observing.observer.time.monotonic",
            side_effect=lambda: next(clock),
        ),
        patch.object(
            observer.stop_event,
            "wait",
            side_effect=lambda timeout=None: observer.stop_event.is_set(),
        ),
    ):
        yield


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

    # Persistent sync_time=0 (SNAP not synchronized); stop the loop once
    # the watchdog has ticked twice. get_header is the per-iteration hook
    # on this path (read is never reached), so it doubles as the stop
    # driver — deterministic, no wall-clock Timer.
    calls = itertools.count(1)

    def get_header_side_effect():
        if next(calls) >= 3:
            observer.stop_event.set()
        return {"sync_time": 0}

    caplog.set_level(logging.ERROR, logger="eigsep_observing.observer")
    with (
        _deterministic_watchdog(observer),
        patch.object(
            observer.corr_config,
            "get_header",
            side_effect=get_header_side_effect,
        ),
    ):
        observer.record_corr_data(
            "/tmp/test", ntimes=1, timeout=5, liveness_timeout=0.2
        )

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

    # get_header raises every iteration (no cached header); count the
    # calls and stop once the watchdog has ticked twice.
    calls = itertools.count(1)

    def get_header_side_effect():
        if next(calls) >= 3:
            observer.stop_event.set()
        raise ValueError("no header")

    caplog.set_level(logging.ERROR, logger="eigsep_observing.observer")
    with (
        _deterministic_watchdog(observer),
        patch.object(
            observer.corr_config,
            "get_header",
            side_effect=get_header_side_effect,
        ),
    ):
        observer.record_corr_data(
            "/tmp/test", ntimes=1, timeout=5, liveness_timeout=0.2
        )

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

    # read raises TimeoutError every iteration; count the calls and stop
    # once the watchdog has ticked twice.
    calls = itertools.count(1)

    def read_side_effect(*a, **k):
        if next(calls) >= 3:
            observer.stop_event.set()
        raise TimeoutError("no data")

    caplog.set_level(logging.ERROR, logger="eigsep_observing.observer")
    with (
        _deterministic_watchdog(observer),
        patch.object(
            observer.corr_reader, "read", side_effect=read_side_effect
        ),
    ):
        observer.record_corr_data(
            "/tmp/test", ntimes=1, timeout=0.05, liveness_timeout=0.2
        )

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

    # read returns (None, {}) every iteration (stream not created yet);
    # count the calls and stop once the watchdog has ticked twice.
    calls = itertools.count(1)

    def read_side_effect(*a, **k):
        if next(calls) >= 3:
            observer.stop_event.set()
        return (None, {})

    caplog.set_level(logging.ERROR, logger="eigsep_observing.observer")
    with (
        _deterministic_watchdog(observer),
        patch.object(
            observer.corr_reader, "read", side_effect=read_side_effect
        ),
    ):
        observer.record_corr_data(
            "/tmp/test", ntimes=1, timeout=5, liveness_timeout=0.2
        )

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
    """With the panda metadata stream reachable, the drained readings
    are forwarded to ``file.add_data``. This exercises the success-path
    branch that touches cross-class logic. The drain is gated on stream
    reachability, not the heartbeat — see
    ``test_record_corr_data_drains_metadata_without_heartbeat`` for the
    no-heartbeat case and ``test_record_corr_data`` for the empty-stream
    → ``metadata=None`` case.
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
    #
    # Fixture deviation: stream ``sensor_a`` and field ``temp_c`` are
    # synthetic — neither is in ``SENSOR_SCHEMAS``. Intentional: this
    # test exercises the drain → ``file.add_data`` forwarding path,
    # which runs below the schema layer (``add_data`` is mocked, so the
    # ``_avg_sensor_values`` reduction never runs). A real producer
    # payload would add no coverage; see ``tests/test_io.py`` for the
    # end-to-end producer→avg→write→read fixture round-trip.
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


@patch("eigsep_observing.io.File")
def test_record_corr_data_drains_metadata_without_heartbeat(
    mock_file_class, observer_snap_only, transport_panda_down
):
    """The metadata sidecar is drained whenever the panda Redis is
    reachable, even with no heartbeat.

    A manual session stops ``panda_observe`` (so the heartbeat goes away
    and ``panda_connected`` is False), but the picos keep publishing
    metadata via the always-on pico-manager service. The corr loop must
    still attach that metadata: the drain is gated on Redis reachability
    (``ConnectionError``), not on ``panda_connected``. Regression guard
    for the heartbeat-decoupling fix — under the old heartbeat gate this
    integration would have been written with ``metadata=None``.
    """
    observer = observer_snap_only
    assert observer.panda_connected is False  # no heartbeat seeded

    observer.corr_config.upload_header({"sync_time": 1713200000.0})

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    mock_data = generate_data(ntimes=1)

    # Picos publish metadata via pico-manager even with panda_observe
    # stopped. Push one reading and rewind the read pointer so drain()
    # picks it up (mirrors test_..._panda_connected_drains_metadata, but
    # against the no-heartbeat transport).
    #
    # Fixture deviation: stream ``sensor_a`` and field ``temp_c`` are
    # synthetic — neither is in ``SENSOR_SCHEMAS``. Intentional: this
    # test exercises the ConnectionError-gated drain → ``file.add_data``
    # forwarding path, which runs below the schema layer (``add_data`` is
    # mocked, so the ``_avg_sensor_values`` reduction never runs). A real
    # producer payload would add no coverage; see ``tests/test_io.py``
    # for the end-to-end producer→avg→write→read fixture round-trip.
    sample = {"temp_c": 23.5, "status": "update"}
    MetadataWriter(transport_panda_down).add("sensor_a", sample)
    transport_panda_down.set_last_read_id("stream:sensor_a", "0-0")

    def read_side_effect(*a, **kw):
        observer.stop_event.set()
        return (123, mock_data)

    with patch.object(
        observer.corr_reader, "read", side_effect=read_side_effect
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    mock_file.add_data.assert_called_once()
    call_kwargs = mock_file.add_data.call_args.kwargs
    assert call_kwargs["metadata"] == {"stream:sensor_a": [sample]}


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


@patch("eigsep_observing.io.write_s11_file")
def test_record_vna_data_survives_transient_connection_error(
    mock_write, observer_panda_only, transport_panda, dummy_vna
):
    """A single ``redis.exceptions.ConnectionError`` from
    ``vna_reader.read`` (panda drops between the ``panda_connected``
    gate and the xread, or mid-xread) must not crash the VNA thread.
    The loop logs, backs off, and resumes — and once the panda is
    reachable again the next read writes a file.

    Pre-refactor (commit 240b354) only ``TimeoutError`` was caught;
    a ``ConnectionError`` would propagate and kill the thread for the
    rest of the observer's lifetime, contradicting the
    "VNA thread idles on an empty stream" docstring claim.
    """
    import redis.exceptions

    observer = observer_panda_only
    s11, header, metadata = _make_vna_payload(dummy_vna)
    VnaWriter(transport_panda).add(s11, header=header, metadata=metadata)
    transport_panda.set_last_read_id("stream:vna", "0-0")

    real_read = observer.vna_reader.read
    raised = [False]

    def flaky_read(timeout=0):
        if not raised[0]:
            raised[0] = True
            raise redis.exceptions.ConnectionError("panda dropped")
        return real_read(timeout=timeout)

    def stop_after_write():
        while not mock_write.called:
            time.sleep(0.01)
        observer.stop_event.set()

    stop_thread = threading.Thread(target=stop_after_write)
    stop_thread.start()

    with patch.object(observer.vna_reader, "read", side_effect=flaky_read):
        observer.record_vna_data("/tmp/test_vna", timeout=5)
    stop_thread.join()

    assert raised[0], "ConnectionError branch never exercised"
    mock_write.assert_called_once()


def test_record_vna_data_tolerates_permanent_connection_error(
    observer_panda_only,
):
    """A persistently-raising ``vna_reader.read`` must leave the loop
    in a clean idle state — it logs the disconnect, waits on
    ``stop_event``, and exits when set. The corr loop's
    "panda is down" path stays open; the VNA thread can't crash and
    block observer shutdown.
    """
    import redis.exceptions

    observer = observer_panda_only

    call_count = [0]

    def always_fails(timeout=0):
        call_count[0] += 1
        if call_count[0] >= 3:
            observer.stop_event.set()
        raise redis.exceptions.ConnectionError("panda gone")

    with patch.object(observer.vna_reader, "read", side_effect=always_fails):
        observer.record_vna_data("/tmp/test_vna", timeout=1)

    assert call_count[0] >= 3, (
        "loop should iterate at least three times before stop"
    )


def test_record_vna_data_panda_down_warning_is_throttled(
    observer_panda_only, caplog
):
    """A persistently-disconnected panda must not flood the log with
    one ``"Waiting for LattePanda Redis connection..."`` WARNING per
    iteration. The three panda-down branches in ``record_vna_data``
    share a single ``_DRAIN_WARN_INTERVAL_S``-gated throttle, so the
    full outage produces at most one WARNING per 60 s window — matching
    the drain-warn cadence in ``record_corr_data``.
    """
    observer = observer_panda_only

    iter_count = [0]

    def always_disconnected():
        iter_count[0] += 1
        if iter_count[0] >= 5:
            observer.stop_event.set()
        return False

    caplog.set_level(logging.WARNING, logger="eigsep_observing.observer")
    with patch.object(
        observer.heartbeat_reader,
        "check",
        side_effect=always_disconnected,
    ):
        observer.record_vna_data("/tmp/test_vna", timeout=1)

    wait_warnings = [
        rec
        for rec in caplog.records
        if "Waiting for LattePanda Redis connection" in rec.message
    ]
    assert iter_count[0] >= 5, "loop didn't iterate enough to test throttle"
    assert len(wait_warnings) == 1, (
        f"expected one throttled WARNING, got {len(wait_warnings)}: "
        f"{[r.message for r in wait_warnings]}"
    )


@patch("eigsep_observing.io.File")
def test_record_corr_data_resumes_when_panda_arrives_after_startup(
    mock_file_class, observer_snap_only, transport_snap, caplog
):
    """Anchors the opportunistic-panda contract end-to-end: an observer
    that booted with the panda unreachable (no heartbeat → an empty
    DummyTransport) must keep writing corr files with empty metadata,
    and the *moment* the panda comes back (heartbeat goes alive +
    sensor data appears) the next corr integration's metadata sidecar
    must include the new entries — with no observer restart.

    Pre-refactor (commit 240b354) ``EigObserver.__init__`` only built
    panda surfaces when ``transport_panda is not None``, so a
    startup-down panda left ``self.metadata_stream`` permanently
    missing and reconnection was impossible. The lazy-Transport
    refactor in this PR closes that gap: surfaces are always built
    against a real (possibly disconnected) transport, every
    panda-touching call catches ``ConnectionError`` at use time, and
    reconnection is implicit.
    """
    from eigsep_redis.keys import METADATA_STREAMS_SET

    observer = observer_snap_only
    transport_panda = observer.transport_panda
    assert observer.panda_connected is False, (
        "fixture must start with panda offline"
    )

    observer.corr_config.upload_header({"sync_time": 1713200000.0})

    # Outage-era publish: register the stream while the panda is
    # nominally "down" (heartbeat absent). On reconnect, the corr loop
    # calls skip_to_latest() to discard this backlog — the test then
    # publishes a *post-reconnect* sample (below) and asserts that
    # entry flows through the metadata sidecar, not the stale one.
    MetadataWriter(transport_panda).add(
        "sensor_a", {"temp_c": 99.0, "status": "outage-backlog"}
    )

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    mock_data = generate_data(ntimes=1)

    read_count = [0]
    add_data_metadata = []

    def remember_metadata(*args, **kwargs):
        add_data_metadata.append(kwargs.get("metadata"))

    mock_file.add_data.side_effect = remember_metadata

    def read_side_effect(*a, **kw):
        read_count[0] += 1
        if read_count[0] == 2:
            # Panda comes online: heartbeat goes alive. The corr loop's
            # reconnect branch will run skip_to_latest() and drop the
            # outage backlog seeded above.
            HeartbeatWriter(transport_panda).set(alive=True)
        if read_count[0] == 3:
            # Post-reconnect publish: the next drain must pick this up.
            MetadataWriter(transport_panda).add(
                "sensor_a", {"temp_c": 25.0, "status": "update"}
            )
        if read_count[0] >= 5:
            observer.stop_event.set()
        return (123, mock_data)

    caplog.set_level(logging.INFO, logger="eigsep_observing.observer")
    with patch.object(
        observer.corr_reader, "read", side_effect=read_side_effect
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    # First integration: panda still offline → metadata is empty.
    assert add_data_metadata[0] in (None, {}), add_data_metadata
    # By the final integration, the post-recovery sensor sample must
    # have made it into the file — proving the metadata stream picked
    # up implicitly without an observer restart.
    fresh_seen = any(
        isinstance(md, dict)
        and any(
            isinstance(entries, list)
            and any(
                isinstance(e, dict) and e.get("temp_c") == 25.0
                for e in entries
            )
            for entries in md.values()
        )
        for md in add_data_metadata[1:]
    )
    assert fresh_seen, (
        "post-reconnect sensor sample did not appear in any metadata "
        f"sidecar: {add_data_metadata!r}"
    )
    # And the stream is registered (panda did publish) — sanity check.
    assert (
        transport_panda.r.sismember(METADATA_STREAMS_SET, b"stream:sensor_a")
        == 1
    )


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


# ---------------------------------------------------------------------------
# Defensive panda handling (corr-data-is-sacred)
# ---------------------------------------------------------------------------


def test_panda_connected_tolerates_connection_error(observer_panda_only):
    """A dropped panda Redis connection makes ``heartbeat_reader.check``
    raise ``ConnectionError``. ``panda_connected`` must return ``False``
    rather than propagate, so the corr loop's "if panda_connected: drain"
    gate stays safe.
    """
    import redis.exceptions

    with patch.object(
        observer_panda_only.heartbeat_reader,
        "check",
        side_effect=redis.exceptions.ConnectionError("connection refused"),
    ):
        assert observer_panda_only.panda_connected is False


@patch("eigsep_observing.io.File")
def test_record_corr_data_drain_failure_continues_with_empty_metadata(
    mock_file_class, observer_both, caplog
):
    """``metadata_stream.drain`` raising ``ConnectionError`` must not
    kill the corr loop. The integration is written with ``metadata=None``
    (corr data is sacred) and an ERROR is logged once per throttle
    window — the CLAUDE.md rule for narrow safety nets around non-corr
    processing.
    """
    import redis.exceptions

    observer = observer_both
    observer.corr_config.upload_header({"sync_time": 1713200000.0})

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    mock_data = generate_data(ntimes=1)

    def read_side_effect(*a, **kw):
        observer.stop_event.set()
        return (123, mock_data)

    caplog.set_level(logging.WARNING, logger="eigsep_observing.observer")
    with (
        patch.object(
            observer.corr_reader, "read", side_effect=read_side_effect
        ),
        patch.object(
            observer.metadata_stream,
            "drain",
            side_effect=redis.exceptions.ConnectionError("connection refused"),
        ),
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    mock_file.add_data.assert_called_once()
    assert mock_file.add_data.call_args.kwargs["metadata"] is None
    assert any(
        "Panda metadata drain failed" in rec.message for rec in caplog.records
    )


@patch("eigsep_observing.io.File")
def test_record_corr_data_drain_failure_error_is_throttled(
    mock_file_class, observer_both, caplog
):
    """Repeated ``drain`` failures over the same throttle window emit
    only one ERROR. The corr loop runs at multi-Hz cadence; without
    throttling a dead panda would flood the log.
    """
    import redis.exceptions

    observer = observer_both
    observer.corr_config.upload_header({"sync_time": 1713200000.0})

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    mock_data = generate_data(ntimes=1)

    read_count = [0]

    def read_side_effect(*a, **kw):
        read_count[0] += 1
        if read_count[0] >= 5:
            observer.stop_event.set()
        return (123, mock_data)

    caplog.set_level(logging.WARNING, logger="eigsep_observing.observer")
    with (
        patch.object(
            observer.corr_reader, "read", side_effect=read_side_effect
        ),
        patch.object(
            observer.metadata_stream,
            "drain",
            side_effect=redis.exceptions.ConnectionError("down"),
        ),
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    # Filter by exact logger name: the StatusStreamHandler mirrors the
    # ERROR onto the panda status stream, and ``status_logger`` re-emits
    # what it reads back on the ``panda_relay`` child logger. A substring
    # match on ``rec.message`` would race-catch that relayed echo and
    # double-count the (correctly throttled) original. The throttle lives
    # on the original emit site, so the assertion belongs on records from
    # that site alone.
    drain_errors = [
        rec
        for rec in caplog.records
        if rec.name == "eigsep_observing.observer"
        and "Panda metadata drain failed" in rec.message
    ]
    assert read_count[0] >= 5, "loop didn't iterate enough to test throttle"
    assert len(drain_errors) == 1, (
        f"expected one throttled ERROR, got {len(drain_errors)}"
    )
    assert drain_errors[0].levelname == "ERROR", drain_errors[0].levelname


@patch("eigsep_observing.io.File")
def test_record_corr_data_skip_to_tail_on_panda_recover(
    mock_file_class, observer_both, transport_panda, caplog
):
    """After a drain failure, when the panda comes back the next
    successful drain must skip backlog and start at the current stream
    tails. This keeps metadata aligned with the corr integration
    window instead of smearing outage-era readings into new rows.
    """
    import redis.exceptions
    from eigsep_redis.keys import METADATA_STREAMS_SET

    observer = observer_both
    observer.corr_config.upload_header({"sync_time": 1713200000.0})

    mock_file = Mock()
    mock_file.counter = 0
    mock_file.__len__ = Mock(return_value=0)
    mock_file_class.return_value = mock_file

    # Seed an "outage backlog" entry into the metadata stream before the
    # recovery iteration runs. Without skip-to-tail the recovery drain
    # would pick this up.
    #
    # Fixture deviation: the stream name (``"sensor_a"``), field
    # (``"temp_c"``), and status values (``"stale"``, ``"fresh"``) are
    # synthetic — none correspond to entries in ``SENSOR_SCHEMAS``.
    # This is intentional: the test exercises the metadata-stream
    # read-pointer mechanics (does the recovery path's
    # ``skip_to_latest`` advance past outage backlog?), which operate
    # below the schema layer. A real producer payload would add no
    # coverage here, and using a real schema would couple this test to
    # ``_avg_sensor_values`` reductions that the recovery path doesn't
    # touch. See ``tests/test_io.py`` for the end-to-end fixture
    # round-trip that validates the producer→avg→write→read contract.
    backlog_sample = {"temp_c": 99.0, "status": "stale"}
    MetadataWriter(transport_panda).add("sensor_a", backlog_sample)

    mock_data = generate_data(ntimes=1)
    drain_calls = []
    real_drain = observer.metadata_stream.drain

    def drain_side_effect(*a, **kw):
        # First call: simulate connection error (outage). Subsequent
        # calls behave normally — the loop's skip-to-tail recovery
        # should have advanced the read pointer past the backlog.
        drain_calls.append(time.monotonic())
        if len(drain_calls) == 1:
            raise redis.exceptions.ConnectionError("down")
        return real_drain(*a, **kw)

    read_count = [0]

    def read_side_effect(*a, **kw):
        read_count[0] += 1
        if read_count[0] == 2:
            # Add a *post-recovery* entry; the recovery iteration should
            # see only this one, not the backlog seeded above.
            MetadataWriter(transport_panda).add(
                "sensor_a", {"temp_c": 25.0, "status": "fresh"}
            )
        if read_count[0] >= 3:
            observer.stop_event.set()
        return (123, mock_data)

    caplog.set_level(logging.INFO, logger="eigsep_observing.observer")
    with (
        patch.object(
            observer.corr_reader, "read", side_effect=read_side_effect
        ),
        patch.object(
            observer.metadata_stream, "drain", side_effect=drain_side_effect
        ),
    ):
        observer.record_corr_data("/tmp/test", ntimes=1, timeout=5)

    # Confirm we actually exercised the recover-and-resume path.
    assert len(drain_calls) >= 2
    assert any(
        "metadata pipeline back" in rec.message for rec in caplog.records
    ), [r.message for r in caplog.records]

    # No call to add_data carried the stale backlog reading.
    seen_metadata = [
        call.kwargs.get("metadata")
        for call in mock_file.add_data.call_args_list
    ]
    stale_seen = any(
        isinstance(m, dict)
        and any(backlog_sample in v for v in m.values() if isinstance(v, list))
        for m in seen_metadata
    )
    assert not stale_seen, (
        f"stale backlog reading leaked into corr file: {seen_metadata}"
    )

    # Sanity: stream is registered so the skip helper actually had
    # something to advance.
    assert transport_panda.r.smembers(METADATA_STREAMS_SET)
