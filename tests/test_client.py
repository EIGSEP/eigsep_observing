import copy
import json
import logging
import threading
import time
from unittest.mock import patch

import pytest
import yaml

from cmt_vna.testing import DummyVNA
from eigsep_redis import ConfigStore, HeartbeatReader, StatusReader
from eigsep_redis.keys import STATUS_STREAM
from eigsep_redis.testing import DummyTransport
from picohost.proxy import PicoProxy

import eigsep_observing
from eigsep_observing.testing import DummyPandaClient
from eigsep_observing.testing.utils import compare_dicts


@pytest.fixture(scope="module")
def module_tmpdir(tmp_path_factory):
    """
    Create a temporary directory for the module scope.
    This will be used to store VNA files and other temporary data.
    """
    return tmp_path_factory.mktemp("module_tmpdir")


@pytest.fixture()
def dummy_cfg(module_tmpdir):
    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg["vna_save_dir"] = str(module_tmpdir)
    return cfg


@pytest.fixture
def transport():
    return DummyTransport()


@pytest.fixture
def client(transport, dummy_cfg):
    c = DummyPandaClient(transport, default_cfg=dummy_cfg)
    yield c
    c.stop()


def _status_reader(client):
    """Build a StatusReader on the client's transport for test-side reads."""
    return StatusReader(client.transport)


def _arm_status_reader(client):
    """Anchor the test-side status reader at the current stream tip.

    ``DummyPandaClient`` construction starts a real ``PicoManager``
    which publishes a ``PicoManager started`` entry onto the shared
    ``stream:status``. Production consumers subscribe at the current
    tip and read only forward; tests need to mirror that so
    setup-noise isn't observed as test-subject output. Call this
    *after* ``DummyPandaClient(...)`` and *before* the action under
    test; subsequent ``_status_reader(client).read(...)`` calls will
    then see only entries produced by the action.
    """
    tip = client.transport._get_last_read_id(STATUS_STREAM)
    client.transport._set_last_read_id(STATUS_STREAM, tip)


def _heartbeat_reader(client):
    return HeartbeatReader(client.transport)


def test_client(client):
    # client is initialized with heartbeat ticking
    assert _heartbeat_reader(client).check()
    # sw_proxy is always created as a generic PicoProxy
    assert client.sw_proxy is not None
    assert isinstance(client.sw_proxy, PicoProxy)
    # vna should be initialized if use_vna is true in config
    if client.cfg.get("use_vna", False):
        assert isinstance(client.vna, DummyVNA)
    else:
        assert client.vna is None


def test_get_cfg(caplog, dummy_cfg):
    caplog.set_level("INFO")

    # should be no config in redis at start
    t = DummyTransport()  # different transport for test isolation
    with pytest.raises(ValueError):
        ConfigStore(t).get()
    client2 = DummyPandaClient(t, default_cfg={})
    client3 = None
    try:
        # should have created a logger warning about missing config
        for record in caplog.records:
            if "No configuration found in Redis" in record.getMessage():
                assert record.levelname == "WARNING"
        # after init of client2, the cfg should be in redis
        cfg_in_redis = client2._get_cfg()
        assert "upload_time" in cfg_in_redis

        # upload the dummy config to client2's redis
        client2.config.upload(dummy_cfg)

        # check that they're the same
        retrieved_cfg = client2._get_cfg()
        retrieved_cfg_copy = retrieved_cfg.copy()
        del retrieved_cfg_copy["upload_time"]
        dummy_cfg_serialized = json.loads(json.dumps(dummy_cfg))
        compare_dicts(dummy_cfg_serialized, retrieved_cfg_copy)

        # if reinit client2, it should get the config from redis
        client3 = DummyPandaClient(t, default_cfg={})
        retrieved_cfg2 = client3._get_cfg()
        compare_dicts(client3.cfg, retrieved_cfg2)

        # check logging
        for record in caplog.records:
            if "Using config from Redis" in record.getMessage():
                assert record.levelname == "INFO"
    finally:
        client2.stop()
        if client3 is not None:
            client3.stop()


def test_switch_proxy_created(client):
    """sw_proxy is a PicoProxy that can see PicoManager's rfswitch."""
    assert isinstance(client.sw_proxy, PicoProxy)
    assert client.sw_proxy.is_available
    assert client.sw_proxy.name == "rfswitch"


def test_pico_manager_devices_visible(client):
    """PicoManager's registered devices are visible in Redis."""
    available = client.transport.r.smembers("picos")
    names = {n.decode() if isinstance(n, bytes) else n for n in available}
    expected = {
        "tempctrl",
        "potmon",
        "imu_el",
        "imu_az",
        "lidar",
        "rfswitch",
        "motor",
    }
    assert names == expected


def test_vna_loop_returns_when_vna_is_none(caplog, client):
    """vna_loop must return promptly when self.vna is None — no
    polling. Regression for a bare `threading.Event().wait(5)` that
    ignored stop_client. Also asserts the warning rides both channels
    (local + status stream) so the ground observer sees the failure."""
    caplog.set_level("WARNING")
    assert client.vna is None  # dummy_config has use_vna: false
    _arm_status_reader(client)
    t0 = time.monotonic()
    client.vna_loop()
    elapsed = time.monotonic() - t0
    assert elapsed < 1.0, f"vna_loop did not return promptly ({elapsed}s)"
    assert any("VNA not initialized" in r.getMessage() for r in caplog.records)

    level, status = _status_reader(client).read(timeout=1)
    assert level == logging.WARNING
    assert "VNA not initialized" in status


def test_switch_loop_does_not_mutate_cfg_schedule(client):
    """switch_loop must not mutate self.cfg['switch_schedule'] when
    filtering out zero-wait modes. Regression for `del schedule[mode]`
    on the live cfg reference."""
    original = copy.deepcopy(client.cfg["switch_schedule"])
    assert any(v == 0 for v in original.values()), (
        "test needs a zero-wait mode in dummy_config to exercise the filter"
    )
    client.stop_client.set()  # bypass the main while loop after validation
    client.switch_loop()
    assert client.cfg["switch_schedule"] == original


def test_cfg_is_get_cfg_result_without_extra_roundtrip(client):
    """``self.cfg`` must equal what ``_get_cfg`` returns. The previous
    ``json.loads(json.dumps(cfg))`` step was dead code — ``config.get``
    already returns a JSON-normalized dict (via ``json.loads`` on the
    serialized payload) — and would silently re-introduce drift if
    re-added on top of a different storage path."""
    assert client.cfg == client._get_cfg()


def test_read_switch_mode_from_redis_returns_published_mode(client):
    """The helper maps the rfswitch's last-published ``sw_state`` int
    back to a mode string. This is the reconcile path that replaces the
    panda-side shadow ``current_switch_state`` — the published state is
    the single source of truth across PandaClient/PicoManager restarts.
    """
    # Drive the rfswitch to a non-default mode and wait for the firmware
    # status publish to land in Redis.
    assert client._safe_switch("RFNON")
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if client._read_switch_mode_from_redis() == "RFNON":
            break
        time.sleep(0.05)
    assert client._read_switch_mode_from_redis() == "RFNON"


def test_read_switch_mode_from_redis_no_rfswitch_data(client):
    """Returns ``None`` if the rfswitch hasn't published yet — caller
    decides the fallback (``vna_loop`` falls back to RFANT with a
    warning)."""
    # Wipe the rfswitch entry from the metadata snapshot.
    client.transport.r.hdel("metadata", "rfswitch")
    assert client._read_switch_mode_from_redis() is None


def test_read_switch_mode_from_redis_unmapped_sw_state(client):
    """Returns ``None`` if the published ``sw_state`` doesn't map to a
    known mode — guards against firmware drift."""
    bogus = json.dumps({"sensor_name": "rfswitch", "sw_state": 99999}).encode()
    client.transport.r.hset("metadata", "rfswitch", bogus)
    assert client._read_switch_mode_from_redis() is None


def test_vna_loop_uses_redis_published_mode_for_switch_back(
    transport, dummy_cfg, caplog
):
    """vna_loop reads ``prev_mode`` from Redis (PicoManager truth), not
    from a panda-side shadow. After a PandaClient restart that finds
    the rfswitch already in RFNOFF, the post-VNA switch-back must
    target RFNOFF — not the previously-shadowed RFANT default."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 60  # long: only one iteration before stop
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        # Pre-seed the rfswitch in Redis to simulate a state PicoManager
        # set before this PandaClient process started.
        assert client._safe_switch("RFNOFF")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if client._read_switch_mode_from_redis() == "RFNOFF":
                break
            time.sleep(0.05)
        assert client._read_switch_mode_from_redis() == "RFNOFF"

        switch_calls = []
        original_safe_switch = client._safe_switch

        # Stop after the post-VNA switch-back. The VNA's internal
        # switch_fn only touches VNA* modes (VNAANT, VNARF, ...);
        # RFNOFF is uniquely vna_loop's switch-back target. Setting
        # stop_client here (instead of patching stop_client.wait)
        # avoids racing the heartbeat thread, which shares the same
        # Event and would otherwise see the patched wait().
        def recording_safe_switch(state):
            switch_calls.append(state)
            result = original_safe_switch(state)
            if state == "RFNOFF":
                client.stop_client.set()
            return result

        with patch.object(
            client, "_safe_switch", side_effect=recording_safe_switch
        ):
            caplog.set_level("INFO")
            client.vna_loop()

        # The last _safe_switch call from vna_loop itself is the
        # switch-back; intermediate calls come from VNA OSL/ant/rec.
        assert switch_calls, "vna_loop made no switch calls"
        assert switch_calls[-1] == "RFNOFF", (
            f"expected switch-back to RFNOFF (Redis truth), got "
            f"{switch_calls[-1]!r}; full sequence: {switch_calls}"
        )
        assert any(
            "previous mode: RFNOFF" in r.getMessage() for r in caplog.records
        )
    finally:
        client.stop()


def test_vna_loop_warns_and_defaults_when_rfswitch_absent(
    transport, dummy_cfg, caplog
):
    """If the rfswitch hasn't published, vna_loop logs a WARNING and
    falls back to RFANT — making the contract violation visible
    instead of silently switching to the wrong place."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 60
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        # Wipe the rfswitch entry so the helper returns None.
        client.transport.r.hdel("metadata", "rfswitch")

        switch_calls = []
        original_safe_switch = client._safe_switch

        # See sibling test: RFANT is the fallback switch-back target and
        # isn't hit by the VNA's internal VNA*-mode switching, so it's
        # safe to key the stop on it without racing the heartbeat.
        def recording_safe_switch(state):
            switch_calls.append(state)
            result = original_safe_switch(state)
            if state == "RFANT":
                client.stop_client.set()
            return result

        _arm_status_reader(client)
        with patch.object(
            client, "_read_switch_mode_from_redis", return_value=None
        ):
            with patch.object(
                client, "_safe_switch", side_effect=recording_safe_switch
            ):
                caplog.set_level("WARNING")
                client.vna_loop()

        assert switch_calls[-1] == "RFANT"
        assert any(
            "rfswitch state unavailable in Redis" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        )

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "rfswitch state unavailable in Redis" in status
    finally:
        client.stop()


def test_vna_loop_warns_on_failed_switch_back(transport, dummy_cfg, caplog):
    """If the post-VNA ``_safe_switch(prev_mode)`` returns falsy, vna_loop
    logs a WARNING — mirrors switch_loop's "Failed to switch" pattern so
    a hardware-stuck calibrator is operator-visible instead of silent."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 60  # long: only one iteration before stop
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        assert client._safe_switch("RFNOFF")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if client._read_switch_mode_from_redis() == "RFNOFF":
                break
            time.sleep(0.05)
        assert client._read_switch_mode_from_redis() == "RFNOFF"

        original_safe_switch = client._safe_switch

        # Only fail the switch-back (RFNOFF). The VNA's internal
        # switch_fn touches VNA* modes and must continue to succeed, or
        # the test would short-circuit before reaching the tail.
        def failing_switch_back(state):
            if state == "RFNOFF":
                client.stop_client.set()
                return None
            return original_safe_switch(state)

        _arm_status_reader(client)
        with patch.object(
            client, "_safe_switch", side_effect=failing_switch_back
        ):
            caplog.set_level("WARNING")
            client.vna_loop()

        assert any(
            "Failed to switch back to RFNOFF" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), (
            "expected 'Failed to switch back to RFNOFF' warning; "
            f"got records: {[r.getMessage() for r in caplog.records]}"
        )

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "Failed to switch back to RFNOFF" in status
    finally:
        client.stop()


def test_switch_loop_warns_on_failed_switch(transport, dummy_cfg, caplog):
    """A failing ``_safe_switch`` inside ``switch_loop`` must warn on both
    the local logger and the Redis status stream so the ground observer
    sees a stuck calibrator without SSHing into the panda."""
    cfg = dict(dummy_cfg)
    # Give the loop exactly one mode to try; cap wait so the stop event
    # breaks us out after the first iteration.
    cfg["switch_schedule"] = {"RFNOFF": 0.01}
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:

        def failing_switch(state):
            client.stop_client.set()
            return None

        _arm_status_reader(client)
        with patch.object(client, "_safe_switch", side_effect=failing_switch):
            caplog.set_level("WARNING")
            client.switch_loop()

        assert any(
            "Failed to switch to RFNOFF" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "Failed to switch to RFNOFF" in status
    finally:
        client.stop()


def _wait_for_published_mode(client, expected, timeout=2.0):
    """Spin until ``_read_switch_mode_from_redis`` sees ``expected``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client._read_switch_mode_from_redis() == expected:
            return
        time.sleep(0.05)
    assert client._read_switch_mode_from_redis() == expected


def test_switch_session_auto_restores_on_exit(client, caplog):
    """Happy path: enter with published mode=RFANT, switch to RFNOFF
    inside, exit → session auto-restores to RFANT. Matches the REPL
    "switch, measure, switch back" use case."""
    assert client._safe_switch("RFANT")
    _wait_for_published_mode(client, "RFANT")

    switch_calls = []
    original_safe_switch = client._safe_switch

    def recording(state):
        switch_calls.append(state)
        return original_safe_switch(state)

    caplog.set_level("WARNING")
    with patch.object(client, "_safe_switch", side_effect=recording):
        with client.switch_session() as sw:
            assert sw("RFNOFF") is True

    assert switch_calls == ["RFNOFF", "RFANT"], (
        f"expected RFNOFF then auto-restore to RFANT; got {switch_calls}"
    )
    # No warnings expected on the happy path.
    assert not any(
        "switch_session" in r.getMessage() for r in caplog.records
    ), [r.getMessage() for r in caplog.records]


def test_switch_session_noop_block_skips_restore(client):
    """If the caller enters a session but never invokes ``sw``, the
    context manager must not emit a restore ``_safe_switch`` — the user
    didn't change state, so no bookkeeping is required."""
    assert client._safe_switch("RFANT")
    _wait_for_published_mode(client, "RFANT")

    switch_calls = []
    original_safe_switch = client._safe_switch

    def recording(state):
        switch_calls.append(state)
        return original_safe_switch(state)

    with patch.object(client, "_safe_switch", side_effect=recording):
        with client.switch_session():
            pass

    assert switch_calls == [], (
        f"no-op switch_session block must not call _safe_switch; "
        f"got {switch_calls}"
    )


def test_switch_session_unknown_entry_mode_skips_restore(client, caplog):
    """If the rfswitch hasn't published on entry, the session has no
    mode to restore to. Skip the restore and log a warning — auto-
    guessing RFANT would be surprising at the REPL."""
    client.transport.r.hdel("metadata", "rfswitch")
    assert client._read_switch_mode_from_redis() is None

    switch_calls = []
    original_safe_switch = client._safe_switch

    def recording(state):
        switch_calls.append(state)
        return original_safe_switch(state)

    caplog.set_level("WARNING")
    with patch.object(client, "_safe_switch", side_effect=recording):
        with client.switch_session() as sw:
            assert sw("RFNOFF") is True

    assert switch_calls == ["RFNOFF"], (
        f"unknown-entry-mode session must not restore; got {switch_calls}"
    )
    assert any(
        "entry mode unknown" in r.getMessage() and r.levelname == "WARNING"
        for r in caplog.records
    ), [r.getMessage() for r in caplog.records]


def test_switch_session_warns_on_failed_restore(client, caplog):
    """If the auto-restore ``_safe_switch`` returns falsy, the session
    logs a warning but still releases the lock — a stuck switch must
    not wedge the session."""
    assert client._safe_switch("RFANT")
    _wait_for_published_mode(client, "RFANT")

    original_safe_switch = client._safe_switch

    # Succeed on RFNOFF (user's own switch) but fail the RFANT restore.
    def restore_fails(state):
        if state == "RFANT":
            return None
        return original_safe_switch(state)

    caplog.set_level("WARNING")
    with patch.object(client, "_safe_switch", side_effect=restore_fails):
        with client.switch_session() as sw:
            assert sw("RFNOFF") is True

    assert any(
        "switch_session: failed to restore to RFANT" in r.getMessage()
        and r.levelname == "WARNING"
        for r in caplog.records
    ), [r.getMessage() for r in caplog.records]
    # Lock must be released post-exit.
    assert client.switch_lock.acquire(blocking=False)
    client.switch_lock.release()


def test_switch_session_sw_warns_and_returns_false_on_failure(client, caplog):
    """The yielded callable warns and returns ``False`` when the
    underlying switch fails, so interactive users can branch on
    success without having to plumb ``_safe_switch``'s falsy sentinel."""
    assert client._safe_switch("RFANT")
    _wait_for_published_mode(client, "RFANT")

    caplog.set_level("WARNING")
    with patch.object(client, "_safe_switch", return_value=None):
        with client.switch_session() as sw:
            assert sw("RFNOFF") is False

    assert any(
        "Failed to switch to RFNOFF" in r.getMessage()
        and r.levelname == "WARNING"
        for r in caplog.records
    ), [r.getMessage() for r in caplog.records]


def test_switch_session_restores_even_on_exception(client):
    """An exception raised inside the ``with`` block must propagate
    (the caller's measurement failed), but auto-restore and lock
    release must still happen — that's the whole point of using a
    context manager for this."""
    assert client._safe_switch("RFANT")
    _wait_for_published_mode(client, "RFANT")

    switch_calls = []
    original_safe_switch = client._safe_switch

    def recording(state):
        switch_calls.append(state)
        return original_safe_switch(state)

    class _Boom(Exception):
        pass

    with patch.object(client, "_safe_switch", side_effect=recording):
        with pytest.raises(_Boom):
            with client.switch_session() as sw:
                sw("RFNOFF")
                raise _Boom("measurement failed")

    assert switch_calls == ["RFNOFF", "RFANT"], (
        f"exception inside block must still trigger restore; "
        f"got {switch_calls}"
    )
    assert client.switch_lock.acquire(blocking=False)
    client.switch_lock.release()


def test_switch_session_serializes_with_switch_loop(transport, dummy_cfg):
    """The session holds ``switch_lock`` for the whole block, so a
    concurrent ``switch_loop`` thread must block on the lock — its
    first ``_safe_switch`` call fires only *after* the session exits.
    Distinguishes switch_loop's calls from the session's own by
    thread identity, since both go through the patched ``_safe_switch``.
    """
    cfg = dict(dummy_cfg)
    # Keep the schedule simple and long so switch_loop blocks on the
    # lock instead of racing through many iterations during the test.
    cfg["switch_schedule"] = {"RFANT": 60.0}
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        assert client._safe_switch("RFANT")
        _wait_for_published_mode(client, "RFANT")

        main_ident = threading.get_ident()
        inside_session = threading.Event()
        loop_got_lock = threading.Event()
        loop_fired_during_session = threading.Event()
        original_safe_switch = client._safe_switch

        # Only calls from the switch_loop thread (not the main-thread
        # session's own ``sw(...)``) count as "loop got the lock."
        def observing_switch(state):
            if threading.get_ident() != main_ident:
                if inside_session.is_set():
                    loop_fired_during_session.set()
                loop_got_lock.set()
            return original_safe_switch(state)

        with patch.object(
            client, "_safe_switch", side_effect=observing_switch
        ):
            t = threading.Thread(target=client.switch_loop, daemon=True)
            # Take the session lock *before* starting switch_loop so
            # switch_loop is guaranteed to block on the first iter.
            with client.switch_session() as sw:
                inside_session.set()
                t.start()
                # switch_loop should attempt to acquire the lock and
                # block. A brief grace window gives it time to reach
                # that block point.
                time.sleep(0.2)
                assert not loop_got_lock.is_set(), (
                    "switch_loop acquired switch_lock while session held it"
                )
                sw("RFNOFF")
                time.sleep(0.1)
                assert not loop_got_lock.is_set(), (
                    "switch_loop acquired switch_lock after session's "
                    "sw() call — lock must still be held"
                )
                # Clear the flag while the lock is still held so any
                # switch_loop call observed after the session exits
                # can't be misattributed to the session's interval.
                inside_session.clear()
            # Session released the lock → switch_loop should proceed.
            assert loop_got_lock.wait(timeout=2.0), (
                "switch_loop did not acquire lock within 2s of session exit"
            )
            assert not loop_fired_during_session.is_set(), (
                "switch_loop executed _safe_switch while session was still "
                "inside its block"
            )

        client.stop_client.set()
        t.join(timeout=2.0)
        assert not t.is_alive()
    finally:
        client.stop()


def test_no_current_switch_state_attribute(client):
    """Regression: the panda-side shadow ``current_switch_state`` is
    gone — its replacement is :meth:`_read_switch_mode_from_redis`.
    A new attribute creeping back in would re-introduce the drift."""
    assert not hasattr(client, "current_switch_state")


def test_stop_joins_heartbeat_and_emits_goodbye(client):
    """stop() sets stop_client, joins the heartbeat thread, and the
    thread's final alive=False is visible to readers. Idempotent."""
    assert client.heartbeat_thd.is_alive()
    assert _heartbeat_reader(client).check() is True
    client.stop()
    assert not client.heartbeat_thd.is_alive()
    assert _heartbeat_reader(client).check() is False
    client.stop()  # idempotent — must not raise


def test_send_heartbeat_cycles_and_announces_shutdown(transport, dummy_cfg):
    """_send_heartbeat must tick more than once while running (i.e. it
    loops, not just publishes once at startup) and must emit its
    alive=False farewell on shutdown. A one-shot implementation that
    set alive=True in __init__ and never looped would still pass the
    sibling ``test_stop_joins_heartbeat_and_emits_goodbye`` (the TTL
    hasn't expired yet), so we explicitly count ticks here by counting
    invocations of the writer's ``set`` method over a ~1.2s window."""
    calls = []
    client = DummyPandaClient(transport, default_cfg=dummy_cfg)
    try:
        original_set = client.heartbeat.set

        def recording_set(*args, **kwargs):
            calls.append(kwargs)
            return original_set(*args, **kwargs)

        with patch.object(client.heartbeat, "set", side_effect=recording_set):
            # Wait for at least two 1s loop cycles so a non-looping
            # implementation would record zero ticks during the patch.
            time.sleep(1.2)
            alive_ticks = len(calls)
            client.stop()

        assert alive_ticks >= 1, (
            f"expected at least one heartbeat tick in the 1.2s window; "
            f"got {alive_ticks} (calls={calls})"
        )
        # stop() drives the loop's final iteration AND the explicit
        # alive=False farewell; both appear after the wait window.
        assert any(c.get("alive") is False for c in calls), (
            f"no alive=False shutdown tick recorded (calls={calls})"
        )
        # The alive=True ticks must carry the configured 60s TTL so a
        # crashed client is distinguishable from a cleanly-stopped one.
        alive_true_calls = [c for c in calls if c.get("alive", True)]
        assert alive_true_calls, (
            f"expected at least one alive=True tick (calls={calls})"
        )
        assert all(c.get("ex") == 60 for c in alive_true_calls), (
            f"alive=True ticks must set ex=60 for watchdog semantics; "
            f"got {alive_true_calls}"
        )
    finally:
        if client.heartbeat_thd.is_alive():
            client.stop()


def test_measure_s11_rejects_invalid_mode(client):
    """measure_s11 is restricted to ``ant``/``rec``. An unknown mode is
    a producer-side bug (wrong caller), not a runtime input; raise
    before touching the VNA so the failure is loud and local."""
    with pytest.raises(ValueError, match="Unknown VNA mode"):
        client.measure_s11("bogus")


def test_measure_s11_requires_initialized_vna(client):
    """measure_s11 must fail loudly when self.vna is None. The dummy
    config ships with use_vna=False so this is the default client
    fixture's state — use it as the canary."""
    assert client.vna is None
    with pytest.raises(RuntimeError, match="VNA not initialized"):
        client.measure_s11("ant")


def test_measure_s11_contract_violation_emits_on_both_channels(
    transport, dummy_cfg, caplog
):
    """A producer-side contract violation in ``measure_s11`` must log
    loudly locally *and* push a status-stream message so the ground
    observer sees it without SSHing. Panda-side ``self.logger`` writes
    only to a local RotatingFileHandler — see
    project_status_stream_log_bridge memory. Force a real violation by
    patching the header validator to return a canned list; the check
    under test is "both channels receive the message," not the
    validator's own logic (already covered by the producer-contract
    test)."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        _arm_status_reader(client)
        violations = ["missing key 'npoints'", "key 'mode': expected str"]
        with patch(
            "eigsep_observing.client._validate_vna_s11_header",
            return_value=violations,
        ):
            caplog.set_level(logging.WARNING, logger="eigsep_observing.client")
            client.measure_s11("ant")

        # Panda-local log channel.
        warning_msgs = [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.WARNING
            and "VNA S11 producer contract violation" in r.getMessage()
        ]
        assert len(warning_msgs) == 1, caplog.records
        assert "missing key 'npoints'" in warning_msgs[0]
        assert "key 'mode': expected str" in warning_msgs[0]
        assert "mode='ant'" in warning_msgs[0]

        # Ground-visible status stream. Reader was anchored at the tip
        # pre-action so it now sees only the entry the producer just
        # pushed.
        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert status == warning_msgs[0]
    finally:
        client.stop()


def test_measure_s11_clean_payload_does_not_send_status(transport, dummy_cfg):
    """Complement to the contract-violation test: on the happy path
    (real DummyVNA output passing the real validators), neither channel
    should emit a violation — we must not spam the bounded 5-entry
    status stream during normal operation."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        _arm_status_reader(client)
        client.measure_s11("ant")
        level, status = _status_reader(client).read(timeout=0.2)
    finally:
        client.stop()
    assert (level, status) == (None, None), (
        f"clean measure_s11 must not emit a status message, got "
        f"level={level!r} status={status!r}"
    )


# The tests below exercise ``_safe_switch``'s exception-to-bool
# translation end-to-end by patching the firmware-side ``switch()``
# method on the DummyPicoRFSwitch. PicoManager's exception handler
# converts method exceptions into status:"error" responses, which
# the proxy re-raises as RuntimeError — so patching the dummy device
# is how we drive a real RuntimeError at the consumer boundary.
# TimeoutError is induced by shortening ``sw_proxy.timeout`` and
# sleeping past it inside the patched switch.


def test_safe_switch_returns_false_on_runtime_error(client, caplog):
    """Regression: a firmware-side RuntimeError bypassed the old bool
    check entirely and the proxy exception crashed the observing loop.
    ``_safe_switch`` must catch it and return False."""
    pico = client._manager.picos["rfswitch"]
    caplog.set_level("WARNING")
    with patch.object(pico, "switch", side_effect=RuntimeError("fw boom")):
        assert client._safe_switch("RFNOFF") is False
    assert any(
        "RF switch to RFNOFF failed" in r.getMessage()
        and "RuntimeError" in r.getMessage()
        and "fw boom" in r.getMessage()
        and r.levelname == "WARNING"
        for r in caplog.records
    ), [r.getMessage() for r in caplog.records]


def test_safe_switch_returns_false_on_timeout(client, caplog):
    """Regression: a proxy-level TimeoutError bypassed the old bool
    check and crashed the loop. Stall the dummy switch longer than
    sw_proxy.timeout so _wait_response raises TimeoutError end-to-end
    — the real path, not a mocked exception at the proxy boundary."""
    pico = client._manager.picos["rfswitch"]
    client.sw_proxy.timeout = 0.1
    caplog.set_level("WARNING")

    def stall(state):
        time.sleep(0.3)

    with patch.object(pico, "switch", side_effect=stall):
        assert client._safe_switch("RFNOFF") is False
    assert any(
        "RF switch to RFNOFF failed" in r.getMessage()
        and "TimeoutError" in r.getMessage()
        and r.levelname == "WARNING"
        for r in caplog.records
    ), [r.getMessage() for r in caplog.records]


def test_safe_switch_returns_true_on_success(client):
    """Happy-path regression: unpatched dummy stack round-trips a
    truthy ``{"action":"switch","result":True}`` and ``_safe_switch``
    returns True."""
    assert client._safe_switch("RFNOFF") is True


# ``_switch`` is the raise-on-failure surface wired as cmt_vna's
# ``switch_fn`` (eigsep-vna 1.3+ contract). It mirrors ``_safe_switch``'s
# end-to-end failure paths but propagates instead of translating to
# bool, so that a mid-S11 switch failure aborts ``measure_*`` rather
# than contaminating the calibration.


def test_switch_raises_on_runtime_error(client):
    """Firmware-side RuntimeError must propagate out of ``_switch`` —
    cmt_vna 1.3 relies on this to abort an in-flight measure_*."""
    pico = client._manager.picos["rfswitch"]
    with patch.object(pico, "switch", side_effect=RuntimeError("fw boom")):
        with pytest.raises(RuntimeError, match="fw boom"):
            client._switch("RFNOFF")


def test_switch_raises_on_timeout(client):
    """Proxy-level TimeoutError must propagate out of ``_switch``."""
    pico = client._manager.picos["rfswitch"]
    client.sw_proxy.timeout = 0.1

    def stall(state):
        time.sleep(0.3)

    with patch.object(pico, "switch", side_effect=stall):
        with pytest.raises(TimeoutError):
            client._switch("RFNOFF")


def test_switch_raises_on_unregistered_device(client):
    """If ``sw_proxy.send_command`` returns ``None`` (device not
    registered with PicoManager), ``_switch`` must raise so that
    cmt_vna sees a switch failure instead of a silent no-op."""
    with patch.object(client.sw_proxy, "send_command", return_value=None):
        with pytest.raises(RuntimeError, match="not registered"):
            client._switch("RFNOFF")


def test_switch_returns_none_on_success(client):
    """``_switch`` has no meaningful return on success — the contract
    is raise-on-failure, return value ignored. Confirms the dummy
    round-trip doesn't accidentally raise."""
    assert client._switch("RFNOFF") is None


def test_switch_loop_survives_firmware_error(transport, dummy_cfg, caplog):
    """switch_loop must not propagate a RuntimeError out of the proxy
    boundary — the old bool check let it escape and crashed the loop.
    Both warning channels (local logger and Redis status stream) must
    still fire so the ground observer sees the stuck switch."""
    cfg = dict(dummy_cfg)
    cfg["switch_schedule"] = {"RFNOFF": 0.01}
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        pico = client._manager.picos["rfswitch"]

        def raise_and_stop(state):
            client.stop_client.set()
            raise RuntimeError("fw boom")

        _arm_status_reader(client)
        with patch.object(pico, "switch", side_effect=raise_and_stop):
            caplog.set_level("WARNING")
            client.switch_loop()  # must return, not raise

        assert any(
            "Failed to switch to RFNOFF" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]
        assert any(
            "RuntimeError" in r.getMessage() and "fw boom" in r.getMessage()
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "Failed to switch to RFNOFF" in status
    finally:
        client.stop()


def test_switch_loop_survives_timeout(transport, dummy_cfg, caplog):
    """switch_loop must not propagate a TimeoutError out of the proxy.
    Same regression as the RuntimeError sibling, different raise
    path."""
    cfg = dict(dummy_cfg)
    cfg["switch_schedule"] = {"RFNOFF": 0.01}
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        client.sw_proxy.timeout = 0.1
        pico = client._manager.picos["rfswitch"]

        def stall_and_stop(state):
            client.stop_client.set()
            time.sleep(0.3)

        _arm_status_reader(client)
        with patch.object(pico, "switch", side_effect=stall_and_stop):
            caplog.set_level("WARNING")
            client.switch_loop()

        assert any(
            "Failed to switch to RFNOFF" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]
        assert any("TimeoutError" in r.getMessage() for r in caplog.records), [
            r.getMessage() for r in caplog.records
        ]

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "Failed to switch to RFNOFF" in status
    finally:
        client.stop()


def test_vna_loop_survives_switch_back_error(transport, dummy_cfg, caplog):
    """Regression: a post-VNA switch-back RuntimeError used to unwind
    vna_loop. After the fix, the warning rides both channels and the
    loop exits cleanly."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 60
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        assert client._safe_switch("RFNOFF")
        _wait_for_published_mode(client, "RFNOFF")

        pico = client._manager.picos["rfswitch"]
        original_switch = pico.switch

        # Only fail the switch-back. The VNA's internal OSL path
        # touches VNA* modes and must continue to succeed.
        def switch_back_raises(state):
            if state == "RFNOFF":
                client.stop_client.set()
                raise RuntimeError("stuck calibrator")
            return original_switch(state=state)

        _arm_status_reader(client)
        with patch.object(pico, "switch", side_effect=switch_back_raises):
            caplog.set_level("WARNING")
            client.vna_loop()  # must return, not raise

        assert any(
            "Failed to switch back to RFNOFF" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]
        assert any(
            "RuntimeError" in r.getMessage()
            and "stuck calibrator" in r.getMessage()
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "Failed to switch back to RFNOFF" in status
    finally:
        client.stop()


def test_switch_session_restore_survives_timeout(client, caplog):
    """Regression: a TimeoutError on the auto-restore used to escape
    switch_session's finally block, leaving switch_lock held. After
    the fix, the warning logs, the session exits, and the lock is
    released."""
    assert client._safe_switch("RFANT")
    _wait_for_published_mode(client, "RFANT")

    pico = client._manager.picos["rfswitch"]
    original_switch = pico.switch
    client.sw_proxy.timeout = 0.1

    # Succeed on RFNOFF (user's own switch) but stall on the RFANT
    # restore so _wait_response times out end-to-end.
    def stall_on_restore(state):
        if state == "RFANT":
            time.sleep(0.3)
            return
        return original_switch(state=state)

    caplog.set_level("WARNING")
    with patch.object(pico, "switch", side_effect=stall_on_restore):
        with client.switch_session() as sw:
            assert sw("RFNOFF") is True

    assert any(
        "switch_session: failed to restore to RFANT" in r.getMessage()
        and r.levelname == "WARNING"
        for r in caplog.records
    ), [r.getMessage() for r in caplog.records]
    assert any("TimeoutError" in r.getMessage() for r in caplog.records), [
        r.getMessage() for r in caplog.records
    ]

    assert client.switch_lock.acquire(blocking=False)
    client.switch_lock.release()


def test_measure_s11_uses_mode_specific_power_dbm(transport, dummy_cfg):
    """The per-mode ``power_dBm`` from ``vna_settings`` must be applied
    to the VNA before each measurement. Regression guard for a unified
    or hardcoded power that would silently bias either mode."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        client.measure_s11("rec")
        expected_rec = cfg["vna_settings"]["power_dBm"]["rec"]
        assert client.vna.power_dBm == expected_rec
        client.measure_s11("ant")
        expected_ant = cfg["vna_settings"]["power_dBm"]["ant"]
        assert client.vna.power_dBm == expected_ant
    finally:
        client.stop()


def test_boot_drives_rfswitch_to_rfant(client):
    """Boot invariant: ``PandaClient.__init__`` must drive the rfswitch
    to RFANT so the system always wakes up in the physically safe
    (all-switches-off) state, regardless of the previous switch
    position. Side benefit: PicoManager publishes ``sw_state_name``
    before any observing-loop iteration, so downstream
    ``_read_switch_mode_from_redis`` has a truth to read from the
    first read."""
    _wait_for_published_mode(client, "RFANT")


def test_boot_warns_when_rfant_initialization_fails(
    transport, dummy_cfg, caplog
):
    """If the boot-time RFANT switch reports failure (rfswitch
    unreachable, PicoManager error), the operator must see a loud
    warning so the broken boot invariant is visible. Construction
    still succeeds — the Python client cannot enforce the hardware
    default on its own, so we log and continue rather than refusing
    to start."""
    caplog.set_level("WARNING")
    with patch.object(
        eigsep_observing.PandaClient, "_safe_switch", return_value=False
    ):
        client = DummyPandaClient(transport, default_cfg=dummy_cfg)
    try:
        assert any(
            "Boot-time RFANT initialization failed" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]
    finally:
        client.stop()


def test_vna_loop_recovers_to_rfant_on_measurement_exception(
    transport, dummy_cfg, caplog
):
    """When ``measure_s11`` raises mid-sweep (``_switch`` raising under
    the eigsep-vna 1.3 contract, a VNA instrument ``TimeoutError``, a
    Redis write failure), ``vna_loop`` must recover the switch to
    RFANT rather than die — ``prev_mode`` is stale (the VNA has been
    driving the switch through VNA* states) and RFANT is the
    physically safe fallback. The loop must stay up so the next
    ``vna_interval`` tick (and the concurrent ``switch_loop``) can
    resume normal operation."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 60  # long: one iteration then stop via RFANT
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        # Pre-seed a non-RFANT prev_mode so the recovery to RFANT is
        # distinguishable from a restore to prev_mode.
        assert client._safe_switch("RFNOFF")
        _wait_for_published_mode(client, "RFNOFF")

        switch_calls = []
        original_safe_switch = client._safe_switch

        def recording_safe(state):
            switch_calls.append(state)
            result = original_safe_switch(state)
            # Stop after the recovery fires so the while loop exits
            # on the next iteration check.
            if state == "RFANT":
                client.stop_client.set()
            return result

        def raising_measure(mode):
            raise RuntimeError(f"simulated mid-sweep failure in {mode}")

        _arm_status_reader(client)
        with (
            patch.object(client, "_safe_switch", side_effect=recording_safe),
            patch.object(client, "measure_s11", side_effect=raising_measure),
        ):
            caplog.set_level("WARNING")
            client.vna_loop()  # must return, not raise

        assert switch_calls[-1] == "RFANT", (
            f"expected recovery to RFANT (not prev_mode RFNOFF); "
            f"got sequence {switch_calls}"
        )
        assert any(
            "VNA cycle aborted" in r.getMessage()
            and "RuntimeError" in r.getMessage()
            and "simulated mid-sweep failure" in r.getMessage()
            and "recovering rfswitch to RFANT" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "VNA cycle aborted" in status
    finally:
        client.stop()
