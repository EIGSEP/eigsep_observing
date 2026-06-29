import copy
import json
import logging
import threading
import time
from unittest.mock import patch

import pytest

from cmt_vna.testing import DummyVNA
from eigsep_redis import (
    ConfigStore,
    HeartbeatReader,
    MetadataWriter,
    StatusReader,
)
from eigsep_redis.keys import STATUS_STREAM
from eigsep_redis.testing import DummyTransport
from picohost.proxy import PicoProxy

import eigsep_observing
from eigsep_observing import MotorClient, PandaClient, TempCtrlClient, run_tag
from eigsep_observing._test_fixtures import tempctrl_post_handler_reading
from eigsep_observing.testing import DummyPandaClient
from eigsep_observing.testing.utils import compare_dicts


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
    tip = client.transport.get_last_read_id(STATUS_STREAM)
    client.transport.set_last_read_id(STATUS_STREAM, tip)


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


def test_panda_client_uses_caller_cfg_without_touching_redis(dummy_cfg):
    """``PandaClient(..., cfg=<dict>)`` uses the caller's cfg directly
    and does not write it to Redis. Persistent Redis cfg is the
    province of uploader scripts (panda_observe, vna_position_sweep,
    no_switch_observation) — bring-up tools must not mutate it."""
    t = DummyTransport()
    with pytest.raises(ValueError):
        ConfigStore(t).get()  # baseline: Redis is empty
    client = DummyPandaClient(t, cfg=dummy_cfg)
    try:
        assert client.cfg == dummy_cfg
        # No side-effect upload: ConfigStore still sees an empty Redis.
        with pytest.raises(ValueError):
            ConfigStore(t).get()
    finally:
        client.stop()


def test_panda_client_raises_when_cfg_kwarg_none_and_redis_empty():
    """Bare ``PandaClient(transport)`` with no ``cfg=`` kwarg and an
    empty Redis raises ``RuntimeError`` rather than silently bootstrapping
    a default. The packaged default was the source of the
    ``vna_manual``-style trust-eroding upload; the loud raise forces the
    caller to either start an uploader or pass an explicit
    ``cfg=<dict>``."""
    t = DummyTransport()
    with pytest.raises(RuntimeError, match="No obs_config in Redis"):
        PandaClient(t)


def test_panda_client_reads_cfg_from_redis_when_cfg_kwarg_none(
    caplog, dummy_cfg
):
    """``PandaClient(..., cfg=None)`` reads the persistent cfg from
    Redis when an uploader has seeded it. Exercises the production
    happy path: panda_observe uploads cfg, then constructs the client
    without an explicit ``cfg=`` kwarg."""
    caplog.set_level("INFO")
    t = DummyTransport()
    ConfigStore(t).upload(dummy_cfg)
    # cfg=None → DummyPandaClient reads from Redis (mirrors parent
    # semantics), so the cfg actually used comes from the Redis upload.
    client = DummyPandaClient(t)
    try:
        cfg_copy = client.cfg.copy()
        del cfg_copy["upload_time"]
        dummy_cfg_serialized = json.loads(json.dumps(dummy_cfg))
        compare_dicts(dummy_cfg_serialized, cfg_copy)
        assert any(
            "Using config from Redis" in r.getMessage()
            and r.levelname == "INFO"
            for r in caplog.records
        )
    finally:
        client.stop()


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
    """vna_loop must return promptly when the VNA is disabled
    (``use_vna=false``) — no polling. Regression for a bare
    `threading.Event().wait(5)` that ignored stop_client. Also asserts
    the warning rides both channels (local + status stream) so the
    ground observer sees it."""
    caplog.set_level("WARNING")
    assert client.vna is None
    _arm_status_reader(client)
    t0 = time.monotonic()
    client.vna_loop()
    elapsed = time.monotonic() - t0
    assert elapsed < 1.0, f"vna_loop did not return promptly ({elapsed}s)"
    assert any(
        "VNA disabled in config" in r.getMessage() for r in caplog.records
    )

    level, status = _status_reader(client).read(timeout=1)
    assert level == logging.WARNING
    assert "VNA disabled in config" in status


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


def test_cfg_is_get_cfg_result_without_extra_roundtrip(dummy_cfg):
    """On the ``cfg=None`` + Redis-pre-seeded path, ``self.cfg`` must
    equal what ``_get_cfg`` returns. ``config.get`` already returns a
    JSON-normalized dict (via ``json.loads`` on the serialized
    payload), so the previous ``json.loads(json.dumps(cfg))`` wrapper
    was dead code; this guards against re-introducing it on top of a
    different storage path.

    The ``client`` fixture passes ``cfg=<dict>`` and so bypasses Redis
    entirely (per the new constructor contract); the invariant only
    applies to the production happy path where an uploader has seeded
    Redis first."""
    t = DummyTransport()
    ConfigStore(t).upload(dummy_cfg)
    client = DummyPandaClient(t)
    try:
        assert client.cfg == client._get_cfg()
    finally:
        client.stop()


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
    # Patch the snapshot reader rather than writing to Redis: the live
    # DummyPicoRFSwitch emulator publishes every 50 ms and would
    # overwrite any direct hset/hdel before the assertion under xdist.
    with patch.object(
        client.metadata_snapshot, "get", side_effect=KeyError("rfswitch")
    ):
        assert client._read_switch_mode_from_redis() is None


def test_read_switch_mode_from_redis_unmapped_sw_state(client):
    """Returns ``None`` if the published ``sw_state`` doesn't map to a
    known mode — guards against firmware drift."""
    # Patch the snapshot reader rather than writing to Redis: the live
    # DummyPicoRFSwitch emulator publishes every 50 ms and would
    # overwrite bogus Redis data before the assertion under xdist.
    bogus = {"sensor_name": "rfswitch", "sw_state": 99999}
    with patch.object(client.metadata_snapshot, "get", return_value=bogus):
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
    client = DummyPandaClient(transport, cfg=cfg)
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
    client = DummyPandaClient(transport, cfg=cfg)
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
    client = DummyPandaClient(transport, cfg=cfg)
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
    client = DummyPandaClient(transport, cfg=cfg)
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
    switch_calls = []
    original_safe_switch = client._safe_switch

    def recording(state):
        switch_calls.append(state)
        return original_safe_switch(state)

    caplog.set_level("WARNING")
    # Patch the snapshot reader rather than hdel-ing Redis: the live
    # DummyPicoRFSwitch emulator republishes sw_state_name every ~50 ms
    # and would overwrite a raw hdel before switch_session reads the
    # entry mode (flaky under load/xdist). Same technique as
    # test_read_switch_mode_from_redis_no_rfswitch_data.
    with patch.object(
        client, "_read_switch_mode_from_redis", return_value=None
    ):
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
    assert client.coord.lock.acquire(blocking=False)
    client.coord.lock.release()


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
    assert client.coord.lock.acquire(blocking=False)
    client.coord.lock.release()


def test_switch_session_serializes_with_switch_loop(transport, dummy_cfg):
    """The session holds the switch lock for the whole block, so a
    concurrent ``switch_loop`` thread must block on the lock — its
    first ``_safe_switch`` call fires only *after* the session exits.
    Distinguishes switch_loop's calls from the session's own by
    thread identity, since both go through the patched ``_safe_switch``.
    """
    cfg = dict(dummy_cfg)
    # Keep the schedule simple and long so switch_loop blocks on the
    # lock instead of racing through many iterations during the test.
    cfg["switch_schedule"] = {"RFANT": 60.0}
    client = DummyPandaClient(transport, cfg=cfg)
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
    client = DummyPandaClient(transport, cfg=dummy_cfg)
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
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        _arm_status_reader(client)
        violations = ["missing key 'npoints'", "key 'mode': expected str"]
        with patch(
            "eigsep_observing.vna._validate_vna_s11_header",
            return_value=violations,
        ):
            caplog.set_level(logging.WARNING, logger="eigsep_observing.client")
            with client.vna_session():
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
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        _arm_status_reader(client)
        with client.vna_session():
            client.measure_s11("ant")
        level, status = _status_reader(client).read(timeout=0.2)
    finally:
        client.stop()
    assert (level, status) == (None, None), (
        f"clean measure_s11 must not emit a status message, got "
        f"level={level!r} status={status!r}"
    )


def test_measure_s11_returns_published_payload(transport, dummy_cfg):
    """measure_s11 must return (s11, header, metadata) matching what it
    just published, so callers (notably the vna_manual bring-up script)
    can write a local artifact without racing the VNA stream reader."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        with client.vna_session():
            result = client.measure_s11("ant")
        assert isinstance(result, tuple) and len(result) == 3
        s11, header, metadata = result
        assert set(("ant", "noise", "load")).issubset(s11.keys())
        assert {"cal:VNAO", "cal:VNAS", "cal:VNAL"}.issubset(s11.keys())
        assert header["mode"] == "ant"
        assert "freqs" in header
        assert isinstance(metadata, dict)
    finally:
        client.stop()


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
    client = DummyPandaClient(transport, cfg=cfg)
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
    client = DummyPandaClient(transport, cfg=cfg)
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
    client = DummyPandaClient(transport, cfg=cfg)
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
    switch_session's finally block, leaving the switch lock held.
    After the fix, the warning logs, the session exits, and the lock
    is released."""
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

    assert client.coord.lock.acquire(blocking=False)
    client.coord.lock.release()


def test_measure_s11_uses_mode_specific_power_dbm(transport, dummy_cfg):
    """The per-mode ``power_dBm`` from ``vna_settings`` must be applied
    to the VNA before each measurement. Regression guard for a unified
    or hardcoded power that would silently bias either mode."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        with client.vna_session():
            client.measure_s11("rec")
            expected_rec = cfg["vna_settings"]["power_dBm"]["rec"]
            assert client.vna.power_dBm == expected_rec
            client.measure_s11("ant")
            expected_ant = cfg["vna_settings"]["power_dBm"]["ant"]
            assert client.vna.power_dBm == expected_ant
    finally:
        client.stop()


def test_measure_s11_injects_overlay_sentinels(transport, dummy_cfg):
    """No run_tag / owner published → ``UNKNOWN`` / ``0.0`` sentinels +
    ``obs_config`` snapshot from ``self.cfg``."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        with patch.object(client.vna_writer, "add") as mock_add:
            with client.vna_session():
                client.measure_s11("ant")
        assert mock_add.called
        header = mock_add.call_args.kwargs["header"]
        assert header["run_tag"] == "UNKNOWN"
        assert header["run_started_at_unix"] == 0.0
        assert header["obs_config_owner"] == "UNKNOWN"
        assert header["obs_config_owner_uploaded_unix"] == 0.0
        assert header["obs_config"]["use_vna"] is True
        # Sanity: the existing fields are still present.
        assert header["mode"] == "ant"
        assert "metadata_snapshot_unix" in header
    finally:
        client.stop()


def test_measure_s11_injects_published_run_tag(transport, dummy_cfg):
    """Publishing a run_tag flows into the VNA header on next
    measure_s11 call."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        run_tag.publish(transport, "vna_position_sweep", started_unix=12345.0)
        with patch.object(client.vna_writer, "add") as mock_add:
            with client.vna_session():
                client.measure_s11("rec")
        header = mock_add.call_args.kwargs["header"]
        assert header["run_tag"] == "vna_position_sweep"
        assert header["run_started_at_unix"] == 12345.0
    finally:
        client.stop()


def test_measure_s11_injects_published_owner(transport, dummy_cfg):
    """publish_owner flows into the VNA header on next measure_s11 call."""
    from eigsep_observing import obs_config_owner

    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        obs_config_owner.publish_owner(
            transport, "panda_observe", uploaded_at_unix=7.5
        )
        with patch.object(client.vna_writer, "add") as mock_add:
            with client.vna_session():
                client.measure_s11("ant")
        header = mock_add.call_args.kwargs["header"]
        assert header["obs_config_owner"] == "panda_observe"
        assert header["obs_config_owner_uploaded_unix"] == 7.5
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


def test_boot_errors_when_rfant_initialization_fails(
    transport, dummy_cfg, caplog
):
    """If the boot-time RFANT switch reports failure (rfswitch
    unreachable, PicoManager error), the operator must see a loud
    ERROR so the broken boot invariant is visible and actionable.
    Per CLAUDE.md, safety nets around non-corr processing must log
    at ERROR, not WARNING. Construction still succeeds — the Python
    client cannot enforce the hardware default on its own, so we log
    and continue rather than refusing to start."""
    caplog.set_level("ERROR")
    with patch.object(
        eigsep_observing.PandaClient, "_safe_switch", return_value=False
    ):
        client = DummyPandaClient(transport, cfg=dummy_cfg)
    try:
        assert any(
            "Boot-time RFANT initialization failed" in r.getMessage()
            and r.levelname == "ERROR"
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
    client = DummyPandaClient(transport, cfg=cfg)
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
            caplog.set_level("ERROR")
            client.vna_loop()  # must return, not raise

        assert switch_calls[-1] == "RFANT", (
            f"expected recovery to RFANT (not prev_mode RFNOFF); "
            f"got sequence {switch_calls}"
        )
        # Per CLAUDE.md, this exception-swallowing safety net must log
        # at ERROR (not WARNING) so the upstream fault is actionable.
        assert any(
            "VNA cycle aborted" in r.getMessage()
            and "RuntimeError" in r.getMessage()
            and "simulated mid-sweep failure" in r.getMessage()
            and "recovering rfswitch to RFANT" in r.getMessage()
            and r.levelname == "ERROR"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.ERROR
        assert "VNA cycle aborted" in status
    finally:
        client.stop()


# ``motor_loop`` is the sibling of ``vna_loop``: periodic action, loud
# error recovery, honors ``stop_client``. These tests patch
# ``motor_client.scan`` to drive the loop deterministically — the real
# scan path (grid traversal, emulator tick cadence) is covered by
# tests/test_motor_client.py.


def test_use_motor_false_leaves_motor_client_none(client):
    """Default dummy_config has ``use_motor: false``, so
    ``PandaClient.__init__`` must leave ``motor_client`` as ``None``.
    Mirrors the ``self.vna is None`` convention."""
    assert client.cfg.get("use_motor", False) is False
    assert client.motor_client is None


def test_use_motor_true_builds_motor_client(transport, dummy_cfg):
    """With ``use_motor: true``, ``motor_client`` is a real
    ``MotorClient`` bound to the same transport — so ``motor_loop`` has
    something to drive and the dummy PicoManager's motor pico is
    addressable through the client's proxy."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert isinstance(client.motor_client, MotorClient)
        # Motor client shares the panda client's transport, i.e. the
        # PicoManager is reachable through the same fake redis.
        assert client.motor_client.transport is client.transport
    finally:
        client.stop()


def test_motor_client_receives_limit_kwargs(transport, dummy_cfg):
    """Limit kwargs in ``motor_client_kwargs`` flow through
    ``init_motor_client`` into the ``MotorClient`` constructor verbatim.
    Verifies that the ``**kwargs`` splat in ``init_motor_client`` does
    not filter travel-limit keys."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_client_kwargs"] = {"el_limits_deg": [-30.0, 30.0]}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert tuple(client.motor_client.el_limits_deg) == (-30.0, 30.0)
    finally:
        client.stop()


def test_motor_loop_returns_when_motor_client_is_none(caplog, client):
    """motor_loop must return promptly when ``motor_client`` is None —
    no polling, no silent spin. The warning must ride both channels
    (local log + status stream) so the ground observer sees the
    misconfiguration."""
    assert client.motor_client is None  # dummy_config has use_motor: false
    _arm_status_reader(client)
    caplog.set_level("WARNING")
    t0 = time.monotonic()
    client.motor_loop()
    elapsed = time.monotonic() - t0
    assert elapsed < 1.0, f"motor_loop did not return promptly ({elapsed}s)"
    assert any(
        "Motor client not initialized" in r.getMessage()
        for r in caplog.records
    )

    level, status = _status_reader(client).read(timeout=1)
    assert level == logging.WARNING
    assert "Motor client not initialized" in status


def test_motor_loop_invalid_interval_returns(transport, dummy_cfg, caplog):
    """An invalid (non-positive / non-numeric) ``motor_interval`` is a
    config-side bug; motor_loop must refuse to run and warn loudly
    rather than fall into a tight no-sleep scan loop."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 0  # invalid
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        _arm_status_reader(client)
        caplog.set_level("WARNING")
        t0 = time.monotonic()
        client.motor_loop()
        elapsed = time.monotonic() - t0
        assert elapsed < 1.0
        assert any(
            "Invalid motor_interval" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        )
        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "Invalid motor_interval" in status
    finally:
        client.stop()


def test_motor_loop_calls_scan_with_stop_event_and_cfg_kwargs(
    transport, dummy_cfg
):
    """motor_loop forwards ``motor_scan`` kwargs to ``scan`` and injects
    ``stop_event=self.stop_client`` so a mid-scan ``stop()`` unwinds the
    traversal instead of having to wait for the current move to finish."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 60  # long; only one scan before stop fires
    cfg["motor_scan"] = {
        "az_range_deg": [-1.0, 0.0, 1.0],
        "el_range_deg": [-1.0, 0.0, 1.0],
        "repeat_count": 1,
    }
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        scan_calls = []

        def fake_scan(**kwargs):
            scan_calls.append(kwargs)
            client.stop_client.set()

        with patch.object(client.motor_client, "scan", side_effect=fake_scan):
            client.motor_loop()

        assert len(scan_calls) == 1, scan_calls
        call = scan_calls[0]
        assert call["stop_event"] is client.stop_client
        assert call["az_range_deg"] == [-1.0, 0.0, 1.0]
        assert call["el_range_deg"] == [-1.0, 0.0, 1.0]
        assert call["repeat_count"] == 1
    finally:
        client.stop()


def test_motor_loop_survives_scan_timeout_error_and_parks_motors(
    transport, dummy_cfg, caplog
):
    """A scan ``TimeoutError`` (stall, pico unreachable) must not unwind
    motor_loop. The error rides both channels, the loop attempts a
    post-failure ``home()`` to park the motors at (0,0) so a subsequent
    power loss doesn't leave the rig in a random position, and the loop
    stays up for the next retry. ``scan`` itself halts the motors in
    its own ``finally`` block, so motor_loop does not double-halt."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 60
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        home_calls = []

        def raising_scan(**kwargs):
            client.stop_client.set()
            raise TimeoutError("motor stalled for 30.0s without progress")

        def recording_home():
            home_calls.append(True)

        _arm_status_reader(client)
        with (
            patch.object(
                client.motor_client, "scan", side_effect=raising_scan
            ),
            patch.object(
                client.motor_client, "home", side_effect=recording_home
            ),
        ):
            caplog.set_level("INFO")
            client.motor_loop()  # must return, not raise

        assert home_calls == [True], (
            "motor_loop must attempt to park motors at home after scan "
            "failure so a later power loss doesn't strand the rig at an "
            "arbitrary grid point"
        )
        assert any(
            "Motor scan aborted" in r.getMessage()
            and "TimeoutError" in r.getMessage()
            and r.levelname == "ERROR"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]
        assert any(
            "Motors parked at home after scan failure" in r.getMessage()
            and r.levelname == "INFO"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.ERROR
        assert "Motor scan aborted" in status
    finally:
        client.stop()


def test_motor_loop_survives_scan_runtime_error(transport, dummy_cfg, caplog):
    """Sibling of the TimeoutError test. A firmware RuntimeError from
    the scan must log at ERROR, park via ``home()``, and keep the loop
    up."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 60
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        home_calls = []

        def raising_scan(**kwargs):
            client.stop_client.set()
            raise RuntimeError("fw boom")

        def recording_home():
            home_calls.append(True)

        _arm_status_reader(client)
        with (
            patch.object(
                client.motor_client, "scan", side_effect=raising_scan
            ),
            patch.object(
                client.motor_client, "home", side_effect=recording_home
            ),
        ):
            caplog.set_level("ERROR")
            client.motor_loop()

        assert home_calls == [True]
        assert any(
            "Motor scan aborted" in r.getMessage()
            and "RuntimeError" in r.getMessage()
            and "fw boom" in r.getMessage()
            and r.levelname == "ERROR"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.ERROR
        assert "Motor scan aborted" in status
    finally:
        client.stop()


def test_motor_loop_logs_error_when_post_failure_home_also_fails(
    transport, dummy_cfg, caplog
):
    """If the post-failure ``home()`` also raises (pico still
    unreachable, mechanical stall persists), motor_loop logs a second
    ERROR on both channels so the operator knows motors are in an
    unknown position — we've done what we can from Python, operator
    intervention is required. The loop still stays up for the next
    retry."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 60
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:

        def raising_scan(**kwargs):
            client.stop_client.set()
            raise TimeoutError("stall")

        def raising_home():
            raise TimeoutError("home stalled too")

        _arm_status_reader(client)
        with (
            patch.object(
                client.motor_client, "scan", side_effect=raising_scan
            ),
            patch.object(
                client.motor_client, "home", side_effect=raising_home
            ),
        ):
            caplog.set_level("ERROR")
            client.motor_loop()

        # Both errors are logged — the original scan abort *and* the
        # park-failure diagnosis.
        msgs = [r.getMessage() for r in caplog.records]
        assert any(
            "Motor scan aborted" in m and "TimeoutError" in m for m in msgs
        ), msgs
        assert any(
            "Post-failure home() also failed" in m
            and "home stalled too" in m
            and "motors in unknown position" in m
            for m in msgs
        ), msgs

        # Both rode the status stream so the ground observer sees them.
        level1, status1 = _status_reader(client).read(timeout=1)
        level2, status2 = _status_reader(client).read(timeout=1)
        assert level1 == logging.ERROR
        assert level2 == logging.ERROR
        statuses = [status1, status2]
        assert any("Motor scan aborted" in s for s in statuses)
        assert any("Post-failure home() also failed" in s for s in statuses)
    finally:
        client.stop()


def test_motor_loop_waits_motor_interval_when_inline_park_succeeds(
    transport, dummy_cfg
):
    """When a scan fails but the inline ``home()`` succeeds, motor_loop
    is back to a safe state — motors parked, no recovery needed — and
    the next wait uses the normal ``motor_interval`` cadence, not the
    fast retry. The fast retry is reserved for recovery mode (scan
    failure + inline park also failed)."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 3600
    cfg["motor_failure_retry_s"] = 5
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        motor_waits = []

        def recording_wait(timeout):
            if timeout == 1.0:
                return client.stop_client.is_set()
            motor_waits.append(timeout)
            client.stop_client.set()
            return True

        with (
            patch.object(
                client.motor_client,
                "scan",
                side_effect=TimeoutError("stall"),
            ),
            patch.object(client.motor_client, "home"),  # park succeeds
            patch.object(
                client.stop_client, "wait", side_effect=recording_wait
            ),
        ):
            client.motor_loop()

        assert motor_waits == [3600], (
            f"inline park succeeded → expected motor_interval=3600s wait, "
            f"got {motor_waits}"
        )
    finally:
        client.stop()


def test_motor_loop_uses_failure_retry_interval_after_failed_park(
    transport, dummy_cfg
):
    """When BOTH the scan and the inline ``home()`` fail, motor_loop
    enters recovery mode and waits ``motor_failure_retry_s`` (fast
    retry) instead of the full ``motor_interval``. Transient faults
    recover quickly; without the short retry the rig could sit stuck
    at an arbitrary position for a full hour."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 3600
    cfg["motor_failure_retry_s"] = 5
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        motor_waits = []

        def recording_wait(timeout):
            if timeout == 1.0:
                return client.stop_client.is_set()
            motor_waits.append(timeout)
            client.stop_client.set()
            return True

        with (
            patch.object(
                client.motor_client,
                "scan",
                side_effect=TimeoutError("stall"),
            ),
            patch.object(
                client.motor_client,
                "home",
                side_effect=TimeoutError("park also failed"),
            ),
            patch.object(
                client.stop_client, "wait", side_effect=recording_wait
            ),
        ):
            client.motor_loop()

        assert motor_waits == [5], (
            f"scan+park both failed → expected motor_failure_retry_s=5s "
            f"wait, got {motor_waits}"
        )
    finally:
        client.stop()


def test_motor_loop_recovery_retries_home_only_not_full_scan(
    transport, dummy_cfg
):
    """During recovery mode (scan + inline park both failed), motor_loop
    must retry ``home()`` only at the fast cadence and NOT re-run the
    full scan. Re-running the grid every failure_retry_s during a
    persistent fault would churn the motors and flood the bounded
    status stream; once parked, we're safe, so there's no benefit."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 3600
    cfg["motor_failure_retry_s"] = 5
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        scan_calls = []
        home_calls = []

        def raising_scan(**kwargs):
            scan_calls.append(kwargs)
            raise TimeoutError("stall")

        def raising_home():
            home_calls.append(True)
            raise TimeoutError("home stalled")

        # Let the loop iterate twice so we observe one recovery pass
        # before we shut it down. The second motor_loop wait returns
        # True, setting stop.
        motor_waits = []

        def recording_wait(timeout):
            if timeout == 1.0:
                return client.stop_client.is_set()
            motor_waits.append(timeout)
            if len(motor_waits) >= 2:
                client.stop_client.set()
                return True
            return False  # proceed to next iteration

        with (
            patch.object(
                client.motor_client, "scan", side_effect=raising_scan
            ),
            patch.object(
                client.motor_client, "home", side_effect=raising_home
            ),
            patch.object(
                client.stop_client, "wait", side_effect=recording_wait
            ),
        ):
            client.motor_loop()

        # First iteration: scan + inline home (both fail).
        # Second iteration: recovery — home only, scan NOT called again.
        assert len(scan_calls) == 1, (
            f"recovery mode must not re-run scan; got {len(scan_calls)} "
            f"scan calls"
        )
        assert len(home_calls) == 2, (
            f"recovery mode must retry home() after the inline park "
            f"failure; got {len(home_calls)} home calls"
        )
        # Both waits used the fast-retry cadence.
        assert motor_waits == [5, 5], motor_waits
    finally:
        client.stop()


def test_motor_loop_exits_recovery_when_home_finally_succeeds(
    transport, dummy_cfg
):
    """Once recovery-mode ``home()`` finally succeeds (transient fault
    cleared), motor_loop exits recovery and the next iteration runs a
    full scan again at ``motor_interval`` cadence. Guards against a
    bug where ``needs_park`` never clears."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 3600
    cfg["motor_failure_retry_s"] = 5
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        scan_calls = []
        home_calls = []

        def failing_then_success_scan(**kwargs):
            scan_calls.append(kwargs)
            if len(scan_calls) == 1:
                raise TimeoutError("first scan stalls")
            # second scan (post-recovery) succeeds; trigger shutdown
            client.stop_client.set()

        def home_side_effect():
            home_calls.append(True)
            if len(home_calls) == 1:
                # inline park after first scan failure — raise
                raise TimeoutError("park fails first")
            # second home (recovery retry) — succeed

        def fast_wait(timeout):
            # Pass everything through instantly — we don't care about
            # wait durations in this test, only the call sequence.
            return client.stop_client.is_set()

        with (
            patch.object(
                client.motor_client,
                "scan",
                side_effect=failing_then_success_scan,
            ),
            patch.object(
                client.motor_client, "home", side_effect=home_side_effect
            ),
            patch.object(client.stop_client, "wait", side_effect=fast_wait),
        ):
            client.motor_loop()

        # Expected sequence across iterations:
        #   1. scan fails → inline home fails → needs_park = True
        #   2. recovery: home succeeds → needs_park = False
        #   3. scan succeeds → sets stop
        assert len(scan_calls) == 2, (
            f"recovery must exit and run scan again; got {len(scan_calls)} "
            f"scan calls"
        )
        assert len(home_calls) == 2, (
            f"expected inline home + one recovery home; got {len(home_calls)}"
        )
    finally:
        client.stop()


def test_motor_loop_uses_motor_interval_after_successful_scan(
    transport, dummy_cfg
):
    """After a *successful* scan, the cadence must return to
    ``motor_interval`` — the short retry only applies while we're
    recovering from a failure. Guards against a bug where
    ``last_failed`` never resets."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 3600
    cfg["motor_failure_retry_s"] = 5
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        motor_waits = []

        def recording_wait(timeout):
            if timeout == 1.0:
                return client.stop_client.is_set()
            motor_waits.append(timeout)
            client.stop_client.set()
            return True

        def successful_scan(**kwargs):
            pass  # no-op: scan completed normally

        with (
            patch.object(
                client.motor_client, "scan", side_effect=successful_scan
            ),
            patch.object(
                client.stop_client, "wait", side_effect=recording_wait
            ),
        ):
            client.motor_loop()

        assert motor_waits == [3600]
    finally:
        client.stop()


def test_motor_loop_invalid_failure_retry_s_falls_back_to_interval(
    transport, dummy_cfg, caplog
):
    """An invalid ``motor_failure_retry_s`` is a config bug; motor_loop
    must warn loudly and fall back to ``motor_interval`` for retries,
    rather than falling into a tight no-sleep retry loop. Chosen over
    "refuse to run" because the steady-state scan cadence is still
    useful even without the fast-recovery tweak. Drives the fallback
    through the recovery-mode wait path by failing both scan and
    inline home, so the wait observed is specifically the
    failure_retry_s fallback and not the normal-cadence wait."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 42
    cfg["motor_failure_retry_s"] = 0  # invalid
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        motor_waits = []

        def recording_wait(timeout):
            if timeout == 1.0:
                return client.stop_client.is_set()
            motor_waits.append(timeout)
            client.stop_client.set()
            return True

        _arm_status_reader(client)
        with (
            patch.object(
                client.motor_client,
                "scan",
                side_effect=TimeoutError("stall"),
            ),
            patch.object(
                client.motor_client,
                "home",
                side_effect=TimeoutError("park also stalled"),
            ),
            patch.object(
                client.stop_client, "wait", side_effect=recording_wait
            ),
        ):
            caplog.set_level("WARNING")
            client.motor_loop()

        assert motor_waits == [42], (
            f"expected fallback wait of motor_interval=42s, got {motor_waits}"
        )
        assert any(
            "Invalid motor_failure_retry_s" in r.getMessage()
            and "falling back to motor_interval" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]
    finally:
        client.stop()


def test_motor_loop_set_delay_failure_is_warning_not_fatal(
    transport, dummy_cfg, caplog
):
    """A ``set_delay`` failure at loop entry is warned but non-fatal —
    the scan call is the real retry surface and will surface the same
    fault cleanly through the ERROR path on the next iteration."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 60
    cfg["motor_scan"] = {"repeat_count": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:

        def fake_scan(**kwargs):
            client.stop_client.set()

        _arm_status_reader(client)
        with (
            patch.object(
                client.motor_client,
                "set_delay",
                side_effect=RuntimeError("pico unreachable"),
            ),
            patch.object(client.motor_client, "scan", side_effect=fake_scan),
        ):
            caplog.set_level("WARNING")
            client.motor_loop()  # must proceed past set_delay failure

        assert any(
            "Motor set_delay failed" in r.getMessage()
            and "RuntimeError" in r.getMessage()
            and "pico unreachable" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]
        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "Motor set_delay failed" in status
    finally:
        client.stop()


def test_motor_loop_homes_closed_loop_after_scan(transport, dummy_cfg):
    """With ``home_after_scan: true``, motor_loop calls
    ``motor_homer.home`` (with ``stop_event``) after a successful scan.
    The closed-loop homer is NOT called on the failure park path."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 3600
    cfg["motor_scan"] = {"repeat_count": 1}
    cfg["home_after_scan"] = True
    cfg["motor_homer_kwargs"] = {"settle_s": 0.0, "max_iters": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert client.motor_homer is not None, (
            "home_after_scan=True must build motor_homer in init_motor_client"
        )
        homer_calls = []

        def fake_scan(**kwargs):
            pass  # scan succeeds

        def recording_homer_home(stop_event=None):
            homer_calls.append(stop_event)
            client.stop_client.set()
            from eigsep_observing.motor_homer import HomeResult

            return HomeResult(
                converged=True,
                iterations=1,
                residual_az_deg=0.0,
                residual_el_deg=0.0,
                degraded=False,
                reset_count=False,
            )

        with (
            patch.object(client.motor_client, "scan", side_effect=fake_scan),
            patch.object(
                client.motor_homer, "home", side_effect=recording_homer_home
            ),
        ):
            client.motor_loop()

        assert len(homer_calls) == 1, (
            f"motor_homer.home must be called once after a successful scan; "
            f"got {len(homer_calls)} calls"
        )
        assert homer_calls[0] is client.stop_client, (
            "motor_homer.home must receive stop_event=self.stop_client"
        )
    finally:
        client.stop()


def test_motor_loop_failure_park_stays_open_loop(transport, dummy_cfg):
    """With ``home_after_scan: true``, the FAILURE/recovery park path
    must use the open-loop ``motor_client.home()`` only — NOT the
    closed-loop homer. The homer is for the healthy post-scan path."""
    cfg = dict(dummy_cfg)
    cfg["use_motor"] = True
    cfg["motor_interval"] = 3600
    cfg["motor_scan"] = {"repeat_count": 1}
    cfg["home_after_scan"] = True
    cfg["motor_homer_kwargs"] = {"settle_s": 0.0, "max_iters": 1}
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        homer_calls = []
        open_loop_calls = []

        def raising_scan(**kwargs):
            client.stop_client.set()
            raise TimeoutError("motor stalled")

        def recording_open_home():
            open_loop_calls.append(True)

        def recording_homer_home(stop_event=None):
            homer_calls.append(True)

        _arm_status_reader(client)
        with (
            patch.object(
                client.motor_client, "scan", side_effect=raising_scan
            ),
            patch.object(
                client.motor_client, "home", side_effect=recording_open_home
            ),
            patch.object(
                client.motor_homer, "home", side_effect=recording_homer_home
            ),
        ):
            client.motor_loop()

        assert open_loop_calls, (
            "failure park must call the open-loop motor_client.home()"
        )
        assert not homer_calls, (
            "failure park must NOT call the closed-loop motor_homer.home()"
        )
    finally:
        client.stop()


# ``tempctrl_loop`` mirrors ``motor_loop``: periodic action gated by a
# ``use_*`` flag. Unit tests for the command dispatch + emulator state
# live in tests/test_tempctrl_client.py; these tests cover the
# gating and loop-level behavior.


def test_use_tempctrl_false_leaves_client_none(transport, dummy_cfg):
    """With ``use_tempctrl: false``, PandaClient must leave
    ``self.tempctrl`` as ``None`` and skip the init call."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = False
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert client.cfg.get("use_tempctrl", False) is False
        assert client.tempctrl is None
    finally:
        client.stop()


def test_use_tempctrl_true_builds_client(transport, dummy_cfg):
    """With ``use_tempctrl: true``, ``self.tempctrl`` is a real
    ``TempCtrlClient`` bound to the same transport."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert isinstance(client.tempctrl, TempCtrlClient)
        assert client.tempctrl.transport is client.transport
        # Settings from dummy_config land on the client object.
        assert client.tempctrl.settings.get("watchdog_timeout_ms") == 30000
        assert client.tempctrl.settings["LNA"]["target_C"] == 25.0
    finally:
        client.stop()


def test_init_tempctrl_warns_when_cooling_disabled(
    transport, dummy_cfg, caplog
):
    """``cooling_enabled: False`` is a deliberate non-default safety
    setting — surface it loudly at init so operators know the
    asymmetric-clamp guard is active. Fires per affected channel."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    cfg["tempctrl_settings"] = {
        "LNA": {
            "enable": True,
            "cooling_enabled": False,
            "target_C": 25.0,
        },
        "LOAD": {
            "enable": True,
            "cooling_enabled": True,
            "target_C": 25.0,
        },
    }
    caplog.set_level("WARNING")
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        warnings = [
            r.getMessage() for r in caplog.records if r.levelname == "WARNING"
        ]
        # LNA fires (cooling disabled); LOAD does not.
        assert any("LNA cooling disabled" in m for m in warnings), (
            f"expected LNA cooling warning in: {warnings}"
        )
        assert not any("LOAD cooling disabled" in m for m in warnings), (
            f"unexpected LOAD warning in: {warnings}"
        )
    finally:
        client.stop()


def test_init_tempctrl_no_warning_when_cooling_default(
    transport, dummy_cfg, caplog
):
    """Default config (no ``cooling_enabled`` key) leaves firmware
    default True — no cooling-disabled warning should fire."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    # dummy_cfg already has tempctrl_settings without cooling_enabled.
    caplog.set_level("WARNING")
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        warnings = [
            r.getMessage() for r in caplog.records if r.levelname == "WARNING"
        ]
        assert not any("cooling disabled" in m for m in warnings), (
            f"unexpected cooling warning with default config: {warnings}"
        )
    finally:
        client.stop()


def test_init_tempctrl_warns_when_channel_uninstalled(
    transport, dummy_cfg, caplog
):
    """``installed: False`` is a deliberate hardware descope — surface
    it once at init so operators know the channel is dark on purpose,
    not from a fault. Fires per affected channel."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    cfg["tempctrl_settings"] = {
        "LNA": {"installed": False, "enable": False},
        "LOAD": {"installed": True, "enable": True, "target_C": 25.0},
    }
    caplog.set_level("WARNING")
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        warnings = [
            r.getMessage() for r in caplog.records if r.levelname == "WARNING"
        ]
        assert any("LNA not installed" in m for m in warnings), (
            f"expected LNA not-installed warning in: {warnings}"
        )
        assert not any("LOAD not installed" in m for m in warnings), (
            f"unexpected LOAD warning in: {warnings}"
        )
    finally:
        client.stop()


def test_init_tempctrl_no_warning_when_installed_default(
    transport, dummy_cfg, caplog
):
    """Default config (no ``installed`` key) means both modules present
    — no not-installed warning should fire."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    # dummy_cfg's tempctrl_settings carries no installed keys.
    caplog.set_level("WARNING")
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        warnings = [
            r.getMessage() for r in caplog.records if r.levelname == "WARNING"
        ]
        assert not any("not installed" in m for m in warnings), (
            f"unexpected not-installed warning with default config: {warnings}"
        )
    finally:
        client.stop()


def test_tempctrl_health_check_skips_uninstalled_channel(
    transport, dummy_cfg, caplog
):
    """End-to-end descope guard: with ``LNA.installed: false``, the
    merged status from the real ``TempCtrlClient.get_status`` carries
    no LNA keys — even with a leftover ``tempctrl_lna`` hash entry in
    its real error-row shape (the reboot burst) — so the health check
    emits no LNA warnings. If the stream were read, the error row
    would trip "LNA thermistor in error state"; its absence proves the
    skip, not a lucky healthy row."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    cfg["tempctrl_settings"] = {
        "LNA": {"installed": False, "enable": False},
        "LOAD": {"installed": True, "enable": True, "target_C": 25.0},
    }
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        writer = MetadataWriter(client.transport)
        writer.add(
            "tempctrl_lna",
            tempctrl_post_handler_reading("tempctrl_lna", sensor_error="LNA"),
        )
        writer.add(
            "tempctrl_load", tempctrl_post_handler_reading("tempctrl_load")
        )
        caplog.set_level("WARNING")
        status = client.tempctrl.get_status()
        assert status is not None
        assert not any(k.startswith("LNA_") for k in status)
        # Drop the init-time "LNA not installed" descope warning (by
        # design, tested separately) so the assertion scopes to the
        # health check alone.
        caplog.clear()
        client._tempctrl_health_check(status)
        assert not any("LNA" in r.getMessage() for r in caplog.records), (
            f"unexpected LNA warning: {[r.getMessage() for r in caplog.records]}"
        )
    finally:
        client.stop()


def test_tempctrl_settings_non_dict_disables_client(
    transport, dummy_cfg, caplog
):
    """A non-dict ``tempctrl_settings`` is a config-side bug. Bail out
    loudly and leave the client disabled; do not construct a
    ``TempCtrlClient`` against garbage."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    cfg["tempctrl_settings"] = "not a dict"
    caplog.set_level("WARNING")
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert client.tempctrl is None
        assert any(
            "Invalid tempctrl_settings" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        )
    finally:
        client.stop()


def test_tempctrl_settings_bad_numeric_disables_client(
    transport, dummy_cfg, caplog
):
    """A typo'd numeric field (e.g. ``target_C: "twenty-five"``) must
    fail up front at ``init_tempctrl`` — otherwise the coercion
    ``TypeError``/``ValueError`` would only surface inside
    ``apply_settings`` and unwind the ``tempctrl_loop`` thread on the
    first iteration, stopping the health check entirely."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    cfg["tempctrl_settings"] = {
        "LNA": {"target_C": "twenty-five"},
    }
    caplog.set_level("WARNING")
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert client.tempctrl is None
        assert any(
            "Invalid tempctrl_settings" in r.getMessage()
            and "target_C" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        )
    finally:
        client.stop()


def test_tempctrl_loop_returns_when_client_is_none(
    caplog, transport, dummy_cfg
):
    """``tempctrl_loop`` must return promptly when disabled — no tight
    spin, warning rides both channels."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = False
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert client.tempctrl is None
        _arm_status_reader(client)
        caplog.set_level("WARNING")
        t0 = time.monotonic()
        client.tempctrl_loop()
        elapsed = time.monotonic() - t0
        assert elapsed < 1.0
        assert any(
            "Tempctrl not initialized" in r.getMessage()
            for r in caplog.records
        )
        level, status = _status_reader(client).read(timeout=1)
        assert level == logging.WARNING
        assert "Tempctrl not initialized" in status
    finally:
        client.stop()


def test_tempctrl_loop_invalid_interval_returns(transport, dummy_cfg, caplog):
    """Non-positive / non-numeric ``tempctrl_interval`` → refuse to run."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    cfg["tempctrl_interval"] = 0
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        _arm_status_reader(client)
        caplog.set_level("WARNING")
        t0 = time.monotonic()
        client.tempctrl_loop()
        elapsed = time.monotonic() - t0
        assert elapsed < 1.0
        assert any(
            "Invalid tempctrl_interval" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        )
    finally:
        client.stop()


def test_tempctrl_loop_applies_settings_once_then_stops(transport, dummy_cfg):
    """One pass through the loop calls ``apply_settings`` and honors
    ``stop_client``. Gating contract + single-seed contract: only the
    first iteration seeds firmware config; picohost owns reboot replay,
    so there is no periodic re-apply."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    cfg["tempctrl_interval"] = 60  # long; only one iteration
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        apply_calls = []

        def fake_apply():
            apply_calls.append(True)
            client.stop_client.set()

        with patch.object(
            client.tempctrl, "apply_settings", side_effect=fake_apply
        ):
            client.tempctrl_loop()
        assert apply_calls == [True]
    finally:
        client.stop()


def test_tempctrl_loop_does_not_reapply_after_success(transport, dummy_cfg):
    """After apply_settings succeeds once, subsequent iterations run
    the health check only — no re-apply. picohost's PicoPeltier caches
    the last-applied config and replays it on reconnect, which makes
    panda-side periodic re-apply redundant. Regression guard so we
    don't drift back to the old "apply every iteration" shape."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    cfg["tempctrl_interval"] = 0.01  # fast so we run many iterations
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        apply_calls = []
        health_calls = []

        def fake_apply():
            apply_calls.append(True)

        def fake_health(status):
            health_calls.append(status)
            if len(health_calls) >= 5:
                client.stop_client.set()

        with (
            patch.object(
                client.tempctrl, "apply_settings", side_effect=fake_apply
            ),
            patch.object(
                client.tempctrl,
                "get_status",
                return_value={
                    "watchdog_tripped": False,
                    "LNA_status": "update",
                    "LOAD_status": "update",
                },
            ),
            patch.object(
                client, "_tempctrl_health_check", side_effect=fake_health
            ),
        ):
            client.tempctrl_loop()
        assert apply_calls == [True]  # seeded once, not re-applied
        assert len(health_calls) >= 5  # health check runs every iteration
    finally:
        client.stop()


def test_tempctrl_loop_retries_apply_until_success(
    transport, dummy_cfg, caplog
):
    """If the initial apply_settings raises RuntimeError / TimeoutError
    (proxy/manager transient), retry on the same cadence until it
    sticks, then lock into health-check-only mode. Guards both the
    failure-survival contract (loop doesn't unwind on RuntimeError)
    and the retry-on-init-failure contract."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    cfg["tempctrl_interval"] = 0.01
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        _arm_status_reader(client)
        apply_calls = []

        def flaky_apply():
            apply_calls.append(True)
            if len(apply_calls) < 3:
                raise RuntimeError(f"transient #{len(apply_calls)}")
            # third call succeeds; stop after one health-check iteration
            # so the rest of the loop is pure health check.

        health_calls = []

        def fake_health(status):
            health_calls.append(status)
            if len(health_calls) >= 2:
                client.stop_client.set()

        with (
            patch.object(
                client.tempctrl, "apply_settings", side_effect=flaky_apply
            ),
            patch.object(
                client.tempctrl,
                "get_status",
                return_value={
                    "watchdog_tripped": False,
                    "LNA_status": "update",
                    "LOAD_status": "update",
                },
            ),
            patch.object(
                client, "_tempctrl_health_check", side_effect=fake_health
            ),
        ):
            caplog.set_level("WARNING")
            client.tempctrl_loop()
        # Two failed attempts + one success; no re-apply after success.
        assert apply_calls == [True, True, True]
        assert (
            sum(
                1
                for r in caplog.records
                if "Tempctrl apply_settings failed" in r.getMessage()
                and "RuntimeError" in r.getMessage()
                and r.levelname == "WARNING"
            )
            == 2
        )
    finally:
        client.stop()


# The three `_tempctrl_health_check` tests below pass sparse snapshots
# (3–11 of the 24 fields in the full tempctrl SENSOR_SCHEMAS shape — see
# io.py). The deviation from real-data shape is deliberate and defensible:
# `_tempctrl_health_check` is a pure reader that uses `.get()` with
# None-guards on every field, so unspecified keys are silently treated
# as "not present / not-relevant." Each test drives a single fault
# branch (watchdog / thermistor-error / saturated-drive), and including
# the other ~20 irrelevant fields would only add noise without changing
# behavior. A producer-side contract drift (a field renamed or dropped
# in the real snapshot) is caught by the producer-contract suite in
# contract_tests/test_producer_contracts.py, not by these branch tests.


def test_tempctrl_loop_warns_on_watchdog_tripped(transport, dummy_cfg, caplog):
    """When the metadata snapshot reports ``watchdog_tripped``, the
    health check emits an operator-visible WARNING so the peltiers
    going dark is visible ground-side."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        caplog.set_level("WARNING")
        # Sparse fixture — see branch-test rationale above.
        client._tempctrl_health_check(
            {
                "watchdog_tripped": True,
                "LNA_status": "update",
                "LOAD_status": "update",
            }
        )
        assert any(
            "Tempctrl firmware watchdog tripped" in r.getMessage()
            for r in caplog.records
        )
    finally:
        client.stop()


def test_tempctrl_loop_warns_on_channel_error(transport, dummy_cfg, caplog):
    """Per-channel ``status == "error"`` (thermistor read failed) must
    ride the status stream so the operator sees the dead sensor."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        caplog.set_level("WARNING")
        # Sparse fixture — see branch-test rationale above.
        client._tempctrl_health_check(
            {
                "watchdog_tripped": False,
                "LNA_status": "error",
                "LOAD_status": "update",
            }
        )
        assert any(
            "LNA thermistor in error state" in r.getMessage()
            for r in caplog.records
        )
        assert not any(
            "LOAD thermistor in error state" in r.getMessage()
            for r in caplog.records
        )
    finally:
        client.stop()


def test_tempctrl_loop_warns_on_saturated_drive(transport, dummy_cfg, caplog):
    """Drive saturated at the clamp while still >1°C from target =
    peltier can't keep up. Log loudly for the operator."""
    cfg = dict(dummy_cfg)
    cfg["use_tempctrl"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        caplog.set_level("WARNING")
        # Sparse fixture — see branch-test rationale above. The drive/
        # clamp/T_now/T_target quartet per channel is the full input to
        # the saturation check; the rest of the 24-field schema is
        # irrelevant to this branch.
        client._tempctrl_health_check(
            {
                "watchdog_tripped": False,
                "LNA_status": "update",
                "LOAD_status": "update",
                "LNA_drive_level": 0.6,
                "LNA_clamp": 0.6,
                "LNA_T_now": 35.0,
                "LNA_T_target": 25.0,
                "LOAD_drive_level": 0.05,
                "LOAD_clamp": 0.6,
                "LOAD_T_now": 25.1,
                "LOAD_T_target": 25.0,
            }
        )
        assert any(
            "LNA drive saturated at clamp" in r.getMessage()
            for r in caplog.records
        )
        assert not any(
            "LOAD drive saturated" in r.getMessage() for r in caplog.records
        )
    finally:
        client.stop()


# ---------------------------------------------------------------------
# MotionSwitchCoordinator + run_calibration_sequence
# ---------------------------------------------------------------------


def test_panda_client_builds_coord_with_default_off(client):
    """Default behavior: ``serialize_motion_and_switching`` absent or
    false → coord built with ``serialize=False`` → motor_loop is
    byte-for-byte preserved.
    """
    assert client.coord is not None
    assert client.coord.serialize is False
    # ``coord.lock`` is the public handle on the panda's underlying
    # ``_switch_lock``; tests probe it through the coord rather than
    # reaching into the private attribute.
    assert client.coord.lock is client._switch_lock


def test_panda_client_builds_coord_serialize_on_when_flag_set(
    transport, dummy_cfg
):
    """When the yaml flag is true, the coord serializes."""
    cfg = dict(dummy_cfg)
    cfg["serialize_motion_and_switching"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert client.coord.serialize is True
    finally:
        client.stop()


def test_coord_lock_is_rlock(client):
    """The panda's switch lock must be an RLock so the no-switch-
    observation script's outer ``switch_session`` can host an inner
    per-move ``motion_section`` from the same thread without
    deadlocking.
    """
    assert client.coord.lock.acquire(blocking=False)
    # Re-entry from the same thread succeeds for an RLock; would
    # block forever for a plain Lock.
    assert client.coord.lock.acquire(blocking=False)
    client.coord.lock.release()
    client.coord.lock.release()


def test_run_calibration_sequence_skips_vna_when_disabled(client, caplog):
    """use_vna=false → calibration logs a warning and proceeds to
    dwells without opening a VNA session.
    """
    assert client.vna is None  # dummy_cfg has use_vna: false
    caplog.set_level("WARNING")
    schedule = {"RFANT": 60, "RFNOFF": 0.05}
    completed = client.run_calibration_sequence(schedule=schedule)
    assert completed is True
    assert any(
        "VNA disabled (use_vna=false)" in r.getMessage()
        for r in caplog.records
    )


def test_run_calibration_sequence_filters_rfant(transport, dummy_cfg):
    """RFANT entries in the schedule must be skipped during
    calibration; only non-RFANT modes should drive the switch.
    """
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        seen = []
        with patch.object(client, "measure_s11", lambda mode: None):
            with patch.object(
                client,
                "_safe_switch",
                side_effect=lambda state: seen.append(state) or True,
            ):
                schedule = {
                    "RFANT": 0.05,
                    "RFNOFF": 0.05,
                    "RFNON": 0.05,
                }
                completed = client.run_calibration_sequence(schedule=schedule)
        assert completed is True
        assert "RFANT" not in seen
        assert seen == ["RFNOFF", "RFNON"]
    finally:
        client.stop()


def test_run_calibration_sequence_vna_first_then_dwells(transport, dummy_cfg):
    """``measure_s11`` must run before any non-RFANT switch dwell, so
    the VNA cal solution corresponds to the moment of bracketing
    (before noise dwells warm anything up).
    """
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        order = []
        with patch.object(
            client,
            "measure_s11",
            side_effect=lambda mode: order.append(f"vna:{mode}"),
        ):
            with patch.object(
                client,
                "_safe_switch",
                side_effect=lambda state: (
                    order.append(f"switch:{state}") or True
                ),
            ):
                schedule = {"RFNOFF": 0.05}
                client.run_calibration_sequence(schedule=schedule)
        assert order == ["vna:ant", "vna:rec", "switch:RFNOFF"]
    finally:
        client.stop()


def test_run_calibration_sequence_respects_stop_event(transport, dummy_cfg):
    """Setting ``stop_client`` mid-dwell must return False promptly so
    the no-switch-observation script can unwind cleanly.
    """
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        with patch.object(client, "measure_s11", lambda mode: None):
            with patch.object(client, "_safe_switch", return_value=True):
                # Long dwell; trip stop_client from a thread.
                def tripper():
                    time.sleep(0.1)
                    client.stop_client.set()

                t = threading.Thread(target=tripper, daemon=True)
                t.start()

                started = time.monotonic()
                completed = client.run_calibration_sequence(
                    schedule={"RFNOFF": 60.0}
                )
                elapsed = time.monotonic() - started
                t.join(timeout=1.0)
        assert completed is False
        assert elapsed < 2.0, (
            f"calibration did not honor stop_client (elapsed={elapsed:.2f}s)"
        )
    finally:
        client.stop()


def test_run_calibration_sequence_rejects_non_dict_schedule(
    transport, dummy_cfg
):
    """Non-dict ``switch_schedule`` is a config-shape bug; raise
    ``ValueError`` rather than warn-and-skip, matching how
    ``no_switch_observation.py`` validates ``motor_scan``.
    """
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = False
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        with pytest.raises(ValueError, match="switch_schedule must be a dict"):
            client.run_calibration_sequence(schedule=["RFNOFF", 0.05])
    finally:
        client.stop()


def test_run_calibration_sequence_skips_invalid_modes(
    transport, dummy_cfg, caplog
):
    """Unknown switch modes in the schedule are warned and skipped —
    don't poison the calibration flow with a typo.
    """
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        seen = []
        caplog.set_level("WARNING")
        with patch.object(client, "measure_s11", lambda mode: None):
            with patch.object(
                client,
                "_safe_switch",
                side_effect=lambda state: seen.append(state) or True,
            ):
                schedule = {"NOT_A_MODE": 0.05, "RFNOFF": 0.05}
                client.run_calibration_sequence(schedule=schedule)
        assert seen == ["RFNOFF"]
        assert any(
            "invalid switch mode" in r.getMessage()
            and "NOT_A_MODE" in r.getMessage()
            for r in caplog.records
        )
    finally:
        client.stop()


def test_vna_lazy_none_until_session(transport, dummy_cfg):
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        assert client.vna is None  # lazy: not built at construction
        assert client.vna_enabled is True
        with client.vna_session():
            assert isinstance(client.vna, DummyVNA)
        assert client.vna is None  # torn down on exit
    finally:
        client.stop()


def test_vna_session_nests_without_early_teardown(transport, dummy_cfg):
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        with client.vna_session():
            outer = client.vna
            with client.vna_session():
                assert client.vna is outer  # inner reuses the object
            assert client.vna is outer  # still alive after inner exit
        assert client.vna is None
    finally:
        client.stop()


def test_vna_open_rejected_when_disabled(client):
    # default fixture cfg has use_vna=False
    assert client.vna_enabled is False
    with pytest.raises(RuntimeError, match="use_vna=false"):
        client.vna_open()


def test_vna_session_starts_and_stops_service(
    transport, dummy_cfg, monkeypatch
):
    # Force the real (service-managed) path even on the dummy client.
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    monkeypatch.setattr(client, "_manage_vna_service", True)
    events = []
    from eigsep_observing import vna_service

    monkeypatch.setattr(vna_service, "start", lambda: events.append("start"))
    monkeypatch.setattr(vna_service, "stop", lambda: events.append("stop"))
    monkeypatch.setattr(
        vna_service, "wait_ready", lambda ip, port, **k: events.append("ready")
    )
    try:
        with client.vna_session():
            assert events == ["start", "ready"]
        assert events == ["start", "ready", "stop"]
    finally:
        client.stop()


def test_vna_session_stops_service_on_ready_failure(
    transport, dummy_cfg, monkeypatch
):
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    monkeypatch.setattr(client, "_manage_vna_service", True)
    events = []
    from eigsep_observing import vna_service

    monkeypatch.setattr(vna_service, "start", lambda: events.append("start"))
    monkeypatch.setattr(vna_service, "stop", lambda: events.append("stop"))

    def boom(ip, port, **k):
        raise TimeoutError("not ready")

    monkeypatch.setattr(vna_service, "wait_ready", boom)
    try:
        with pytest.raises(TimeoutError):
            client.vna_open()
        assert events == ["start", "stop"]  # service stopped on failure
        assert client._vna_depth == 0
    finally:
        client.stop()


def test_vna_open_resets_vna_on_build_failure(
    transport, dummy_cfg, monkeypatch
):
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    client = DummyPandaClient(transport, cfg=cfg)
    monkeypatch.setattr(client, "_manage_vna_service", True)
    from eigsep_observing import vna_service

    events = []
    monkeypatch.setattr(vna_service, "start", lambda: events.append("start"))
    monkeypatch.setattr(vna_service, "stop", lambda: events.append("stop"))
    monkeypatch.setattr(vna_service, "wait_ready", lambda ip, port, **k: None)

    closed = {"n": 0}

    class FakeSock:
        def close(self):
            closed["n"] += 1

    class HalfVNA:
        s = FakeSock()

    def failing_init():
        # mimic init_VNA assigning self.vna then setup() raising
        client.vna = HalfVNA()
        raise RuntimeError("setup failed")

    monkeypatch.setattr(client, "init_VNA", failing_init)
    try:
        with pytest.raises(RuntimeError, match="setup failed"):
            client.vna_open()
        assert client.vna is None  # not leaked
        assert closed["n"] == 1  # socket closed
        assert events == ["start", "stop"]  # service started then stopped
        assert client._vna_depth == 0
    finally:
        client.stop()


def test_vna_loop_measures_then_stops(transport, dummy_cfg):
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 0.05
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        from eigsep_observing.keys import VNA_STREAM

        # Stop after the first iteration's measurements land.
        def stopper():
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if client.transport.r.xlen(VNA_STREAM) >= 2:
                    break
                time.sleep(0.01)
            client.stop_client.set()

        t = threading.Thread(target=stopper, daemon=True)
        t.start()
        client.vna_loop()  # returns once stop_client is set
        t.join(timeout=5)
        # ant + rec bundles were published in at least one session.
        assert client.transport.r.xlen(VNA_STREAM) >= 2
        # Session torn down on loop exit.
        assert client.vna is None
    finally:
        client.stop()


def test_vna_loop_survives_session_open_failure(
    transport, dummy_cfg, monkeypatch, caplog
):
    """A failed vna_open (service won't start) must not kill vna_loop:
    it logs and continues to the next iteration."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 0.01
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        from eigsep_observing.keys import VNA_STREAM

        calls = {"n": 0}
        orig_open = client.vna_open

        def flaky_open():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("service start failed")
            return orig_open()

        monkeypatch.setattr(client, "vna_open", flaky_open)

        def stopper():
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if client.transport.r.xlen(VNA_STREAM) >= 2:
                    break
                time.sleep(0.01)
            client.stop_client.set()

        caplog.set_level(logging.ERROR, logger="eigsep_observing.client")
        t = threading.Thread(target=stopper, daemon=True)
        t.start()
        client.vna_loop()
        t.join(timeout=5)

        assert calls["n"] >= 2  # first open failed, a later one succeeded
        assert any(
            "VNA session failed" in r.getMessage() for r in caplog.records
        )
        assert client.vna is None
    finally:
        client.stop()


def test_run_calibration_sequence_uses_session(transport, dummy_cfg):
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["switch_schedule"] = {}  # skip dwell phase; test the VNA block
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        from eigsep_observing.keys import VNA_STREAM

        assert client.run_calibration_sequence() is True
        assert client.transport.r.xlen(VNA_STREAM) >= 2  # ant + rec
        assert client.vna is None  # session closed after the block
    finally:
        client.stop()
