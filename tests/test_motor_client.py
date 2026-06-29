"""Tests for ``MotorClient`` driven against the dummy PicoManager.

The ``client`` fixture starts an in-process ``PicoManager`` with a
``DummyPicoMotor`` whose ``MotorEmulator`` advances deterministically
on every tick (``EMULATOR_CADENCE_MS=50`` ms, up to 60 pulses per
tick). All tests use small degree ranges so each move completes in a
few ticks.
"""

import threading
import time
from unittest.mock import patch

import numpy as np
import pytest

from eigsep_observing import (
    MotionSwitchCoordinator,
    MotorClient,
    MotorLimitError,
)
from eigsep_observing.motor_cal import cal_motor
from eigsep_redis.testing import DummyTransport


SMALL_RANGE = np.array([-1.0, 0.0, 1.0])
LONG_TIMEOUT = 5.0


def _motor(transport, *, coord=None):
    return MotorClient(
        transport,
        poll_interval_s=0.02,
        stall_timeout_s=LONG_TIMEOUT,
        # A true no-op move (e.g. homing an axis already at step 0, or a
        # serpentine turnaround that repeats a target) waits out the
        # start window before concluding nothing moved. Keep it short so
        # the emulator-backed scan tests don't pay 1 s per no-op.
        start_timeout_s=0.3,
        coord=coord,
    )


def test_set_delay_forwards_kwargs(client):
    motor = _motor(client.transport)
    motor.set_delay(az_up_delay_us=1234)
    motor_pico = client._manager.picos["motor"]
    # The firmware emulator runs on its own thread and processes commands
    # asynchronously — wait for the delay to propagate into the stepper
    # state rather than racing the single-tick latency.
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if motor_pico._emulator.azimuth.up_delay_us == 1234:
            break
        time.sleep(0.02)
    assert motor_pico._emulator.azimuth.up_delay_us == 1234


def test_scan_hits_home_before_and_after(client):
    motor = _motor(client.transport)
    motor.scan(
        az_range_deg=SMALL_RANGE,
        el_range_deg=SMALL_RANGE,
        repeat_count=1,
    )
    motor_pico = client._manager.picos["motor"]
    assert motor_pico._emulator.azimuth.target_pos == 0
    assert motor_pico._emulator.elevation.target_pos == 0
    assert motor_pico._emulator.azimuth.position == 0
    assert motor_pico._emulator.elevation.position == 0


def test_scan_covers_grid_with_pause(client):
    """With ``pause_s`` set, axis2 is stepped through every grid value."""
    motor = _motor(client.transport)
    motor_pico = client._manager.picos["motor"]
    observed_az_targets = []

    real_wait = motor._wait_for_stop

    def recording_wait(*args, **kwargs):
        observed_az_targets.append(motor_pico._emulator.azimuth.target_pos)
        return real_wait(*args, **kwargs)

    motor._wait_for_stop = recording_wait
    motor.scan(
        az_range_deg=SMALL_RANGE,
        el_range_deg=SMALL_RANGE,
        repeat_count=1,
        pause_s=0.0,
    )
    expected_steps = {motor_pico.deg_to_steps(float(v)) for v in SMALL_RANGE}
    assert expected_steps.issubset(set(observed_az_targets))


def test_scan_stop_event_returns_early(client):
    """Setting the stop event mid-scan breaks out of the loop before
    ``repeat_count`` is reached and halts the motor."""
    motor = _motor(client.transport)
    stop_event = threading.Event()

    # Long pause + huge repeat_count means the scan will run forever
    # unless the stop_event fires. Trip the event from a background
    # thread shortly after the scan begins.
    def tripper():
        time.sleep(0.3)
        stop_event.set()

    t = threading.Thread(target=tripper, daemon=True)
    t.start()

    started = time.monotonic()
    motor.scan(
        az_range_deg=SMALL_RANGE,
        el_range_deg=SMALL_RANGE,
        repeat_count=10_000,
        pause_s=1.0,
        stop_event=stop_event,
    )
    elapsed = time.monotonic() - started
    t.join()
    # Sanity: we exited well before a full pass of ``10_000`` passes
    # at 1 s pauses would complete.
    assert elapsed < 5.0


def test_wait_for_stop_timeout(client):
    """If the emulator stops advancing while a target is still
    outstanding, ``_wait_for_stop`` raises ``TimeoutError``."""
    motor = MotorClient(
        client.transport,
        poll_interval_s=0.02,
        stall_timeout_s=0.3,
    )
    motor_pico = client._manager.picos["motor"]
    # Wait for the reader thread to publish at least one status before
    # issuing commands — the firmware's wait_for_start calls is_moving
    # which requires a populated last_status.
    assert _wait_until_motor_status_available(motor)
    # Command a long move so the emulator is mid-traversal.
    motor._proxy.send_command("az_target_deg", target_deg=100.0)
    # Wait for Redis metadata to reflect the new non-zero target.
    assert _wait_until_metadata_target_non_zero(motor)
    # Freeze the emulator mid-move — pos stays below target forever.
    motor_pico._emulator.stop()
    with pytest.raises(TimeoutError):
        motor._wait_for_stop(timeout=0.3)


def _wait_until_motor_status_available(motor, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if motor._motor_status() is not None:
            return True
        time.sleep(0.02)
    return False


def _wait_until_metadata_target_non_zero(motor, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = motor._motor_status()
        if status and status.get("az_target_pos"):
            return True
        time.sleep(0.02)
    return False


def test_wait_for_stop_returns_when_aligned(client):
    """Reach home first so az_pos == az_target_pos; _wait_for_stop
    should return immediately without raising."""
    motor = _motor(client.transport)
    motor.home()
    # Now positions are aligned at zero; another wait should no-op.
    motor._wait_for_stop(timeout=0.5)


def test_halt_swallows_timeout(client, monkeypatch):
    """halt() must not propagate proxy failures — callers lean on it
    from finally blocks and interrupt handlers."""
    motor = _motor(client.transport)

    def boom(*_a, **_k):
        raise TimeoutError("simulated")

    monkeypatch.setattr(motor._proxy, "send_command", boom)
    motor.halt()  # should log + swallow


def test_scan_el_first_swaps_axes(client):
    """el_first=True makes azimuth the outer loop. The emulator's
    ``az.target_pos`` should plateau while el steps through the inner
    range at each az value."""
    motor = _motor(client.transport)
    motor_pico = client._manager.picos["motor"]
    observed_pairs = []

    real_wait = motor._wait_for_stop

    def recording_wait(*args, **kwargs):
        observed_pairs.append(
            (
                motor_pico._emulator.azimuth.target_pos,
                motor_pico._emulator.elevation.target_pos,
            )
        )
        return real_wait(*args, **kwargs)

    motor._wait_for_stop = recording_wait
    motor.scan(
        az_range_deg=SMALL_RANGE,
        el_range_deg=SMALL_RANGE,
        el_first=True,
        repeat_count=1,
        pause_s=0.0,
    )
    # With el_first=True: for each az value, el sweeps the full range.
    # The list contains (az_target, el_target) pairs recorded *before*
    # each wait. At least one consecutive pair should share the same
    # az while el changes.
    saw_az_plateau = False
    for (az1, el1), (az2, el2) in zip(observed_pairs, observed_pairs[1:]):
        if az1 == az2 and el1 != el2:
            saw_az_plateau = True
            break
    assert saw_az_plateau


def test_default_coord_is_serialize_off(client):
    """Standalone ``MotorClient`` (no ``coord=`` kwarg) must build an
    internal coordinator with ``serialize=False``. Otherwise existing
    callers (``scripts/motor_scan.py``, the ``motor_loop`` path
    when ``serialize_motion_and_switching`` is unset) would silently
    start serializing against switching.
    """
    motor = _motor(client.transport)
    assert motor.coord.serialize is False


def test_move_to_az_only(client):
    """``move_to(az_deg=...)`` issues only the az command and waits."""
    motor = _motor(client.transport)
    motor_pico = client._manager.picos["motor"]
    expected = motor_pico.deg_to_steps(1.0)
    motor.move_to(az_deg=1.0)
    assert motor_pico._emulator.azimuth.target_pos == expected
    assert motor_pico._emulator.elevation.target_pos == 0


def test_move_to_az_and_el(client):
    """Both axes drive sequentially; final positions match targets."""
    motor = _motor(client.transport)
    motor_pico = client._manager.picos["motor"]
    az_expected = motor_pico.deg_to_steps(1.0)
    el_expected = motor_pico.deg_to_steps(-1.0)
    motor.move_to(az_deg=1.0, el_deg=-1.0)
    assert motor_pico._emulator.azimuth.target_pos == az_expected
    assert motor_pico._emulator.elevation.target_pos == el_expected


def test_move_to_no_args_is_noop(client):
    """``move_to()`` with neither axis supplied does nothing."""
    motor = _motor(client.transport)
    motor_pico = client._manager.picos["motor"]
    motor.move_to()
    assert motor_pico._emulator.azimuth.target_pos == 0
    assert motor_pico._emulator.elevation.target_pos == 0


def test_move_to_axis_order_drives_el_first(client):
    """``axis_order=("el","az")`` drives el before az. The target
    plateau pattern proves the order: while el is still settling, az
    has not yet been issued."""
    motor = _motor(client.transport)
    seen_targets = []
    real_send = motor._proxy.send_command

    def recording_send(action, **kwargs):
        seen_targets.append(action)
        return real_send(action, **kwargs)

    motor._proxy.send_command = recording_send
    motor.move_to(az_deg=1.0, el_deg=-1.0, axis_order=("el", "az"))
    moves = [a for a in seen_targets if "_target_deg" in a]
    assert moves == ["el_target_deg", "az_target_deg"]


def test_move_to_acquires_coord_when_serialized(client):
    """With ``serialize=True`` a competing switch_section blocks while
    a ``move_to`` is in flight. With ``serialize=False`` it does not.
    Drives the move from a worker thread so the test thread can probe
    the lock without re-entering it.
    """
    coord = MotionSwitchCoordinator(threading.RLock(), serialize=True)
    motor = _motor(client.transport, coord=coord)
    motor_pico = client._manager.picos["motor"]

    move_started = threading.Event()
    move_done = threading.Event()
    real_wait = motor._wait_for_stop
    release_wait = threading.Event()

    def gated_wait(*args, **kwargs):
        # Hold inside the motion_section so the test can attempt a
        # switch_section acquire while the move is "in flight".
        move_started.set()
        release_wait.wait(timeout=2.0)
        return real_wait(*args, **kwargs)

    motor._wait_for_stop = gated_wait

    def runner():
        motor.move_to(az_deg=1.0)
        move_done.set()

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    assert move_started.wait(timeout=1.0)

    acquired = threading.Event()

    def probe():
        with coord.switch_section():
            acquired.set()

    p = threading.Thread(target=probe, daemon=True)
    p.start()
    assert not acquired.wait(timeout=0.2), (
        "switch_section acquired during a serialized move_to"
    )
    release_wait.set()
    assert acquired.wait(timeout=2.0)
    assert move_done.wait(timeout=2.0)
    t.join(timeout=1.0)
    p.join(timeout=1.0)
    assert motor_pico._emulator.azimuth.target_pos == motor_pico.deg_to_steps(
        1.0
    )


def test_move_to_does_not_block_switch_when_serialize_off(client):
    """With ``serialize=False``, a switch_section acquires immediately
    while a move is in flight — the byte-for-byte preserved
    pre-coordinator behavior of ``motor_loop``.
    """
    coord = MotionSwitchCoordinator(threading.RLock(), serialize=False)
    motor = _motor(client.transport, coord=coord)

    move_started = threading.Event()
    move_done = threading.Event()
    real_wait = motor._wait_for_stop
    release_wait = threading.Event()

    def gated_wait(*args, **kwargs):
        move_started.set()
        release_wait.wait(timeout=2.0)
        return real_wait(*args, **kwargs)

    motor._wait_for_stop = gated_wait

    def runner():
        motor.move_to(az_deg=1.0)
        move_done.set()

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    assert move_started.wait(timeout=1.0)

    with coord.switch_section():
        # Acquired immediately even though a "move" is mid-wait.
        pass

    release_wait.set()
    assert move_done.wait(timeout=2.0)
    t.join(timeout=1.0)


def test_home_stop_event_bails_without_raising(client):
    """A set ``stop_event`` makes ``home()`` halt and return promptly
    even when the motor can't reach step 0 — mirrors ``scan()``'s
    cooperative cancellation. Without the event this same frozen-emulator
    state raises ``TimeoutError`` (see ``test_wait_for_stop_timeout``);
    with it, ``home()`` must return cleanly so the manual UI's background
    home thread can be cancelled mid-move.
    """
    motor = MotorClient(
        client.transport, poll_interval_s=0.02, stall_timeout_s=5.0
    )
    assert _wait_until_motor_status_available(motor)
    # Drive az off zero, then freeze mid-move so home()'s subsequent
    # drive to step 0 can never complete on its own.
    motor._proxy.send_command("az_target_deg", target_deg=100.0)
    assert _wait_until_metadata_target_non_zero(motor)
    client._manager.picos["motor"]._emulator.stop()

    stop_event = threading.Event()
    stop_event.set()

    started = time.monotonic()
    motor.home(stop_event=stop_event)  # must return, not raise or block
    assert time.monotonic() - started < 1.0


def test_send_and_wait_waits_for_move_to_start(transport):
    """Regression for the home double-move bug.

    After issuing a move, ``_send_and_wait`` must keep polling until the
    commanded axis is actually *moving* before it concludes the move has
    *stopped*. The failure mode it guards is the snapshot-propagation
    race: the just-sent command has not yet been reflected in the
    metadata hash, so the first stop-poll reads a stale at-rest snapshot
    (``pos == target``) and returns immediately — letting ``home`` fire
    the el command while az is still moving.

    The injected sequence mirrors real motor status fields (``az_pos`` /
    ``az_target_pos`` / ``el_pos`` / ``el_target_pos``): a stale at-rest
    frame, then the move registering and progressing, then arrival.
    """
    motor = MotorClient(transport, poll_interval_s=0.001, stall_timeout_s=2.0)
    seq = [
        # [0] command not yet reflected — looks at-rest (the trap)
        {"az_pos": 0, "az_target_pos": 0, "el_pos": 0, "el_target_pos": 0},
        # [1] target now registered, az under way
        {"az_pos": 0, "az_target_pos": 100, "el_pos": 0, "el_target_pos": 0},
        # [2] still moving
        {"az_pos": 60, "az_target_pos": 100, "el_pos": 0, "el_target_pos": 0},
        # [3] arrived
        {"az_pos": 100, "az_target_pos": 100, "el_pos": 0, "el_target_pos": 0},
    ]
    idx = {"n": 0}

    def fake_status():
        i = min(idx["n"], len(seq) - 1)
        idx["n"] += 1
        return seq[i]

    with (
        patch.object(motor._proxy, "send_command", return_value=None),
        patch.object(motor, "_motor_status", side_effect=fake_status),
    ):
        motor._send_and_wait(
            "az_target_steps", label="home az", target_steps=100
        )
    # If it had returned on the stale at-rest frame [0] it would have read
    # exactly once. Reaching arrival proves it waited through start->stop.
    assert idx["n"] >= 4


def test_send_and_wait_noop_returns_after_start_timeout(transport):
    """A move that is already at its target (no motion ever observed)
    must return after ``start_timeout_s`` rather than racing straight
    through the wait. This is the genuine no-op path (e.g. ``home`` when
    an axis is already at step 0); it must not hang, and must not return
    instantly the way the old stop-only wait did.
    """
    motor = MotorClient(transport, poll_interval_s=0.005, stall_timeout_s=2.0)
    motor.start_timeout_s = 0.15
    at_rest = {
        "az_pos": 0,
        "az_target_pos": 0,
        "el_pos": 0,
        "el_target_pos": 0,
    }
    with (
        patch.object(motor._proxy, "send_command", return_value=None),
        patch.object(motor, "_motor_status", return_value=at_rest),
    ):
        started = time.monotonic()
        motor._send_and_wait("az_target_steps", label="noop", target_steps=0)
        elapsed = time.monotonic() - started
    assert elapsed >= 0.15
    assert elapsed < 1.0


def test_scan_uses_send_and_wait_helper(client):
    """Sanity-check the refactor: scan() calls ``_send_and_wait``
    rather than the inline send + wait pattern. A regression that
    drops the helper would silently bypass per-move serialization.
    """
    motor = _motor(client.transport)
    calls = []
    real = motor._send_and_wait

    def recording(action, *, label, timeout=None, **kwargs):
        calls.append((action, label))
        return real(action, label=label, timeout=timeout, **kwargs)

    motor._send_and_wait = recording
    motor.scan(
        az_range_deg=SMALL_RANGE,
        el_range_deg=SMALL_RANGE,
        repeat_count=1,
        pause_s=0.0,
    )
    actions = {a for a, _ in calls}
    assert "az_target_deg" in actions
    assert "el_target_deg" in actions
    # home() also routes through _send_and_wait (via az_target_steps /
    # el_target_steps), so per-move serialization covers the home calls.
    assert any(a == "az_target_steps" for a, _ in calls)
    assert any(a == "el_target_steps" for a, _ in calls)


# ---------------------------------------------------------------------------
# Task-1 additions: MotorLimitError + per-axis limit kwargs + shared cal
# ---------------------------------------------------------------------------


def _client(**kw):
    return MotorClient(DummyTransport(), **kw)


def test_motor_limit_error_is_valueerror():
    assert issubclass(MotorLimitError, ValueError)


def test_default_limits_are_symmetric_180():
    mc = _client()
    assert mc.az_limits_deg == (-180.0, 180.0)
    assert mc.el_limits_deg == (-180.0, 180.0)
    assert mc.pot_az_v_limits is None
    assert mc.imu_el_limits_deg is None


def test_limits_overridable():
    mc = _client(el_limits_deg=(-30.0, 30.0), pot_az_v_limits=(0.2, 3.1))
    assert mc.el_limits_deg == (-30.0, 30.0)
    assert mc.pot_az_v_limits == (0.2, 3.1)


def test_cal_motor_roundtrips_steps_deg():
    cal = cal_motor()
    steps = cal.deg_to_steps(90.0)
    assert isinstance(steps, int)
    assert abs(cal.steps_to_deg(steps) - 90.0) < 1.0


# ---------------------------------------------------------------------------
# Task-2 additions: commanded-target guard in _send_and_wait
# ---------------------------------------------------------------------------


def _client_seeded(az_target=0, el_target=0, **kw):
    """Client whose snapshot reports a known at-rest target on both axes."""
    mc = MotorClient(DummyTransport(), **kw)
    status = {
        "az_pos": az_target,
        "az_target_pos": az_target,
        "el_pos": el_target,
        "el_target_pos": el_target,
    }
    mc._motor_status = lambda: status
    return mc


def test_absolute_target_deg_beyond_limit_raises_before_send():
    mc = _client_seeded()
    with patch.object(mc._proxy, "send_command") as send:
        with pytest.raises(MotorLimitError):
            mc.move_to(az_deg=200.0)
        send.assert_not_called()


def test_absolute_target_deg_within_limit_sends():
    mc = _client_seeded()
    # short-circuit the wait so the test only exercises the guard + send
    mc._wait_for_start = lambda *a, **k: None
    mc._wait_for_stop = lambda *a, **k: None
    with patch.object(mc._proxy, "send_command") as send:
        mc.move_to(az_deg=170.0)
        send.assert_called_once()


def test_relative_jog_past_limit_uses_absolute_result():
    # current target 175 deg -> a +10 jog lands at 185 -> blocked
    cal = MotorClient(DummyTransport())._cal
    steps_175 = cal.deg_to_steps(175.0)
    mc = _client_seeded(az_target=steps_175)
    with patch.object(mc._proxy, "send_command") as send:
        with pytest.raises(MotorLimitError):
            mc.jog_az(10.0)
        send.assert_not_called()


def test_el_window_narrowed_by_config():
    mc = _client_seeded(el_limits_deg=(-30.0, 30.0))
    with patch.object(mc._proxy, "send_command") as send:
        with pytest.raises(MotorLimitError):
            mc.move_to(el_deg=45.0)
        send.assert_not_called()


def test_home_and_halt_never_blocked():
    mc = _client_seeded(az_target=999999, el_target=999999)  # absurd count
    mc._wait_for_start = lambda *a, **k: None
    mc._wait_for_stop = lambda *a, **k: None
    with patch.object(mc._proxy, "send_command") as send:
        mc.home()  # target steps 0 -> 0 deg, always in window
        send.assert_called()
