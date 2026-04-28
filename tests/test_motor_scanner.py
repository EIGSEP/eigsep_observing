"""Tests for ``MotorScanner`` driven against the dummy PicoManager.

The ``client`` fixture starts an in-process ``PicoManager`` with a
``DummyPicoMotor`` whose ``MotorEmulator`` advances deterministically
on every tick (``EMULATOR_CADENCE_MS=50`` ms, up to 60 pulses per
tick). All tests use small degree ranges so each move completes in a
few ticks.
"""

import threading
import time

import numpy as np
import pytest

from eigsep_observing import MotionSwitchCoordinator, MotorScanner


SMALL_RANGE = np.array([-1.0, 0.0, 1.0])
LONG_TIMEOUT = 5.0


def _scanner(transport, *, coord=None):
    return MotorScanner(
        transport,
        poll_interval_s=0.02,
        stall_timeout_s=LONG_TIMEOUT,
        coord=coord,
    )


def test_set_delay_forwards_kwargs(client):
    scanner = _scanner(client.transport)
    scanner.set_delay(az_up_delay_us=1234)
    motor = client._manager.picos["motor"]
    # The firmware emulator runs on its own thread and processes commands
    # asynchronously — wait for the delay to propagate into the stepper
    # state rather than racing the single-tick latency.
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if motor._emulator.azimuth.up_delay_us == 1234:
            break
        time.sleep(0.02)
    assert motor._emulator.azimuth.up_delay_us == 1234


def test_scan_hits_home_before_and_after(client):
    scanner = _scanner(client.transport)
    scanner.scan(
        az_range_deg=SMALL_RANGE,
        el_range_deg=SMALL_RANGE,
        repeat_count=1,
    )
    motor = client._manager.picos["motor"]
    assert motor._emulator.azimuth.target_pos == 0
    assert motor._emulator.elevation.target_pos == 0
    assert motor._emulator.azimuth.position == 0
    assert motor._emulator.elevation.position == 0


def test_scan_covers_grid_with_pause(client):
    """With ``pause_s`` set, axis2 is stepped through every grid value."""
    scanner = _scanner(client.transport)
    motor = client._manager.picos["motor"]
    observed_az_targets = []

    real_wait = scanner._wait_for_stop

    def recording_wait(*args, **kwargs):
        observed_az_targets.append(motor._emulator.azimuth.target_pos)
        return real_wait(*args, **kwargs)

    scanner._wait_for_stop = recording_wait
    scanner.scan(
        az_range_deg=SMALL_RANGE,
        el_range_deg=SMALL_RANGE,
        repeat_count=1,
        pause_s=0.0,
    )
    expected_steps = {motor.deg_to_steps(float(v)) for v in SMALL_RANGE}
    assert expected_steps.issubset(set(observed_az_targets))


def test_scan_stop_event_returns_early(client):
    """Setting the stop event mid-scan breaks out of the loop before
    ``repeat_count`` is reached and halts the motor."""
    scanner = _scanner(client.transport)
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
    scanner.scan(
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
    scanner = MotorScanner(
        client.transport,
        poll_interval_s=0.02,
        stall_timeout_s=0.3,
    )
    motor = client._manager.picos["motor"]
    # Wait for the reader thread to publish at least one status before
    # issuing commands — the firmware's wait_for_start calls is_moving
    # which requires a populated last_status.
    assert _wait_until_motor_status_available(scanner)
    # Command a long move so the emulator is mid-traversal.
    scanner._proxy.send_command("az_target_deg", target_deg=100.0)
    # Wait for Redis metadata to reflect the new non-zero target.
    assert _wait_until_metadata_target_non_zero(scanner)
    # Freeze the emulator mid-move — pos stays below target forever.
    motor._emulator.stop()
    with pytest.raises(TimeoutError):
        scanner._wait_for_stop(timeout=0.3)


def _wait_until_motor_status_available(scanner, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if scanner._motor_status() is not None:
            return True
        time.sleep(0.02)
    return False


def _wait_until_metadata_target_non_zero(scanner, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = scanner._motor_status()
        if status and status.get("az_target_pos"):
            return True
        time.sleep(0.02)
    return False


def test_wait_for_stop_returns_when_aligned(client):
    """Reach home first so az_pos == az_target_pos; _wait_for_stop
    should return immediately without raising."""
    scanner = _scanner(client.transport)
    scanner.home()
    # Now positions are aligned at zero; another wait should no-op.
    scanner._wait_for_stop(timeout=0.5)


def test_halt_swallows_timeout(client, monkeypatch):
    """halt() must not propagate proxy failures — callers lean on it
    from finally blocks and interrupt handlers."""
    scanner = _scanner(client.transport)

    def boom(*_a, **_k):
        raise TimeoutError("simulated")

    monkeypatch.setattr(scanner._proxy, "send_command", boom)
    scanner.halt()  # should log + swallow


def test_scan_el_first_swaps_axes(client):
    """el_first=True makes azimuth the outer loop. The emulator's
    ``az.target_pos`` should plateau while el steps through the inner
    range at each az value."""
    scanner = _scanner(client.transport)
    motor = client._manager.picos["motor"]
    observed_pairs = []

    real_wait = scanner._wait_for_stop

    def recording_wait(*args, **kwargs):
        observed_pairs.append(
            (
                motor._emulator.azimuth.target_pos,
                motor._emulator.elevation.target_pos,
            )
        )
        return real_wait(*args, **kwargs)

    scanner._wait_for_stop = recording_wait
    scanner.scan(
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
    """Standalone ``MotorScanner`` (no ``coord=`` kwarg) must build an
    internal coordinator with ``serialize=False``. Otherwise existing
    callers (``scripts/motor_control.py``, the ``motor_loop`` path
    when ``serialize_motion_and_switching`` is unset) would silently
    start serializing against switching.
    """
    scanner = _scanner(client.transport)
    assert scanner.coord.serialize is False


def test_move_to_az_only(client):
    """``move_to(az_deg=...)`` issues only the az command and waits."""
    scanner = _scanner(client.transport)
    motor = client._manager.picos["motor"]
    expected = motor.deg_to_steps(1.0)
    scanner.move_to(az_deg=1.0)
    assert motor._emulator.azimuth.target_pos == expected
    assert motor._emulator.elevation.target_pos == 0


def test_move_to_az_and_el(client):
    """Both axes drive sequentially; final positions match targets."""
    scanner = _scanner(client.transport)
    motor = client._manager.picos["motor"]
    az_expected = motor.deg_to_steps(1.0)
    el_expected = motor.deg_to_steps(-1.0)
    scanner.move_to(az_deg=1.0, el_deg=-1.0)
    assert motor._emulator.azimuth.target_pos == az_expected
    assert motor._emulator.elevation.target_pos == el_expected


def test_move_to_no_args_is_noop(client):
    """``move_to()`` with neither axis supplied does nothing."""
    scanner = _scanner(client.transport)
    motor = client._manager.picos["motor"]
    scanner.move_to()
    assert motor._emulator.azimuth.target_pos == 0
    assert motor._emulator.elevation.target_pos == 0


def test_move_to_axis_order_drives_el_first(client):
    """``axis_order=("el","az")`` drives el before az. The target
    plateau pattern proves the order: while el is still settling, az
    has not yet been issued."""
    scanner = _scanner(client.transport)
    seen_targets = []
    real_send = scanner._proxy.send_command

    def recording_send(action, **kwargs):
        seen_targets.append(action)
        return real_send(action, **kwargs)

    scanner._proxy.send_command = recording_send
    scanner.move_to(az_deg=1.0, el_deg=-1.0, axis_order=("el", "az"))
    moves = [a for a in seen_targets if "_target_deg" in a]
    assert moves == ["el_target_deg", "az_target_deg"]


def test_move_to_acquires_coord_when_serialized(client):
    """With ``serialize=True`` a competing switch_section blocks while
    a ``move_to`` is in flight. With ``serialize=False`` it does not.
    Drives the move from a worker thread so the test thread can probe
    the lock without re-entering it.
    """
    coord = MotionSwitchCoordinator(threading.RLock(), serialize=True)
    scanner = _scanner(client.transport, coord=coord)
    motor = client._manager.picos["motor"]

    move_started = threading.Event()
    move_done = threading.Event()
    real_wait = scanner._wait_for_stop
    release_wait = threading.Event()

    def gated_wait(*args, **kwargs):
        # Hold inside the motion_section so the test can attempt a
        # switch_section acquire while the move is "in flight".
        move_started.set()
        release_wait.wait(timeout=2.0)
        return real_wait(*args, **kwargs)

    scanner._wait_for_stop = gated_wait

    def runner():
        scanner.move_to(az_deg=1.0)
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
    assert motor._emulator.azimuth.target_pos == motor.deg_to_steps(1.0)


def test_move_to_does_not_block_switch_when_serialize_off(client):
    """With ``serialize=False``, a switch_section acquires immediately
    while a move is in flight — the byte-for-byte preserved
    pre-coordinator behavior of ``motor_loop``.
    """
    coord = MotionSwitchCoordinator(threading.RLock(), serialize=False)
    scanner = _scanner(client.transport, coord=coord)

    move_started = threading.Event()
    move_done = threading.Event()
    real_wait = scanner._wait_for_stop
    release_wait = threading.Event()

    def gated_wait(*args, **kwargs):
        move_started.set()
        release_wait.wait(timeout=2.0)
        return real_wait(*args, **kwargs)

    scanner._wait_for_stop = gated_wait

    def runner():
        scanner.move_to(az_deg=1.0)
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


def test_scan_uses_send_and_wait_helper(client):
    """Sanity-check the refactor: scan() calls ``_send_and_wait``
    rather than the inline send + wait pattern. A regression that
    drops the helper would silently bypass per-move serialization.
    """
    scanner = _scanner(client.transport)
    calls = []
    real = scanner._send_and_wait

    def recording(action, *, label, timeout=None, **kwargs):
        calls.append((action, label))
        return real(action, label=label, timeout=timeout, **kwargs)

    scanner._send_and_wait = recording
    scanner.scan(
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
