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

from eigsep_observing import MotorScanner


SMALL_RANGE = np.array([-1.0, 0.0, 1.0])
LONG_TIMEOUT = 5.0


def _scanner(transport):
    return MotorScanner(
        transport,
        poll_interval_s=0.02,
        stall_timeout_s=LONG_TIMEOUT,
    )


def test_set_delay_forwards_kwargs(client):
    scanner = _scanner(client.transport)
    scanner.set_delay(az_up_delay_us=1234)
    motor = client._manager.picos["motor"]
    # The firmware emulator's stepper state carries the per-axis delays.
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
