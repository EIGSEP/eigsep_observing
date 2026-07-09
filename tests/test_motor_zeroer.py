"""Tests for ``MotorZeroer`` driven against the dummy PicoManager."""

import time

import pytest

from eigsep_redis.testing import DummyTransport

from eigsep_observing import MotorZeroer
from eigsep_observing.motor_client import MotorClient, MotorLimitError
from eigsep_observing.motor_zeroer import _CAL_MOTOR, _format_pos


_KEY_ENTER = ord("\n")


def _zeroer(transport):
    return MotorZeroer(transport)


class _BlockingHomeClient:
    """Stand-in ``MotorClient`` whose ``home`` blocks on the stop event.

    Lets a test observe ``is_homing == True`` deterministically (the
    real emulator reaches step 0 too fast to catch the flag) and proves
    the cancel path: ``home`` only returns once the ``stop_event`` is
    set, so ``cancel_home`` unwinding the thread is observable.
    """

    def __init__(self):
        self.home_calls = 0
        self.last_stop_event = None

    def home(self, stop_event=None):
        self.home_calls += 1
        self.last_stop_event = stop_event
        if stop_event is not None:
            stop_event.wait(timeout=2.0)


def _wait_until(pred, timeout=2.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def test_jog_az_advances_target(client):
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    before = motor._emulator.azimuth.target_pos
    zeroer.jog_az(1.0)
    after = motor._emulator.azimuth.target_pos
    assert after - before == motor.deg_to_steps(1.0)


def test_jog_el_advances_target(client):
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    before = motor._emulator.elevation.target_pos
    zeroer.jog_el(-2.0)
    after = motor._emulator.elevation.target_pos
    assert after - before == motor.deg_to_steps(-2.0)


def test_zero_halts_and_resets(client):
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    # First, offset both axes so we can confirm zeroing actually resets them.
    zeroer.jog_az(3.0)
    zeroer.jog_el(3.0)
    assert _wait_until(
        lambda: (
            motor._emulator.azimuth.position
            == motor._emulator.azimuth.target_pos
        ),
        timeout=3.0,
    )
    zeroer.zero()
    # Emulator processes the reset_step_position command on its own
    # thread — wait for the state to converge rather than race it.
    assert _wait_until(
        lambda: (
            motor._emulator.azimuth.position == 0
            and motor._emulator.elevation.position == 0
        ),
        timeout=2.0,
    )
    assert motor._emulator.azimuth.target_pos == 0
    assert motor._emulator.elevation.target_pos == 0


def test_jogs_block_until_move_completes(client):
    """Serialized jogs (the 'up then left moves both' fix).

    Each jog keystroke must block until its move finishes, so a
    cross-axis jog issued on the very next keystroke cannot run while the
    previous axis is still moving.

    The emulator advances 60 steps per ~1 ms tick (only *status* is
    throttled to 50 ms), so a normal jog finishes faster than the
    manager's ``wait_for_start`` returns — which would mask the
    non-blocking bug. Slowing the steppers to one step per tick makes the
    move outlast ``wait_for_start``, so the old fire-and-forget jog would
    return with the emulator still far from target (the fail-first).
    """
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    el = motor._emulator.elevation
    az = motor._emulator.azimuth
    el.max_pulses = 1
    az.max_pulses = 1

    zeroer.handle_key(ord("u"), 10.0)  # jog el up — must block until done
    assert el.target_pos != 0  # precondition: a real move was commanded
    assert el.position == el.target_pos  # blocked until el actually stopped
    el_target = el.target_pos

    zeroer.handle_key(ord("l"), 10.0)  # jog az left — must block until done
    assert az.target_pos != 0
    assert az.position == az.target_pos
    # el never moved during the az jog: no concurrent two-axis motion.
    assert el.target_pos == el_target
    assert el.position == el_target


def test_status_text_waiting_when_no_metadata(client, monkeypatch):
    zeroer = _zeroer(client.transport)

    # The live PicoManager republishes the motor metadata row every
    # ~50ms, so an hdel here races the publisher and loses under load
    # (it re-creates the row ~40-80ms later). Force the reader to report
    # the row absent instead — a missing key is exactly what
    # MetadataSnapshotReader.get surfaces as KeyError, and that is the
    # condition status_text() must handle. The heartbeat stays real, so
    # the connected assertion still exercises genuine manager liveness.
    def _raise_keyerror(keys=None):
        raise KeyError(keys)

    monkeypatch.setattr(zeroer._reader, "get", _raise_keyerror)
    az, el, connected = zeroer.status_text()
    assert (az, el) == ("WAITING", "---")
    # Heartbeat is still alive even if metadata is missing.
    assert connected is True


def test_status_text_reflects_live_metadata(client):
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    zeroer.jog_az(1.0)
    # Wait for the emulator to reach the target and a status packet to
    # land in the metadata snapshot.
    assert _wait_until(
        lambda: (
            zeroer._reader.get("motor").get("az_pos")
            == motor._emulator.azimuth.target_pos
        ),
        timeout=3.0,
    )
    expected_az = zeroer._reader.get("motor")["az_pos"]
    az, _, connected = zeroer.status_text()
    # Detailed rendering behavior belongs in
    # test_format_pos_renders_axis_degrees; here we only verify that
    # status_text reflects the live metadata using the shared formatter.
    assert az == _format_pos(expected_az)
    assert connected is True


def test_jog_retries_on_require_status(client, monkeypatch):
    """If the firmware raises because ``last_status`` is empty, we
    retry once after a short sleep. Jogs now route through the shared
    MotorClient, so the flaky send is patched on its proxy.
    """
    zeroer = _zeroer(client.transport)
    calls = {"n": 0}
    real = zeroer._motor_client._proxy.send_command

    def flaky(action, **kw):
        calls["n"] += 1
        if action == "az_move_deg" and calls["n"] == 1:
            raise RuntimeError("No status from motor yet")
        return real(action, **kw)

    monkeypatch.setattr(zeroer._motor_client._proxy, "send_command", flaky)
    # No exception means the retry worked.
    zeroer.jog_az(1.0)
    assert calls["n"] == 2


def test_jog_propagates_unrelated_runtime_error(client, monkeypatch):
    zeroer = _zeroer(client.transport)

    def boom(*_a, **_k):
        raise RuntimeError("unrelated firmware error")

    monkeypatch.setattr(zeroer._motor_client._proxy, "send_command", boom)
    with pytest.raises(RuntimeError, match="unrelated"):
        zeroer.jog_az(1.0)


def test_handle_key_noop_on_no_input(client):
    zeroer = _zeroer(client.transport)
    new_deg, should_exit, zeroed = zeroer.handle_key(-1, 1.0)
    assert (new_deg, should_exit, zeroed) == (1.0, False, False)


def test_handle_key_q_exits_without_zeroing(client):
    zeroer = _zeroer(client.transport)
    new_deg, should_exit, zeroed = zeroer.handle_key(ord("q"), 1.0)
    assert should_exit is True
    assert zeroed is False


def test_handle_key_plus_minus_adjusts_step(client):
    zeroer = _zeroer(client.transport)
    deg, _, _ = zeroer.handle_key(ord("+"), 1.0)
    assert deg == 2.0
    deg, _, _ = zeroer.handle_key(ord("-"), 2.0)
    assert deg == 1.0
    # Floor at 0.1.
    deg, _, _ = zeroer.handle_key(ord("-"), 0.5)
    assert deg == pytest.approx(0.1)


def test_handle_key_plus_recovers_integer_steps_from_floor(client):
    # After bottoming out at 0.1, "+" must snap back to a clean
    # integer ladder instead of producing 1.1, 2.1, 3.1, ...
    zeroer = _zeroer(client.transport)
    deg, _, _ = zeroer.handle_key(ord("+"), 0.1)
    assert deg == 1.0
    deg, _, _ = zeroer.handle_key(ord("+"), deg)
    assert deg == 2.0


def test_handle_key_enter_arms_confirmation_without_zeroing(client):
    """A single Enter must NOT zero — it arms a confirmation prompt.

    Zeroing redefines scan home, so an accidental Enter (or a new
    operator who doesn't know what Enter does) must not be able to
    trigger it in one keystroke.
    """
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    zeroer.jog_az(2.0)
    assert _wait_until(
        lambda: (
            motor._emulator.azimuth.position
            == motor._emulator.azimuth.target_pos
        ),
        timeout=3.0,
    )
    offset = motor._emulator.azimuth.target_pos
    assert offset != 0  # precondition: we are off-home
    _, should_exit, zeroed = zeroer.handle_key(_KEY_ENTER, 1.0)
    assert zeroer.pending_zero is True
    assert should_exit is False
    assert zeroed is False
    # No reset happened — position is untouched.
    assert motor._emulator.azimuth.target_pos == offset


def test_handle_key_confirm_y_zeroes_and_exits(client):
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    zeroer.jog_az(2.0)
    assert _wait_until(
        lambda: (
            motor._emulator.azimuth.position
            == motor._emulator.azimuth.target_pos
        ),
        timeout=3.0,
    )
    zeroer.handle_key(_KEY_ENTER, 1.0)  # arm
    _, should_exit, zeroed = zeroer.handle_key(ord("y"), 1.0)
    assert should_exit is True
    assert zeroed is True
    assert zeroer.pending_zero is False
    assert _wait_until(
        lambda: motor._emulator.azimuth.position == 0,
        timeout=2.0,
    )
    assert motor._emulator.azimuth.target_pos == 0


def test_handle_key_confirm_uppercase_y_also_zeroes(client):
    zeroer = _zeroer(client.transport)
    zeroer.handle_key(_KEY_ENTER, 1.0)  # arm
    _, should_exit, zeroed = zeroer.handle_key(ord("Y"), 1.0)
    assert should_exit is True
    assert zeroed is True


def test_handle_key_pending_other_key_cancels_without_zeroing(client):
    """While the prompt is armed, any non-'y' key cancels it and is
    otherwise swallowed — it must neither zero nor execute as a jog.
    """
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    zeroer.jog_az(2.0)
    assert _wait_until(
        lambda: (
            motor._emulator.azimuth.position
            == motor._emulator.azimuth.target_pos
        ),
        timeout=3.0,
    )
    offset = motor._emulator.azimuth.target_pos
    zeroer.handle_key(_KEY_ENTER, 1.0)  # arm
    # 'l' is normally a jog-left; while armed it must only cancel.
    _, should_exit, zeroed = zeroer.handle_key(ord("l"), 1.0)
    assert zeroer.pending_zero is False
    assert should_exit is False
    assert zeroed is False
    # Unchanged target proves neither a zero nor a jog fired.
    assert motor._emulator.azimuth.target_pos == offset


def test_handle_key_pending_no_input_preserves_prompt(client):
    """``getch()`` returns -1 every ~100ms when idle; that must keep the
    confirmation prompt armed rather than silently cancelling it.
    """
    zeroer = _zeroer(client.transport)
    zeroer.handle_key(_KEY_ENTER, 1.0)  # arm
    _, should_exit, zeroed = zeroer.handle_key(-1, 1.0)
    assert zeroer.pending_zero is True
    assert should_exit is False
    assert zeroed is False


def test_handle_key_directional_jogs(client):
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    for key, getter, factor in [
        ("u", lambda: motor._emulator.elevation.target_pos, +1),
        ("d", lambda: motor._emulator.elevation.target_pos, -1),
        ("l", lambda: motor._emulator.azimuth.target_pos, +1),
        ("r", lambda: motor._emulator.azimuth.target_pos, -1),
    ]:
        before = getter()
        zeroer.handle_key(ord(key), 1.0)
        after = getter()
        assert after - before == factor * motor.deg_to_steps(1.0)


def test_handle_key_h_drives_to_home(client):
    """Pressing 'h' drives both axes back to step 0 via the real
    ``MotorClient.home`` path (background thread), and clears
    ``is_homing`` when the move completes."""
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    zeroer.jog_az(2.0)
    zeroer.jog_el(2.0)
    assert _wait_until(
        lambda: (
            motor._emulator.azimuth.position
            == motor._emulator.azimuth.target_pos
            and motor._emulator.elevation.position
            == motor._emulator.elevation.target_pos
        ),
        timeout=3.0,
    )
    assert motor._emulator.azimuth.target_pos != 0  # precondition

    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(
        lambda: (
            motor._emulator.azimuth.position == 0
            and motor._emulator.elevation.position == 0
        ),
        timeout=5.0,
    )
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)


def test_handle_key_h_sets_homing_and_any_key_cancels(client):
    """'h' starts a background home (``is_homing`` True); while homing,
    any subsequent key cancels it (stops the motor, unwinds the thread)
    and must NOT execute as a jog."""
    fake = _BlockingHomeClient()
    zeroer = MotorZeroer(client.transport, motor_client=fake)

    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: zeroer.is_homing, timeout=1.0)
    assert fake.home_calls == 1

    motor = client._manager.picos["motor"]
    before = motor._emulator.azimuth.target_pos
    # 'l' is normally jog-left; while homing it must only cancel.
    _, should_exit, zeroed = zeroer.handle_key(ord("l"), 1.0)
    assert should_exit is False
    assert zeroed is False
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)
    assert motor._emulator.azimuth.target_pos == before


def test_handle_key_h_ignored_when_unavailable(client, monkeypatch):
    """When the manager is unreachable, 'h' is a no-op — no home thread
    is spawned."""
    fake = _BlockingHomeClient()
    zeroer = MotorZeroer(client.transport, motor_client=fake)
    monkeypatch.setattr(
        MotorZeroer, "is_available", property(lambda self: False)
    )
    zeroer.handle_key(ord("h"), 1.0)
    assert zeroer.is_homing is False
    assert fake.home_calls == 0


def test_handle_key_noop_during_homing_preserves_state(client):
    """A ``-1`` idle tick (getch timeout) while homing must keep the
    home running rather than silently cancelling it."""
    fake = _BlockingHomeClient()
    zeroer = MotorZeroer(client.transport, motor_client=fake)
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: zeroer.is_homing, timeout=1.0)
    _, should_exit, zeroed = zeroer.handle_key(-1, 1.0)
    assert zeroer.is_homing is True
    assert should_exit is False
    assert zeroed is False
    zeroer.cancel_home()  # cleanup


def test_format_pos_renders_axis_degrees():
    # The user-facing round trip: a 30 deg jog becomes deg_to_steps(30)
    # motor steps, and reading that step count back must display
    # "30.0 deg" — the same calibration the mover uses, not a duplicated
    # copy. Locks the steps<->deg display contract.
    steps = _CAL_MOTOR.deg_to_steps(30.0)
    assert _format_pos(steps) == f"{steps} (30.0 deg)"
    # Positions are published as floats (PicoMotor._motor_redis_handler
    # casts the firmware's int step counts); they round to int steps.
    assert _format_pos(float(steps)) == f"{steps} (30.0 deg)"
    # Non-numeric sentinels (e.g. a missing position key) pass through
    # verbatim so a partial status dict never crashes the UI.
    assert _format_pos("?") == "?"


def test_zeroer_jog_inherits_travel_limit():
    # inject a MotorClient pinned near the +az edge; a further +jog must raise
    transport = DummyTransport()
    cal = MotorClient(transport)._cal
    mc = MotorClient(transport)
    steps = cal.deg_to_steps(179.0)
    mc._motor_status = lambda: {
        "az_pos": steps,
        "az_target_pos": steps,
        "el_pos": 0,
        "el_target_pos": 0,
    }
    zeroer = MotorZeroer(transport, motor_client=mc)
    with pytest.raises(MotorLimitError):
        zeroer.jog_az(5.0)  # 179 + 5 = 184 -> outside ±180


# ---------------------------------------------------------------------------
# Task D2: enforce_limits threading
# ---------------------------------------------------------------------------


def test_zeroer_passes_enforce_limits_to_motor_client():
    z = MotorZeroer(DummyTransport(), enforce_limits=False)
    assert z._motor_client.enforce_limits is False


# ---------------------------------------------------------------------------
# Limit denials must not crash the curses loop (field finding, 2026-07-01)
# ---------------------------------------------------------------------------


class _LimitTrippingHomeClient:
    """Stand-in ``MotorClient`` whose ``home`` trips the sensor fence."""

    def home(self, stop_event=None):
        raise MotorLimitError(
            "pot_az_voltage 2.900 V outside safe window [0.500, 2.500]; "
            "refusing az move."
        )


def test_handle_key_swallows_limit_error_and_sets_notice(client):
    """A jog denied by the travel guard must not escape handle_key —
    the curses loop stays alive — and the denial must be surfaced via
    ``notice`` (console logging is off in the manual scripts, so a
    log-only warning is invisible mid-session)."""
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    before = motor._emulator.azimuth.target_pos
    # 200 deg from home is outside the ±180 default window.
    new_deg, should_exit, zeroed = zeroer.handle_key(ord("l"), 200.0)
    assert (new_deg, should_exit, zeroed) == (200.0, False, False)
    assert "outside safe window" in zeroer.notice
    assert motor._emulator.azimuth.target_pos == before  # move refused


def test_handle_key_successful_jog_clears_notice(client):
    zeroer = _zeroer(client.transport)
    zeroer.handle_key(ord("l"), 200.0)  # denied — notice set
    assert zeroer.notice is not None
    zeroer.handle_key(ord("l"), 1.0)  # in-window jog succeeds
    assert zeroer.notice is None


def test_handle_key_jog_failure_sets_notice(client, monkeypatch):
    """Non-limit jog failures (already swallowed today) must also
    surface on screen, not just in the invisible log file."""
    zeroer = _zeroer(client.transport)

    def boom(*_a, **_k):
        raise RuntimeError("unrelated firmware error")

    monkeypatch.setattr(zeroer._motor_client._proxy, "send_command", boom)
    _, should_exit, _ = zeroer.handle_key(ord("l"), 1.0)
    assert should_exit is False
    assert "unrelated firmware error" in zeroer.notice


def test_start_home_survives_limit_error(client):
    """A sensor-fence trip mid-home must unwind the background thread
    cleanly (no traceback splatted over the curses screen) and surface
    the denial via ``notice``."""
    zeroer = MotorZeroer(
        client.transport, motor_client=_LimitTrippingHomeClient()
    )
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)
    assert "outside safe window" in zeroer.notice
