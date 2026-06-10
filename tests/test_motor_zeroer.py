"""Tests for ``MotorZeroer`` driven against the dummy PicoManager."""

import time

import pytest

from eigsep_observing import MotorZeroer
from eigsep_observing.motor_zeroer import _CAL_MOTOR, _format_pos
from eigsep_redis.keys import METADATA_HASH


_KEY_ENTER = ord("\n")


def _zeroer(transport):
    return MotorZeroer(transport)


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


def test_status_text_waiting_when_no_metadata(client):
    zeroer = _zeroer(client.transport)
    # Clear the motor metadata row so the reader raises KeyError.
    client.transport.r.hdel(METADATA_HASH, "motor")
    client.transport.r.hdel(METADATA_HASH, "motor_ts")
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
    retry once after a short sleep."""
    zeroer = _zeroer(client.transport)
    calls = {"n": 0}
    real = zeroer._proxy.send_command

    def flaky(action, **kw):
        calls["n"] += 1
        if action == "az_move_deg" and calls["n"] == 1:
            raise RuntimeError("No status from motor yet")
        return real(action, **kw)

    monkeypatch.setattr(zeroer._proxy, "send_command", flaky)
    # No exception means the retry worked.
    zeroer.jog_az(1.0)
    assert calls["n"] == 2


def test_jog_propagates_unrelated_runtime_error(client, monkeypatch):
    zeroer = _zeroer(client.transport)

    def boom(*_a, **_k):
        raise RuntimeError("unrelated firmware error")

    monkeypatch.setattr(zeroer._proxy, "send_command", boom)
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
