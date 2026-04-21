"""Tests for ``MotorZeroer`` driven against the dummy PicoManager."""

import time

import pytest

from eigsep_observing import MotorZeroer
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
        lambda: motor._emulator.azimuth.position
        == motor._emulator.azimuth.target_pos,
        timeout=3.0,
    )
    zeroer.zero()
    # Emulator processes the reset_step_position command on its own
    # thread — wait for the state to converge rather than race it.
    assert _wait_until(
        lambda: motor._emulator.azimuth.position == 0
        and motor._emulator.elevation.position == 0,
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
        lambda: zeroer._reader.get("motor").get("az_pos")
        == motor._emulator.azimuth.target_pos,
        timeout=3.0,
    )
    az, _, connected = zeroer.status_text()
    assert az == str(motor._emulator.azimuth.target_pos)
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


def test_handle_key_enter_zeroes(client):
    zeroer = _zeroer(client.transport)
    motor = client._manager.picos["motor"]
    zeroer.jog_az(2.0)
    assert _wait_until(
        lambda: motor._emulator.azimuth.position
        == motor._emulator.azimuth.target_pos,
        timeout=3.0,
    )
    _, should_exit, zeroed = zeroer.handle_key(_KEY_ENTER, 1.0)
    assert should_exit is True
    assert zeroed is True
    assert _wait_until(
        lambda: motor._emulator.azimuth.position == 0,
        timeout=2.0,
    )
    assert motor._emulator.azimuth.target_pos == 0


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
