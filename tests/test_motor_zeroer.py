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


class _BlockingHomer:
    """Stand-in ``MotorHomer`` whose ``home`` blocks on the stop event.

    Lets a test observe ``is_homing == True`` deterministically (a fast
    fake returns too quickly to catch the flag) and proves the cancel
    path: ``home`` only returns once the ``stop_event`` is set, so
    ``cancel_home`` unwinding the thread is observable.
    """

    def __init__(self):
        self.home_calls = 0
        self.last_stop_event = None
        self.last_axes = None

    def home(self, stop_event=None, axes=("az", "el")):
        from eigsep_observing.motor_homer import HomeResult

        self.home_calls += 1
        self.last_stop_event = stop_event
        self.last_axes = tuple(axes)
        if stop_event is not None:
            stop_event.wait(timeout=2.0)
        # Mirrors the real ``MotorHomer.home``'s cancellation shape: a
        # stop-event interruption never converges but is not
        # "degraded" (that flag is reserved for sensor unavailability,
        # e.g. dead potmon/IMU) — see ``_home_az``/``_home_el``.
        return HomeResult(
            converged=False,
            iterations=0,
            residual_az_deg=None,
            residual_el_deg=None,
            degraded=False,
            reset_count=False,
        )


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


def test_zeroer_builds_homer_on_shared_motor_client():
    """The default-built homer drives the same MotorClient the jogs
    use, so 'h' inherits the zeroer's limit enforcement."""
    z = MotorZeroer(DummyTransport())
    assert z._homer.motor_client is z._motor_client


def test_handle_key_h_runs_homer_in_background(client):
    """Pressing 'h' runs the cal-defined homer (not step-0
    ``MotorClient.home``) in a background thread, passing the stop
    event, and clears ``is_homing`` when it returns."""

    class _RecordingHomer:
        def __init__(self):
            self.home_calls = 0
            self.last_stop_event = None

        def home(self, stop_event=None, axes=("az", "el")):
            from eigsep_observing.motor_homer import HomeResult

            self.home_calls += 1
            self.last_stop_event = stop_event
            # The real homer always returns a HomeResult (never None);
            # _run's else-branch reads .converged off it.
            return HomeResult(
                converged=True,
                iterations=1,
                residual_az_deg=0.0,
                residual_el_deg=0.0,
                degraded=False,
                reset_count=False,
            )

    fake = _RecordingHomer()
    zeroer = MotorZeroer(client.transport, homer=fake)
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)
    assert fake.home_calls == 1
    assert fake.last_stop_event is not None


def test_handle_key_h_sets_homing_and_any_key_cancels(client):
    """'h' starts a background home (``is_homing`` True); while homing,
    any subsequent key cancels it (stops the motor, unwinds the thread)
    and must NOT execute as a jog."""
    fake = _BlockingHomer()
    zeroer = MotorZeroer(client.transport, homer=fake)

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


def test_handle_key_h_homes_both_axes(client):
    """'h' keeps its home-both behavior: the homer gets both axes."""
    fake = _BlockingHomer()
    zeroer = MotorZeroer(client.transport, homer=fake)
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: fake.last_axes is not None, timeout=1.0)
    assert fake.last_axes == ("az", "el")
    assert zeroer.homing_axes == ("az", "el")
    zeroer.cancel_home()


def test_handle_key_a_homes_az_only(client):
    """'a' starts a background az-only home; ``homing_axes`` feeds the
    UI banner."""
    fake = _BlockingHomer()
    zeroer = MotorZeroer(client.transport, homer=fake)
    zeroer.handle_key(ord("a"), 1.0)
    assert _wait_until(lambda: fake.last_axes is not None, timeout=1.0)
    assert fake.last_axes == ("az",)
    assert zeroer.homing_axes == ("az",)
    zeroer.cancel_home()


def test_handle_key_e_homes_el_only(client):
    """'e' starts a background el-only home."""
    fake = _BlockingHomer()
    zeroer = MotorZeroer(client.transport, homer=fake)
    zeroer.handle_key(ord("e"), 1.0)
    assert _wait_until(lambda: fake.last_axes is not None, timeout=1.0)
    assert fake.last_axes == ("el",)
    assert zeroer.homing_axes == ("el",)
    zeroer.cancel_home()


def test_handle_key_h_ignored_when_unavailable(client, monkeypatch):
    """When the manager is unreachable, 'h' is a no-op — no home thread
    is spawned."""
    fake = _BlockingHomer()
    zeroer = MotorZeroer(client.transport, homer=fake)
    monkeypatch.setattr(
        MotorZeroer, "is_available", property(lambda self: False)
    )
    zeroer.handle_key(ord("h"), 1.0)
    assert zeroer.is_homing is False
    assert fake.home_calls == 0


def test_handle_key_noop_during_homing_preserves_state(client):
    """A ``-1`` idle tick (getch timeout) while homing must keep the
    home running rather than silently cancelling it."""
    fake = _BlockingHomer()
    zeroer = MotorZeroer(client.transport, homer=fake)
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
    steps = cal.deg_to_steps(199.0)
    mc._motor_status = lambda: {
        "az_pos": steps,
        "az_target_pos": steps,
        "el_pos": 0,
        "el_target_pos": 0,
    }
    zeroer = MotorZeroer(transport, motor_client=mc)
    with pytest.raises(MotorLimitError):
        zeroer.jog_az(5.0)  # 199 + 5 = 204 -> outside ±200


# ---------------------------------------------------------------------------
# Task D2: enforce_limits threading
# ---------------------------------------------------------------------------


def test_zeroer_passes_enforce_limits_to_motor_client():
    z = MotorZeroer(DummyTransport(), enforce_limits=False)
    assert z._motor_client.enforce_limits is False


# ---------------------------------------------------------------------------
# Limit denials must not crash the curses loop (field finding, 2026-07-01)
# ---------------------------------------------------------------------------


class _LimitTrippingHomer:
    """Stand-in ``MotorHomer`` whose ``home`` trips the sensor fence."""

    def home(self, stop_event=None, axes=("az", "el")):
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
    # 205 deg from home is outside the ±200 default window.
    new_deg, should_exit, zeroed = zeroer.handle_key(ord("l"), 205.0)
    assert (new_deg, should_exit, zeroed) == (205.0, False, False)
    assert "outside safe window" in zeroer.notice
    assert motor._emulator.azimuth.target_pos == before  # move refused


def test_handle_key_successful_jog_clears_notice(client):
    zeroer = _zeroer(client.transport)
    zeroer.handle_key(ord("l"), 205.0)  # denied — notice set
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
    zeroer = MotorZeroer(client.transport, homer=_LimitTrippingHomer())
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)
    assert "outside safe window" in zeroer.notice


def test_start_home_without_cal_refuses_with_notice(client):
    """'h' with no pot calibration stored must not silently fall back
    to a step-0 home — the real homer raises, the thread unwinds, and
    the operator sees an actionable notice."""
    zeroer = _zeroer(client.transport)  # default homer, no cal seeded
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)
    assert "calibrate-pot" in zeroer.notice


# ---------------------------------------------------------------------------
# confirm_starts_home: field_zero's Enter/y commits a home-and-zero
# ---------------------------------------------------------------------------


class _ConvergingHomer:
    """Stand-in ``MotorHomer`` that converges immediately."""

    def __init__(self):
        self.home_calls = 0

    def home(self, stop_event=None, axes=("az", "el")):
        from eigsep_observing.motor_homer import HomeResult

        self.home_calls += 1
        return HomeResult(
            converged=True,
            iterations=1,
            residual_az_deg=0.5,
            residual_el_deg=0.2,
            degraded=False,
            reset_count=True,
        )


def test_confirm_starts_home_runs_homer_not_origin_reset(client):
    """In confirm_starts_home mode, Enter/y starts the background home
    (home-and-zero at the cal-defined home) instead of resetting the
    step counters at the current pose, and does not exit the loop —
    the caller watches ``is_homing`` / ``last_home_result``."""
    fake = _ConvergingHomer()
    zeroer = MotorZeroer(
        client.transport, homer=fake, confirm_starts_home=True
    )
    motor = client._manager.picos["motor"]
    zeroer.jog_az(2.0)
    assert _wait_until(
        lambda: (
            motor._emulator.azimuth.position
            == motor._emulator.azimuth.target_pos
        ),
        timeout=3.0,
    )
    off_origin = motor._emulator.azimuth.position
    assert off_origin != 0  # precondition: away from origin

    zeroer.handle_key(_KEY_ENTER, 1.0)
    assert zeroer.pending_zero is True
    _, should_exit, committed = zeroer.handle_key(ord("y"), 1.0)
    assert should_exit is False
    assert committed is True
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)
    assert fake.home_calls == 1
    assert zeroer.last_home_result is not None
    assert zeroer.last_home_result.converged is True
    # counters were NOT reset at the confirmed pose (the fake homer owns
    # the reset on convergence)
    assert motor._emulator.azimuth.position == off_origin


def test_confirm_default_mode_still_zeroes_at_pose(client):
    """Without confirm_starts_home (motor_manual), y keeps its
    zero-at-pose scan-origin meaning and exits."""
    zeroer = _zeroer(client.transport)
    zeroer.handle_key(_KEY_ENTER, 1.0)
    _, should_exit, zeroed = zeroer.handle_key(ord("y"), 1.0)
    assert should_exit is True
    assert zeroed is True


def test_last_home_result_reset_on_new_home(client):
    """A stale result from a previous home must not leak into a new
    one — start_home clears it before the thread runs."""
    fake = _BlockingHomer()
    zeroer = MotorZeroer(client.transport, homer=fake)
    zeroer.last_home_result = object()  # simulate a previous run
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: zeroer.is_homing, timeout=1.0)
    assert zeroer.last_home_result is None
    zeroer.cancel_home()


def test_zeroer_forwards_az_step0_fallback():
    z = MotorZeroer(DummyTransport(), az_step0_fallback=True)
    assert z._homer.az_step0_fallback is True


def test_zeroer_default_no_az_step0_fallback():
    z = MotorZeroer(DummyTransport())
    assert z._homer.az_step0_fallback is False


# ---------------------------------------------------------------------------
# start_home notice: unconverged/degraded results must reach the curses UI
# ---------------------------------------------------------------------------


class _ResultHomer:
    """Stand-in ``MotorHomer`` that returns a fixed ``HomeResult``.

    The manual scripts run with ``console=False``, so log warnings are
    invisible mid-session; ``notice`` is the curses UI's only channel.
    """

    def __init__(self, result):
        self._result = result
        self.home_calls = 0

    def home(self, stop_event=None, axes=("az", "el")):
        self.home_calls += 1
        return self._result


def test_home_notice_set_on_degraded_result(client):
    """A degraded (sensor-unavailable) home result must surface an
    actionable notice — a dead potmon skipping az must not pass
    silently in a manual session."""
    from eigsep_observing.motor_homer import HomeResult

    fake = _ResultHomer(
        HomeResult(
            converged=False,
            iterations=0,
            residual_az_deg=float("nan"),
            residual_el_deg=None,
            degraded=True,
            reset_count=False,
        )
    )
    zeroer = MotorZeroer(client.transport, homer=fake)
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)
    assert "sensor unavailable" in zeroer.notice


def test_home_notice_set_on_unconverged_result(client):
    """A non-degraded home that simply failed to converge (e.g. the
    iteration budget was exhausted) must also surface a notice, worded
    distinctly from the degraded/sensor-unavailable case."""
    from eigsep_observing.motor_homer import HomeResult

    fake = _ResultHomer(
        HomeResult(
            converged=False,
            iterations=5,
            residual_az_deg=3.0,
            residual_el_deg=2.0,
            degraded=False,
            reset_count=False,
        )
    )
    zeroer = MotorZeroer(client.transport, homer=fake)
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)
    assert "did not converge" in zeroer.notice


def test_home_notice_stays_none_on_converged_result(client):
    """A converged home must not set any notice."""
    fake = _ConvergingHomer()
    zeroer = MotorZeroer(client.transport, homer=fake)
    zeroer.handle_key(ord("h"), 1.0)
    assert _wait_until(lambda: not zeroer.is_homing, timeout=2.0)
    assert zeroer.notice is None
