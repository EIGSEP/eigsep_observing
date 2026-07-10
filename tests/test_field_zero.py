import importlib.util
import pathlib
import sys

import pytest

from eigsep_redis.testing import DummyTransport

from eigsep_observing import MotorLimitError, MotorZeroer
from eigsep_observing.motor_homer import HomeResult

_spec = importlib.util.spec_from_file_location(
    "field_zero",
    pathlib.Path(__file__).parents[1] / "scripts" / "field_zero.py",
)
field_zero = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(field_zero)


def _make_dummy_transport():
    """Fakeredis-backed transport, per the repo DummyTransport convention."""
    return DummyTransport()


def test_slip_verdict_bands():
    assert field_zero.slip_verdict(1.0, 1.00) == "ok"
    assert field_zero.slip_verdict(1.0, 0.96) == "ok"  # 4% short
    assert field_zero.slip_verdict(1.0, 0.93) == "warn"  # 7% short
    assert field_zero.slip_verdict(1.0, 0.80) == "fail"  # 20% short
    assert field_zero.slip_verdict(0.0, 0.0) == "fail"  # zero expected
    # Overshoot is never slip: slip/stall under-travels the pot. A
    # larger-than-expected swing means the stored slope is too steep
    # (stale cal), so it warns without blocking (field finding,
    # 2026-07-01: a real zero was wrongly denied on overshoot).
    assert field_zero.slip_verdict(1.0, 1.04) == "ok"  # 4% over
    assert field_zero.slip_verdict(1.0, 1.07) == "overshoot"  # 7% over
    assert field_zero.slip_verdict(1.0, 1.50) == "overshoot"  # gross


def test_rezero_pot_is_gone():
    """The intercept re-pin is no longer a side effect of zeroing; it
    lives only in calibrate-pot --mode rezero as a deliberate recal.
    field_zero must not carry a rezero path at all."""
    assert not hasattr(field_zero, "rezero_pot")
    assert not hasattr(field_zero, "_write_home_ref")


def test_run_slip_check_uses_pot_swing():
    volts = iter([2.00, 2.15])  # pot voltage before, after a +move_deg jog

    class Snap:
        def get(self, *a):
            return {"potmon": {"pot_az_voltage": next(volts)}}

    class Motor:
        def jog_az(self, d, **k):
            pass

    verdict, exp, meas = field_zero.run_slip_check(
        Motor(), Snap(), slope_m=200.0, move_deg=30.0
    )
    assert round(exp, 4) == round(30.0 / 200.0, 4)  # 0.15 V expected
    assert round(meas, 4) == 0.15  # 2.15 - 2.00
    assert verdict == "ok"


def test_run_slip_check_aborts_without_pot():
    class Snap:
        def get(self, *a):
            return {"potmon": {}}  # no pot_az_voltage

    class Motor:
        def jog_az(self, d, **k):
            pass

    with pytest.raises(SystemExit):
        field_zero.run_slip_check(Motor(), Snap(), slope_m=200.0)


def test_run_slip_check_probe_denied_exits_cleanly():
    """A probe jog denied by the travel guard (or aborted by the
    sensor fence) must surface as an actionable SystemExit, not a
    MotorLimitError traceback."""

    class Snap:
        def get(self, *a):
            return {"potmon": {"pot_az_voltage": 2.0}}

    class Motor:
        def jog_az(self, d, **k):
            raise MotorLimitError(
                "az move to 204.0 deg outside safe window "
                "[-200.0, 200.0]; refusing to send az_move_deg."
            )

    with pytest.raises(SystemExit, match="denied by travel limit"):
        field_zero.run_slip_check(Motor(), Snap(), slope_m=200.0)


def test_no_slip_check_flag_parses(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["field_zero", "--no-slip-check"])
    args = field_zero._parse_args()
    assert args.no_slip_check is True


def test_override_limits_flag_parses(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["field_zero", "--override-limits"])
    args = field_zero._parse_args()
    assert args.override_limits is True


def test_prompt_override_yes(monkeypatch, capsys):
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")
    assert field_zero._prompt_override(0.150, 0.050) is True
    out = capsys.readouterr().out
    # Operator decides with the numbers in hand.
    assert "0.050" in out and "0.150" in out


def test_prompt_override_default_is_no(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _prompt: "")
    assert field_zero._prompt_override(0.150, 0.050) is False


def test_prompt_override_eof_is_no(monkeypatch):
    def _eof(_prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", _eof)
    assert field_zero._prompt_override(0.150, 0.050) is False


# ---------------------------------------------------------------------------
# _curses_main: confirm commits a home-and-zero, UI exits on convergence
# ---------------------------------------------------------------------------


class _FakeScreen:
    """Minimal stand-in for the curses screen: replays a key sequence,
    then idle (-1) ticks. Deviation from real curses is deliberate —
    curses needs a tty; the loop under test only pumps keystrokes and
    draws strings."""

    def __init__(self, keys, max_ticks=500):
        self._keys = iter(keys)
        self._ticks = 0
        self._max = max_ticks

    def timeout(self, ms):
        pass

    def clear(self):
        pass

    def refresh(self):
        pass

    def addstr(self, *a):
        pass

    def getmaxyx(self):
        return (24, 80)

    def getch(self):
        self._ticks += 1
        if self._ticks > self._max:
            raise AssertionError("UI never exited")
        return next(self._keys, -1)


class _EmptySnap:
    def get(self):
        return {}


def test_curses_main_exits_with_result_on_converged_home(client, monkeypatch):
    """Enter → y commits the home-and-zero; the loop stays alive while
    the background home runs and exits returning the HomeResult once it
    converges."""
    monkeypatch.setattr(field_zero.curses, "noecho", lambda: None)

    class _Homer:
        def home(self, stop_event=None, axes=("az", "el")):
            return HomeResult(
                converged=True,
                iterations=1,
                residual_az_deg=0.5,
                residual_el_deg=0.2,
                degraded=False,
                reset_count=True,
            )

    zeroer = MotorZeroer(
        client.transport, homer=_Homer(), confirm_starts_home=True
    )
    screen = _FakeScreen([ord("\n"), ord("y")])
    result = field_zero._curses_main(screen, zeroer, _EmptySnap(), 1.0)
    assert result is not None
    assert result.converged is True


def test_curses_main_stays_alive_on_unconverged_home(client, monkeypatch):
    """A home that fails to converge must NOT exit the UI — the
    operator keeps jogging and can retry; a later q leaves with no
    result."""
    monkeypatch.setattr(field_zero.curses, "noecho", lambda: None)

    class _Homer:
        def home(self, stop_event=None, axes=("az", "el")):
            return HomeResult(
                converged=False,
                iterations=6,
                residual_az_deg=9.0,
                residual_el_deg=0.2,
                degraded=False,
                reset_count=False,
            )

    zeroer = MotorZeroer(
        client.transport, homer=_Homer(), confirm_starts_home=True
    )

    class _Screen(_FakeScreen):
        """Enter, y, then idle until the home thread unwinds (so the q
        cannot race the in-flight home and be swallowed as a cancel),
        then q to leave."""

        def getch(self):
            self._ticks += 1
            if self._ticks > self._max:
                raise AssertionError("UI never exited")
            nxt = next(self._keys, None)
            if nxt is not None:
                return nxt
            if zeroer.is_homing or zeroer.last_home_result is None:
                return -1
            return ord("q")

    screen = _Screen([ord("\n"), ord("y")])
    result = field_zero._curses_main(screen, zeroer, _EmptySnap(), 1.0)
    assert result is None
