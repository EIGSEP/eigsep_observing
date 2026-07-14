"""Unit tests for the azimuth pot-verify divergence guard and verifier."""

import pytest

from eigsep_observing.motor_az_verify import (
    AzPotVerifier,
    _AzAngleDivergenceGuard,
)
from eigsep_observing.motor_client import MotorLimitError


def test_guard_silent_on_converging_reads():
    # angle approaches target 0 monotonically -> guard never trips
    reads = iter([20.0, 15.0, 10.0, 5.0, 1.0])
    g = _AzAngleDivergenceGuard(lambda: next(reads), 0.0, 5.0)
    for _ in range(5):
        assert g() is None


def test_guard_trips_when_receding_past_allowance():
    # closest approach is 1.0; growing to 7.0 is 6.0 past it (> 5.0)
    reads = iter([2.0, 1.0, 4.0, 7.0])
    g = _AzAngleDivergenceGuard(lambda: next(reads), 0.0, 5.0)
    g()  # dist 2 -> min 2
    g()  # dist 1 -> min 1
    g()  # dist 4 -> 4-1=3 <=5, ok
    with pytest.raises(MotorLimitError):
        g()  # dist 7 -> 7-1=6 > 5


def test_guard_skips_none_reads():
    reads = iter([2.0, None, 10.0])
    g = _AzAngleDivergenceGuard(lambda: next(reads), 0.0, 5.0)
    g()  # min 2
    assert g() is None  # None -> skipped, min unchanged
    with pytest.raises(MotorLimitError):
        g()  # dist 10 -> 10-2=8 > 5


class _FakeAzMotor:
    """In-process az motor. ``jog_az`` advances a simulated pot angle by
    the commanded delta when ``moves`` (healthy axis converges in one
    jog); when not ``moves`` the pot never advances (slip)."""

    def __init__(self, angle=0.0, moves=True):
        self.angle = angle
        self.moves = moves
        self.jogs = []
        self.jog_guards = []

    def jog_az(self, delta_deg, *, stop_event=None, guard=None):
        self.jogs.append(delta_deg)
        self.jog_guards.append(guard)
        if self.moves:
            self.angle += delta_deg


class _FakeReader:
    """Reflects the fake motor's live pot angle as a potmon snapshot."""

    def __init__(self, fake, near_rail=False, angle_none=False):
        self.fake = fake
        self.near_rail = near_rail
        self.angle_none = angle_none

    def get(self, key):
        if key != "potmon":
            return {}
        return {
            "pot_az_angle": None if self.angle_none else self.fake.angle,
            "pot_az_near_rail": self.near_rail,
        }


def _verifier(fake, reader=None, **kw):
    kw.setdefault("settle_s", 0.0)
    kw.setdefault("integrate_s", 0.0)  # single sample per read: exact
    return AzPotVerifier(fake, reader or _FakeReader(fake), **kw)


def test_verify_within_tol_does_not_jog():
    fake = _FakeAzMotor(angle=1.0)
    r = _verifier(fake, tol_az_deg=3.0).verify(0.0)
    assert r.converged and not r.degraded
    assert r.iters == 0 and fake.jogs == []


def test_verify_healthy_converges_in_one_jog():
    fake = _FakeAzMotor(angle=0.0, moves=True)
    r = _verifier(fake, tol_az_deg=3.0, max_iters=3).verify(20.0)
    assert r.converged and r.iters == 1
    assert fake.jogs == [pytest.approx(20.0)]
    assert r.residual_deg == pytest.approx(0.0)


def test_verify_slip_gives_up_after_max_iters():
    fake = _FakeAzMotor(angle=0.0, moves=False)  # jog never moves pot
    r = _verifier(fake, tol_az_deg=3.0, max_iters=3).verify(20.0)
    assert not r.converged and not r.degraded
    assert r.iters == 3 and len(fake.jogs) == 3


def test_verify_degraded_when_near_rail():
    fake = _FakeAzMotor(angle=0.0)
    reader = _FakeReader(fake, near_rail=True)
    r = _verifier(fake, reader=reader).verify(20.0)
    assert r.degraded and fake.jogs == []


def test_verify_degraded_when_uncalibrated_angle_none():
    fake = _FakeAzMotor(angle=0.0)
    reader = _FakeReader(fake, angle_none=True)
    r = _verifier(fake, reader=reader).verify(20.0)
    assert r.degraded and fake.jogs == []


def test_verify_degraded_when_potmon_absent():
    fake = _FakeAzMotor(angle=0.0)

    class _Absent:
        def get(self, key):
            raise KeyError(key)

    r = _verifier(fake, reader=_Absent()).verify(20.0)
    assert r.degraded and fake.jogs == []


def test_verify_passes_divergence_guard_to_jog():
    fake = _FakeAzMotor(angle=0.0, moves=True)
    _verifier(fake).verify(20.0)
    assert fake.jog_guards[0] is not None
