"""Unit tests for the azimuth pot-verify divergence guard and verifier."""

import pytest

from eigsep_observing.motor_az_verify import _AzAngleDivergenceGuard
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
