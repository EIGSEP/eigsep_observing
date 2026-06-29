import pytest
from eigsep_redis.testing import DummyTransport
from eigsep_observing.motor_homer import MotorHomer, HomeResult


def _homer(**kw):
    return MotorHomer(DummyTransport(), motor_client=object(), **kw)


def test_az_residual_volts_times_gain():
    h = _homer(az_gain_deg_per_volt=100.0)
    ref = {"pot_az_voltage_v0": 1.0, "imu_el_deg_home": 0.0}
    # pot reads 1.1 V, home is 1.0 V -> -0.1 V -> -10 deg residual
    assert h._az_residual_deg(ref, 1.1) == pytest.approx(-10.0)


def test_az_residual_none_when_pot_missing():
    h = _homer(az_gain_deg_per_volt=100.0)
    ref = {"pot_az_voltage_v0": 1.0, "imu_el_deg_home": 0.0}
    assert h._az_residual_deg(ref, None) is None


def test_el_residual_signed_when_primary():
    from eigsep_observing.el_sensor import ElEstimate

    h = _homer()
    ref = {"pot_az_voltage_v0": 1.0, "imu_el_deg_home": 0.0}
    res, mag_only = h._el_residual(ref, ElEstimate(-8.0, False, "imu_el"))
    assert res == pytest.approx(8.0)  # home(0) - (-8) = +8
    assert mag_only is False


def test_el_residual_magnitude_only_failover():
    from eigsep_observing.el_sensor import ElEstimate

    h = _homer()
    ref = {"pot_az_voltage_v0": 1.0, "imu_el_deg_home": 0.0}
    res, mag_only = h._el_residual(ref, ElEstimate(8.0, True, "imu_az"))
    # magnitude-only: drive |el| toward |home|(0): residual magnitude 8
    assert abs(res) == pytest.approx(8.0)
    assert mag_only is True


def test_within_tol():
    h = _homer(tol_az_deg=3.0, tol_el_deg=2.0)
    assert h._within_tol(2.0, 1.0) is True
    assert h._within_tol(4.0, 1.0) is False
    assert h._within_tol(2.0, 3.0) is False
    assert h._within_tol(None, 1.0) is True  # axis with no reading: skip


def test_home_result_constructible():
    r = HomeResult(
        converged=True,
        iterations=2,
        residual_az_deg=1.0,
        residual_el_deg=0.5,
        degraded=False,
        reset_count=True,
    )
    assert r.converged is True
