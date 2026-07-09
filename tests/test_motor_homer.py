import logging

import pytest
from eigsep_redis.testing import DummyTransport
from picohost.buses import PotCalStore

from eigsep_observing.el_sensor import ElEstimate
from eigsep_observing.motor_homer import HomeResult, MotorHomer
from eigsep_observing.motor_limits import publish_motor_limits


def _seed_cal(t, m=100.0, b=-100.0):
    """Store a pot cal whose zero-angle voltage is v_home = -b/m.

    Defaults give v_home = 1.0 V, matching the fake motor below.
    """
    PotCalStore(t).upload({"pot_az": [m, b]})


def _homer(**kw):
    return MotorHomer(DummyTransport(), motor_client=object(), **kw)


def test_az_residual_volts_times_gain():
    h = _homer(az_gain_deg_per_volt=100.0)
    # pot reads 1.1 V, home is 1.0 V -> -0.1 V -> -10 deg residual
    assert h._az_residual_deg(1.0, 1.1) == pytest.approx(-10.0)


def test_az_residual_none_when_pot_missing():
    h = _homer(az_gain_deg_per_volt=100.0)
    assert h._az_residual_deg(1.0, None) is None


def test_el_residual_signed_when_primary():
    h = _homer()
    res, mag_only = h._el_residual(ElEstimate(-8.0, False, "imu_el"))
    assert res == pytest.approx(8.0)  # level(0) - (-8) = +8
    assert mag_only is False


def test_el_residual_magnitude_only_failover():
    h = _homer()
    res, mag_only = h._el_residual(ElEstimate(8.0, True, "imu_az"))
    # magnitude-only: drive |el| toward level (0): residual magnitude 8
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


def test_az_home_voltage_derived_from_cal():
    t = DummyTransport()
    _seed_cal(t, m=200.0, b=-300.0)  # angle = 200 v - 300 = 0 at 1.5 V
    h = MotorHomer(t, motor_client=object())
    assert h.az_home_voltage() == pytest.approx(1.5)


def test_az_home_voltage_raises_without_cal():
    h = MotorHomer(DummyTransport(), motor_client=object())
    with pytest.raises(RuntimeError, match="calibrate-pot"):
        h.az_home_voltage()


# ---------------------------------------------------------------------------
# home() loop tests
# ---------------------------------------------------------------------------


class _FakeMotor:
    """In-process motor whose jogs move a simulated pot voltage / el toward
    home, so the homer's loop actually converges. gain matches the homer's
    az gain so a damped jog shrinks the residual."""

    def __init__(self, pot=1.30, el=10.0, deg_per_volt=100.0):
        self.pot = pot
        self.el = el
        self.dpv = deg_per_volt
        self.homed = 0
        self.reset = []

    def home(self, stop_event=None):
        self.homed += 1

    def jog_az(self, delta_deg, stop_event=None):
        self.pot += delta_deg / self.dpv  # +deg lowers residual toward v0

    def jog_el(self, delta_deg, stop_event=None):
        self.el += delta_deg  # +deg moves el toward level (0)

    def reset_step_position(self, az_step=0, el_step=0):
        self.reset.append((az_step, el_step))


def _homer_with_fake(t, fake, **kw):
    h = MotorHomer(
        t,
        motor_client=fake,
        az_gain_deg_per_volt=fake.dpv,
        settle_s=0.0,
        damping=1.0,
        max_iters=10,
        **kw,
    )
    # snapshot reflects the fake's live state
    h.snapshot.get = lambda key: (
        {"pot_az_voltage": fake.pot}
        if key == "potmon"
        else {"el_deg": fake.el}
        if key == "imu_el"
        else {}
    )
    return h


def test_home_raises_without_cal():
    h = MotorHomer(DummyTransport(), motor_client=_FakeMotor())
    with pytest.raises(RuntimeError, match="calibrate-pot"):
        h.home()


def test_home_converges_onto_cal_zero_and_resets_count():
    t = DummyTransport()
    _seed_cal(t)  # v_home = 1.0 V; el home is IMU-level (0 deg)
    fake = _FakeMotor(pot=1.30, el=10.0)
    h = _homer_with_fake(t, fake)
    res = h.home()
    assert res.converged is True
    assert abs(res.residual_az_deg) <= h.tol_az_deg
    assert abs(res.residual_el_deg) <= h.tol_el_deg
    # the pot itself landed at the cal's zero-angle voltage
    assert fake.pot == pytest.approx(1.0, abs=h.tol_az_deg / fake.dpv)
    assert fake.reset == [(0, 0)]  # re-zeroed on convergence
    assert fake.homed >= 1  # coarse approach happened


def test_home_tracks_cal_rezero_immediately():
    """A recal (new intercept) moves home for the next home() call — no
    intermediate K/V to refresh."""
    t = DummyTransport()
    _seed_cal(t, m=100.0, b=-100.0)  # v_home 1.0 V
    fake = _FakeMotor(pot=1.30, el=0.0)
    h = _homer_with_fake(t, fake)
    assert h.home().converged is True
    assert fake.pot == pytest.approx(1.0, abs=h.tol_az_deg / fake.dpv)
    _seed_cal(t, m=100.0, b=-120.0)  # rezero: v_home now 1.2 V
    assert h.home().converged is True
    assert fake.pot == pytest.approx(1.2, abs=h.tol_az_deg / fake.dpv)


def test_home_refuses_when_cal_zero_outside_pot_window(caplog):
    """A cal whose zero-angle voltage lies outside the rig's pot fence is
    broken; refuse before moving rather than driving into the fence."""
    t = DummyTransport()
    _seed_cal(t, m=100.0, b=-100.0)  # v_home 1.0 V
    publish_motor_limits(
        t,
        az_limits_deg=[-180.0, 180.0],
        el_limits_deg=[-180.0, 180.0],
        pot_az_v_limits=[1.5, 2.5],  # window excludes v_home
        imu_el_limits_deg=None,
    )
    fake = _FakeMotor(pot=2.0, el=0.0)
    h = _homer_with_fake(t, fake)
    with pytest.raises(RuntimeError, match="outside"):
        h.home()
    assert fake.homed == 0  # never moved


def test_home_degrades_when_sensors_down(caplog):
    t = DummyTransport()
    _seed_cal(t)
    fake = _FakeMotor()
    h = MotorHomer(t, motor_client=fake)
    h.snapshot.get = lambda key: {}  # nothing published
    with caplog.at_level(logging.WARNING):
        res = h.home()
    assert res.degraded is True
    assert res.converged is False
    assert fake.homed == 1  # open-loop fallback park
    assert any("open-loop" in r.message for r in caplog.records)


def test_mid_loop_sensor_loss_aborts_without_rezero(caplog):
    """If all sensors go silent inside the loop, abort with degraded=True and
    no re-zero — re-zeroing at an unverified position is a silent
    wrong-success."""
    t = DummyTransport()
    _seed_cal(t)
    fake = _FakeMotor(pot=1.30, el=10.0)
    h = _homer_with_fake(t, fake)
    # Override _read_sensors so it returns valid data on the first pre-loop
    # call (which happens before the main loop) and then all-None thereafter,
    # triggering the mid-loop guard on iteration 1.
    call_count = 0

    def _read_sensors_stub():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # first call: pre-loop check — sensors look fine, proceed to loop
            return fake.pot, ElEstimate(fake.el, False, "imu_el")
        # subsequent calls (inside loop): all sensors lost
        return None, ElEstimate(None, False, "none")

    h._read_sensors = _read_sensors_stub
    with caplog.at_level(logging.WARNING):
        result = h.home()
    assert result.converged is False
    assert result.degraded is True
    assert fake.reset == []  # step counter must NOT be re-zeroed
    assert any("mid-loop" in r.message for r in caplog.records)


def test_az_sign_autodetect_flips_when_residual_grows():
    # fake where +az jog INCREASES the residual (wrong initial sign) until
    # the homer flips; convergence proves the flip happened.
    t = DummyTransport()
    _seed_cal(t)

    class _Reversed(_FakeMotor):
        def jog_az(self, delta_deg, stop_event=None):
            self.pot -= delta_deg / self.dpv  # opposite sign

    fake = _Reversed(pot=1.20, el=0.0)
    h = _homer_with_fake(t, fake)
    res = h.home()
    assert res.converged is True


# ---------------------------------------------------------------------------
# enforce_limits threading
# ---------------------------------------------------------------------------


def test_homer_passes_enforce_limits_to_motor_client():
    h = MotorHomer(DummyTransport(), enforce_limits=False)
    assert h.motor_client.enforce_limits is False
