import logging

import pytest
from eigsep_redis.testing import DummyTransport
from picohost.buses import PotCalStore

from eigsep_observing.el_sensor import ElEstimate
from eigsep_observing.motor_client import MotorLimitError
from eigsep_observing.motor_homer import (
    HomeResult,
    MotorHomer,
    _AzDivergenceGuard,
)
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


def test_az_residual_signed_slope_sets_direction():
    """The corrective jog has no sign auto-detect, so the residual
    must carry the cal slope's sign: residual = m * (v_home - v)."""
    h = _homer(az_gain_deg_per_volt=-100.0)
    # pot 1.1 V, home 1.0 V, negative slope -> +10 deg (jog positive)
    assert h._az_residual_deg(1.0, 1.1) == pytest.approx(10.0)


def test_read_pot_integrated_averages_samples():
    h = _homer(az_integrate_s=0.6)  # 3 samples at the 0.2 s cadence
    vals = iter([1.0, 2.0, 3.0, 4.0])
    h._read_pot_once = lambda: next(vals)
    assert h._read_pot_integrated() == pytest.approx(2.0)


def test_read_pot_integrated_single_sample_when_zero_window():
    h = _homer(az_integrate_s=0.0)
    h._read_pot_once = lambda: 1.5
    assert h._read_pot_integrated() == pytest.approx(1.5)


def test_read_pot_integrated_none_when_no_samples():
    h = _homer(az_integrate_s=0.0)
    h._read_pot_once = lambda: None
    assert h._read_pot_integrated() is None


def test_read_pot_integrated_skips_dropouts():
    h = _homer(az_integrate_s=0.6)
    vals = iter([1.0, None, 3.0])
    h._read_pot_once = lambda: next(vals)
    assert h._read_pot_integrated() == pytest.approx(2.0)


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
# home() tests — az: single correction; el: convergence loop
# ---------------------------------------------------------------------------


class _FakeMotor:
    """In-process motor: jogs move a simulated pot voltage / el the
    way the rig would, so az's single correction lands exactly and
    el's loop converges. ``dpv`` is the SIGNED cal slope the pot
    simulation honours (positive az motion changes voltage by
    ``delta/dpv``)."""

    def __init__(self, pot=1.30, el=10.0, deg_per_volt=100.0):
        self.pot = pot
        self.el = el
        self.dpv = deg_per_volt
        self.homed = 0
        self.home_axes = []
        self.home_guards = []
        self.az_jogs = []
        self.az_jog_guards = []
        self.el_jogs = []
        self.reset = []

    def home(self, stop_event=None, axes=("az", "el"), guard=None):
        self.homed += 1
        self.home_axes.append(tuple(axes))
        self.home_guards.append(guard)

    def jog_az(self, delta_deg, stop_event=None, guard=None):
        self.az_jogs.append(delta_deg)
        self.az_jog_guards.append(guard)
        self.pot += delta_deg / self.dpv

    def jog_el(self, delta_deg, stop_event=None, guard=None):
        self.el_jogs.append(delta_deg)
        self.el += delta_deg  # +deg moves el toward level (0)

    def reset_step_position(self, az_step=0, el_step=0):
        self.reset.append((az_step, el_step))


def _homer_with_fake(t, fake, **kw):
    kw.setdefault("az_gain_deg_per_volt", fake.dpv)
    kw.setdefault("damping", 1.0)
    h = MotorHomer(
        t,
        motor_client=fake,
        settle_s=0.0,
        az_integrate_s=0.0,  # single pot sample per read: fake is exact
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


def test_home_az_single_correction_el_loop_and_per_axis_reset():
    """Both-axes home: az takes coarse + exactly ONE corrective jog
    (full signed residual, no damping even though damping=0.5), el
    converges via its loop; each axis re-zeros only its own counter,
    az first."""
    t = DummyTransport()
    _seed_cal(t)  # v_home = 1.0 V
    fake = _FakeMotor(pot=1.30, el=10.0)
    h = _homer_with_fake(t, fake, damping=0.5)
    res = h.home()
    assert res.converged is True
    assert fake.az_jogs == [pytest.approx(-30.0)]  # one full-residual jog
    assert fake.pot == pytest.approx(1.0)
    assert abs(fake.el) <= h.tol_el_deg
    assert fake.home_axes == [("az",), ("el",)]  # az coarse first
    assert fake.reset == [(0, None), (None, 0)]  # per-axis re-zero
    assert res.iterations >= 1  # el loop ran


def test_home_az_within_tol_after_coarse_never_jogs():
    t = DummyTransport()
    _seed_cal(t)
    fake = _FakeMotor(pot=1.01, el=0.0)  # 1 deg residual < tol 3
    h = _homer_with_fake(t, fake)
    res = h.home(axes=("az",))
    assert res.converged is True
    assert fake.az_jogs == []
    assert fake.reset == [(0, None)]


def test_home_az_correction_direction_from_negative_slope():
    """Negative cal slope flips the jog sign — direction is defined
    by the pot cal, not trial and error."""
    t = DummyTransport()
    _seed_cal(t, m=-100.0, b=100.0)  # v_home = 1.0 V, slope -100
    fake = _FakeMotor(pot=1.30, el=0.0, deg_per_volt=-100.0)
    h = _homer_with_fake(t, fake)
    res = h.home(axes=("az",))
    assert res.converged is True
    assert fake.az_jogs == [pytest.approx(30.0)]  # -100 * (1.0-1.3)
    assert fake.pot == pytest.approx(1.0)


def test_home_az_still_out_after_correction_warns_no_rezero(caplog):
    """A stuck az (jog moves nothing) gets exactly one corrective
    attempt, then a loud warning and NO re-zero."""
    t = DummyTransport()
    _seed_cal(t)

    class _Stuck(_FakeMotor):
        def jog_az(self, delta_deg, stop_event=None, guard=None):
            self.az_jogs.append(delta_deg)  # motor doesn't move

    fake = _Stuck(pot=1.30, el=0.0)
    h = _homer_with_fake(t, fake)
    with caplog.at_level(logging.WARNING):
        res = h.home(axes=("az",))
    assert res.converged is False
    assert res.degraded is False
    assert len(fake.az_jogs) == 1  # no second attempt, no hunting
    assert fake.reset == []
    assert any("not re-zeroing" in r.message for r in caplog.records)


def test_home_az_moves_carry_divergence_guard():
    t = DummyTransport()
    _seed_cal(t)
    fake = _FakeMotor(pot=1.30, el=10.0)
    h = _homer_with_fake(t, fake)
    h.home()
    assert all(g is not None for g in fake.az_jog_guards)
    # az coarse guarded; el coarse not (pot says nothing about el)
    assert fake.home_guards[0] is not None
    assert fake.home_guards[1] is None
    # coarse and jog share one guard: closest approach carries over
    assert fake.home_guards[0] is fake.az_jog_guards[0]


def test_home_az_pot_lost_after_coarse_aborts_without_rezero(caplog):
    """Pot dies between the coarse approach and the read: warn,
    no jog, no re-zero, degraded."""
    t = DummyTransport()
    _seed_cal(t)

    class _PotDies(_FakeMotor):
        def home(self, stop_event=None, axes=("az", "el"), guard=None):
            super().home(stop_event=stop_event, axes=axes, guard=guard)
            self.pot = None

    fake = _PotDies(pot=1.30, el=0.0)
    h = _homer_with_fake(t, fake)
    with caplog.at_level(logging.WARNING):
        res = h.home(axes=("az",))
    assert res.converged is False
    assert res.degraded is True
    assert fake.az_jogs == []
    assert fake.reset == []
    assert any("potmon lost" in r.message for r in caplog.records)


def test_home_az_pot_missing_skips_az_by_default(caplog):
    """Default az_step0_fallback=False: dead potmon means NO az
    motion at all — with the pot dead the pot fence is inert too."""
    t = DummyTransport()
    _seed_cal(t)
    fake = _FakeMotor()
    h = _homer_with_fake(t, fake)
    h.snapshot.get = lambda key: {"el_deg": fake.el} if key == "imu_el" else {}
    with caplog.at_level(logging.WARNING):
        res = h.home(axes=("az",))
    assert res.degraded is True
    assert res.converged is False
    assert fake.home_axes == []  # az never moved
    assert fake.az_jogs == []
    assert fake.reset == []
    assert any("az_step0_fallback" in r.message for r in caplog.records)


def test_home_az_pot_missing_step0_fallback_parks_open_loop(caplog):
    """az_step0_fallback=True: dead potmon still parks az at step 0
    open-loop, degraded, never re-zeroed."""
    t = DummyTransport()
    _seed_cal(t)
    fake = _FakeMotor()
    h = _homer_with_fake(t, fake, az_step0_fallback=True)
    h.snapshot.get = lambda key: {"el_deg": fake.el} if key == "imu_el" else {}
    with caplog.at_level(logging.WARNING):
        res = h.home(axes=("az",))
    assert res.degraded is True
    assert res.converged is False
    assert fake.home_axes == [("az",)]  # open-loop park happened
    assert fake.reset == []
    assert any("potmon unavailable" in r.message for r in caplog.records)


def test_home_tracks_cal_rezero_immediately():
    """A recal (new intercept) moves home for the next home() call — no
    intermediate K/V to refresh."""
    t = DummyTransport()
    _seed_cal(t, m=100.0, b=-100.0)  # v_home 1.0 V
    fake = _FakeMotor(pot=1.30, el=0.0)
    h = _homer_with_fake(t, fake)
    # az_gain override would pin the slope; let it track the cal too
    h.az_gain_deg_per_volt = None
    assert h.home().converged is True
    assert fake.pot == pytest.approx(1.0)
    _seed_cal(t, m=100.0, b=-120.0)  # rezero: v_home now 1.2 V
    assert h.home().converged is True
    assert fake.pot == pytest.approx(1.2)


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
    """All sensors dead: az is skipped (pot-referenced, default no
    fallback), el falls back to the open-loop el-only park."""
    t = DummyTransport()
    _seed_cal(t)
    fake = _FakeMotor()
    h = MotorHomer(t, motor_client=fake, settle_s=0.0, az_integrate_s=0.0)
    h.snapshot.get = lambda key: {}  # nothing published
    with caplog.at_level(logging.WARNING):
        res = h.home()
    assert res.degraded is True
    assert res.converged is False
    assert fake.home_axes == [("el",)]  # el open-loop park only
    assert fake.reset == []
    assert any("open-loop" in r.message for r in caplog.records)


def test_el_mid_loop_sensor_loss_aborts_without_rezero(caplog):
    """If the IMUs go silent inside the el loop, abort with
    degraded=True and no re-zero — re-zeroing at an unverified
    position is a silent wrong-success."""
    t = DummyTransport()
    fake = _FakeMotor(pot=1.30, el=10.0)
    h = _homer_with_fake(t, fake)
    call_count = 0

    def _read_el_stub():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # pre-coarse check: IMU looks fine, proceed to the loop
            return ElEstimate(fake.el, False, "imu_el")
        return ElEstimate(None, False, "none")

    h._read_el = _read_el_stub
    with caplog.at_level(logging.WARNING):
        result = h.home(axes=("el",))
    assert result.converged is False
    assert result.degraded is True
    assert fake.reset == []  # step counter must NOT be re-zeroed
    assert any("mid-loop" in r.message for r in caplog.records)


def test_home_az_only_converges_without_touching_el():
    """``axes=("az",)`` corrects az, never jogs el (even though el is
    far off level), and preserves el's step counter on the re-zero."""
    t = DummyTransport()
    _seed_cal(t)
    fake = _FakeMotor(pot=1.30, el=10.0)  # el well outside tol_el_deg
    h = _homer_with_fake(t, fake)
    res = h.home(axes=("az",))
    assert res.converged is True
    assert res.iterations == 0  # no el loop ran
    assert fake.el == 10.0  # el never jogged
    assert res.residual_el_deg is None
    assert fake.pot == pytest.approx(1.0)
    assert fake.home_axes == [("az",)]  # coarse approach az-only
    assert fake.reset == [(0, None)]  # el counter untouched


def test_home_el_only_needs_no_pot_cal():
    """``axes=("el",)`` converges el without a stored pot calibration —
    the pot-cal requirement is purely an az concern — and never jogs az."""
    t = DummyTransport()  # deliberately no cal seeded
    fake = _FakeMotor(pot=1.30, el=10.0)
    h = _homer_with_fake(t, fake)
    res = h.home(axes=("el",))
    assert res.converged is True
    assert fake.pot == pytest.approx(1.30)  # az never jogged
    assert res.residual_az_deg is None
    assert abs(fake.el) <= h.tol_el_deg
    assert fake.home_axes == [("el",)]  # coarse approach el-only
    assert fake.reset == [(None, 0)]  # az counter untouched


def test_home_invalid_axes_raises():
    h = _homer()
    with pytest.raises(ValueError, match="axes"):
        h.home(axes=("foo",))
    with pytest.raises(ValueError, match="axes"):
        h.home(axes=())


def test_home_el_only_degrades_when_imu_missing(caplog):
    """An el-only home cares only about the IMUs: with both IMUs silent
    it falls back to the open-loop el-only park even though the pot is
    up."""
    t = DummyTransport()
    fake = _FakeMotor()
    h = _homer_with_fake(t, fake)
    h.snapshot.get = lambda key: (
        {"pot_az_voltage": fake.pot} if key == "potmon" else {}
    )
    with caplog.at_level(logging.WARNING):
        res = h.home(axes=("el",))
    assert res.degraded is True
    assert res.converged is False
    assert fake.home_axes == [("el",)]  # open-loop fallback, el only
    assert fake.reset == []
    assert any("open-loop" in r.message for r in caplog.records)


def test_el_sign_autodetect_flips_when_residual_grows():
    # fake where +el jog INCREASES the residual (wrong initial sign)
    # until the homer flips; convergence proves the flip happened.
    t = DummyTransport()

    class _Reversed(_FakeMotor):
        def jog_el(self, delta_deg, stop_event=None, guard=None):
            self.el -= delta_deg  # opposite sign

    fake = _Reversed(pot=1.0, el=8.0)
    h = _homer_with_fake(t, fake)
    res = h.home(axes=("el",))
    assert res.converged is True


# ---------------------------------------------------------------------------
# enforce_limits threading
# ---------------------------------------------------------------------------


def test_homer_passes_enforce_limits_to_motor_client():
    h = MotorHomer(DummyTransport(), enforce_limits=False)
    assert h.motor_client.enforce_limits is False


# ---------------------------------------------------------------------------
# az divergence guard
# ---------------------------------------------------------------------------


def _guard_over(values, v_home=1.0, slope=100.0, diverge_deg=20.0):
    it = iter(values)
    return _AzDivergenceGuard(lambda: next(it), v_home, slope, diverge_deg)


def test_divergence_guard_trips_after_min_plus_threshold():
    # dist(deg): 50, 20, 10(min), 30 (=min+20, no trip), 35 (>min+20)
    g = _guard_over([1.5, 1.2, 1.1, 1.3, 1.35])
    for _ in range(4):
        g()
    with pytest.raises(MotorLimitError, match="diverging"):
        g()


def test_divergence_guard_monotonic_approach_never_trips():
    g = _guard_over([1.5, 1.4, 1.3, 1.2, 1.1, 1.0])
    for _ in range(6):
        g()  # must not raise


def test_divergence_guard_overshoot_past_home_trips():
    # crosses home (dist -> 0) then keeps going: trips once past
    # min + threshold. dists: 20, 10, 0(min), 10, 20 (=min+20, no
    # trip — strictly greater), then 30 trips.
    g = _guard_over([1.2, 1.1, 1.0, 0.9, 0.8, 0.7])
    for _ in range(5):
        g()
    with pytest.raises(MotorLimitError, match="diverging"):
        g()


def test_divergence_guard_skips_missing_samples():
    g = _guard_over([1.2, None, 1.19])
    for _ in range(3):
        g()  # None sample is skipped, no state change, no trip


def test_divergence_guard_negative_slope_uses_magnitude():
    g = _guard_over([1.2, 1.1, 1.45], slope=-100.0)
    g()
    g()  # min dist 10 deg
    with pytest.raises(MotorLimitError, match="diverging"):
        g()  # 45 deg > 10 + 20
