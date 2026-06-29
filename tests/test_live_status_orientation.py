"""Test compute_orientation for live-status antenna pointing panel."""

from eigsep_observing.live_status import orientation


def test_orientation_signals_registered():
    from eigsep_observing.live_status.signals import SIGNAL_REGISTRY

    for name in (
        "orientation.az_consensus_deg",
        "orientation.el_consensus_deg",
        "orientation.az_spread_deg",
        "orientation.el_spread_deg",
    ):
        assert name in SIGNAL_REGISTRY
    # spread signals disable staleness (recomputed each tick)
    assert SIGNAL_REGISTRY["orientation.az_spread_deg"].max_age_s is None
    assert SIGNAL_REGISTRY["orientation.el_spread_deg"].max_age_s is None


def test_compute_orientation_consensus_and_spread():
    meta = {
        "motor": {"value": {"az_pos": 8000.0, "el_pos": 960.0}},
        "potmon": {"value": {"pot_az_angle": 100.5}},
        "imu_az": {"value": {"az_deg": 100.0, "el_deg": 12.0}},
        "imu_el": {"value": {"el_deg": 12.4}},
    }
    out = orientation.compute_orientation(
        meta, steps_to_deg=lambda s: s / 80.0
    )
    # az sources: motor 8000/80=100.0, potmon 100.5, imu_az 100.0
    assert out["az"]["motor"] == 100.0
    assert out["az"]["potmon"] == 100.5
    assert out["az"]["consensus"] == 100.0  # median([100.0,100.5,100.0])
    assert round(out["az"]["spread"], 3) == 0.5  # 100.5 - 100.0
    # el sources: motor 960/80=12.0, imu_az 12.0, imu_el 12.4
    assert out["el"]["consensus"] == 12.0
    assert round(out["el"]["spread"], 3) == 0.4


def test_compute_orientation_omits_missing():
    meta = {"imu_el": {"value": {"el_deg": 12.4}}}  # only one el source
    out = orientation.compute_orientation(
        meta, steps_to_deg=lambda s: s / 80.0
    )
    assert out["az"] == {}  # no az sources
    assert out["el"]["consensus"] == 12.4
    assert out["el"]["spread"] is None  # <2 sources
