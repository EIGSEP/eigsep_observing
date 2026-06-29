import logging

import redis

from eigsep_observing.el_sensor import ElEstimate, read_el_estimate


class _Reader:
    def __init__(self, snap):
        self._snap = snap

    def get(self, key):
        return self._snap.get(key, {})


def test_prefers_signed_imu_el():
    r = _Reader({"imu_el": {"el_deg": -12.0}, "imu_az": {"el_deg": 12.0}})
    est = read_el_estimate(r)
    assert est == ElEstimate(-12.0, False, "imu_el")


def test_failover_to_imu_az_magnitude():
    r = _Reader({"imu_az": {"el_deg": 7.0}})  # imu_el absent
    est = read_el_estimate(r)
    assert est == ElEstimate(7.0, True, "imu_az")


def test_both_absent_returns_none():
    assert read_el_estimate(_Reader({})) == ElEstimate(None, False, "none")


def test_crosscheck_warns_on_divergence(caplog):
    r = _Reader({"imu_el": {"el_deg": -5.0}, "imu_az": {"el_deg": 40.0}})
    with caplog.at_level(logging.WARNING):
        est = read_el_estimate(r, logger=logging.getLogger("t"))
    assert est.source == "imu_el"  # still trusts signed primary
    assert any("disagree" in rec.message for rec in caplog.records)


def test_crosscheck_silent_when_consistent(caplog):
    r = _Reader({"imu_el": {"el_deg": -5.0}, "imu_az": {"el_deg": 5.2}})
    with caplog.at_level(logging.WARNING):
        read_el_estimate(r, logger=logging.getLogger("t"))
    assert not caplog.records


def test_connection_error_returns_none_sentinel():
    class _ErrorReader:
        def get(self, key):
            raise redis.exceptions.ConnectionError("down")

    assert read_el_estimate(_ErrorReader()) == ElEstimate(None, False, "none")
