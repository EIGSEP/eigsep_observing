from eigsep_redis.testing import DummyTransport

from eigsep_observing.home_ref import publish_home_ref, read_home_ref


def test_read_returns_none_when_unset():
    assert read_home_ref(DummyTransport()) is None


def test_round_trip():
    t = DummyTransport()
    publish_home_ref(t, pot_az_voltage_v0=1.234, imu_el_deg_home=0.5)
    ref = read_home_ref(t)
    assert ref["pot_az_voltage_v0"] == 1.234
    assert ref["imu_el_deg_home"] == 0.5


def test_imu_el_home_may_be_none():
    t = DummyTransport()
    publish_home_ref(t, pot_az_voltage_v0=2.0, imu_el_deg_home=None)
    assert read_home_ref(t)["imu_el_deg_home"] is None
