from eigsep_redis.testing import DummyTransport
from eigsep_observing.motor_limits import (
    publish_motor_limits,
    read_motor_limits,
)


def test_read_none_when_unset():
    assert read_motor_limits(DummyTransport()) is None


def test_round_trip():
    t = DummyTransport()
    publish_motor_limits(
        t,
        az_limits_deg=[-180.0, 180.0],
        el_limits_deg=[-30.0, 30.0],
        pot_az_v_limits=[0.2, 3.1],
        imu_el_limits_deg=None,
    )
    v = read_motor_limits(t)
    assert v["az_limits_deg"] == [-180.0, 180.0]
    assert v["el_limits_deg"] == [-30.0, 30.0]
    assert v["pot_az_v_limits"] == [0.2, 3.1]
    assert v["imu_el_limits_deg"] is None
