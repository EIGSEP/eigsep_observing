"""Unit tests for the standby toggle helper."""

from eigsep_observing.standby import STANDBY_DEVICES, set_standby


class _FakeProxy:
    def __init__(self, result=None, exc=None):
        self.result = result
        self.exc = exc
        self.calls = []

    def send_command(self, action):
        self.calls.append(action)
        if self.exc is not None:
            raise self.exc
        return self.result


def test_standby_devices_are_the_three_rfi_sensors():
    assert STANDBY_DEVICES == ("imu_el", "imu_az", "lidar")


def test_set_standby_on_sends_standby():
    p = _FakeProxy(result={})
    assert set_standby(p, True) == "ok"
    assert p.calls == ["standby"]


def test_set_standby_off_sends_resume():
    p = _FakeProxy(result={})
    assert set_standby(p, False) == "ok"
    assert p.calls == ["resume"]


def test_set_standby_unavailable_when_proxy_returns_none():
    assert set_standby(_FakeProxy(result=None), True) == "unavailable"


def test_set_standby_reports_error_on_runtime_error():
    msg = set_standby(_FakeProxy(exc=RuntimeError("boom")), True)
    assert msg.startswith("err RuntimeError")


def test_set_standby_reports_error_on_timeout():
    msg = set_standby(_FakeProxy(exc=TimeoutError("slow")), False)
    assert msg.startswith("err TimeoutError")
