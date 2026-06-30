import importlib.util
import pathlib
import sys

import pytest

from eigsep_redis.testing import DummyTransport
from picohost.buses import PotCalStore

from eigsep_observing.home_ref import read_home_ref

_spec = importlib.util.spec_from_file_location(
    "field_zero",
    pathlib.Path(__file__).parents[1] / "scripts" / "field_zero.py",
)
field_zero = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(field_zero)


def _make_dummy_transport():
    """Fakeredis-backed transport, per the repo DummyTransport convention."""
    return DummyTransport()


def test_slip_verdict_bands():
    assert field_zero.slip_verdict(1.0, 1.00) == "ok"
    assert field_zero.slip_verdict(1.0, 0.96) == "ok"  # 4% off
    assert field_zero.slip_verdict(1.0, 0.93) == "warn"  # 7% off
    assert field_zero.slip_verdict(1.0, 0.80) == "fail"  # 20% off
    assert field_zero.slip_verdict(0.0, 0.0) == "fail"  # zero expected


def test_rezero_pot_pins_intercept():
    t = _make_dummy_transport()
    PotCalStore(t).upload({"pot_az": [200.0, -999.0]})  # stale intercept

    class FakeProxy:
        def __init__(self):
            self.calls = []

        def send_command(self, *a, **k):
            self.calls.append((a, k))

    proxy = FakeProxy()
    m, b = field_zero.rezero_pot(t, proxy, v0=1.5)
    assert m == 200.0
    assert b == -200.0 * 1.5  # b = -m*v0 = -300.0
    assert PotCalStore(t).get()["pot_az"] == [200.0, -300.0]
    assert proxy.calls
    assert proxy.calls[0][0][0] == "set_calibration"
    assert proxy.calls[0][1]["pot_az_params"] == [200.0, -300.0]


def test_rezero_pot_raises_without_stored_cal():
    t = DummyTransport()

    class FakeProxy:
        def send_command(self, *a, **k):
            pass

    with pytest.raises(RuntimeError):
        field_zero.rezero_pot(t, FakeProxy(), v0=1.5)


def test_run_slip_check_uses_pot_swing():
    volts = iter([2.00, 2.15])  # pot voltage before, after a +move_deg jog

    class Snap:
        def get(self, *a):
            return {"potmon": {"pot_az_voltage": next(volts)}}

    class Motor:
        def jog_az(self, d, **k):
            pass

    verdict, exp, meas = field_zero.run_slip_check(
        Motor(), Snap(), slope_m=200.0, move_deg=30.0
    )
    assert round(exp, 4) == round(30.0 / 200.0, 4)  # 0.15 V expected
    assert round(meas, 4) == 0.15  # 2.15 - 2.00
    assert verdict == "ok"


def test_run_slip_check_aborts_without_pot():
    class Snap:
        def get(self, *a):
            return {"potmon": {}}  # no pot_az_voltage

    class Motor:
        def jog_az(self, d, **k):
            pass

    with pytest.raises(SystemExit):
        field_zero.run_slip_check(Motor(), Snap(), slope_m=200.0)


def test_no_slip_check_flag_parses(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["field_zero", "--no-slip-check"])
    args = field_zero._parse_args()
    assert args.no_slip_check is True


def test_override_limits_flag_parses(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["field_zero", "--override-limits"])
    args = field_zero._parse_args()
    assert args.override_limits is True


def test_write_home_ref_stores_pot_and_imu():
    t = DummyTransport()

    class FakeSnapshot:
        def get(self):
            return {"imu_el": {"el_deg": 0.3}}

    field_zero._write_home_ref(t, FakeSnapshot(), 1.7)
    ref = read_home_ref(t)
    assert ref["pot_az_voltage_v0"] == 1.7
    assert ref["imu_el_deg_home"] == pytest.approx(0.3)
