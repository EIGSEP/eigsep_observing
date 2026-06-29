import importlib.util
import pathlib

from eigsep_redis.testing import DummyTransport

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
    from picohost.buses import PotCalStore

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
    assert proxy.calls[0][1]["pot_az_params"] == [200.0, -300.0]
