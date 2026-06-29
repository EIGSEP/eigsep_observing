import importlib.util
import pathlib

_spec = importlib.util.spec_from_file_location(
    "field_zero",
    pathlib.Path(__file__).parents[1] / "scripts" / "field_zero.py",
)
field_zero = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(field_zero)


def test_slip_verdict_bands():
    assert field_zero.slip_verdict(1.0, 1.00) == "ok"
    assert field_zero.slip_verdict(1.0, 0.96) == "ok"  # 4% off
    assert field_zero.slip_verdict(1.0, 0.93) == "warn"  # 7% off
    assert field_zero.slip_verdict(1.0, 0.80) == "fail"  # 20% off
    assert field_zero.slip_verdict(0.0, 0.0) == "fail"  # zero expected
