import importlib.util
from argparse import Namespace
from pathlib import Path

from eigsep_redis.testing import DummyTransport
from eigsep_observing.motor_limits import read_motor_limits

_REPO = Path(__file__).resolve().parent.parent


def _load(name):
    path = _REPO / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_publishes_limits():
    sml = _load("set_motor_limits")
    t = DummyTransport()
    sml.publish_from_args(
        t,
        az_limits=[-180.0, 180.0],
        el_limits=[-30.0, 30.0],
        pot_az_v=[0.2, 3.1],
        imu_el=None,
    )
    v = read_motor_limits(t)
    assert v["el_limits_deg"] == [-30.0, 30.0]
    assert v["pot_az_v_limits"] == [0.2, 3.1]
    assert v["imu_el_limits_deg"] is None


def test_run_show_prints_limits(capsys):
    sml = _load("set_motor_limits")
    t = DummyTransport()
    sml.publish_from_args(
        t,
        az_limits=[-180.0, 180.0],
        el_limits=[-30.0, 30.0],
        pot_az_v=[0.2, 3.1],
        imu_el=[-30.0, 30.0],
    )
    sml.run(t, Namespace(show=True))
    out = capsys.readouterr().out
    assert "Current motor limits:" in out
    assert "unset" not in out


def test_run_no_pot_fence_maps_to_none():
    sml = _load("set_motor_limits")
    t = DummyTransport()
    sml.run(
        t,
        Namespace(
            show=False,
            az_limits=[-180.0, 180.0],
            el_limits=[-30.0, 30.0],
            pot_az_v=[0.2, 3.1],
            no_pot_fence=True,
            imu_el=None,
            no_imu_fence=True,
        ),
    )
    v = read_motor_limits(t)
    assert v["pot_az_v_limits"] is None
    assert v["imu_el_limits_deg"] is None
