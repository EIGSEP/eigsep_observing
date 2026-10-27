"""Tests for scripts/record_simple_therm.py (imported by file location,
same pattern as test_record_metadata.py)."""

import importlib.util
import threading
from pathlib import Path

import numpy as np
from eigsep_redis import MetadataStreamReader, MetadataWriter

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load():
    path = SCRIPTS_DIR / "record_simple_therm.py"
    spec = importlib.util.spec_from_file_location("record_simple_therm", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sample(temps=(21.5, 22.0, 22.5), die=27.0):
    """Shape of one simple_therm status packet as published to Redis by
    picohost's PicoSimpleTherm (src/simple_therm.c status tick). An
    invalid channel arrives as None (firmware NaN -> JSON null)."""
    sample = {
        "sensor_name": "simple_therm",
        "app_id": 7,
        "status": "update",
        "die_T": die,
    }
    for i, t in enumerate(temps):
        sample[f"therm{i}_T"] = t
        sample[f"therm{i}_voltage"] = 2.5
        sample[f"therm{i}_resistance"] = None if t is None else 10000.0
    return sample


def test_drain_and_save_round_trip(transport, tmp_path):
    rst = _load()
    writer = MetadataWriter(transport)
    reader = MetadataStreamReader(transport)
    log = rst.ThermLog()

    writer.add("simple_therm", _sample())
    rst.drain_into(reader, log)  # prime the stream read position
    writer.add("simple_therm", _sample(temps=(30.0, None, 31.0), die=28.0))
    writer.add("simple_therm", _sample(temps=(30.5, 22.0, 31.5)))
    assert rst.drain_into(reader, log) == 2

    out = tmp_path / "therm.npz"
    log.save(out)
    data = np.load(out)
    assert data["temp_c"].shape == (2, 3)
    np.testing.assert_allclose(data["temp_c"][0], [30.0, np.nan, 31.0])
    assert np.isnan(data["resistance"][0, 1])
    np.testing.assert_allclose(data["die_temp_c"], [28.0, 27.0])
    assert np.all(np.diff(data["t_unix"]) >= 0)
    np.testing.assert_array_equal(data["channel_gpio"], [26, 27, 28])
    assert not (tmp_path / "therm.npz.tmp").exists()


def test_run_saves_periodically_and_on_stop(transport, tmp_path):
    rst = _load()
    writer = MetadataWriter(transport)
    # Stream must exist before run() so skip_to_latest anchors on it.
    writer.add("simple_therm", _sample())
    out = tmp_path / "therm.npz"
    stop = threading.Event()

    def _produce():
        stop.wait(0.1)  # let run() anchor at the stream tail first
        for k in range(10):
            writer.add("simple_therm", _sample(temps=(20.0 + k, 21, 22)))
            stop.wait(0.02)
        stop.wait(0.2)
        stop.set()

    t = threading.Thread(target=_produce)
    t.start()
    log = rst.run(transport, out, 0.02, 0.05, stop)
    t.join()

    assert len(log) == 10
    data = np.load(out)
    np.testing.assert_allclose(data["temp_c"][:, 0], 20.0 + np.arange(10))
