"""Smoke tests for scripts/record_metadata.py.

The script lives under ``scripts/`` (not on the package path) so we
import it by file location, same pattern as test_motor_scripts.py.

We drive ``_run`` against a ``DummyTransport`` with an active
``DummyPandaClient`` publishing pico metadata, then inspect the
resulting HDF5 file. The picos publish at the picohost cadence
(``STATUS_CADENCE_MS = 200``), so a short window already yields
several rows per stream.
"""

import importlib.util
import threading
import time
from pathlib import Path

import h5py
import numpy as np

from eigsep_observing.io import SENSOR_SCHEMAS


SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load_record_metadata():
    path = SCRIPTS_DIR / "record_metadata.py"
    spec = importlib.util.spec_from_file_location("record_metadata", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _wait_for_streams(transport, expected_count, timeout=5.0):
    """Block until at least ``expected_count`` metadata streams are
    registered, or ``timeout`` elapses."""
    from eigsep_redis import MetadataStreamReader

    reader = MetadataStreamReader(transport)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        streams = reader.streams
        if len(streams) >= expected_count:
            return streams
        time.sleep(0.05)
    return reader.streams


def test_record_metadata_captures_dummy_pico_streams(client, tmp_path):
    """End-to-end: the dummy picos publish, ``_run`` drains, and the
    HDF5 file carries one group per stream with monotonic timestamps."""
    rm = _load_record_metadata()
    transport = client.transport

    # Give the embedded PicoManager a moment to register heartbeats
    # and have its devices publish a first round of status updates.
    streams = _wait_for_streams(transport, expected_count=3, timeout=5.0)
    assert streams, "dummy picos never registered metadata streams"

    out_path = tmp_path / "metadata_test.h5"
    stop_event = threading.Event()

    def _runner():
        rm._run(transport, out_path, interval=0.2, stop_event=stop_event)

    t = threading.Thread(target=_runner)
    t.start()
    # 2s is enough for several rows per stream at 200 ms producer cadence.
    time.sleep(2.0)
    stop_event.set()
    t.join(timeout=5.0)
    assert not t.is_alive(), "record_metadata thread did not stop"

    # File should be readable and contain at least the IMU + one tempctrl
    # group, with non-empty datasets and strictly monotonic timestamps.
    with h5py.File(out_path, "r") as f:
        group_names = list(f.keys())
        assert "imu_el" in group_names
        assert "imu_az" in group_names
        assert any(g.startswith("tempctrl_") for g in group_names)

        for stream_name in group_names:
            grp = f[stream_name]
            assert "_ts_unix" in grp, f"{stream_name} missing _ts_unix"
            ts = grp["_ts_unix"][:]
            assert ts.size > 0, f"{stream_name} has no samples"
            assert ts.dtype == np.float64
            assert np.all(np.diff(ts) >= 0), (
                f"{stream_name} timestamps not monotonic: {ts}"
            )


def test_record_metadata_uses_sensor_schema_dtypes(client, tmp_path):
    """Schema-driven dtypes: ``imu_el`` declares all floats + a few
    str/int fields, so the resulting datasets should match."""
    rm = _load_record_metadata()
    transport = client.transport

    _wait_for_streams(transport, expected_count=3, timeout=5.0)

    out_path = tmp_path / "metadata_schema.h5"
    stop_event = threading.Event()

    def _runner():
        rm._run(transport, out_path, interval=0.2, stop_event=stop_event)

    t = threading.Thread(target=_runner)
    t.start()
    time.sleep(2.0)
    stop_event.set()
    t.join(timeout=5.0)

    schema = SENSOR_SCHEMAS["imu_el"]
    with h5py.File(out_path, "r") as f:
        grp = f["imu_el"]
        for field, py_type in schema.items():
            assert field in grp, f"imu_el missing schema field {field!r}"
            dset = grp[field]
            if py_type is float:
                assert dset.dtype == np.float64
            elif py_type is int:
                assert dset.dtype == np.int64
            elif py_type is bool:
                assert dset.dtype == np.uint8
            elif py_type is str:
                assert h5py.check_string_dtype(dset.dtype) is not None


def test_stream_writer_handles_lazy_field(tmp_path):
    """A field that wasn't in the schema appears mid-stream; back-fill
    rows should carry sentinels so per-row indices align with _ts_unix."""
    rm = _load_record_metadata()
    out = tmp_path / "lazy.h5"
    with h5py.File(out, "w") as f:
        w = rm._StreamWriter(f, "stream_a", schema={"x": float})
        w.append(1000.0, {"x": 1.0})
        w.append(1001.0, {"x": 2.0, "y": "hello"})  # 'y' is new
        w.append(1002.0, {"x": 3.0, "y": "world"})

        grp = f["stream_a"]
        assert grp["_ts_unix"].shape == (3,)
        assert grp["x"].shape == (3,)
        assert grp["y"].shape == (3,)
        # Row 0 had no 'y'; should be sentinel (empty string for str dtype).
        y_vals = grp["y"][:]
        # h5py returns variable-length strings as bytes by default.
        assert y_vals[0] in (b"", "")
        assert y_vals[1] in (b"hello", "hello")
        assert y_vals[2] in (b"world", "world")


def test_group_name_strips_stream_prefix():
    rm = _load_record_metadata()
    assert rm._group_name("stream:imu_el") == "imu_el"
    assert rm._group_name("imu_el") == "imu_el"
