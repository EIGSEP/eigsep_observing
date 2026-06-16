"""Tests for scripts/record_metadata.py.

The script lives under ``scripts/`` (not on the package path) so we
import it by file location, same pattern as test_imu_manual.py.

The recorder accumulates raw stream entries in memory and writes them
once, in the same JSON list-of-dicts format as a corr file's metadata
sidecar (see test_read_metadata.py for the io.py round-trip). Here we
cover the script's own pieces: the per-drain capture (``_drain_into``),
the end-to-end ``_collect`` loop against live dummy picos, and the
stream-name helper.
"""

import importlib.util
import threading
import time
from pathlib import Path

from eigsep_redis import MetadataStreamReader, MetadataWriter

from eigsep_observing.io import read_metadata_hdf5, write_metadata_hdf5


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
    reader = MetadataStreamReader(transport)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        streams = reader.streams
        if len(streams) >= expected_count:
            return streams
        time.sleep(0.05)
    return reader.streams


def _motor_sample(**overrides):
    sample = {
        "sensor_name": "motor",
        "status": "update",
        "app_id": 5,
        "az_pos": 1.5,
        "az_target_pos": 2.0,
        "el_pos": -3.0,
        "el_target_pos": -3.0,
    }
    sample.update(overrides)
    return sample


def test_drain_folds_in_ts_unix_and_payload(transport):
    """``_drain_into`` appends one dict per Redis entry, stripping the
    ``stream:`` prefix and folding in a float ``_ts_unix`` alongside the
    raw payload.

    The first publish + drain only establishes the stream's read position
    at the tail (the reader skips pre-existing backlog, exactly as the
    live recorder does); the entry published after that is captured.
    """
    rm = _load_record_metadata()
    writer = MetadataWriter(transport)
    reader = MetadataStreamReader(transport)
    collected = {}

    writer.add("motor", _motor_sample())
    rm._drain_into(reader, collected)  # prime position

    writer.add("motor", _motor_sample(az_pos=2.5))
    rm._drain_into(reader, collected)  # capture this one

    assert list(collected) == ["motor"]
    (row,) = collected["motor"]
    assert row["az_pos"] == 2.5
    assert row["sensor_name"] == "motor"
    assert isinstance(row["_ts_unix"], float)
    assert row["_ts_unix"] > 0


def test_collect_captures_dummy_pico_streams(client, tmp_path):
    """End-to-end: dummy picos publish, ``_collect`` drains into memory,
    and the round-tripped file carries a list of sample dicts per stream
    with monotonic ``_ts_unix`` timestamps."""
    rm = _load_record_metadata()
    transport = client.transport

    # Give the embedded PicoManager a moment to register heartbeats and
    # have its devices publish a first round of status updates.
    streams = _wait_for_streams(transport, expected_count=3, timeout=5.0)
    assert streams, "dummy picos never registered metadata streams"

    collected = {}
    stop_event = threading.Event()
    t = threading.Thread(
        target=rm._collect, args=(transport, collected, 0.2, stop_event)
    )
    t.start()
    # 2s is enough for several rows per stream at 200 ms producer cadence
    # (the first drain only primes read positions).
    time.sleep(2.0)
    stop_event.set()
    t.join(timeout=5.0)
    assert not t.is_alive(), "record_metadata thread did not stop"

    out_path = tmp_path / "metadata_test.h5"
    write_metadata_hdf5(out_path, collected)
    data = read_metadata_hdf5(out_path)

    assert "imu_el" in data
    assert "imu_az" in data
    assert any(name.startswith("tempctrl_") for name in data)

    for stream_name, rows in data.items():
        assert rows, f"{stream_name} captured no samples"
        ts = [r["_ts_unix"] for r in rows]
        assert all(isinstance(x, float) for x in ts)
        assert all(b >= a for a, b in zip(ts, ts[1:])), (
            f"{stream_name} timestamps not monotonic: {ts}"
        )


def test_group_name_strips_stream_prefix():
    rm = _load_record_metadata()
    assert rm._group_name("stream:imu_el") == "imu_el"
    assert rm._group_name("imu_el") == "imu_el"
