"""Tests for eigsep_observing.imu_calibration (read-only consumer of the
picohost-written ``imu_calibration`` Redis key)."""

from __future__ import annotations

import json

from eigsep_redis.testing import DummyTransport
from picohost.buses import ImuCalStore
from picohost.keys import IMU_CAL_KEY

from eigsep_observing._test_fixtures import IMU_CALIBRATION
from eigsep_observing.imu_calibration import read_calibration, upload_unix


def test_fixture_survives_picohost_store_roundtrip():
    """The fixture interoperates with picohost's real writer: upload via
    ImuCalStore, read back via our reader, sections unchanged. Pins the
    cross-repo storage contract (not the fitter's field names — those are
    sourced from picohost/imu_geometry.py and cited at the fixture)."""
    t = DummyTransport()
    ImuCalStore(t).upload(IMU_CALIBRATION)
    out = read_calibration(t)
    assert out["imu_el"] == IMU_CALIBRATION["imu_el"]
    assert out["imu_az"] == IMU_CALIBRATION["imu_az"]
    assert out["metadata"] == IMU_CALIBRATION["metadata"]
    # upload_dict injects a fresh upload_time; assert present + float,
    # not its value.
    assert isinstance(out["upload_time"], float)
    assert isinstance(upload_unix(out), float)


def test_read_missing_key_returns_empty():
    assert read_calibration(DummyTransport()) == {}


def test_read_seeded_blob_returns_verbatim():
    t = DummyTransport()
    t.add_raw(IMU_CAL_KEY, json.dumps({**IMU_CALIBRATION, "upload_time": 5.0}))
    out = read_calibration(t)
    assert out["imu_el"] == IMU_CALIBRATION["imu_el"]
    assert out["upload_time"] == 5.0


def test_read_partial_section_roundtrips():
    """A `--mode elevation` fleet stores only imu_el; must not error."""
    t = DummyTransport()
    t.add_raw(IMU_CAL_KEY, json.dumps({"imu_el": IMU_CALIBRATION["imu_el"]}))
    out = read_calibration(t)
    assert set(out) == {"imu_el"}


def test_read_malformed_json_returns_empty(caplog):
    t = DummyTransport()
    t.add_raw(IMU_CAL_KEY, b"not-json")
    with caplog.at_level("WARNING"):
        assert read_calibration(t) == {}
    assert any("malformed imu_calibration" in r.message for r in caplog.records)


def test_read_non_dict_payload_returns_empty():
    t = DummyTransport()
    t.add_raw(IMU_CAL_KEY, json.dumps([1, 2, 3]))
    assert read_calibration(t) == {}


def test_read_swallows_transport_error(caplog):
    class Boom:
        def get_raw(self, key):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        assert read_calibration(Boom()) == {}
    assert any(
        "failed to read imu_calibration" in r.message for r in caplog.records
    )


def test_upload_unix_present_absent_and_junk():
    assert upload_unix({"upload_time": 12.5}) == 12.5
    assert upload_unix({}) == 0.0
    assert upload_unix({"upload_time": "abc"}) == 0.0
