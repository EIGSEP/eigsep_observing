"""Tests for eigsep_observing.imu_calibration (read-only consumer of the
picohost-written ``imu_calibration`` Redis key)."""

from __future__ import annotations

from eigsep_redis.testing import DummyTransport
from picohost.buses import ImuCalStore

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
