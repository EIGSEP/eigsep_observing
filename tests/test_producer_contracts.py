"""Producer / fixture contract conformance tests.

These tests are structurally distinct from the unit tests in
``test_io.py``: they assert that *external producers* (the
``picohost.testing`` emulators, ``DummyEigsepFpga``) and the shared
golden fixtures themselves still satisfy the schemas in
``eigsep_observing.io.SENSOR_SCHEMAS`` / ``CORR_HEADER_SCHEMA``.

When a test in this file fails, the bug is in the *producer* (or in
the schema), not in ``io.py``. Keeping that signal isolated from the
larger ``test_io.py`` makes producer drift immediately legible in CI
output, and gives a natural home for new producer-conformance tests
as more sensors come online.

The end-to-end ``DummyEigsepFpga → File → corr_write → read_hdf5``
round-trip lives here as well: it's the canonical contract test that
ties the producer side to the file format on disk, and conceptually
belongs with the rest of the producer-conformance suite.
"""

import glob
import tempfile
from pathlib import Path

import numpy as np
from picohost.testing import (
    ImuEmulator,
    LidarEmulator,
    RFSwitchWithImuEmulator,
    TempCtrlEmulator,
    TempMonEmulator,
)

from conftest import HEADER, IMU_READING
from eigsep_observing import io
from eigsep_observing.testing import DummyEigsepFpga


def test_test_header_conforms_to_corr_schema():
    """The HEADER fixture used by every test in this file must
    itself satisfy CORR_HEADER_SCHEMA. Catches drift in the fixture."""
    assert io._validate_corr_header(HEADER) == []


def test_test_imu_reading_conforms_to_imu_panda_schema():
    """The IMU_READING fixture used by metadata tests must satisfy
    the imu_panda schema. Catches drift in the fixture."""
    assert (
        io._validate_metadata(IMU_READING, io.SENSOR_SCHEMAS["imu_panda"])
        == []
    )


def test_dummy_eigsep_fpga_header_conforms_to_corr_schema():
    """The DummyEigsepFpga in eigsep_observing/testing must produce
    a header that satisfies CORR_HEADER_SCHEMA. This is the consumer
    asserting its contract on the producer side: any future change
    to the dummy or to the schema that breaks conformance fails CI
    immediately."""
    fpga = DummyEigsepFpga(program=False)
    violations = io._validate_corr_header(fpga.header)
    assert violations == [], f"DummyEigsepFpga header drift: {violations}"


def test_picohost_imu_emulator_conforms_to_schema():
    """picohost.testing.ImuEmulator output must satisfy imu_panda
    schema. Locks in the producer ↔ schema contract."""
    reading = ImuEmulator().get_status()
    assert io._validate_metadata(reading, io.SENSOR_SCHEMAS["imu_panda"]) == []


def test_picohost_lidar_emulator_conforms_to_schema():
    """picohost.testing.LidarEmulator output must satisfy lidar
    schema."""
    reading = LidarEmulator().get_status()
    assert io._validate_metadata(reading, io.SENSOR_SCHEMAS["lidar"]) == []


def test_picohost_tempmon_emulator_conforms_to_schema():
    """picohost.testing.TempMonEmulator output must satisfy temp_mon
    schema."""
    reading = TempMonEmulator().get_status()
    assert io._validate_metadata(reading, io.SENSOR_SCHEMAS["temp_mon"]) == []


def test_picohost_tempctrl_emulator_conforms_to_schema():
    """picohost.testing.TempCtrlEmulator output must satisfy tempctrl
    schema."""
    reading = TempCtrlEmulator().get_status()
    assert io._validate_metadata(reading, io.SENSOR_SCHEMAS["tempctrl"]) == []


def test_picohost_rfswitch_emulator_conforms_to_schemas():
    """picohost.testing.RFSwitchWithImuEmulator returns a list of
    two readings (imu_antenna + rfswitch) that must each validate
    against their respective schemas."""
    readings = RFSwitchWithImuEmulator().get_status()
    assert isinstance(readings, list)
    imu_reading = next(
        r for r in readings if r["sensor_name"] == "imu_antenna"
    )
    rfsw_reading = next(r for r in readings if r["sensor_name"] == "rfswitch")
    assert (
        io._validate_metadata(imu_reading, io.SENSOR_SCHEMAS["imu_antenna"])
        == []
    )
    assert (
        io._validate_metadata(rfsw_reading, io.SENSOR_SCHEMAS["rfswitch"])
        == []
    )


def test_dummy_fpga_header_round_trips_through_file():
    """End-to-end contract enforcement: a DummyEigsepFpga header
    flows through File → corr_write → read_hdf5, and the read-back
    header still satisfies CORR_HEADER_SCHEMA. Catches drift between
    the producer, the file format, and the schema all at once — the
    most load-bearing conformance test in the suite."""
    fpga = DummyEigsepFpga(program=False)
    cfg = fpga.cfg

    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 3
        f = io.File(save_dir, ["0"], ntimes, cfg)

        spec_len = io.data_shape(1, cfg["acc_bins"], cfg["nchan"])[1]
        dtype = np.dtype(cfg["dtype"])
        for i in range(ntimes):
            d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
            f.add_data(i + 1, 0.0, d)
        f._write_queue.join()

        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1
        _, read_header, _ = io.read_hdf5(files[0])

        violations = io._validate_corr_header(read_header)
        assert violations == [], f"round-trip header drift: {violations}"

        f.close()
