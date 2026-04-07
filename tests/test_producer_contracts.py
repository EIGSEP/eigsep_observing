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
import pytest
from picohost import PicoPotentiometer
from picohost.testing import (
    ImuEmulator,
    LidarEmulator,
    PotMonEmulator,
    RFSwitchEmulator,
    TempCtrlEmulator,
)

from conftest import HEADER, IMU_READING
from eigsep_observing import io
from eigsep_observing.testing import DummyEigsepFpga


def _potmon_post_handler_reading():
    """Return a calibrated potmon reading after _pot_redis_handler.

    The actual ``potmon`` producer is the composition
    ``PotMonEmulator.get_status()`` + ``PicoPotentiometer._pot_redis_handler``
    — the emulator only emits raw voltages, and the device handler is
    where the calibration slope/intercept and the derived angle are
    added. The contract this test enforces is the *post-handler* shape
    (what reaches Redis), not the raw emulator shape, so the fixture
    has to compose the two.

    We bypass ``PicoPotentiometer.__init__`` (which would open a real
    serial port) by constructing the instance via ``__new__`` and
    populating just the attributes ``_pot_redis_handler`` reads. The
    base handler is stubbed to capture the published dict. A calibrated
    reading is used so every cal/angle field is exercised as a real
    float — an uncalibrated reading would publish ``None`` for those
    fields, which the validator silently passes (per ``_validate_metadata``
    short-circuit on ``None``) but tells us less about producer drift.
    """
    pot = PicoPotentiometer.__new__(PicoPotentiometer)
    pot._cal = {
        "pot_el": (100.0, -50.0),
        "pot_az": (200.0, -100.0),
    }
    captured = {}
    pot._base_redis_handler = lambda d: captured.update(d)
    raw = PotMonEmulator().get_status()
    pot._pot_redis_handler(raw)
    return captured


# Registry mapping each sensor in SENSOR_SCHEMAS to a zero-arg callable
# that returns a single fresh reading from the corresponding picohost
# emulator. Used by test_every_schema_has_conforming_emulator below to
# mechanically enforce that adding an entry to SENSOR_SCHEMAS without a
# real-producer contract test is impossible: the parametrized test runs
# over SENSOR_SCHEMAS keys, so a missing registration fails CI loudly.
SENSOR_EMULATORS = {
    "imu_el": lambda: ImuEmulator(app_id=3).get_status(),
    "imu_az": lambda: ImuEmulator(app_id=6).get_status(),
    "rfswitch": lambda: RFSwitchEmulator().get_status(),
    "tempctrl": lambda: TempCtrlEmulator().get_status(),
    "lidar": lambda: LidarEmulator().get_status(),
    "potmon": _potmon_post_handler_reading,
}


def test_test_header_conforms_to_corr_schema():
    """The HEADER fixture used by every test in this file must
    itself satisfy CORR_HEADER_SCHEMA. Catches drift in the fixture."""
    assert io._validate_corr_header(HEADER) == []


def test_test_imu_reading_conforms_to_imu_el_schema():
    """The IMU_READING fixture used by metadata tests must satisfy
    the imu_el schema. Catches drift in the fixture."""
    assert (
        io._validate_metadata(IMU_READING, io.SENSOR_SCHEMAS["imu_el"]) == []
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


@pytest.mark.parametrize("sensor_name", sorted(io.SENSOR_SCHEMAS))
def test_every_schema_has_conforming_emulator(sensor_name):
    """For every entry in SENSOR_SCHEMAS, a real picohost emulator
    must produce output that validates against it. The parametrize
    iterates over SENSOR_SCHEMAS (not SENSOR_EMULATORS), so adding a
    schema without registering an emulator fails CI by construction —
    the rule "if you add a sensor, add a contract test" is enforced
    mechanically rather than by reviewer discipline."""
    assert sensor_name in SENSOR_EMULATORS, (
        f"SENSOR_SCHEMAS has '{sensor_name}' but no emulator is "
        f"registered in SENSOR_EMULATORS. Add one so producer drift "
        f"in this sensor is caught by CI."
    )
    reading = SENSOR_EMULATORS[sensor_name]()
    violations = io._validate_metadata(reading, io.SENSOR_SCHEMAS[sensor_name])
    assert violations == [], f"{sensor_name} producer drift: {violations}"


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
