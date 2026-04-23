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

This module ships under ``src/`` rather than ``tests/`` so that the
eigsep-field CLI (``eigsep-field verify``) can run it on installs that
only have the wheel and no test tree — see ``contract_tests/__init__.py``.
"""

import glob
import tempfile
import time
from pathlib import Path

import numpy as np
import pytest
import yaml
from picohost import PicoPotentiometer
from picohost.base import PicoRFSwitch
from picohost.testing import (
    ImuEmulator,
    LidarEmulator,
    PotMonEmulator,
    RFSwitchEmulator,
    TempCtrlEmulator,
)

from eigsep_redis.keys import STATUS_STREAM  # noqa: F401
from eigsep_redis.testing import DummyTransport

import eigsep_observing
from eigsep_observing import io
from eigsep_observing._test_fixtures import HEADER, IMU_READING
from eigsep_observing.testing import (
    DummyEigsepFpga,
    DummyPandaClient,
)
from eigsep_observing.vna import VnaReader


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


def _rfswitch_post_handler_reading():
    """Return an rfswitch reading after _rfswitch_redis_handler.

    The actual ``rfswitch`` producer is the composition
    ``RFSwitchEmulator.get_status()`` + ``PicoRFSwitch._rfswitch_redis_handler``
    — the emulator only emits the raw ``sw_state`` int, and the device
    handler is where ``sw_state_name`` is added. The contract this test
    enforces is the *post-handler* shape (what reaches Redis), so the
    fixture has to compose the two. Mirrors ``_potmon_post_handler_reading``.
    """
    sw = PicoRFSwitch.__new__(PicoRFSwitch)
    sw._name_by_state = {v: k for k, v in sw.__class__.paths.fget(sw).items()}
    captured = {}
    sw._base_redis_handler = lambda d: captured.update(d)
    sw._rfswitch_redis_handler(RFSwitchEmulator().get_status())
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
    "rfswitch": _rfswitch_post_handler_reading,
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


@pytest.mark.parametrize("mode", sorted(io.VNA_S11_MODE_DATA_KEYS))
def test_measure_s11_publishes_conforming_payload(mode, tmp_path):
    """``PandaClient.measure_s11`` is the producer of the VNA stream.
    Drive the real method end-to-end through DummyPandaClient +
    DummyVNA, read the entry back off the stream, and validate the
    payload against ``VNA_S11_HEADER_SCHEMA`` + the per-mode data
    contract. This is the canonical guard rail against silent drift in
    the s11 header-merge shape (``cal:*`` prefix, ``mode``,
    ``metadata_snapshot_unix``) — a rename on either side fails CI
    immediately. Parametrizes over ``VNA_S11_MODE_DATA_KEYS`` (not a
    literal tuple) so adding a mode without a contract test is
    impossible by construction."""
    cfg_path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    with open(cfg_path) as f:
        cfg = yaml.safe_load(f)
    cfg["use_vna"] = True
    cfg["vna_save_dir"] = str(tmp_path)
    transport = DummyTransport()
    client = DummyPandaClient(transport, default_cfg=cfg)
    try:
        client.measure_s11(mode)
        # The reader skips producer-backlog by design (see
        # test_vna_reader_skips_producer_backlog); rewind to stream
        # origin so this single-threaded test picks up the entry the
        # producer just pushed.
        client.transport._set_last_read_id("stream:vna", "0-0")
        data, header, metadata = VnaReader(client.transport).read(timeout=1)
    finally:
        client.stop()

    data_violations = io._validate_vna_s11_data(data, mode)
    assert data_violations == [], (
        f"measure_s11({mode!r}) data drift: {data_violations}"
    )

    header_violations = io._validate_vna_s11_header(header)
    assert header_violations == [], (
        f"measure_s11({mode!r}) header drift: {header_violations}"
    )

    # The ``mode`` field must round-trip through VnaWriter's JSON
    # encoding unchanged — the test-vs-producer mode arg is the same
    # string the consumer reads back.
    assert header["mode"] == mode

    # ``metadata_snapshot_unix`` must be a plausible wallclock (in
    # seconds, not ms or ns) recorded at publish time. Guarding the
    # unit here catches a producer that accidentally switches to
    # monotonic/ms/ns — which is silently-valid under the float type
    # check but breaks downstream freshness interpretation.
    now = time.time()
    assert abs(header["metadata_snapshot_unix"] - now) < 60.0, (
        f"metadata_snapshot_unix {header['metadata_snapshot_unix']!r} is "
        f"not a recent Unix timestamp (now={now})"
    )

    # Metadata snapshot was captured (rfswitch publishes on startup via
    # DummyPicoRFSwitch). Ensures ``measure_s11`` routes the snapshot
    # reader through to the VNA stream as opposed to dropping it.
    assert isinstance(metadata, dict)


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
        # Mirror observer.py: after construction, overlay the corr
        # header (which carries wiring / pol_delay dict / sync_time
        # from ``EigsepFpga.header``) on top of the cfg-only base.
        f.set_header(header=fpga.header)

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
