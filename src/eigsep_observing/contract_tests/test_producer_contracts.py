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
import json
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
import yaml
from picohost import PicoPotentiometer
from picohost.base import (
    PicoIMU,
    PicoLidar,
    PicoPeltier,
    PicoRFSwitch,
    redis_handler,
)
from picohost.motor import PicoMotor
from picohost.testing import (
    ImuEmulator,
    LidarEmulator,
    MotorEmulator,
    PotMonEmulator,
    RFSwitchEmulator,
    TempCtrlEmulator,
)

from eigsep_redis.keys import STATUS_STREAM  # noqa: F401
from eigsep_redis.testing import DummyTransport

import eigsep_observing
from eigsep_observing import io
from eigsep_observing._test_fixtures import (
    HEADER,
    IMU_AZ_READING,
    IMU_READING,
    tempctrl_post_handler_reading,
)
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
        "pot_az": (200.0, -100.0),
    }
    captured = {}
    pot._base_redis_handler = lambda d: captured.update(d)
    raw = PotMonEmulator().get_status()
    pot._pot_redis_handler(raw)
    return captured


def _motor_post_handler_reading():
    """Return a motor reading after the full publish boundary.

    The actual ``motor`` producer is the composition
    ``MotorEmulator.get_status()`` + ``PicoMotor._motor_redis_handler``
    + the ``picohost.base.redis_handler`` publish closure. The emulator
    (and the C firmware) emit position fields as ``int`` (raw step
    counts); since picohost 4.3 the float cast lives in the publish
    closure (``PicoMotor._REDIS_FLOAT_FIELDS``), not in
    ``_motor_redis_handler`` itself, so the consumer-side reduction
    picks the float→mean policy rather than int→min. The contract this
    test enforces is the shape that reaches Redis, so the fixture
    composes all three, with a capture-only writer standing in for the
    ``MetadataWriter``. Mirrors ``_potmon_post_handler_reading``.
    """

    class _CaptureWriter:
        def add(self, name, data):
            captured.update(data)

    motor = PicoMotor.__new__(PicoMotor)
    captured = {}
    motor._base_redis_handler = redis_handler(
        _CaptureWriter(), PicoMotor._REDIS_FLOAT_FIELDS
    )
    # The handler also drives the position checkpoint / boot-detection
    # path (picohost 3.7.0); a None store makes it inert. This test is
    # about the published payload shape, not the checkpoint logic —
    # picohost's test_motor_position.py covers that.
    motor._motor_pos_store = None
    motor._motor_redis_handler(MotorEmulator().get_status())

    for field in (
        "az_pos",
        "az_target_pos",
        "el_pos",
        "el_target_pos",
    ):
        value = captured.get(field)
        assert value is None or isinstance(value, float), (
            f"{field} must be float or None after "
            f"_motor_redis_handler, got {type(value).__name__}"
        )
    return captured


def _peltier_post_handler_reading(stream_name):
    """Return one per-channel tempctrl reading after _peltier_redis_handler.

    Delegates to ``tempctrl_post_handler_reading`` in ``_test_fixtures``
    so the contract test and the shared golden fixtures (and the inline
    fixtures in ``test_io.py``) are anchored to a single emulator-derived
    source. Mirrors ``_potmon_post_handler_reading``.
    """
    return tempctrl_post_handler_reading(stream_name)


def _rfswitch_post_handler_readings():
    """Return [rfswitch, rfswitch_therm] after _rfswitch_redis_handler.

    The rfswitch producer fans the three PCB thermistors out of the
    switch-state line into a separate rfswitch_therm stream (raw volts +
    host-derived degrees C), the same two-publish shape as
    PicoLidar -> system_current. Compose RFSwitchEmulator.get_status()
    through the real handler and capture both publishes in order. Mirrors
    _lidar_post_handler_readings.
    """
    sw = PicoRFSwitch.__new__(PicoRFSwitch)
    sw._name_by_state = {v: k for k, v in sw.__class__.paths.fget(sw).items()}
    captured = []
    sw._base_redis_handler = lambda d: captured.append(dict(d))
    sw._rfswitch_redis_handler(RFSwitchEmulator().get_status())
    assert len(captured) == 2, (
        f"expected rfswitch + rfswitch_therm publishes, got {len(captured)}"
    )
    return captured[0], captured[1]


def _lidar_post_handler_readings():
    """Return (lidar_dict, system_current_dict) after _lidar_redis_handler.

    The lidar Pico's firmware emits one merged line (distance +
    current_voltage); ``PicoLidar._lidar_redis_handler`` splits it into
    two metadata publishes — ``metadata['lidar']`` (distance, current
    stripped) and ``metadata['system_current']`` (current_voltage +
    derived current_a). The contract these tests enforce is the
    post-handler shape of each, so compose ``LidarEmulator.get_status()``
    through the real handler and capture both publishes in order. Mirrors
    ``_rfswitch_post_handler_readings`` / ``_motor_post_handler_reading``.
    ``_current_cal`` is normally set in ``PicoLidar.__init__`` (two-point
    current cal, picohost); ``__new__`` bypasses that, so set a measured cal
    here to exercise the calibrated system_current shape — same bypass pattern
    as ``_motor_post_handler_reading`` setting ``_motor_pos_store = None``.
    """
    lidar = PicoLidar.__new__(PicoLidar)
    # A measured cal in the stored amps-vs-volts form (slope A/V, intercept A)
    # so the system_current entry carries float cal scalars — the calibrated
    # post-handler shape, mirroring _potmon_post_handler_reading. The
    # uncalibrated (all-None) shape is covered by picohost's
    # TestLidarRedisHandler and the reduction tests in test_io.py.
    # (picohost >= 3.11 has no nominal fallback: None cal -> None.)
    lidar._current_cal = (8.4223, -12.5248)
    captured = []
    lidar._base_redis_handler = lambda d: captured.append(dict(d))
    lidar._lidar_redis_handler(LidarEmulator().get_status())
    assert len(captured) == 2, (
        f"expected lidar + system_current publishes, got {len(captured)}"
    )
    return captured[0], captured[1]


def _adc_stats_post_publish_reading():
    """Return an adc_stats reading after EigsepFpga._publish_adc_stats.

    adc_stats isn't a picohost emulator — it's produced by
    ``EigsepFpga._publish_adc_stats``, which reads the on-FPGA
    ``rms_levels`` register via ``Input.get_stats`` and composes the
    per-core payload. The contract this test enforces is the
    *post-publish* Redis shape, so the fixture drives the real method
    end-to-end and reads the payload back off the transport hash.
    Mirrors the _potmon / _rfswitch post-handler pattern — same
    "compose producer + real publish path to get the Redis shape"
    idea, with an FPGA-side producer in place of a pico.
    """
    fpga = DummyEigsepFpga(program=False)
    # Synthetic but schema-realistic: 12 cores × 3 stats, shape matches
    # the real Input.get_stats(sum_cores=False) return.
    means = np.linspace(-0.1, 0.1, 12)
    powers = np.linspace(5.0, 15.0, 12)
    rmss = np.sqrt(powers)
    with patch.object(
        fpga.inp, "get_stats", return_value=(means, powers, rmss)
    ):
        fpga._publish_adc_stats()
    raw = fpga.transport.r.hget("metadata", "adc_stats")
    return json.loads(raw.decode("utf-8"))


def _imu_post_handler_reading(name, app_id):
    """Return an IMU reading after PicoIMU._imu_redis_handler.

    Composes ImuEmulator.get_status() with the real handler, which adds
    the calibrate-imu derived fields (el_deg for both; imu_az's is
    |theta|). The handler is run UNCALIBRATED, so derived fields are
    None — the genuine state of a freshly-deployed, not-yet-calibrated
    IMU, which is what the schema's None-when-uncalibrated contract
    describes. Mirrors _potmon_post_handler_reading (bypass __init__,
    capture via _base_redis_handler). The float-valued reduction path
    is covered separately by the round-trip test in test_io.py.
    """
    raw = ImuEmulator(app_id=app_id).get_status()
    raw.setdefault("sensor_name", name)
    imu = PicoIMU.__new__(PicoIMU)
    imu._imu_cal = {}
    captured = {}
    imu._base_redis_handler = lambda d: captured.update(d)
    imu._imu_redis_handler(raw)
    return captured


# Registry mapping each sensor in SENSOR_SCHEMAS to a zero-arg callable
# that returns a single fresh reading from the corresponding producer.
# Most entries are picohost emulators; ``adc_stats`` is an FPGA-side
# adapter (register read + compose-in-publish) but structurally behaves
# like a pico from the schema's perspective, so it lives in the same
# registry. Used by test_every_schema_has_conforming_emulator below to
# mechanically enforce that adding an entry to SENSOR_SCHEMAS without a
# real-producer contract test is impossible: the parametrized test runs
# over SENSOR_SCHEMAS keys, so a missing registration fails CI loudly.
SENSOR_EMULATORS = {
    "imu_el": lambda: _imu_post_handler_reading("imu_el", 3),
    "imu_az": lambda: _imu_post_handler_reading("imu_az", 6),
    "rfswitch": lambda: _rfswitch_post_handler_readings()[0],
    "rfswitch_therm": lambda: _rfswitch_post_handler_readings()[1],
    "tempctrl_lna": lambda: _peltier_post_handler_reading("tempctrl_lna"),
    "tempctrl_load": lambda: _peltier_post_handler_reading("tempctrl_load"),
    "lidar": lambda: _lidar_post_handler_readings()[0],
    "potmon": _potmon_post_handler_reading,
    "motor": _motor_post_handler_reading,
    "adc_stats": _adc_stats_post_publish_reading,
    "system_current": lambda: _lidar_post_handler_readings()[1],
}


def test_uninstalled_tempctrl_channel_publishes_nothing():
    """Descope contract: a channel whose firmware ``installed`` flag is
    false is fanned out as *nothing* — clean stream absence downstream
    (no corr-file column, no snapshot staleness warnings) rather than a
    permanent error stream off the dead thermistor divider. The
    surviving channel still conforms to its schema, and the installed
    flag itself never enters the published per-channel shape, so
    ``_PELTIER_SCHEMA`` is untouched by the descope feature."""
    pel = PicoPeltier.__new__(PicoPeltier)
    captured = []
    pel._base_redis_handler = lambda d: captured.append(dict(d))
    emu = TempCtrlEmulator()
    emu.server({"LNA_installed": 0})
    emu.op()
    pel._peltier_redis_handler(emu.get_status())
    assert [e["sensor_name"] for e in captured] == ["tempctrl_load"]
    entry = captured[0]
    assert "installed" not in entry
    assert (
        io._validate_metadata(entry, io.SENSOR_SCHEMAS["tempctrl_load"]) == []
    )


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


def test_test_imu_az_reading_conforms_to_imu_az_schema():
    """The IMU_AZ_READING fixture is shared across VNA_METADATA,
    CORR_METADATA, test_io.py, and conftest.py; pin it against the
    imu_az schema so drift in one fixture can't silently rot every
    test that touches it."""
    assert (
        io._validate_metadata(IMU_AZ_READING, io.SENSOR_SCHEMAS["imu_az"])
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
    # Consumer dispatch in `avg_metadata` keys SENSOR_SCHEMAS by the
    # producer-emitted ``sensor_name`` field, not by the stream name.
    # If the producer drifts away from the schema key, validation is
    # silently skipped — surfacing only as the "No schema for sensor X"
    # warning at runtime. Pin the convention here so the mismatch
    # fails CI loudly instead.
    assert reading.get("sensor_name") == sensor_name, (
        f"Producer for '{sensor_name}' emits sensor_name="
        f"{reading.get('sensor_name')!r}; consumer dispatch keys on "
        f"this value, so a mismatch causes silent validation skip."
    )
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
    transport = DummyTransport()
    client = DummyPandaClient(transport, cfg=cfg)
    try:
        with client.vna_session():
            client.measure_s11(mode)
        # The reader skips producer-backlog by design (see
        # test_vna_reader_skips_producer_backlog); rewind to stream
        # origin so this single-threaded test picks up the entry the
        # producer just pushed.
        client.transport.set_last_read_id("stream:vna", "0-0")
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
