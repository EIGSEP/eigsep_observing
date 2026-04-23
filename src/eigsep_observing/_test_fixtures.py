"""Golden fixtures for the eigsep_observing test suite and for the
producer-contract tests shipped under ``eigsep_observing.contract_tests``.

These constants and helpers are deliberately constructed to mirror the
shape and types of real production data. See the "Testing philosophy"
section in CLAUDE.md: fixtures should look like what producers actually
emit so tests catch contract drift, and any deviations should be called
out explicitly.

They live under ``src/`` (rather than in ``tests/conftest.py``) so the
producer-contract suite can ship inside the installed wheel — the
eigsep-field CLI runs it via ``pytest --pyargs
eigsep_observing.contract_tests`` on nodes that only have the wheel
(the Pi), not the test tree. The leading underscore marks this module
as private: it is not part of the supported public API of
eigsep_observing and its shape can change without a deprecation cycle.
The in-repo ``tests/conftest.py`` re-exports from here so existing
``from conftest import HEADER`` imports in ``test_io.py`` keep working
unchanged.

These are kept as plain module-level constants rather than
``@pytest.fixture`` functions because they are referenced from inside
nested data structures (e.g. ``CORR_METADATA``) and from helper module
imports — both of which the parameter-injection style does not support
without significant test rewrites.
"""

import numpy as np

# One corr file accumulates NTIMES integrations, each of duration
# INTEGRATION_TIME seconds. FILE_TIME = NTIMES * INTEGRATION_TIME is the
# wall-clock duration of the file and is included in HEADER so the
# relationship is explicit at the fixture level (rather than being two
# independently-set numbers that can drift apart).
NTIMES = 60
INTEGRATION_TIME = 1.0  # seconds
FILE_TIME = NTIMES * INTEGRATION_TIME  # seconds

# HEADER mimics EigsepFpga.header: the static-configuration portion of a
# corr file. Units match corr_config.yaml: sample_rate is in MHz (NOT Hz).
#
# ``pol_delay`` is a nested dict (one key per pol-pair) matching the
# shape emitted by the real header property — not three flat keys.
# ``wiring`` is the hardware manifest (split out of the old ``rf_chain``
# key in corr_config.yaml); a single antenna with no ``pam:`` block is
# included so consumers exercise the PAM-absent code path.
HEADER = {
    "dtype": ">i4",
    "acc_bins": 2,
    "avg_even_odd": True,
    "nchan": 1024,
    "fpg_file": "fpg_files/eigsep_fengine.fpg",
    "fpg_version": [0, 0],
    "corr_acc_len": 2**28,
    "corr_scalar": 2**9,
    "pol_delay": {"01": 0, "23": 0, "45": 0},
    "fft_shift": 0x00FF,
    "sample_rate": 500.0,  # MHz, matching corr_config.yaml convention
    "adc_gain": 4,
    "wiring": {
        "snap_id": "C000069",
        "ants": {
            "viv1-N": {
                "fem": {"id": 32, "pol": "N"},
                "snap": {"input": 2, "label": "N4"},
            },
        },
    },
    "sync_time": 1748732903.4203713,
    "integration_time": INTEGRATION_TIME,
    "file_time": FILE_TIME,
}

# Schema-conformant raw IMU reading (as emitted by a pico and pushed into
# stream:imu_el by picohost). Mirrors the BNO085 UART RVC payload
# introduced in picohost 1.0.0: yaw/pitch/roll orientation in degrees and
# accel_x/y/z in m/s². Used to build CORR_METADATA entries and by tests
# that feed raw stream data into File.add_data.
IMU_READING = {
    "sensor_name": "imu_el",
    "status": "update",
    "app_id": 3,
    "yaw": 0.0,
    "pitch": 0.0,
    "roll": 0.0,
    "accel_x": 0.0,
    "accel_y": 0.0,
    "accel_z": 9.81,
}


def _imu_avg_entry(yaw):
    """One per-sample IMU entry as avg_metadata would emit it.

    All numeric fields are float and take the float→mean reduction in
    ``_avg_sensor_values``. ``yaw`` is the per-sample varying axis used
    by tests that need to assert on a non-constant float field.
    """
    return {
        "sensor_name": "imu_el",
        "status": "update",
        "app_id": 3,
        "yaw": yaw,
        "pitch": 0.0,
        "roll": 0.0,
        "accel_x": 0.0,
        "accel_y": 0.0,
        "accel_z": 9.81,
    }


def _lidar_avg_entry(distance_m):
    """One per-sample lidar entry as avg_metadata would emit it."""
    return {
        "sensor_name": "lidar",
        "status": "update",
        "app_id": 4,
        "distance_m": distance_m,
    }


def _tempctrl_channel_entry(t_now, timestamp):
    """One per-sample tempctrl channel (LNA or LOAD) after add_data's split.

    The top-level fields returned by ``_avg_temp_metadata`` are dropped
    by the LNA/LOAD split in ``File.add_data``; only the per-channel
    sub-dict survives. The bool/float fault flags are constant in this
    fixture (steady-state operation); tests that need to exercise
    fault-flag transitions construct their own samples. ``T_target`` is
    a user-configured setpoint that is constant over the lifetime of a
    real run, so it is hard-coded to ``25.0`` here rather than tracking
    the per-sample ``t_now`` — matching the pattern used by every
    inline tempctrl fixture in ``test_io.py``.
    """
    return {
        "status": "update",
        "T_now": t_now,
        "timestamp": timestamp,
        "T_target": 25.0,
        "drive_level": 0.0,
        "enabled": True,
        "active": True,
        "int_disabled": False,
        "hysteresis": 0.5,
        "clamp": 100.0,
    }


def _potmon_avg_entry(pot_el_voltage):
    """One per-sample potmon entry as avg_metadata would emit it.

    Mirrors the post-``_pot_redis_handler`` shape that lands in Redis:
    raw voltages plus the flattened cal slope/intercept and the derived
    angle. All-scalar per the picohost scalar-only contract; the cal
    fields are de-facto invariants for the lifetime of a stream.

    A *calibrated* reading is used here so every cal/angle field is a
    real float, exercising the float→mean reduction in
    ``_avg_sensor_values``. The uncalibrated-stream case (cal/angle
    fields all ``None``) is a first-class producer state — see the
    ``potmon`` schema comment in ``io.py`` — but it is intentionally
    not exercised by this golden fixture because it would force the
    round-trip assertion to special-case ``None`` survivors and obscure
    the steady-state contract this fixture is meant to pin. Tests that
    need to cover the uncalibrated path should build their own samples.
    Same rationale as ``_potmon_post_handler_reading`` in
    ``test_producer_contracts.py``.
    """
    return {
        "sensor_name": "potmon",
        "status": "update",
        "app_id": 2,
        "pot_el_voltage": pot_el_voltage,
        "pot_az_voltage": 1.5,
        "pot_el_cal_slope": 100.0,
        "pot_el_cal_intercept": -50.0,
        "pot_az_cal_slope": 200.0,
        "pot_az_cal_intercept": -100.0,
        "pot_el_angle": 100.0 * pot_el_voltage - 50.0,
        "pot_az_angle": 200.0 * 1.5 - 100.0,
    }


ERROR_INTEGRATION_INDEX = 30


def _imu_errored_integration_entry(yaw):
    return {**_imu_avg_entry(yaw), "status": "error"}


CORR_METADATA = {
    "imu_el": [
        _imu_avg_entry(0.001 * i)
        if i != ERROR_INTEGRATION_INDEX
        else _imu_errored_integration_entry(0.001 * i)
        for i in range(NTIMES)
    ],
    "imu_az": [
        {**_imu_avg_entry(0.002 * i), "sensor_name": "imu_az", "app_id": 6}
        for i in range(NTIMES)
    ],
    "lidar": [_lidar_avg_entry(1.5 + 0.001 * i) for i in range(NTIMES)],
    "potmon": [_potmon_avg_entry(1.5 + 0.001 * i) for i in range(NTIMES)],
    "tempctrl_lna": [
        _tempctrl_channel_entry(30.0 + 0.01 * i, 1.0 + i)
        for i in range(NTIMES)
    ],
    "tempctrl_load": [
        _tempctrl_channel_entry(25.0 + 0.01 * i, 1.0 + i)
        for i in range(NTIMES)
    ],
    "rfswitch": (
        ["RFANT"] * 20  # steady state
        + ["UNKNOWN"] * 5  # transition window
        + ["RFNOFF"] * 20  # new steady state
        + [None] * 5  # sensor dropout (gap-fill pad in _insert_sample)
        + ["RFNOFF"] * 10  # recovery
    ),
}

# VNA_METADATA mirrors the flat ``{key: value}`` dict returned by
# ``MetadataSnapshotReader.get()`` — the snapshot path used by the VNA
# code in ``PandaClient.measure_s11``. Values are whatever the producer
# last pushed via ``MetadataWriter.add``:
#   - picohost pushes the raw sensor dict for each sensor key
#   - ``MetadataWriter.add`` auto-appends a ``{key}_ts`` Unix-seconds float
#   - misc. scalars (e.g. ``corr_sync_time``) go in as floats
# There is NO averaging on this path; unlike CORR_METADATA, values are
# scalars or nested dicts, never per-sample lists.
_SNAPSHOT_TS = 1775997296.789012
VNA_METADATA = {
    "imu_el": IMU_READING,
    "imu_el_ts": _SNAPSHOT_TS,
    "imu_az": {**IMU_READING, "sensor_name": "imu_az", "app_id": 6},
    "imu_az_ts": _SNAPSHOT_TS,
    "lidar": {
        "sensor_name": "lidar",
        "status": "update",
        "app_id": 4,
        "distance_m": 1.52,
    },
    "lidar_ts": _SNAPSHOT_TS,
    "potmon": {
        "sensor_name": "potmon",
        "status": "update",
        "app_id": 2,
        "pot_el_voltage": 1.5,
        "pot_az_voltage": 1.5,
        "pot_el_cal_slope": 100.0,
        "pot_el_cal_intercept": -50.0,
        "pot_az_cal_slope": 200.0,
        "pot_az_cal_intercept": -100.0,
        "pot_el_angle": 100.0,
        "pot_az_angle": 200.0,
    },
    "potmon_ts": _SNAPSHOT_TS,
    "rfswitch": {
        "sensor_name": "rfswitch",
        "status": "update",
        "app_id": 5,
        "sw_state": 3,
        "sw_state_name": "VNAS",
    },
    "rfswitch_ts": _SNAPSHOT_TS,
    "corr_sync_time": 1748732903.4203713,
    "corr_sync_time_ts": _SNAPSHOT_TS,
}

S11_HEADER = {
    "fstart": 1e6,
    "fstop": 250e6,
    "npoints": 1000,
    "ifbw": 100,
    "power_dBm": 0,
    "freqs": np.linspace(1e6, 250e6, 1000),
    "mode": "ant",
    "metadata_snapshot_unix": 1748734379.905014,
}
