"""Shared test fixtures for the eigsep_observing test suite.

These constants and helpers are deliberately constructed to mirror the
shape and types of real production data. See the "Testing philosophy"
section in CLAUDE.md: fixtures should look like what producers actually
emit so tests catch contract drift, and any deviations should be called
out explicitly.

They live in conftest.py so both ``test_io.py`` and
``test_producer_contracts.py`` can import them by name (pytest's default
``importmode=prepend`` puts the tests directory on sys.path during
collection, so ``from conftest import HEADER`` works in any test file
under ``tests/``).

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

# HEADER mimics EigsepFpga().header: the static-configuration portion of a
# corr file. Units match corr_config.yaml: sample_rate is in MHz (NOT Hz).
HEADER = {
    "dtype": ">i4",
    "acc_bins": 2,
    "nchan": 1024,
    "fgp_file": "fpg_files/eigsep_fengine.fpg",
    "fpg_version": [0, 0],
    "corr_acc_len": 2**28,
    "corr_scalar": 2**9,
    "pol01_delay": 0,
    "pol23_delay": 0,
    "pol45_delay": 0,
    "fft_shift": 0x00FF,
    "sample_rate": 500.0,  # MHz, matching corr_config.yaml convention
    "gain": 4,
    "pam_atten": {"0": 8, "1": 8, "2": 8},
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
    fault-flag transitions construct their own samples.
    """
    return {
        "status": "update",
        "T_now": t_now,
        "timestamp": timestamp,
        "T_target": t_now,
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
        [0] * 20  # steady state
        + ["UNKNOWN"] * 5  # transition window
        + [1] * 20  # new steady state
        + [None] * 5  # sensor dropout (gap-fill pad in _insert_sample)
        + [1] * 10  # recovery
    ),
}

# VNA_METADATA mirrors the flat ``{key: value}`` dict returned by
# ``EigsepRedis.get_live_metadata()`` — the snapshot path used by the VNA
# code in ``PandaClient.measure_s11``. Values are whatever the producer
# last pushed via ``add_metadata``:
#   - picohost pushes the raw sensor dict for each sensor key
#   - ``add_metadata`` auto-appends a ``{key}_ts`` ISO-8601 string
#   - misc. scalars (e.g. ``corr_sync_time``) go in as floats
# There is NO averaging on this path; unlike CORR_METADATA, values are
# scalars or nested dicts, never per-sample lists.
_SNAPSHOT_TS = "2026-04-07T12:34:56.789012+00:00"
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
