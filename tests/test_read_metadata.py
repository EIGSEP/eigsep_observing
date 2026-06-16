"""Round-trip + format-match tests for the io.py metadata pair.

``write_metadata_hdf5`` writes standalone pico metadata the *same* way a
corr file's sidecar is written: one JSON list-of-dicts per stream under a
``metadata`` group, so a ``None`` field (a nulled sensor reading) survives
as ``None`` rather than a zero/empty sentinel. ``read_metadata_hdf5``
inverts it to ``{stream: [dict, ...]}`` — the same shape and None-faithful
JSON decoding ``read_hdf5`` returns for a corr file's ``metadata`` group.

The format-match test asserts a standalone metadata file and a corr file
written from identical metadata read back equal, so the two
representations can't drift. (The recorder script that produces these
files is exercised in ``test_record_metadata.py``.)
"""

import numpy as np
import pytest

from eigsep_observing.io import (
    read_hdf5,
    read_metadata_hdf5,
    write_hdf5,
    write_metadata_hdf5,
)


# Production-shaped raw samples, each carrying the ``_ts_unix`` the
# recorder folds in from the Redis stream entry ID. The ``None`` fields
# are real (a sensor error nulls fields per SENSOR_SCHEMAS) and are the
# reason for matching io.py's None-faithful JSON over a typed-column
# sentinel: a dropped reading must read back as None, not 0.0.
SAMPLES = {
    "motor": [
        {
            "_ts_unix": 1000.0,
            "sensor_name": "motor",
            "status": "update",
            "app_id": 5,
            "az_pos": 1.5,
            "az_target_pos": 2.0,
            "el_pos": -3.0,
            "el_target_pos": -3.0,
        },
        {
            "_ts_unix": 1000.5,
            "sensor_name": "motor",
            "status": "error",
            "app_id": 5,
            "az_pos": None,
            "az_target_pos": 2.0,
            "el_pos": None,
            "el_target_pos": -3.0,
        },
    ],
    "imu_el": [
        {
            "_ts_unix": 1000.1,
            "sensor_name": "imu_el",
            "status": "update",
            "app_id": 3,
            "yaw": 12.5,
            "pitch": -3.25,
            "roll": 1.75,
            "accel_x": 0.05,
            "accel_y": -0.12,
            "accel_z": 9.78,
        },
    ],
}


@pytest.fixture
def recorder_file(tmp_path):
    """A metadata file written through the recorder's file-I/O path."""
    out = tmp_path / "metadata_test.h5"
    write_metadata_hdf5(out, SAMPLES)
    return out


def test_round_trips_streams_as_lists_of_dicts(recorder_file):
    assert read_metadata_hdf5(recorder_file) == SAMPLES


def test_none_fields_survive_as_none(recorder_file):
    motor = read_metadata_hdf5(recorder_file)["motor"]
    assert motor[1]["az_pos"] is None
    assert motor[1]["el_pos"] is None
    # ints/floats/strings on the same row are untouched.
    assert motor[1]["app_id"] == 5
    assert motor[1]["el_target_pos"] == -3.0
    assert motor[1]["status"] == "error"


def test_matches_io_corr_metadata_serialization(tmp_path, recorder_file):
    """Identical metadata written through the corr file writer reads back
    equal — the recorder format *is* the corr sidecar format."""
    corr = tmp_path / "corr.h5"
    write_hdf5(
        corr,
        data={"auto0": np.zeros((1, 2), dtype=np.int32)},
        header={"nchan": 1},
        metadata=SAMPLES,
    )
    _, _, corr_meta = read_hdf5(corr)
    assert read_metadata_hdf5(recorder_file) == corr_meta


def test_missing_metadata_group_reads_empty(tmp_path):
    """A file with no streams (recorder saw nothing) reads as empty."""
    empty = tmp_path / "empty.h5"
    write_metadata_hdf5(empty, {})
    assert read_metadata_hdf5(empty) == {}
