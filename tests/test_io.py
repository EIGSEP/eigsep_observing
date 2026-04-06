import datetime
import glob
import json
import numpy as np
import os
from pathlib import Path
import pytest
import stat
import tempfile

import h5py
from eigsep_observing import io
from eigsep_observing.testing.utils import (
    compare_dicts,
    generate_data,
    generate_s11_data,
)

# header to use for testing, mimics EigsepFpga().header
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
    "sample_rate": 500e6,
    "gain": 4,
    "pam_atten": {"0": 8, "1": 8, "2": 8},
    "sync_time": 1748732903.4203713,
    "integration_time": 0.1,
    "file_time": 60.3,
}

# metadata to use for testing, mimics output of EigsepRedis.get_metadata
METADATA = {
    "acc_cnt": np.arange(60),
    "updated_unix": np.array([881494457.8234632, 1748734379.905014]),
    "updated_date": np.array(
        ["2025-05-31T16:35:48.324275", "1999-12-03T06:37:00.134234"]
    ),
    "obs_mode": np.array(["VNAO", "VNAS", "VNAL", "VNAANT", "RFN"]),
    "temp_lna": np.linspace(30, 60, 60) + 273.15,
    "temp_vna_load": np.linspace(20, 40, 60) + 273.15,
    "temp_load": np.linspace(10, 20, 60) + 273.15,
    "az_imu": np.array([[0, 10, -30], [0, 20, -40], [0, 30, -50]]),
    "el_imu": np.array([[0, 10, -30], [0, 20, -40], [0, 30, -50]]),
    "az_motor": np.arange(100) * 1.8,
    "el_motor": np.arange(100) * 1.8,
}

# Schema-conformant IMU reading for use in metadata tests.
IMU_READING = {
    "sensor_name": "imu_panda",
    "status": "update",
    "app_id": 3,
    "quat_i": 0.0,
    "quat_j": 0.0,
    "quat_k": 0.0,
    "quat_real": 1.0,
    "accel_x": 0.0,
    "accel_y": 0.0,
    "accel_z": 9.8,
    "lin_accel_x": 0.0,
    "lin_accel_y": 0.0,
    "lin_accel_z": 0.0,
    "gyro_x": 0.0,
    "gyro_y": 0.0,
    "gyro_z": 0.0,
    "mag_x": 0.0,
    "mag_y": 0.0,
    "mag_z": 0.0,
    "calibrated": True,
    "accel_cal": 3,
    "mag_cal": 3,
}

S11_HEADER = {
    "fstart": 1e6,
    "fstop": 250e6,
    "npoints": 1000,
    "ifbw": 100,
    "power_dBm": 0,
    "freqs": np.linspace(1e6, 250e6, 1000),
    "mode": "ant",
}


def test_reshape_data():
    # generate data like it is received from the SNAP
    data, ntimes, nchan = generate_data(reshape=False, return_time_freq=True)
    reshaped_data = io.reshape_data(data, avg_even_odd=False)
    for k in data:
        assert k in reshaped_data
        assert reshaped_data[k].shape == (ntimes, nchan, 2)
        if len(k) == 1:  # autocorrelations
            assert data[k].shape == (ntimes, nchan * 2)
            even = data[k][:, :nchan]
            odd = data[k][:, nchan:]
            np.testing.assert_array_equal(even, reshaped_data[k][:, :, 0])
            np.testing.assert_array_equal(odd, reshaped_data[k][:, :, 1])
            assert reshaped_data[k].dtype == data[k].dtype
        else:
            assert data[k].shape == (ntimes, nchan * 2 * 2)
            even = data[k][:, : 2 * nchan]
            odd = data[k][:, 2 * nchan :]
            for i, spec in enumerate([even, odd]):
                real = spec[:, ::2]
                imag = spec[:, 1::2]
                cdata = real + 1j * imag
                np.testing.assert_array_equal(cdata, reshaped_data[k][:, :, i])
    # test with averaging even and odd time steps
    reshaped_data = io.reshape_data(data, avg_even_odd=True)
    for k in data:
        assert k in reshaped_data
        assert reshaped_data[k].shape == (ntimes, nchan)
        if len(k) == 1:
            even = data[k][:, :nchan]
            odd = data[k][:, nchan:]
            avg = np.mean([even, odd], axis=0)
            np.testing.assert_array_equal(avg, reshaped_data[k])
        else:
            even = data[k][:, : 2 * nchan]
            odd = data[k][:, 2 * nchan :]
            avg = np.mean([even, odd], axis=0)
            real = avg[:, ::2]
            imag = avg[:, 1::2]
            cdata = real + 1j * imag
            np.testing.assert_array_equal(cdata, reshaped_data[k])


def test_write_attr():
    values = {
        bool: True,
        int: 42,
        float: 3.14,
        str: "test",
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / "test.h5"
        with h5py.File(fname, "w") as f:
            group = f.create_group("test_group")
            for typ, value in values.items():
                key = typ.__name__
                io._write_attr(group, key, value)
                assert group.attrs[key] == value
            # write invalid type
            with pytest.raises(TypeError):
                io._write_attr(group, "list", [1, 2, 3])


def test_write_dataset():
    values = {
        list: [1, 2, 3],
        tuple: ("1", "2", "3"),
        np.ndarray: np.array([1, 2, 3]),
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / "test.h5"
        with h5py.File(fname, "w") as f:
            group = f.create_group("test_group")
            for typ, value in values.items():
                key = typ.__name__
                io._write_dataset(group, key, value)
                assert key in group
                if key == "ndarray":
                    # numeric numpy arrays are written natively
                    back = group[key][()]
                    np.testing.assert_array_equal(back, value)
                else:
                    back = json.loads(group[key][()])
                    assert np.all(back == list(value))
            # write invalid type
            with pytest.raises(TypeError):
                io._write_dataset(group, "fcn", lambda x: x + 1)


def _test_write_header_item():
    values = {
        Path: Path("/test/path"),
        datetime.datetime: datetime.datetime(2023, 10, 1, 12, 0),
        set: {"a", "b", "c"},
        complex: 1 + 2j,
        bool: True,
        int: 42,
        float: 3.14,
        str: "test",
        list: [1, 2, 3],
        tuple: ("1", "2", "3"),
        np.ndarray: np.array([1, 2, 3]),
        dict: {"key": "value", "nested": {"a": 1, "b": 2}},
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / "test.h5"
        with h5py.File(fname, "w") as f:
            grp = f.create_group("test_group")
            for typ, value in values.items():
                key = typ.__name__
                io._write_header_item(grp, key, value)
                assert key in grp.attrs
                if isinstance(value, dict):
                    compare_dicts(value, json.loads(grp.attrs[key]))
                elif isinstance(value, (np.ndarray, bytes)):
                    np.testing.assert_array_equal(grp.attrs[key], value)
                else:
                    assert grp.attrs[key] == value
            with pytest.raises(TypeError):
                io._write_header_item(grp, "invalid", lambda x: x + 1)


def test_write_read_hdf5():
    data = generate_data(reshape=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        filename = Path(tmpdir) / "test.h5"
        io.write_hdf5(filename, data, HEADER)
        assert filename.exists()
        read_data, read_header, read_meta = io.read_hdf5(filename)
        compare_dicts(data, read_data)
        compare_dicts(HEADER, read_header)
        assert read_meta == {}

        # test with metadata
        io.write_hdf5(filename, data, HEADER, metadata=METADATA)
        read_data, read_header, read_meta = io.read_hdf5(filename)
        compare_dicts(data, read_data)
        compare_dicts(HEADER, read_header)
        compare_dicts(METADATA, read_meta)

        # test with invalid type in header
        invalid_header = HEADER.copy()
        invalid_header["bad"] = b"invalid"  # bytes not allowed
        with pytest.raises(TypeError):
            io.write_hdf5(filename, data, invalid_header)


def test_write_read_s11_file():
    data, cal_data = generate_s11_data(npoints=S11_HEADER["npoints"], cal=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        # no filename, should create one automatically
        io.write_s11_file(data, S11_HEADER, fname=None, save_dir=tmpdir)
        # check that the file was created
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # might be off by a second, so we use glob to find the file
        # filename format is {mode}s11_{timestamp}.h5, where mode="ant" here
        assert len(list(Path(tmpdir).glob(f"ants11_{now[:-2]}*.h5"))) == 1
        # create a filename manually
        filename = Path(tmpdir) / "test_s11.h5"
        io.write_s11_file(
            data,
            S11_HEADER,
            metadata=METADATA,
            cal_data=cal_data,
            fname=filename,
        )
        assert filename.exists()
        read = io.read_s11_file(filename)
        read_data, read_cal_data, read_header, read_meta = read
        compare_dicts(data, read_data)
        compare_dicts(cal_data, read_cal_data)
        compare_dicts(S11_HEADER, read_header)
        compare_dicts(METADATA, read_meta)
        # not absolute path
        filename = Path("test_relative.h5")
        io.write_s11_file(
            data,
            S11_HEADER,
            metadata=METADATA,
            cal_data=cal_data,
            fname=filename,
            save_dir=tmpdir,
        )
        assert Path(Path(tmpdir) / filename.name).exists()


def test_file():
    # test the File class
    temp_dir = tempfile.TemporaryDirectory()
    save_dir = Path(temp_dir.name)
    autos = [str(i) for i in range(6)]
    cross = ["02", "04", "13", "15", "24", "35"]
    pairs = autos + cross
    ntimes = 60
    test_file = io.File(save_dir, pairs, ntimes, HEADER)

    # __init__
    assert test_file.save_dir.resolve() == save_dir.resolve()
    assert test_file.pairs == pairs
    assert test_file.ntimes == ntimes
    assert test_file.cfg == HEADER

    assert list(test_file.data.keys()) == pairs
    dtype = HEADER["dtype"]
    for p in pairs:
        if len(p) == 1:
            shape = io.data_shape(ntimes, 2, 1024)
        else:
            shape = io.data_shape(ntimes, 2, 1024, cross=True)
        d = test_file.data[p]
        assert d.shape == shape
        assert d.dtype == dtype
        np.testing.assert_array_equal(d, np.zeros(shape, dtype=dtype))

    assert test_file.counter == 0
    assert len(test_file) == 0

    # add_data
    data = generate_data(reshape=False)
    acc_cnt = 1
    sync_time = 0
    for i in range(ntimes - 1):
        to_add = {p: d[i] for p, d in data.items()}
        test_file.add_data(acc_cnt, sync_time, to_add)
        acc_cnt += 1
        assert test_file.counter == i + 1
        assert len(test_file) == i + 1
        for p in pairs:
            assert np.array_equal(test_file.data[p][i], to_add[p])
    to_add = {p: d[-1] for p, d in data.items()}
    test_file.add_data(acc_cnt, sync_time, to_add)
    # reset has been called (buffer swapped)
    assert test_file.counter == 0
    for p in pairs:
        if len(p) == 1:
            shape = io.data_shape(ntimes, 2, 1024)
        else:
            shape = io.data_shape(ntimes, 2, 1024, cross=True)
        d = test_file.data[p]
        assert d.shape == shape
        assert d.dtype == dtype
        np.testing.assert_array_equal(d, np.zeros(shape, dtype=dtype))

    # wait for async writer to finish
    test_file.close()

    # corr_write has been called by add_data
    files = glob.glob(str(save_dir / "*.h5"))
    assert len(files) == 1
    fname = files[0]
    # check that the data is written correctly
    read_data, read_header, read_meta = io.read_hdf5(fname)
    compare_dicts(io.reshape_data(data, avg_even_odd=True), read_data)
    # can't compare header with read_header since extra keys are added
    for key in HEADER:
        assert key in read_header
        assert read_header[key] == HEADER[key]
    assert read_meta == {}

    temp_dir.cleanup()


def test_gap_filling():
    """Verify large gap fills correctly without RecursionError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0", "1"]
        ntimes = 100
        f = io.File(save_dir, pairs, ntimes, HEADER)

        rng = np.random.default_rng(42)
        dtype = np.dtype(HEADER["dtype"])
        nchan = HEADER["nchan"]
        acc_bins = HEADER["acc_bins"]

        # add first sample at acc_cnt=1
        d = {
            p: rng.integers(
                0,
                high=1000,
                size=io.data_shape(1, acc_bins, nchan)[1],
                dtype="=i4",
            ).astype(dtype)
            for p in pairs
        }
        md = {
            "stream:rfswitch": [
                {
                    "sensor_name": "rfswitch",
                    "status": "update",
                    "app_id": 5,
                    "sw_state": 0,
                }
            ]
        }
        f.add_data(1, 0.0, d, metadata=md)
        assert f.counter == 1

        # jump to acc_cnt=52 (gap of 50)
        d2 = {
            p: rng.integers(
                0,
                high=1000,
                size=io.data_shape(1, acc_bins, nchan)[1],
                dtype="=i4",
            ).astype(dtype)
            for p in pairs
        }
        f.add_data(52, 0.0, d2, metadata=md)

        # 1 real + 50 gap-fills + 1 real = 52 entries
        assert f.counter == 52

        # gap-filled data should be zeros
        for p in pairs:
            for i in range(1, 51):
                np.testing.assert_array_equal(
                    f.data[p][i], np.zeros_like(f.data[p][0])
                )

        # metadata should have None for gap-filled samples
        md_list = f.metadata["rfswitch"]
        assert len(md_list) == 52
        assert md_list[0] is not None  # first sample
        for i in range(1, 51):
            assert md_list[i] is None  # gap-filled
        assert md_list[51] is not None  # last sample

        f.close()


def test_gap_filling_no_recursion_error():
    """Ensure a large gap does not cause RecursionError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 2000
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        nchan = HEADER["nchan"]
        acc_bins = HEADER["acc_bins"]
        spec_len = io.data_shape(1, acc_bins, nchan)[1]
        d = {"0": np.ones(spec_len, dtype=dtype)}

        f.add_data(1, 0.0, d)
        # gap of 1500 - would overflow stack with recursive approach
        f.add_data(1501, 0.0, d)
        # 1 + 1499 gap-fills + 1 = 1501
        assert f.counter == 1501
        f.close()


def test_write_error_recovery():
    """Verify buffer data is preserved when write fails."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 5
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        nchan = HEADER["nchan"]
        acc_bins = HEADER["acc_bins"]
        spec_len = io.data_shape(1, acc_bins, nchan)[1]

        # fill the buffer
        for i in range(ntimes):
            d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
            f.add_data(i + 1, 0.0, d)

        # wait for the write to complete
        f._write_queue.join()

        # the file should have been written
        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1

        # now make the directory read-only to force a write failure
        os.chmod(save_dir, stat.S_IRUSR | stat.S_IXUSR)
        try:
            # fill another buffer - write should fail
            for i in range(ntimes):
                d = {"0": np.full(spec_len, i + 100, dtype=dtype)}
                f.add_data(ntimes + i + 1, 0.0, d)
            # wait for the (failed) write attempt
            f._write_queue.join()
            # the second file should not exist (write failed)
            files = glob.glob(str(save_dir / "*.h5"))
            assert len(files) == 1
        finally:
            os.chmod(
                save_dir,
                stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO,
            )
        f.close()


def test_backward_compat_read():
    """Verify read_hdf5 can read files with JSON-encoded numpy datasets."""
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / "old_format.h5"
        arr = np.array([1.0, 2.0, 3.0])
        # write numpy array as JSON (old format)
        with h5py.File(fname, "w") as f:
            data_grp = f.create_group("data")
            data_grp.create_dataset("test", data=arr)
            header_grp = f.create_group("header")
            # write a numpy array as JSON string (old format)
            header_grp.create_dataset("freqs", data=json.dumps(arr.tolist()))
            io._write_attr(header_grp, "nchan", 3)
            meta_grp = f.create_group("metadata")
            meta_grp.create_dataset(
                "temps", data=json.dumps([30.0, 31.0, 32.0])
            )

        data, header, metadata = io.read_hdf5(fname)
        np.testing.assert_array_equal(data["test"], arr)
        # JSON-decoded arrays come back as Python lists
        assert header["freqs"] == [1.0, 2.0, 3.0]
        assert header["nchan"] == 3
        assert metadata["temps"] == [30.0, 31.0, 32.0]


def test_avg_metadata():
    """Test metadata averaging for different sensor types."""
    # rfswitch: consistent state
    sw_data = [
        {
            "sensor_name": "rfswitch",
            "status": "update",
            "app_id": 5,
            "sw_state": 42,
        },
        {
            "sensor_name": "rfswitch",
            "status": "update",
            "app_id": 5,
            "sw_state": 42,
        },
    ]
    assert io.avg_metadata(sw_data) == 42

    # rfswitch: inconsistent state
    sw_data[1] = dict(sw_data[1], sw_state=99)
    assert io.avg_metadata(sw_data) == "UNKNOWN"

    # rfswitch: error status
    sw_data[1] = dict(sw_data[0], status="error")
    assert io.avg_metadata(sw_data) == "UNKNOWN"

    # temp_mon: A/B channels
    temp_data = [
        {
            "sensor_name": "temp_mon",
            "app_id": 2,
            "A_status": "update",
            "A_temp": 30.0,
            "A_timestamp": 1.0,
            "B_status": "update",
            "B_temp": 25.0,
            "B_timestamp": 2.0,
        },
        {
            "sensor_name": "temp_mon",
            "app_id": 2,
            "A_status": "update",
            "A_temp": 32.0,
            "A_timestamp": 3.0,
            "B_status": "error",
            "B_temp": 0.0,
            "B_timestamp": 4.0,
        },
    ]
    result = io.avg_metadata(temp_data)
    assert result["sensor_name"] == "temp_mon"
    assert result["A"]["temp"] == 31.0  # average of 30 and 32
    # B has one error entry, so only non-error value used
    assert result["B"]["temp"] == 25.0

    # generic sensor (IMU) — full schema-conformant data
    imu_data = [
        {**IMU_READING, "quat_i": 0.1},
        {**IMU_READING, "quat_i": 0.3},
    ]
    result = io.avg_metadata(imu_data)
    assert result["quat_i"] == pytest.approx(0.2)
    assert result["calibrated"] is True


def test_gap_fill_acc_cnts_linear():
    """Verify gap-filled acc_cnts are sequential (not exponential)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 100
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        d = {"0": np.ones(spec_len, dtype=dtype)}

        f.add_data(10, 0.0, d)
        # gap of 5: should fill 11, 12, 13, 14, then add 15
        f.add_data(15, 0.0, d)

        assert f.counter == 6  # 1 + 4 gap-fills + 1
        expected = [10, 11, 12, 13, 14, 15]
        np.testing.assert_array_equal(f.acc_cnts[:6], expected)

        f.close()


def test_short_final_file():
    """Verify short final files don't contain trailing zeros."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 10
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]

        # only fill 3 out of 10 slots
        for i in range(3):
            d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
            f.add_data(i + 1, float(i), d)
        assert f.counter == 3

        # manually trigger write (short file)
        f.corr_write()
        f._write_queue.join()

        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1
        data, header, _ = io.read_hdf5(files[0])
        # data should have 3 time samples, not 10
        assert data["0"].shape[0] == 3
        assert len(header["acc_cnt"]) == 3

        f.close()


def test_metadata_new_key_alignment():
    """New metadata keys should be padded to align with existing samples."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 10
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        d = {"0": np.ones(spec_len, dtype=dtype)}

        # add 3 samples with key A only
        md_a = {
            "stream:imu_panda": [{**IMU_READING, "quat_i": 1.0}],
        }
        for i in range(3):
            f.add_data(i + 1, 0.0, d, metadata=md_a)

        # add sample with new key B
        md_b = {
            "stream:imu_panda": [{**IMU_READING, "quat_i": 2.0}],
            "stream:rfswitch": [
                {
                    "sensor_name": "rfswitch",
                    "status": "update",
                    "app_id": 5,
                    "sw_state": 0,
                }
            ],
        }
        f.add_data(4, 0.0, d, metadata=md_b)

        # imu_panda should have 4 entries
        assert len(f.metadata["imu_panda"]) == 4

        # rfswitch should also have 4 entries: 3 None pads + 1 real
        assert len(f.metadata["rfswitch"]) == 4
        for i in range(3):
            assert f.metadata["rfswitch"][i] is None
        assert f.metadata["rfswitch"][3] is not None

        f.close()


def test_stream_metadata_averaging():
    """Verify add_data averages stream-format metadata correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 10
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        d = {"0": np.ones(spec_len, dtype=dtype)}

        # stream format: multiple readings per stream, averaged down
        md = {
            "stream:imu_panda": [
                {**IMU_READING, "quat_i": 0.1},
                {**IMU_READING, "quat_i": 0.3},
            ],
            "stream:rfswitch": [
                {
                    "sensor_name": "rfswitch",
                    "status": "update",
                    "app_id": 5,
                    "sw_state": 0,
                },
            ],
        }
        f.add_data(1, 0.0, d, metadata=md)

        # IMU values should be averaged (stream: prefix stripped)
        assert f.metadata["imu_panda"][0]["quat_i"] == pytest.approx(0.2)
        # rfswitch should return the state directly
        assert f.metadata["rfswitch"][0] == 0

        # second sample without rfswitch stream — should pad with None
        md2 = {
            "stream:imu_panda": [{**IMU_READING, "quat_i": 0.5}],
        }
        f.add_data(2, 0.0, d, metadata=md2)
        assert f.metadata["rfswitch"][1] is None
        assert f.metadata["imu_panda"][1]["quat_i"] == pytest.approx(0.5)

        f.close()


def test_temp_metadata_split():
    """Verify temp sensor A/B channels are split into separate entries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 10
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        d = {"0": np.ones(spec_len, dtype=dtype)}

        md = {
            "stream:temp_mon": [
                {
                    "sensor_name": "temp_mon",
                    "app_id": 2,
                    "A_status": "update",
                    "A_temp": 30.0,
                    "A_timestamp": 1.0,
                    "B_status": "update",
                    "B_temp": 25.0,
                    "B_timestamp": 2.0,
                },
            ],
        }
        f.add_data(1, 0.0, d, metadata=md)

        # A and B should be separate flat entries, not nested
        assert "temp_mon" not in f.metadata
        assert "temp_mon_a" in f.metadata
        assert "temp_mon_b" in f.metadata
        assert f.metadata["temp_mon_a"][0]["temp"] == 30.0
        assert f.metadata["temp_mon_b"][0]["temp"] == 25.0

        f.close()


def test_write_error_surfaced():
    """Verify _write_error is set on failure and logged on next write."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 5
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]

        # fill buffer and trigger write (should succeed)
        for i in range(ntimes):
            d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
            f.add_data(i + 1, 0.0, d)
        f._write_queue.join()
        assert f._write_error is None

        # make directory read-only to force failure
        os.chmod(save_dir, stat.S_IRUSR | stat.S_IXUSR)
        try:
            for i in range(ntimes):
                d = {"0": np.full(spec_len, i + 100, dtype=dtype)}
                f.add_data(ntimes + i + 1, 0.0, d)
            f._write_queue.join()
            # write error should be surfaced
            assert f._write_error is not None
        finally:
            os.chmod(
                save_dir,
                stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO,
            )
        f.close()


def test_avg_metadata_edge_cases():
    """Test avg_metadata with edge cases."""
    # empty list
    assert io.avg_metadata([]) is None

    # non-dict entries
    assert io.avg_metadata(["not", "dicts"]) is None

    # all error status — use schema-conformant lidar (simplest)
    err_data = [
        {
            "sensor_name": "lidar",
            "status": "error",
            "app_id": 4,
            "distance_m": 0.0,
        },
        {
            "sensor_name": "lidar",
            "status": "error",
            "app_id": 4,
            "distance_m": 0.0,
        },
    ]
    result = io.avg_metadata(err_data)
    # all values are error, so numeric keys should be None
    assert result["distance_m"] is None

    # None values in numeric field
    none_data = [
        {
            "sensor_name": "lidar",
            "status": "update",
            "app_id": 4,
            "distance_m": None,
        }
    ]
    result = io.avg_metadata(none_data)
    assert result["distance_m"] is None

    # unknown sensor: falls back to generic path with warning
    unknown_data = [
        {
            "sensor_name": "unknown_sensor",
            "status": "update",
            "app_id": 99,
            "val": 1.0,
        },
        {
            "sensor_name": "unknown_sensor",
            "status": "update",
            "app_id": 99,
            "val": 3.0,
        },
    ]
    result = io.avg_metadata(unknown_data)
    assert result["val"] == pytest.approx(2.0)


def test_validate_metadata():
    """Test schema validation for sensor metadata."""
    schema = io.SENSOR_SCHEMAS["lidar"]

    # valid entry: no violations
    valid = {
        "sensor_name": "lidar",
        "status": "update",
        "app_id": 4,
        "distance_m": 1.5,
    }
    assert io._validate_metadata(valid, schema) == []

    # None values are allowed (sensor error)
    none_entry = {**valid, "distance_m": None}
    assert io._validate_metadata(none_entry, schema) == []

    # int accepted for float field
    int_entry = {**valid, "distance_m": 2}
    assert io._validate_metadata(int_entry, schema) == []

    # missing key
    missing = {k: v for k, v in valid.items() if k != "distance_m"}
    violations = io._validate_metadata(missing, schema)
    assert len(violations) == 1
    assert "missing" in violations[0]

    # extra key
    extra = {**valid, "bogus": 42}
    violations = io._validate_metadata(extra, schema)
    assert len(violations) == 1
    assert "extra" in violations[0]

    # wrong type
    bad_type = {**valid, "distance_m": "not_a_number"}
    violations = io._validate_metadata(bad_type, schema)
    assert len(violations) == 1
    assert "expected float" in violations[0]

    # bool should not pass as int
    bool_as_int = {**valid, "app_id": True}
    violations = io._validate_metadata(bool_as_int, schema)
    assert len(violations) == 1
    assert "expected int" in violations[0]


def test_avg_metadata_schema_violation_no_crash():
    """Schema-violating data produces warnings but still returns."""
    # Data with extra key and missing keys — should warn, not crash
    bad_data = [
        {
            "sensor_name": "lidar",
            "status": "update",
            "app_id": 4,
            "bogus_field": 99.0,
        },
    ]
    result = io.avg_metadata(bad_data)
    # Should still return a dict (best-effort from schema keys)
    assert isinstance(result, dict)
    # Schema key "distance_m" is missing from data, so should be None
    assert result["distance_m"] is None
    # Extra key "bogus_field" is not in schema, so not in result
    assert "bogus_field" not in result


def test_close():
    """Test that close() shuts down the writer thread."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        f = io.File(save_dir, pairs, 10, HEADER)
        assert f._writer_thread.is_alive()
        f.close()
        assert not f._writer_thread.is_alive()
