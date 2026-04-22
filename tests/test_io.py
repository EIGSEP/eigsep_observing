import datetime
import glob
import json
import logging
import numpy as np
import os
from pathlib import Path
import pytest
import stat
import tempfile
import threading
import time

import h5py
from picohost.base import PicoRFSwitch

from conftest import (
    CORR_METADATA,
    ERROR_INTEGRATION_INDEX,
    HEADER,
    IMU_READING,
    NTIMES,
    S11_HEADER,
    VNA_METADATA,
)
from eigsep_observing import io
from eigsep_observing.testing.utils import (
    compare_dicts,
    generate_data,
    generate_s11_data,
)


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
    # test with averaging even and odd time steps — returns int32
    reshaped_data = io.reshape_data(data, avg_even_odd=True)
    for k in data:
        assert k in reshaped_data
        if len(k) == 1:  # autocorrelations
            assert reshaped_data[k].shape == (ntimes, nchan)
            assert reshaped_data[k].dtype == np.int32
            even = data[k][:, :nchan]
            odd = data[k][:, nchan:]
            expected = np.rint(np.mean([even, odd], axis=0)).astype(np.int32)
            np.testing.assert_array_equal(expected, reshaped_data[k])
        else:  # cross-correlations: (ntimes, nchan, 2) int32
            assert reshaped_data[k].shape == (ntimes, nchan, 2)
            assert reshaped_data[k].dtype == np.int32
            even = data[k][:, : 2 * nchan]
            odd = data[k][:, 2 * nchan :]
            avg = np.rint(np.mean([even, odd], axis=0)).astype(np.int32)
            real = avg[:, ::2]
            imag = avg[:, 1::2]
            np.testing.assert_array_equal(real, reshaped_data[k][:, :, 0])
            np.testing.assert_array_equal(imag, reshaped_data[k][:, :, 1])


def test_int32_rounding_unbiased():
    """Banker's rounding in reshape_data introduces no systematic bias.

    Verifies that np.rint(mean) satisfies:
    - Max absolute error ≤ 0.5 LSB (theoretical bound)
    - Mean error ≈ 0 (banker's rounding is unbiased)
    - Max error is orders of magnitude below the radiometric noise
      for typical EIGSEP integration depths
    """
    rng = np.random.default_rng(42)
    n = 1_000_000
    # Match production dtype (big-endian int32 from the SNAP)
    dtype = np.dtype(">i4")
    native_dtype = np.dtype("=i4")

    # --- autos (non-negative, typical range 1e6–1e9) ---
    lo, hi = int(1e6), int(1e9)
    even_auto = rng.integers(lo, high=hi, size=n, dtype=native_dtype).astype(
        dtype
    )
    odd_auto = rng.integers(lo, high=hi, size=n, dtype=native_dtype).astype(
        dtype
    )
    exact_auto = (even_auto.astype(np.float64) + odd_auto) / 2
    rounded_auto = np.rint(exact_auto).astype(dtype)

    err_auto = rounded_auto.astype(np.float64) - exact_auto
    assert np.max(np.abs(err_auto)) <= 0.5
    # Banker's rounding is unbiased: mean error should be near zero
    assert abs(np.mean(err_auto)) < 0.01

    # --- crosses (signed, typical range −1e9 to 1e9) ---
    even_cross = rng.integers(-hi, high=hi, size=n, dtype=native_dtype).astype(
        dtype
    )
    odd_cross = rng.integers(-hi, high=hi, size=n, dtype=native_dtype).astype(
        dtype
    )
    exact_cross = (even_cross.astype(np.float64) + odd_cross) / 2
    rounded_cross = np.rint(exact_cross).astype(dtype)

    err_cross = rounded_cross.astype(np.float64) - exact_cross
    assert np.max(np.abs(err_cross)) <= 0.5
    assert abs(np.mean(err_cross)) < 0.01

    # --- max error vs radiometric noise ---
    # For corr_acc_len = 2^28, signal = 1e9:
    # noise_per_integration = signal / sqrt(corr_acc_len)
    corr_acc_len = 2**28
    signal = 1e9
    noise = signal / np.sqrt(corr_acc_len)
    max_rounding_error = 0.5
    assert max_rounding_error / noise < 1e-3


def test_write_attr():
    # Python natives + numpy scalars must all round-trip through attrs.
    # 3.5 is exact in float32, so float32 → float64 round-trip is lossless.
    values = {
        "bool_py": True,
        "int_py": 42,
        "float_py": 3.14,
        "str_py": "test",
        "bool_np": np.bool_(True),
        "int8_np": np.int8(42),
        "int64_np": np.int64(42),
        "uint16_np": np.uint16(42),
        "float32_np": np.float32(3.5),
        "float64_np": np.float64(3.14),
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / "test.h5"
        with h5py.File(fname, "w") as f:
            group = f.create_group("test_group")
            for key, value in values.items():
                io._write_attr(group, key, value)
                # All numeric scalars compare equal across types.
                assert group.attrs[key] == value
            # All bool values stored as np.bool_ regardless of source.
            assert group.attrs["bool_py"].dtype == np.bool_
            assert group.attrs["bool_np"].dtype == np.bool_
            # All int values stored as int64.
            assert group.attrs["int_py"].dtype == np.int64
            assert group.attrs["int8_np"].dtype == np.int64
            # All float values stored as float64.
            assert group.attrs["float_py"].dtype == np.float64
            assert group.attrs["float32_np"].dtype == np.float64
            # write invalid type
            with pytest.raises(TypeError):
                io._write_attr(group, "list", [1, 2, 3])


def test_header_numpy_python_equivalence():
    """Headers built with numpy scalars must be layout-equivalent to
    headers built with Python natives. C1: numpy scalars route to
    attrs (via _write_attr), not datasets — so header["nchan"] = 1024
    and header["nchan"] = np.int64(1024) produce identical files."""
    py_header = {
        "nchan": 1024,
        "sample_rate": 500e6,
        "calibrated": True,
        "name": "snap",
    }
    np_header = {
        "nchan": np.int64(1024),
        "sample_rate": np.float64(500e6),
        "calibrated": np.bool_(True),
        "name": "snap",
    }

    data = generate_data(reshape=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        py_path = Path(tmpdir) / "py.h5"
        np_path = Path(tmpdir) / "np.h5"
        io.write_hdf5(py_path, data, py_header)
        io.write_hdf5(np_path, data, np_header)

        # Both files must store the scalar header keys as attrs, not
        # as datasets — this is the layout invariant the C1 fix enforces.
        for path in (py_path, np_path):
            with h5py.File(path, "r") as f:
                hdr = f["header"]
                for key in ("nchan", "sample_rate", "calibrated", "name"):
                    assert key in hdr.attrs, (
                        f"{path.name}: {key} should be an attr"
                    )
                    assert key not in hdr, (
                        f"{path.name}: {key} should NOT be a dataset"
                    )

        # And the read-back header values must match across both.
        _, py_read, _ = io.read_hdf5(py_path)
        _, np_read, _ = io.read_hdf5(np_path)
        for key in py_header:
            assert py_read[key] == np_read[key], f"mismatch on {key}"


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


def _as_read_back(data):
    """Convert reshape_data output to the format read_hdf5 returns.

    read_hdf5 reconstructs complex from int32 (re, im) cross datasets,
    so the round-trip comparison needs the written data in the same form.
    """
    out = {}
    for k, arr in data.items():
        if arr.ndim >= 2 and arr.shape[-1] == 2 and arr.dtype.kind == "i":
            arr = arr[..., 0].astype(np.float64) + 1j * arr[..., 1].astype(
                np.float64
            )
        out[k] = arr
    return out


def test_write_read_hdf5():
    data = generate_data(reshape=True)
    expected = _as_read_back(data)
    with tempfile.TemporaryDirectory() as tmpdir:
        filename = Path(tmpdir) / "test.h5"
        io.write_hdf5(filename, data, HEADER)
        assert filename.exists()
        read_data, read_header, read_meta = io.read_hdf5(filename)
        compare_dicts(expected, read_data)
        compare_dicts(HEADER, read_header)
        assert read_meta == {}

        io.write_hdf5(filename, data, HEADER, metadata=CORR_METADATA)
        read_data, read_header, read_meta = io.read_hdf5(filename)
        compare_dicts(expected, read_data)
        compare_dicts(HEADER, read_header)
        compare_dicts(CORR_METADATA, read_meta)

        invalid_header = HEADER.copy()
        invalid_header["bad"] = b"invalid"  # bytes → json.dumps fails
        bad_filename = Path(tmpdir) / "test_bad.h5"
        io.write_hdf5(bad_filename, data, invalid_header)
        assert bad_filename.exists()
        bad_read_data, bad_read_header, _ = io.read_hdf5(bad_filename)
        # corr data still present
        compare_dicts(expected, bad_read_data)
        # bad field skipped, other fields present
        assert "bad" not in bad_read_header
        assert "nchan" in bad_read_header


def test_int32_hdf5_round_trip_dtypes():
    """Int32 corr data round-trips through write_hdf5 → read_hdf5.

    read_hdf5 reconstructs complex from int32 cross datasets, so:
    - Autos: read back as int32
    - Crosses: read back as complex128 (reconstructed from int32)
    """
    data = generate_data(reshape=True)
    expected = _as_read_back(data)
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / "test_int32.h5"
        io.write_hdf5(fname, data, HEADER)
        read_data, _, _ = io.read_hdf5(fname)
        for key in data:
            read_back = read_data[key]
            if len(key) == 1:  # auto
                assert read_back.dtype == np.int32, (
                    f"key '{key}': expected int32, got {read_back.dtype}"
                )
            else:  # cross — reconstructed to complex
                assert read_back.dtype == np.complex128, (
                    f"key '{key}': expected complex128, got {read_back.dtype}"
                )
            np.testing.assert_array_equal(read_back, expected[key])


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
        filename = Path(tmpdir) / "test_s11.h5"
        io.write_s11_file(
            data,
            S11_HEADER,
            metadata=VNA_METADATA,
            cal_data=cal_data,
            fname=filename,
        )
        assert filename.exists()
        read = io.read_s11_file(filename)
        read_data, read_cal_data, read_header, read_meta = read
        compare_dicts(data, read_data)
        compare_dicts(cal_data, read_cal_data)
        compare_dicts(S11_HEADER, read_header)
        compare_dicts(VNA_METADATA, read_meta)
        # not absolute path
        filename = Path("test_relative.h5")
        io.write_s11_file(
            data,
            S11_HEADER,
            metadata=VNA_METADATA,
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
    expected = _as_read_back(io.reshape_data(data, avg_even_odd=True))
    compare_dicts(expected, read_data)
    # can't compare header with read_header since extra keys are added
    for key in HEADER:
        assert key in read_header
        assert read_header[key] == HEADER[key]
    assert read_meta == {}

    temp_dir.cleanup()


def test_gap_filling():
    """Verify large gap fills correctly without RecursionError, and
    that close() flushes the partial buffer (52 < ntimes=100) to disk."""
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
                    "sw_state_name": "RFANT",
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

        # close() must flush the partial buffer to disk — buffer is
        # only 52/100 full so a normal corr_write was not triggered.
        f.close()

        files = sorted(glob.glob(str(save_dir / "*.h5")))
        assert len(files) == 1, (
            f"close() did not flush partial buffer; files: {files}"
        )
        on_disk_data, _, on_disk_md = io.read_hdf5(files[0])
        # Slicing must respect counter — exactly 52 samples on disk,
        # not the full ntimes=100 with trailing zeros.
        for p in pairs:
            assert on_disk_data[p].shape[0] == 52, (
                f"pair {p}: expected 52 samples on disk, got "
                f"{on_disk_data[p].shape[0]}"
            )
        assert len(on_disk_md["rfswitch"]) == 52


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


def test_rename_failure_preserves_temp_file(monkeypatch, caplog):
    """If ``write_hdf5`` succeeds but ``os.rename`` then raises (e.g.
    a transient NFS / filesystem error), the just-written temp file
    must be preserved on disk so an operator can recover it by hand.
    Corr data is sacred — never destroy a successful write because of
    a downstream filesystem hiccup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 3
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]

        # Make os.rename always fail. Patch the os reference inside the
        # io module so the writer thread sees the failing rename without
        # affecting the rest of the test process.
        def failing_rename(*args, **kwargs):
            raise OSError("simulated rename failure")

        monkeypatch.setattr(io.os, "rename", failing_rename)

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            for i in range(ntimes):
                d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
                f.add_data(i + 1, 0.0, d)
            f._write_queue.join()

        # Final .h5 file does NOT exist — rename failed.
        final_files = glob.glob(str(save_dir / "*.h5"))
        assert final_files == [], (
            f"unexpected final file(s) after rename failure: {final_files}"
        )

        # The .h5.tmp file IS preserved on disk for manual recovery.
        tmp_files = glob.glob(str(save_dir / "*.h5.tmp"))
        assert len(tmp_files) == 1, (
            f"expected exactly one preserved .h5.tmp, got: {tmp_files}"
        )

        # The preserved temp file is a real, readable HDF5 with the
        # data we just wrote — proves rename failure didn't corrupt it.
        data, header, _ = io.read_hdf5(tmp_files[0])
        assert "0" in data
        assert data["0"].shape[0] == ntimes

        # The writer surfaced the failure via _write_error and ERROR log.
        assert isinstance(f._write_error, OSError)
        assert "simulated rename failure" in str(f._write_error)
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any("Failed to write" in r.getMessage() for r in errors), (
            f"expected a 'Failed to write' ERROR, got: {[r.getMessage() for r in errors]}"
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
            "sw_state": 0,
            "sw_state_name": "RFANT",
        },
        {
            "sensor_name": "rfswitch",
            "status": "update",
            "app_id": 5,
            "sw_state": 0,
            "sw_state_name": "RFANT",
        },
    ]
    assert io.avg_metadata(sw_data) == "RFANT"

    # rfswitch: inconsistent state
    sw_data[1] = dict(sw_data[1], sw_state_name="RFNOFF")
    assert io.avg_metadata(sw_data) == "UNKNOWN"

    # rfswitch: error status
    sw_data[1] = dict(sw_data[0], status="error")
    assert io.avg_metadata(sw_data) == "UNKNOWN"

    # tempctrl: LNA/LOAD channels (channel-split path)
    tc_data = [
        {
            "sensor_name": "tempctrl",
            "app_id": 1,
            "watchdog_tripped": False,
            "watchdog_timeout_ms": 5000,
            "LNA_status": "update",
            "LNA_T_now": 30.0,
            "LNA_timestamp": 1.0,
            "LNA_T_target": 25.0,
            "LNA_drive_level": 0.5,
            "LNA_enabled": True,
            "LNA_active": True,
            "LNA_int_disabled": False,
            "LNA_hysteresis": 0.1,
            "LNA_clamp": 1.0,
            "LOAD_status": "update",
            "LOAD_T_now": 25.0,
            "LOAD_timestamp": 2.0,
            "LOAD_T_target": 25.0,
            "LOAD_drive_level": 0.5,
            "LOAD_enabled": True,
            "LOAD_active": True,
            "LOAD_int_disabled": False,
            "LOAD_hysteresis": 0.1,
            "LOAD_clamp": 1.0,
        },
        {
            "sensor_name": "tempctrl",
            "app_id": 1,
            "watchdog_tripped": False,
            "watchdog_timeout_ms": 5000,
            "LNA_status": "update",
            "LNA_T_now": 32.0,
            "LNA_timestamp": 3.0,
            "LNA_T_target": 25.0,
            "LNA_drive_level": 0.5,
            "LNA_enabled": True,
            "LNA_active": True,
            "LNA_int_disabled": False,
            "LNA_hysteresis": 0.1,
            "LNA_clamp": 1.0,
            "LOAD_status": "error",
            "LOAD_T_now": 0.0,
            "LOAD_timestamp": 4.0,
            "LOAD_T_target": 25.0,
            "LOAD_drive_level": 0.5,
            "LOAD_enabled": True,
            "LOAD_active": True,
            "LOAD_int_disabled": False,
            "LOAD_hysteresis": 0.1,
            "LOAD_clamp": 1.0,
        },
    ]
    result = io.avg_metadata(tc_data)
    assert result["sensor_name"] == "tempctrl"
    assert result["LNA"]["T_now"] == 31.0  # average of 30 and 32
    # LOAD has one error entry, so only non-error value used
    assert result["LOAD"]["T_now"] == 25.0
    # status averages: LNA has no errors → "update"; LOAD has one → "error"
    assert result["LNA"]["status"] == "update"
    assert result["LOAD"]["status"] == "error"

    # generic sensor (IMU) — full schema-conformant data
    imu_data = [
        {**IMU_READING, "yaw": 0.1},
        {**IMU_READING, "yaw": 0.3},
    ]
    result = io.avg_metadata(imu_data)
    assert result["yaw"] == pytest.approx(0.2)
    # int-typed schema fields take the categorical path: they stay int
    # (not float), and they take value[0] rather than np.mean. See the
    # _avg_sensor_values docstring for the rationale.
    assert result["app_id"] == 3
    assert isinstance(result["app_id"], int)


def test_avg_metadata_tempctrl_forwards_top_level_fields():
    """tempctrl has top-level (non A_/B_-prefixed) fields like
    watchdog_tripped (bool) and watchdog_timeout_ms (int) that earlier
    versions of _avg_temp_metadata silently dropped because the helper
    only enumerated A_/B_ keys. Lock in the fix: top-level fields must
    survive into the averaged output, with types preserved per schema.
    """
    tc_data = [
        {
            "sensor_name": "tempctrl",
            "app_id": 1,
            "watchdog_tripped": False,
            "watchdog_timeout_ms": 5000,
            "LNA_status": "update",
            "LNA_T_now": 30.0,
            "LNA_timestamp": 1.0,
            "LNA_T_target": 25.0,
            "LNA_drive_level": 0.5,
            "LNA_enabled": True,
            "LNA_active": True,
            "LNA_int_disabled": False,
            "LNA_hysteresis": 0.1,
            "LNA_clamp": 1.0,
            "LOAD_status": "update",
            "LOAD_T_now": 32.0,
            "LOAD_timestamp": 2.0,
            "LOAD_T_target": 25.0,
            "LOAD_drive_level": 0.6,
            "LOAD_enabled": True,
            "LOAD_active": True,
            "LOAD_int_disabled": False,
            "LOAD_hysteresis": 0.1,
            "LOAD_clamp": 1.0,
        },
        {
            "sensor_name": "tempctrl",
            "app_id": 1,
            "watchdog_tripped": False,
            "watchdog_timeout_ms": 5000,
            "LNA_status": "update",
            "LNA_T_now": 32.0,  # average with 30.0 → 31.0
            "LNA_timestamp": 3.0,
            "LNA_T_target": 25.0,
            "LNA_drive_level": 0.5,
            "LNA_enabled": True,
            "LNA_active": True,
            "LNA_int_disabled": False,
            "LNA_hysteresis": 0.1,
            "LNA_clamp": 1.0,
            "LOAD_status": "update",
            "LOAD_T_now": 32.0,
            "LOAD_timestamp": 4.0,
            "LOAD_T_target": 25.0,
            "LOAD_drive_level": 0.6,
            "LOAD_enabled": True,
            "LOAD_active": True,
            "LOAD_int_disabled": False,
            "LOAD_hysteresis": 0.1,
            "LOAD_clamp": 1.0,
        },
    ]
    result = io.avg_metadata(tc_data)

    # Top-level fields are forwarded with types preserved.
    assert result["sensor_name"] == "tempctrl"
    assert result["app_id"] == 1
    assert isinstance(result["app_id"], int)
    assert result["watchdog_tripped"] is False
    assert result["watchdog_timeout_ms"] == 5000
    assert isinstance(result["watchdog_timeout_ms"], int)

    # LNA/LOAD sub-dicts still produced and floats still averaged.
    assert result["LNA"]["T_now"] == pytest.approx(31.0)
    assert result["LOAD"]["T_now"] == pytest.approx(32.0)


# ----------------------------------------------------------------------
# Per-type reduction policy in _avg_sensor_values (Design C):
#
#   float → np.mean over non-error survivors
#   int   → min over non-error survivors (worst-case for cal levels)
#   bool  → any over non-error survivors (worst-case for fault flags)
#   str   → first if unanimous, else "UNKNOWN"
#
# Plus: integration "status" collapses to "error" if any sample errored,
# so downstream gets a single per-row fault flag rather than having to
# inspect every numeric field for None to detect that errors happened.
#
# For invariant fields (sensor_name, app_id, watchdog_timeout_ms),
# disagreement also emits a throttled ERROR log — these should never
# change inside an integration.
# ----------------------------------------------------------------------


def test_avg_metadata_status_collapses_to_error_on_any_error():
    """If any sample inside an integration has status='error', the
    integration's averaged status MUST be 'error'. This is the per-row
    fault flag downstream uses to mark suspect data."""
    data = [
        {**IMU_READING, "status": "update", "yaw": 0.1},
        {**IMU_READING, "status": "error", "yaw": 0.2},
        {**IMU_READING, "status": "update", "yaw": 0.3},
    ]
    result = io.avg_metadata(data)
    assert result["status"] == "error"
    # Float averaging filters errored samples → mean of 0.1 and 0.3.
    assert result["yaw"] == pytest.approx(0.2)


def test_avg_metadata_status_stays_update_when_all_clean():
    """All clean samples → status passes through as 'update'."""
    data = [
        {**IMU_READING, "status": "update", "yaw": 0.1},
        {**IMU_READING, "status": "update", "yaw": 0.3},
    ]
    result = io.avg_metadata(data)
    assert result["status"] == "update"
    assert result["yaw"] == pytest.approx(0.2)


def test_avg_metadata_bool_any_on_disagreement():
    """bool fields take any() — fault-flag worst-case. A tempctrl
    integration whose watchdog tripped at any point in the integration
    must record watchdog_tripped=True."""
    base = {
        "sensor_name": "tempctrl",
        "app_id": 1,
        "watchdog_timeout_ms": 5000,
        "LNA_status": "update",
        "LNA_T_now": 30.0,
        "LNA_timestamp": 1.0,
        "LNA_T_target": 25.0,
        "LNA_drive_level": 0.5,
        "LNA_enabled": True,
        "LNA_active": True,
        "LNA_int_disabled": False,
        "LNA_hysteresis": 0.1,
        "LNA_clamp": 1.0,
        "LOAD_status": "update",
        "LOAD_T_now": 30.0,
        "LOAD_timestamp": 1.0,
        "LOAD_T_target": 25.0,
        "LOAD_drive_level": 0.5,
        "LOAD_enabled": True,
        "LOAD_active": True,
        "LOAD_int_disabled": False,
        "LOAD_hysteresis": 0.1,
        "LOAD_clamp": 1.0,
    }
    data = [
        {**base, "watchdog_tripped": False},
        {**base, "watchdog_tripped": True},  # tripped mid-integration
        {**base, "watchdog_tripped": False},
    ]
    result = io.avg_metadata(data)
    assert result["watchdog_tripped"] is True


def test_avg_metadata_str_unknown_on_disagreement_for_non_invariant():
    """str fields take first-if-unanimous, else 'UNKNOWN'. Tested via
    a synthetic schemaless 'unknown_sensor' so we exercise the str
    fallback path. (Every str field in the real schemas is invariant
    today, so this test guards the general policy.)"""
    data = [
        {"sensor_name": "unknown_sensor", "status": "update", "mode": "high"},
        {"sensor_name": "unknown_sensor", "status": "update", "mode": "low"},
    ]
    result = io.avg_metadata(data)
    assert result["mode"] == "UNKNOWN"


def test_avg_metadata_invariant_disagreement_logs_error(caplog):
    """Disagreement on an invariant field (app_id, sensor_name,
    watchdog_timeout_ms) emits an ERROR log. These should never change
    inside an integration, so an ERROR is the right severity."""
    # Reset throttle so the test starts from a clean slate.
    io._last_invariant_log.clear()

    data = [
        {**IMU_READING, "app_id": 3},
        {**IMU_READING, "app_id": 99},  # impossible — Pico misconfig
    ]
    with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
        result = io.avg_metadata(data)

    # The reduction still picks min (3) and the value is int.
    assert result["app_id"] == 3
    assert isinstance(result["app_id"], int)

    # An ERROR was logged naming the stream and field.
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any(
        "app_id" in r.getMessage() and "imu_el" in r.getMessage()
        for r in errors
    ), (
        f"expected app_id ERROR for imu_el, got: {[r.getMessage() for r in errors]}"
    )


def test_avg_metadata_invariant_disagreement_throttled(caplog):
    """A second disagreement within the throttle window must NOT
    re-log. The throttle keeps a chronic producer bug from drowning
    the log file at 14k events/hour."""
    io._last_invariant_log.clear()

    data = [
        {**IMU_READING, "app_id": 3},
        {**IMU_READING, "app_id": 99},
    ]
    # First call should log.
    with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
        io.avg_metadata(data)
    first_count = sum(1 for r in caplog.records if r.levelno == logging.ERROR)
    assert first_count >= 1

    # Second call within the throttle window should NOT log again.
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
        io.avg_metadata(data)
    second_count = sum(1 for r in caplog.records if r.levelno == logging.ERROR)
    assert second_count == 0, (
        f"expected throttled (0) ERRORs on second call, got {second_count}"
    )

    # If we manually expire the throttle, the next call logs again.
    # This proves the throttle is time-based, not "log once and done".
    io._last_invariant_log[("imu_el", "app_id")] = 0.0
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
        io.avg_metadata(data)
    third_count = sum(1 for r in caplog.records if r.levelno == logging.ERROR)
    assert third_count >= 1, (
        f"expected unthrottled re-log after manual expiry, got {third_count}"
    )


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
            "stream:imu_el": [{**IMU_READING, "yaw": 1.0}],
        }
        for i in range(3):
            f.add_data(i + 1, 0.0, d, metadata=md_a)

        # add sample with new key B
        md_b = {
            "stream:imu_el": [{**IMU_READING, "yaw": 2.0}],
            "stream:rfswitch": [
                {
                    "sensor_name": "rfswitch",
                    "status": "update",
                    "app_id": 5,
                    "sw_state": 0,
                    "sw_state_name": "RFANT",
                }
            ],
        }
        f.add_data(4, 0.0, d, metadata=md_b)

        # imu_el should have 4 entries
        assert len(f.metadata["imu_el"]) == 4

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
            "stream:imu_el": [
                {**IMU_READING, "yaw": 0.1},
                {**IMU_READING, "yaw": 0.3},
            ],
            "stream:rfswitch": [
                {
                    "sensor_name": "rfswitch",
                    "status": "update",
                    "app_id": 5,
                    "sw_state": 0,
                    "sw_state_name": "RFANT",
                },
            ],
        }
        f.add_data(1, 0.0, d, metadata=md)

        # IMU values should be averaged (stream: prefix stripped)
        assert f.metadata["imu_el"][0]["yaw"] == pytest.approx(0.2)
        # rfswitch should return the state name directly
        assert f.metadata["rfswitch"][0] == "RFANT"

        # second sample without rfswitch stream — should pad with None
        md2 = {
            "stream:imu_el": [{**IMU_READING, "yaw": 0.5}],
        }
        f.add_data(2, 0.0, d, metadata=md2)
        assert f.metadata["rfswitch"][1] is None
        assert f.metadata["imu_el"][1]["yaw"] == pytest.approx(0.5)

        f.close()


def test_temp_metadata_split():
    """Verify tempctrl LNA/LOAD channels split into separate entries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 10
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        d = {"0": np.ones(spec_len, dtype=dtype)}

        md = {
            "stream:tempctrl": [
                {
                    "sensor_name": "tempctrl",
                    "app_id": 1,
                    "watchdog_tripped": False,
                    "watchdog_timeout_ms": 5000,
                    "LNA_status": "update",
                    "LNA_T_now": 30.0,
                    "LNA_timestamp": 1.0,
                    "LNA_T_target": 25.0,
                    "LNA_drive_level": 0.5,
                    "LNA_enabled": True,
                    "LNA_active": True,
                    "LNA_int_disabled": False,
                    "LNA_hysteresis": 0.1,
                    "LNA_clamp": 1.0,
                    "LOAD_status": "update",
                    "LOAD_T_now": 25.0,
                    "LOAD_timestamp": 2.0,
                    "LOAD_T_target": 25.0,
                    "LOAD_drive_level": 0.5,
                    "LOAD_enabled": True,
                    "LOAD_active": True,
                    "LOAD_int_disabled": False,
                    "LOAD_hysteresis": 0.1,
                    "LOAD_clamp": 1.0,
                },
            ],
        }
        f.add_data(1, 0.0, d, metadata=md)

        # LNA and LOAD should be separate flat entries, not nested
        assert "tempctrl" not in f.metadata
        assert "tempctrl_lna" in f.metadata
        assert "tempctrl_load" in f.metadata
        assert f.metadata["tempctrl_lna"][0]["T_now"] == 30.0
        assert f.metadata["tempctrl_load"][0]["T_now"] == 25.0

        f.close()


def test_metadata_end_to_end_round_trip():
    """Contract test for the full producer → File → HDF5 → reader chain.

    Drives ``File.add_data`` with raw stream-format metadata for every
    sensor kind (IMU, lidar, tempctrl, rfswitch, potmon) across NTIMES
    samples,
    lets the double-buffered writer flush the file, reads it back, and
    asserts the metadata matches what CORR_METADATA predicts. This is
    the guard rail that keeps:

      producer emits stream dict
        → avg_metadata averages / splits / normalizes
        → _insert_sample accumulates per-sample list
        → write_hdf5 JSON-encodes
        → read_hdf5 JSON-decodes

    consistent end-to-end. If any of these links drifts, the assertion
    against CORR_METADATA fires. See CLAUDE.md "Testing philosophy".
    """

    def _stream_payload(i):
        # Raw stream dicts shaped exactly like what picohost pushes to
        # Redis. Numeric values vary per sample so avg_metadata has
        # something to average (each integration averages a single
        # reading here, which is degenerate but contract-identical).
        return {
            "stream:imu_el": [
                {**IMU_READING, "yaw": 0.001 * i},
            ],
            "stream:imu_az": [
                {
                    **IMU_READING,
                    "sensor_name": "imu_az",
                    "app_id": 6,
                    "yaw": 0.002 * i,
                },
            ],
            "stream:lidar": [
                {
                    "sensor_name": "lidar",
                    "status": "update",
                    "app_id": 4,
                    "distance_m": 1.5 + 0.001 * i,
                },
            ],
            "stream:potmon": [
                {
                    "sensor_name": "potmon",
                    "status": "update",
                    "app_id": 2,
                    "pot_el_voltage": 1.5 + 0.001 * i,
                    "pot_az_voltage": 1.5,
                    "pot_el_cal_slope": 100.0,
                    "pot_el_cal_intercept": -50.0,
                    "pot_az_cal_slope": 200.0,
                    "pot_az_cal_intercept": -100.0,
                    "pot_el_angle": 100.0 * (1.5 + 0.001 * i) - 50.0,
                    "pot_az_angle": 200.0 * 1.5 - 100.0,
                },
            ],
            "stream:tempctrl": [
                {
                    "sensor_name": "tempctrl",
                    "app_id": 1,
                    "watchdog_tripped": False,
                    "watchdog_timeout_ms": 5000,
                    "LNA_status": "update",
                    "LNA_T_now": 30.0 + 0.01 * i,
                    "LNA_timestamp": 1.0 + i,
                    "LNA_T_target": 25.0,
                    "LNA_drive_level": 0.0,
                    "LNA_enabled": True,
                    "LNA_active": True,
                    "LNA_int_disabled": False,
                    "LNA_hysteresis": 0.5,
                    "LNA_clamp": 100.0,
                    "LOAD_status": "update",
                    "LOAD_T_now": 25.0 + 0.01 * i,
                    "LOAD_timestamp": 1.0 + i,
                    "LOAD_T_target": 25.0,
                    "LOAD_drive_level": 0.0,
                    "LOAD_enabled": True,
                    "LOAD_active": True,
                    "LOAD_int_disabled": False,
                    "LOAD_hysteresis": 0.5,
                    "LOAD_clamp": 100.0,
                },
            ],
        }

    def _rfswitch_payload(name):
        return {
            "stream:rfswitch": [
                {
                    "sensor_name": "rfswitch",
                    "status": "update",
                    "app_id": 5,
                    "sw_state": PicoRFSwitch.rbin(PicoRFSwitch.path_str[name]),
                    "sw_state_name": name,
                },
            ],
        }

    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        # Use int_time = 0.1s so the rfswitch transition window is 5
        # samples — matches the pattern baked into CORR_METADATA's
        # rfswitch list (20 steady · 5 UNKNOWN · 20 steady · 5 None · 10
        # steady). With HEADER's default 1.0s window would be 1 and the
        # pattern would not line up.
        cfg = HEADER.copy()
        cfg["integration_time"] = 0.1
        f = io.File(save_dir, pairs, NTIMES, cfg)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        d = {"0": np.full(spec_len, 1, dtype=dtype)}

        # Drive NTIMES samples. rfswitch is fed in a pattern that
        # produces the same per-sample sequence as CORR_METADATA:
        #   indices  0..19  → raw state 0
        #   index    20     → raw state 1 (triggers window, flagged UNKNOWN)
        #   indices 21..24  → state 1 still inside forward window → UNKNOWN
        #   indices 25..44  → raw state 1 (window expired)
        #   indices 45..49  → NO rfswitch stream at all → None pad
        #   indices 50..59  → raw state 1
        #
        # At ERROR_INTEGRATION_INDEX, append a SECOND raw IMU sample
        # marked status="error" alongside the normal one. The averager
        # filters the errored sample from every numeric reduction, so
        # the data fields come out the same as if only the normal sample
        # was fed; what changes is that the integration's `status` field
        # collapses to "error" — see CORR_METADATA where index
        # ERROR_INTEGRATION_INDEX uses _imu_errored_integration_entry.
        for i in range(NTIMES):
            md = _stream_payload(i)
            if i == ERROR_INTEGRATION_INDEX:
                md["stream:imu_el"].append(
                    {
                        **IMU_READING,
                        "status": "error",
                        "yaw": 999.0,  # garbage; must NOT reach the file
                    }
                )
            if i < 20:
                md.update(_rfswitch_payload("RFANT"))
            elif i < 45:
                md.update(_rfswitch_payload("RFNOFF"))
            elif i < 50:
                pass  # no rfswitch → None pad
            else:
                md.update(_rfswitch_payload("RFNOFF"))
            f.add_data(i + 1, 0.0, d, metadata=md)

        # The active buffer was swapped to standby on the NTIMES-th
        # add_data, so f.metadata is now empty — that's why we don't
        # check the in-memory dict here. Flush the writer and read
        # the file back.
        f.close()
        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1, f"expected one file, got {files}"
        _data, _hdr, read_meta = io.read_hdf5(files[0])

        # The read-back metadata also matches CORR_METADATA, proving
        # the full round-trip (including JSON encode / decode of
        # list-of-dicts with None gap-fill entries).
        compare_dicts(CORR_METADATA, read_meta)

        # Belt-and-suspenders for the partial-error case: assert
        # explicitly that the errored integration's status flag landed
        # in the file as "error" and that the garbage 999.0 yaw
        # from the errored raw sample was filtered out.
        errored_entry = read_meta["imu_el"][ERROR_INTEGRATION_INDEX]
        assert errored_entry["status"] == "error"
        assert errored_entry["yaw"] == pytest.approx(
            0.001 * ERROR_INTEGRATION_INDEX
        )
        # Surrounding integrations stay clean.
        assert (
            read_meta["imu_el"][ERROR_INTEGRATION_INDEX - 1]["status"]
            == "update"
        )
        assert (
            read_meta["imu_el"][ERROR_INTEGRATION_INDEX + 1]["status"]
            == "update"
        )


def test_potmon_uncalibrated_end_to_end_round_trip(caplog):
    """Round-trip the uncalibrated-potmon producer state.

    The ``potmon`` schema in ``io.py`` documents that for an
    *uncalibrated* stream the cal/angle fields are ``None``:

      - ``_validate_metadata`` short-circuits ``None`` so it does not
        flag ``None`` as a type violation;
      - ``_avg_sensor_values``'s float reduction filters ``None``
        survivors before computing the mean, so an integration whose
        cal/angle samples are all ``None`` averages cleanly to ``None``;
      - ``write_hdf5`` / ``read_hdf5`` JSON-encode the per-sample list
        of dicts and must preserve ``None`` across the round trip.

    The main ``test_metadata_end_to_end_round_trip`` exercises only the
    *calibrated* shape (every cal/angle field is a real float), so the
    promises above are not pinned anywhere else. This test feeds raw
    stream samples with all-``None`` cal/angle fields through the full
    producer → ``avg_metadata`` → ``File.add_data`` → ``write_hdf5`` →
    ``read_hdf5`` chain and asserts (a) no contract-violation warnings
    are logged, and (b) cal/angle survive the round trip as ``None``
    while voltages average correctly.
    """
    n = 5

    def _uncalibrated_payload(i):
        return {
            "stream:potmon": [
                {
                    "sensor_name": "potmon",
                    "status": "update",
                    "app_id": 2,
                    "pot_el_voltage": 1.5 + 0.001 * i,
                    "pot_az_voltage": 1.5,
                    "pot_el_cal_slope": None,
                    "pot_el_cal_intercept": None,
                    "pot_az_cal_slope": None,
                    "pot_az_cal_intercept": None,
                    "pot_el_angle": None,
                    "pot_az_angle": None,
                },
            ],
        }

    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        cfg = HEADER.copy()
        cfg["integration_time"] = 0.1
        f = io.File(save_dir, pairs, n, cfg)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        d = {"0": np.full(spec_len, 1, dtype=dtype)}

        with caplog.at_level(logging.WARNING, logger="eigsep_observing.io"):
            for i in range(n):
                f.add_data(i + 1, 0.0, d, metadata=_uncalibrated_payload(i))
            f.close()

        # No metadata-contract warning should fire for an
        # all-``None`` cal/angle stream — that's the documented
        # short-circuit in ``_validate_metadata``. If a future change
        # tightens validation to reject ``None`` for these fields,
        # this assertion fires loudly.
        contract_warnings = [
            r
            for r in caplog.records
            if "Metadata contract violation" in r.getMessage()
        ]
        assert contract_warnings == [], (
            "uncalibrated potmon stream produced unexpected contract "
            f"warnings: {[r.getMessage() for r in contract_warnings]}"
        )

        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1, f"expected one file, got {files}"
        _data, _hdr, read_meta = io.read_hdf5(files[0])

        assert "potmon" in read_meta
        potmon_rows = read_meta["potmon"]
        assert len(potmon_rows) == n
        for i, row in enumerate(potmon_rows):
            # Voltages averaged from a single survivor → that survivor's
            # value, preserved as float across the JSON round trip.
            assert row["pot_el_voltage"] == pytest.approx(1.5 + 0.001 * i)
            assert row["pot_az_voltage"] == pytest.approx(1.5)
            # Cal/angle survivors were all None → reduction returns None
            # → JSON encode + decode preserves None (not 0.0, not "None",
            # not missing key).
            assert row["pot_el_cal_slope"] is None
            assert row["pot_el_cal_intercept"] is None
            assert row["pot_az_cal_slope"] is None
            assert row["pot_az_cal_intercept"] is None
            assert row["pot_el_angle"] is None
            assert row["pot_az_angle"] is None
            # The integration is clean — no errored samples.
            assert row["status"] == "update"


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


# ----------------------------------------------------------------------
# Phase 6 — Negative tests for the per-sensor averaging helpers.
#
# These functions are called inside the per-stream safety net in
# add_data, which is designed to catch their exceptions and keep
# corr data flowing. The contract here is that the helpers MUST
# raise on malformed input — adding broad try/except inside them
# would silently swallow producer bugs that the safety net is
# supposed to surface as ERROR logs.
#
# These tests lock in the raise-don't-swallow contract so a future
# refactor that "hardens" the helpers also has to update these
# tests, which forces the conversation about whether the helper
# should swallow or propagate.
# ----------------------------------------------------------------------


def test_avg_rfswitch_metadata_raises_on_unhashable_state():
    """Unhashable sw_state_name (e.g., a list) trips the set() call →
    TypeError. Must propagate to the safety net, not be swallowed."""
    value = [
        {
            "sensor_name": "rfswitch",
            "status": "update",
            "app_id": 5,
            "sw_state": 0,
            "sw_state_name": ["RFANT", "RFNOFF"],
        }
    ]
    with pytest.raises(TypeError):
        io._avg_rfswitch_metadata(value)


def test_avg_rfswitch_metadata_raises_on_non_dict_entry():
    """A non-dict entry trips v.get('status') → AttributeError.
    Must propagate."""
    value = [
        {
            "sensor_name": "rfswitch",
            "status": "update",
            "app_id": 5,
            "sw_state": 0,
            "sw_state_name": "RFANT",
        },
        "not a dict",
    ]
    with pytest.raises(AttributeError):
        io._avg_rfswitch_metadata(value)


def test_avg_temp_metadata_raises_on_non_dict_first_entry():
    """A non-dict first entry trips value[0].get('app_id') →
    AttributeError. Must propagate."""
    with pytest.raises(AttributeError):
        io._avg_temp_metadata(
            ["not a dict"],
            "tempctrl",
            io.SENSOR_SCHEMAS["tempctrl"],
        )


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


def test_avg_metadata_schema_violation_no_crash(caplog):
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
    with caplog.at_level(logging.WARNING, logger="eigsep_observing.io"):
        result = io.avg_metadata(bad_data)
    # Should still return a dict (best-effort from schema keys)
    assert isinstance(result, dict)
    # Schema key "distance_m" is missing from data, so should be None
    assert result["distance_m"] is None
    # Extra key "bogus_field" is not in schema, so not in result
    assert "bogus_field" not in result
    # The validator should have warned about the schema violation —
    # this locks in that avg_metadata actually invokes _validate_metadata
    # and surfaces the contract issue (defense in depth on top of the
    # best-effort result).
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "lidar" in w.getMessage() and "violation" in w.getMessage().lower()
        for w in warnings
    ), (
        f"expected a contract-violation warning, got: {[w.getMessage() for w in warnings]}"
    )


def test_corr_data_saved_despite_metadata_crash(monkeypatch, caplog):
    """Corr data must survive a metadata-processing exception.

    Contract: corr data is sacred. Producer-side contract violations
    that escape ``avg_metadata`` must be logged at ERROR level but
    must not prevent the corr sample from being inserted or the file
    from being written. The safety net is per-stream, so a crash on
    one stream must not affect processing of a sibling stream.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 3
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]

        # Make avg_metadata raise for the imu stream but succeed for
        # rfswitch — verifies the safety net is per-stream, not a
        # blanket catch around the whole metadata loop.
        real_avg = io.avg_metadata

        def picky(value):
            if (
                value
                and isinstance(value[0], dict)
                and value[0].get("sensor_name") == "imu_el"
            ):
                raise RuntimeError("simulated producer contract violation")
            return real_avg(value)

        monkeypatch.setattr(io, "avg_metadata", picky)

        md = {
            "stream:imu_el": [{**IMU_READING, "yaw": 0.1}],
            "stream:rfswitch": [
                {
                    "sensor_name": "rfswitch",
                    "status": "update",
                    "app_id": 5,
                    "sw_state": 6,
                    "sw_state_name": "RFNON",
                }
            ],
        }

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            for i in range(ntimes):
                d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
                # Must not raise — corr data is sacred.
                f.add_data(i + 1, 0.0, d, metadata=md)

        # Buffer was full → corr_write was called → counter reset.
        assert f.counter == 0
        f._write_queue.join()

        # File was written despite the metadata crashes.
        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1

        read_data, _, read_meta = io.read_hdf5(files[0])
        # Corr data is intact and matches what was passed in.
        assert read_data["0"].shape[0] == ntimes
        for i in range(ntimes):
            assert np.all(read_data["0"][i] == i + 1)

        # rfswitch processing succeeded despite imu crash —
        # confirms the safety net is per-stream, not blanket.
        assert "rfswitch" in read_meta
        # imu_el was never successfully processed → absent.
        assert "imu_el" not in read_meta

        # An ERROR-level contract-violation log was emitted for each
        # add_data call (so the producer bug is loudly visible).
        contract_errors = [
            r
            for r in caplog.records
            if r.levelno == logging.ERROR
            and "contract violation" in r.getMessage().lower()
        ]
        assert len(contract_errors) == ntimes

        f.close()


def test_add_data_data_none_logs_error_drops_sample(caplog):
    """data=None is a producer contract violation: drop + ERROR."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        f = io.File(save_dir, ["0"], 5, HEADER)

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            f.add_data(1, 0.0, None)

        # Sample dropped (nothing to save).
        assert f.counter == 0

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        msg = errors[0].getMessage()
        assert "data is None" in msg
        assert "contract violation" in msg.lower()

        f.close()


def test_add_data_acc_cnt_none_stores_nan_saves_sample(caplog):
    """acc_cnt=None: keep the sample, store NaN, log ERROR loudly.

    Corr data is sacred — a broken acc_cnt must not cost us the
    spectrum. The NaN sentinel preserves the data and gives downstream
    an unambiguous signal that the timestamp is unknown.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 5
        f = io.File(save_dir, ["0"], ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        d = {"0": np.full(spec_len, 7, dtype=dtype)}

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            f.add_data(None, 0.0, d)

        # Sample saved (counter advanced — A1 from the design discussion).
        assert f.counter == 1
        assert np.isnan(f.acc_cnts[0])
        np.testing.assert_array_equal(f.data["0"][0], d["0"])

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        msg = errors[0].getMessage()
        assert "acc_cnt is None" in msg
        assert "contract violation" in msg.lower()

        # Subsequent valid sample is inserted normally; the NaN
        # _prev_cnt suppresses gap-fill across the boundary, which is
        # the documented tradeoff.
        d2 = {"0": np.full(spec_len, 8, dtype=dtype)}
        f.add_data(10, 0.0, d2)
        assert f.counter == 2
        assert f.acc_cnts[1] == 10
        np.testing.assert_array_equal(f.data["0"][1], d2["0"])

        f.close()


def test_add_data_malformed_metadata_shape_logs_error(caplog):
    """Malformed metadata shape is a producer contract violation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        f = io.File(save_dir, ["0"], 5, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        d = {"0": np.full(spec_len, 1, dtype=np.dtype(HEADER["dtype"]))}

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            # Pass a string instead of a list of dicts — bad shape.
            f.add_data(1, 0.0, d, metadata={"stream:imu_el": "not a list"})

        # Corr data still saved despite the bad metadata.
        assert f.counter == 1
        # The bad stream is dropped from the active metadata.
        assert "imu_el" not in f.metadata

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "imu_el" in e.getMessage() and "non-empty list" in e.getMessage()
            for e in errors
        ), (
            f"expected a producer-contract-violation log, got: {[e.getMessage() for e in errors]}"
        )

        f.close()


def test_close_logs_pending_write_error(caplog):
    """close() must surface any pending _write_error so the operator
    sees end-of-run failures even if no further add_data ever runs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 3
        f = io.File(save_dir, ["0"], ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])

        # Force the write to fail by making the directory read-only.
        os.chmod(save_dir, stat.S_IRUSR | stat.S_IXUSR)
        try:
            for i in range(ntimes):
                d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
                f.add_data(i + 1, 0.0, d)
            f._write_queue.join()
            assert f._write_error is not None

            with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
                f.close()
        finally:
            os.chmod(
                save_dir,
                stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO,
            )

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "shutdown" in e.getMessage().lower()
            and "write error" in e.getMessage().lower()
            for e in errors
        ), (
            f"expected a shutdown write-error log, got: {[e.getMessage() for e in errors]}"
        )


def test_validate_corr_header():
    """Schema validation returns descriptive violations, no exceptions."""
    valid = {
        "acc_bins": 2,
        "avg_even_odd": True,
        "nchan": 1024,
        "dtype": ">i4",
        "integration_time": 0.1,
        "sample_rate": 500.0,  # MHz
    }
    assert io._validate_corr_header(valid) == []

    # missing key
    bad = {k: v for k, v in valid.items() if k != "integration_time"}
    violations = io._validate_corr_header(bad)
    assert len(violations) == 1
    assert "integration_time" in violations[0]
    assert "missing" in violations[0]

    # wrong type
    bad = {**valid, "sample_rate": "fast"}
    violations = io._validate_corr_header(bad)
    assert len(violations) == 1
    assert "sample_rate" in violations[0]
    assert "expected float" in violations[0]

    # int accepted for float field
    assert (
        io._validate_corr_header({**valid, "sample_rate": 500_000_000}) == []
    )
    # numpy scalar accepted
    assert io._validate_corr_header({**valid, "nchan": np.int64(1024)}) == []

    # bool not accepted as int
    violations = io._validate_corr_header({**valid, "acc_bins": True})
    assert any("acc_bins" in v and "expected int" in v for v in violations)

    # unparseable dtype
    violations = io._validate_corr_header({**valid, "dtype": "not-a-dtype"})
    assert any("dtype" in v and "cannot parse" in v for v in violations)


def test_set_header_logs_on_bad_header_no_raise(caplog):
    """set_header logs ERROR per CORR_HEADER_SCHEMA violation but
    must NOT raise — corr data is sacred and a header bug must not
    stop the script."""
    with tempfile.TemporaryDirectory() as tmpdir:
        bad_cfg = HEADER.copy()
        del bad_cfg["integration_time"]
        bad_cfg["sample_rate"] = "not a number"

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            f = io.File(tmpdir, ["0"], 5, bad_cfg)

        # Construction succeeded.
        assert f._writer_thread.is_alive()

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "integration_time" in e.getMessage()
            and "missing" in e.getMessage()
            for e in errors
        ), (
            f"expected missing-integration_time log, got: {[e.getMessage() for e in errors]}"
        )
        assert any(
            "sample_rate" in e.getMessage()
            and "expected float" in e.getMessage()
            for e in errors
        ), (
            f"expected wrong-type sample_rate log, got: {[e.getMessage() for e in errors]}"
        )

        f.close()


def test_corr_data_saved_despite_missing_integration_time(caplog):
    """A header missing integration_time must not block the
    corr-data write. The 'times' field is omitted with an ERROR log;
    everything else is intact."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 3
        bad_cfg = HEADER.copy()
        del bad_cfg["integration_time"]
        f = io.File(save_dir, ["0"], ntimes, bad_cfg)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            for i in range(ntimes):
                d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
                f.add_data(i + 1, 0.0, d)
            f._write_queue.join()

        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1

        read_data, read_header, _ = io.read_hdf5(files[0])
        # Corr data intact.
        assert read_data["0"].shape[0] == ntimes
        for i in range(ntimes):
            assert np.all(read_data["0"][i] == i + 1)
        # 'times' could not be computed → omitted from the file.
        assert "times" not in read_header
        # 'freqs' is independent (uses sample_rate + nchan) → still present.
        assert "freqs" in read_header

        # ERROR log naming the failing computation.
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "times" in e.getMessage()
            and "Header contract violation" in e.getMessage()
            for e in errors
        ), (
            f"expected a 'times' contract-violation log, got: {[e.getMessage() for e in errors]}"
        )

        f.close()


def test_corr_data_saved_despite_bad_header_field(caplog):
    """A header field whose value can't be serialized must not block
    the corr-data write. The bad field is logged + skipped; the rest
    of the header and the corr data are saved."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 3
        f = io.File(save_dir, ["0"], ntimes, HEADER)
        # Inject a header field that breaks _write_header_item:
        # bytes routes to _write_dataset → json.dumps(bytes) → TypeError.
        f.header["unwriteable"] = b"bad"

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            for i in range(ntimes):
                d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
                f.add_data(i + 1, 0.0, d)
            f._write_queue.join()

        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1

        read_data, read_header, _ = io.read_hdf5(files[0])
        # Corr data intact.
        assert read_data["0"].shape[0] == ntimes
        for i in range(ntimes):
            assert np.all(read_data["0"][i] == i + 1)
        # Bad field skipped, other fields present.
        assert "unwriteable" not in read_header
        assert "nchan" in read_header
        assert "freqs" in read_header

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "unwriteable" in e.getMessage()
            and "Header contract violation" in e.getMessage()
            for e in errors
        ), (
            f"expected a header-contract-violation log, got: {[e.getMessage() for e in errors]}"
        )

        f.close()


def test_partial_sample_missing_pair_logs_error_saves_others(caplog):
    """A SNAP batch missing one pair must not cost us the other pairs.
    Per-pair contract violation: log ERROR, leave the missing pair's
    slot at zero, save the other pairs, advance counter."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 5
        f = io.File(save_dir, ["0", "1"], ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])

        # Sample missing pair "1" entirely.
        partial_data = {"0": np.full(spec_len, 7, dtype=dtype)}

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            f.add_data(1, 0.0, partial_data)

        # Sample inserted; counter advanced.
        assert f.counter == 1
        # Pair 0 has its data.
        np.testing.assert_array_equal(f.data["0"][0], partial_data["0"])
        # Pair 1 is zero (visually distinguishable from real data).
        np.testing.assert_array_equal(
            f.data["1"][0], np.zeros(spec_len, dtype=dtype)
        )

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "pair '1'" in e.getMessage()
            and "contract violation" in e.getMessage().lower()
            for e in errors
        ), (
            f"expected pair '1' contract-violation log, got: {[e.getMessage() for e in errors]}"
        )

        f.close()


def test_partial_sample_wrong_shape_drops_pair_saves_others(caplog):
    """A SNAP batch with one pair as a half-spectrum must drop that
    pair wholesale (no half-spectra accepted) and save the others."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 5
        f = io.File(save_dir, ["0", "1"], ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])

        # Pair 0 has the wrong shape (half spectrum); pair 1 is fine.
        bad_data = {
            "0": np.full(spec_len // 2, 9, dtype=dtype),
            "1": np.full(spec_len, 7, dtype=dtype),
        }

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            f.add_data(1, 0.0, bad_data)

        # Sample inserted; counter advanced.
        assert f.counter == 1
        # Pair 0 dropped wholesale (not half-saved) → zero.
        np.testing.assert_array_equal(
            f.data["0"][0], np.zeros(spec_len, dtype=dtype)
        )
        # Pair 1 saved.
        np.testing.assert_array_equal(f.data["1"][0], bad_data["1"])

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "pair '0'" in e.getMessage()
            and "contract violation" in e.getMessage().lower()
            for e in errors
        ), (
            f"expected pair '0' contract-violation log, got: {[e.getMessage() for e in errors]}"
        )

        f.close()


def test_corr_write_drops_buffer_on_writer_hang(caplog):
    """When the writer thread is stuck, corr_write must time out
    and drop the active buffer rather than block the data loop
    forever. The script stays alive, _dropped_buffers is
    incremented, and a loud ERROR is logged. After the writer
    unblocks, subsequent writes proceed normally."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 3
        # Aggressive timeout to keep the test fast.
        f = io.File(save_dir, ["0"], ntimes, HEADER, writer_timeout=0.5)

        # Block the writer inside _do_write on an event we control.
        # We replace the bound method on the instance — the writer
        # loop reads self._do_write at call time, so this takes
        # effect immediately for any future writes.
        unblock = threading.Event()
        original_do_write = f._do_write

        def blocked_do_write(*args, **kwargs):
            unblock.wait(timeout=10)
            return original_do_write(*args, **kwargs)

        f._do_write = blocked_do_write

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])

        # First buffer fill → enqueued, writer starts and blocks in
        # blocked_do_write. _standby_ready is now cleared.
        for i in range(ntimes):
            d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
            f.add_data(i + 1, 0.0, d)

        # Second buffer fill → corr_write should hit the timeout
        # and drop. The data loop must NOT block.
        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            for i in range(ntimes):
                d = {"0": np.full(spec_len, ntimes + i + 1, dtype=dtype)}
                f.add_data(ntimes + i + 1, 0.0, d)

        # Buffer dropped → counter reset, drop counter incremented.
        assert f.counter == 0
        assert f._dropped_buffers == 1

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "Writer thread blocked" in e.getMessage()
            and "dropping buffer" in e.getMessage()
            for e in errors
        ), (
            f"expected a writer-hang drop log, got: {[e.getMessage() for e in errors]}"
        )

        # Release the writer; the first (still-pending) write
        # completes and the writer becomes idle.
        unblock.set()
        f._write_queue.join()

        # Third buffer → must write normally now that the writer
        # is free. Recovery contract: no further drops AND the
        # write hits disk.
        for i in range(ntimes):
            d = {"0": np.full(spec_len, 100 + i, dtype=dtype)}
            f.add_data(2 * ntimes + i + 1, 0.0, d)
        f._write_queue.join()

        # Drop counter unchanged → third buffer was NOT dropped,
        # the writer is healthy again.
        assert f._dropped_buffers == 1
        # Exactly 2 files on disk: the first batch (after unblock)
        # and the third (post-recovery). The second was dropped.
        # Phase 5b filename disambiguation ensures the two writes
        # don't collide on the same second-resolution timestamp.
        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 2

        f.close()


def test_close_logs_dropped_buffer_count(caplog):
    """If any buffers were dropped during the run, close() must
    surface the total at shutdown so the operator sees the loss."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 3
        f = io.File(save_dir, ["0"], ntimes, HEADER, writer_timeout=0.3)

        unblock = threading.Event()
        original_do_write = f._do_write

        def blocked_do_write(*args, **kwargs):
            unblock.wait(timeout=10)
            return original_do_write(*args, **kwargs)

        f._do_write = blocked_do_write

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])

        # First buffer → blocks the writer
        for i in range(ntimes):
            d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
            f.add_data(i + 1, 0.0, d)
        # Second buffer → dropped
        for i in range(ntimes):
            d = {"0": np.full(spec_len, ntimes + i + 1, dtype=dtype)}
            f.add_data(ntimes + i + 1, 0.0, d)

        assert f._dropped_buffers == 1

        # Release and close.
        unblock.set()
        f._write_queue.join()

        with caplog.at_level(logging.ERROR, logger="eigsep_observing.io"):
            f.close()

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "Total buffers dropped" in e.getMessage() and "1" in e.getMessage()
            for e in errors
        ), (
            f"expected a shutdown drop-count log, got: {[e.getMessage() for e in errors]}"
        )


def test_corr_filename_disambiguation_on_same_second_collision():
    """When two corr writes hit the same second-resolution timestamp,
    the second filename must be disambiguated with -1, -2, ... so
    no data is silently overwritten. In production this almost never
    happens (file_time is 60-240s), but it's still a real data-loss
    risk and the fix is essentially free."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 2
        f = io.File(save_dir, ["0"], ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])

        # Three rapid back-to-back fills. With second-resolution
        # timestamps these would all collide on the same filename
        # without disambiguation. With disambiguation: 3 distinct
        # files.
        for batch in range(3):
            for i in range(ntimes):
                d = {"0": np.full(spec_len, batch + 1, dtype=dtype)}
                f.add_data(batch * ntimes + i + 1, 0.0, d)
            f._write_queue.join()

        files = sorted(glob.glob(str(save_dir / "*.h5")))
        assert len(files) == 3, f"expected 3 files, got: {files}"
        # Filenames: corr_<ts>.h5, corr_<ts>-1.h5, corr_<ts>-2.h5
        # (or with different timestamps if the test crossed a second
        # boundary, in which case some won't have suffixes — both
        # outcomes are correct).
        names = [Path(p).name for p in files]
        # All three filenames must be distinct (no overwrites).
        assert len(set(names)) == 3

        f.close()


def test_s11_filename_disambiguation_on_same_second_collision():
    """write_s11_file must also disambiguate same-second collisions
    on auto-generated filenames (mirrors the corr fix)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        data, _ = generate_s11_data(npoints=S11_HEADER["npoints"], cal=True)

        # Two back-to-back writes with auto-generated names.
        io.write_s11_file(data, S11_HEADER, fname=None, save_dir=tmpdir)
        io.write_s11_file(data, S11_HEADER, fname=None, save_dir=tmpdir)
        io.write_s11_file(data, S11_HEADER, fname=None, save_dir=tmpdir)

        files = sorted(glob.glob(str(Path(tmpdir) / "*.h5")))
        assert len(files) == 3, f"expected 3 files, got: {files}"
        names = [Path(p).name for p in files]
        assert len(set(names)) == 3


def test_close():
    """Test that close() shuts down the writer thread."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        f = io.File(save_dir, pairs, 10, HEADER)
        assert f._writer_thread.is_alive()
        f.close()
        assert not f._writer_thread.is_alive()


def test_close_flushes_pending_buffer():
    """close() must flush a non-empty active buffer to disk before
    shutting down the writer thread. This is the contract that lets
    callers simply call ``close()`` at end-of-run without remembering
    to manually call ``corr_write()`` first — the previous behavior
    silently dropped the partial buffer."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        pairs = ["0"]
        ntimes = 10
        f = io.File(save_dir, pairs, ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])

        # Add fewer than ntimes samples so corr_write is NOT triggered
        # by add_data — the buffer is partial when close() runs.
        n_partial = 4
        for i in range(n_partial):
            d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
            f.add_data(i + 1, 0.0, d)
        assert f.counter == n_partial

        # close() must flush the partial buffer.
        f.close()
        assert not f._writer_thread.is_alive()

        # Exactly one file with exactly n_partial samples on disk —
        # no trailing zero padding from the preallocated buffer.
        files = sorted(glob.glob(str(save_dir / "*.h5")))
        assert len(files) == 1, (
            f"close() did not flush partial buffer; files: {files}"
        )
        on_disk_data, _, _ = io.read_hdf5(files[0])
        assert on_disk_data["0"].shape[0] == n_partial
        # Sanity-check the actual sample values made it through.
        for i in range(n_partial):
            assert np.all(on_disk_data["0"][i] == i + 1)


def test_close_with_empty_buffer_writes_no_file():
    """close() with counter == 0 must NOT write a file. The flush
    is conditional on having data — calling close() on an empty
    File (e.g. immediately after init) is a no-op write-wise."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        f = io.File(save_dir, ["0"], 10, HEADER)
        assert f.counter == 0
        f.close()
        files = glob.glob(str(save_dir / "*.h5"))
        assert files == [], f"unexpected files written by empty close: {files}"


# ----------------------------------------------------------------------
# Phase 4b — Buffer/metadata invariant enforcement.
#
# Lock in the contracts that the per-pair safety net (and downstream
# interpretation of "zero == dropped pair") depend on:
#   - __init__ produces zero-filled buffers
#   - reset() zeros buffers and clears metadata
#   - corr_write swap leaves the new active buffer zero
#   - After every add_data, every metadata key list has length ==
#     counter (the 1:1 alignment invariant)
# ----------------------------------------------------------------------


def test_init_creates_zero_filled_buffers():
    """File.__init__ must produce all-zero data buffers, empty
    metadata, and counter=0. Locks in the contract the per-pair
    safety net depends on."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pairs = ["0", "02"]  # one auto, one cross
        ntimes = 10
        f = io.File(tmpdir, pairs, ntimes, HEADER)

        assert f.counter == 0
        assert len(f.metadata) == 0

        dtype = np.dtype(HEADER["dtype"])
        for p in pairs:
            cross = len(p) > 1
            expected_shape = io.data_shape(
                ntimes, HEADER["acc_bins"], HEADER["nchan"], cross=cross
            )
            assert f.data[p].shape == expected_shape
            assert f.data[p].dtype == dtype
            np.testing.assert_array_equal(
                f.data[p], np.zeros(expected_shape, dtype=dtype)
            )

        np.testing.assert_array_equal(f.acc_cnts, np.zeros(ntimes))
        np.testing.assert_array_equal(f.sync_times, np.zeros(ntimes))

        f.close()


def test_reset_zeros_buffers_and_clears_metadata():
    """File.reset() must restore __init__-equivalent state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pairs = ["0"]
        ntimes = 5
        f = io.File(tmpdir, pairs, ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        d = {"0": np.full(spec_len, 7, dtype=dtype)}
        md = {"stream:imu_el": [{**IMU_READING, "yaw": 0.1}]}
        f.add_data(1, 0.0, d, metadata=md)
        f.add_data(2, 0.0, d, metadata=md)

        # Sanity: state is dirty before reset.
        assert f.counter == 2
        assert len(f.metadata) > 0
        assert not np.all(f.data["0"] == 0)

        f.reset()

        assert f.counter == 0
        assert len(f.metadata) == 0
        np.testing.assert_array_equal(f.data["0"], np.zeros_like(f.data["0"]))
        np.testing.assert_array_equal(f.acc_cnts, np.zeros_like(f.acc_cnts))
        np.testing.assert_array_equal(
            f.sync_times, np.zeros_like(f.sync_times)
        )

        f.close()


def test_swap_buffers_preserves_zero_invariant():
    """After corr_write triggers a swap + reset, the new active
    buffer must be zero-filled and metadata empty. The downstream
    "zero == dropped pair" interpretation depends on this."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pairs = ["0"]
        ntimes = 3
        f = io.File(tmpdir, pairs, ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        for i in range(ntimes):
            d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
            f.add_data(i + 1, 0.0, d)
        f._write_queue.join()

        # After the swap + reset, the active buffer is fresh.
        assert f.counter == 0
        assert len(f.metadata) == 0
        np.testing.assert_array_equal(f.data["0"], np.zeros_like(f.data["0"]))

        f.close()


def test_metadata_invariant_after_normal_add_data():
    """After normal add_data calls, every metadata key has a list
    of length equal to counter (1:1 alignment)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pairs = ["0"]
        ntimes = 10
        f = io.File(tmpdir, pairs, ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        d = {"0": np.full(spec_len, 1, dtype=dtype)}
        md = {"stream:imu_el": [{**IMU_READING, "yaw": 0.1}]}

        for i in range(3):
            f.add_data(i + 1, 0.0, d, metadata=md)
            for key, lst in f.metadata.items():
                assert len(lst) == f.counter, (
                    f"metadata['{key}'] length {len(lst)} != "
                    f"counter {f.counter} after sample {i + 1}"
                )

        f.close()


def test_metadata_invariant_after_dropped_pair():
    """A per-pair drop must not break the metadata 1:1 alignment.
    The sample is still inserted; metadata still tracks it."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pairs = ["0", "1"]
        ntimes = 10
        f = io.File(tmpdir, pairs, ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        # Pair 1 missing — will be dropped + zeroed.
        partial = {"0": np.full(spec_len, 1, dtype=dtype)}
        md = {"stream:imu_el": [{**IMU_READING, "yaw": 0.1}]}

        f.add_data(1, 0.0, partial, metadata=md)

        assert f.counter == 1
        for key, lst in f.metadata.items():
            assert len(lst) == f.counter, (
                f"metadata['{key}'] length {len(lst)} != counter "
                f"{f.counter} after dropped-pair sample"
            )

        f.close()


def test_metadata_invariant_after_gap_fill():
    """A gap-fill burst (multiple synthetic samples in one add_data
    call) must preserve the metadata 1:1 alignment AND fill the
    synthetic positions with None — the documented "no reading at
    this sample" sentinel that downstream consumers rely on."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pairs = ["0"]
        ntimes = 100
        f = io.File(tmpdir, pairs, ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        d = {"0": np.full(spec_len, 1, dtype=dtype)}
        md = {"stream:imu_el": [{**IMU_READING, "yaw": 0.1}]}

        # First sample at acc_cnt=1.
        f.add_data(1, 0.0, d, metadata=md)
        # Jump to acc_cnt=20 → gap-fill 18 samples + insert 1 real.
        f.add_data(20, 0.0, d, metadata=md)

        assert f.counter == 20
        for key, lst in f.metadata.items():
            assert len(lst) == f.counter, (
                f"metadata['{key}'] length {len(lst)} != counter "
                f"{f.counter} after gap-fill"
            )

        # Filler invariant: gap-filled positions are None, real
        # positions are not. Locks in the None sentinel against
        # future drift to other values (0, "", {}, NaN, ...).
        imu_list = f.metadata["imu_el"]
        assert imu_list[0] is not None  # real (acc_cnt=1)
        for i in range(1, 19):
            assert imu_list[i] is None, (
                f"gap-filled imu_list[{i}] expected None, got {imu_list[i]!r}"
            )
        assert imu_list[19] is not None  # real (acc_cnt=20)

        f.close()


def test_metadata_filler_is_none_when_stream_appears_late():
    """A metadata stream that first appears on sample N must be
    back-filled with None for samples 0..N-1, NOT zero, empty
    string, empty dict, or any other sentinel. Locks in the None
    filler invariant explicitly so a future refactor can't silently
    swap in a different sentinel."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pairs = ["0"]
        ntimes = 5
        f = io.File(tmpdir, pairs, ntimes, HEADER)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        d = {"0": np.full(spec_len, 1, dtype=dtype)}

        # First two samples with no metadata at all.
        f.add_data(1, 0.0, d)
        f.add_data(2, 0.0, d)
        assert "imu_el" not in f.metadata

        # Third sample introduces imu_el — back-fill must be None.
        md = {"stream:imu_el": [{**IMU_READING, "yaw": 0.5}]}
        f.add_data(3, 0.0, d, metadata=md)

        assert f.counter == 3
        imu_list = f.metadata["imu_el"]
        assert len(imu_list) == 3
        assert imu_list[0] is None
        assert imu_list[1] is None
        assert imu_list[2] is not None
        assert imu_list[2]["yaw"] == pytest.approx(0.5)

        f.close()


# Producer/fixture contract conformance tests live in
# src/eigsep_observing/contract_tests/test_producer_contracts.py — they
# test external producers (picohost emulators, DummyEigsepFpga) against
# the schemas in io.py rather than testing io.py itself.


# Production correlation pair lists. CURRENT is what corr_config.yaml
# ships today (12 pairs). FUTURE is what the upcoming firmware change
# will produce after the main antenna becomes single-pol (11 pairs:
# 5 autos with input 1 dropped + 6 crosses involving input 0 with all
# others, plus the existing matched-pol aux-aux crosses). The test
# below is parametrized over both so the contract is locked in for
# the migration window. See issue #35 — when the firmware change
# lands, drop PRODUCTION_PAIRS_CURRENT and the future case becomes
# the new baseline.
PRODUCTION_PAIRS_CURRENT = [
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "02",
    "04",
    "13",
    "15",
    "24",
    "35",
]
PRODUCTION_PAIRS_FUTURE = [
    "0",
    "2",
    "3",
    "4",
    "5",
    "02",
    "03",
    "04",
    "05",
    "24",
    "35",
]


@pytest.mark.parametrize(
    "pairs,expected_count",
    [
        (PRODUCTION_PAIRS_CURRENT, 12),
        (PRODUCTION_PAIRS_FUTURE, 11),
    ],
    ids=["current_12", "future_11"],
)
def test_file_writes_all_configured_pairs_with_aligned_axes(
    pairs, expected_count
):
    """Every configured correlation pair must end up in the written
    file with a per-sample length matching every other per-sample
    axis (acc_cnt, times, every metadata stream).

    Three contracts in one test:
      1. **No missing pairs:** every entry in the configured pairs
         list appears as a dataset in the read file.
      2. **No extra pairs:** the file contains exactly the
         configured pairs, nothing else.
      3. **Equal length on the time axis:** data per pair, header
         acc_cnt, header times, and every metadata stream all have
         the same first-axis length (== ntimes for full files).

    Parametrized over the current 12-pair and upcoming 11-pair
    layouts so the contract holds across the migration. The
    expected_count sanity check catches silent config drift in
    either direction.
    """
    assert len(pairs) == expected_count, (
        f"PRODUCTION_PAIRS_* drifted: expected {expected_count} pairs, "
        f"got {len(pairs)}. If this is intentional, update the test."
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 5
        f = io.File(save_dir, pairs, ntimes, HEADER)

        dtype = np.dtype(HEADER["dtype"])
        nchan = HEADER["nchan"]
        acc_bins = HEADER["acc_bins"]
        rng = np.random.default_rng(2026)

        # Synthesize one full buffer's worth of samples. We don't
        # use generate_data() because it's hardcoded to the current
        # 12-pair layout — see issue #35.
        md_template = {
            "stream:imu_el": [{**IMU_READING, "yaw": 0.1}],
            "stream:rfswitch": [
                {
                    "sensor_name": "rfswitch",
                    "status": "update",
                    "app_id": 5,
                    "sw_state": 0,
                    "sw_state_name": "RFANT",
                }
            ],
        }
        for i in range(ntimes):
            sample = {}
            for p in pairs:
                spec_len = io.data_shape(1, acc_bins, nchan, cross=len(p) > 1)[
                    1
                ]
                sample[p] = rng.integers(
                    0, 1000, size=spec_len, dtype="=i4"
                ).astype(dtype)
            f.add_data(i + 1, 0.0, sample, metadata=md_template)
        f._write_queue.join()

        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1
        read_data, read_header, read_meta = io.read_hdf5(files[0])

        # Contract 1 + 2: read_data contains exactly the configured
        # pairs — no missing, no extras.
        assert set(read_data.keys()) == set(pairs), (
            f"pair set mismatch — missing: {set(pairs) - set(read_data)}, "
            f"extra: {set(read_data) - set(pairs)}"
        )

        # Contract 3: equal length on the time axis everywhere.
        for p in pairs:
            assert read_data[p].shape[0] == ntimes, (
                f"pair {p!r}: data length {read_data[p].shape[0]} "
                f"!= ntimes {ntimes}"
            )
        assert len(read_header["acc_cnt"]) == ntimes, (
            f"header acc_cnt length {len(read_header['acc_cnt'])} "
            f"!= ntimes {ntimes}"
        )
        assert len(read_header["times"]) == ntimes, (
            f"header times length {len(read_header['times'])} "
            f"!= ntimes {ntimes}"
        )
        for key, vals in read_meta.items():
            assert len(vals) == ntimes, (
                f"metadata key {key!r}: length {len(vals)} != ntimes {ntimes}"
            )

        # Sanity: autos and crosses should each have a consistent
        # frequency-axis length within their type.
        autos = [p for p in pairs if len(p) == 1]
        crosses = [p for p in pairs if len(p) == 2]
        if autos:
            auto_freq_len = read_data[autos[0]].shape[1]
            for p in autos:
                assert read_data[p].shape[1] == auto_freq_len, (
                    f"auto pair {p!r}: freq length "
                    f"{read_data[p].shape[1]} != {auto_freq_len}"
                )
        if crosses:
            cross_freq_len = read_data[crosses[0]].shape[1]
            for p in crosses:
                assert read_data[p].shape[1] == cross_freq_len, (
                    f"cross pair {p!r}: freq length "
                    f"{read_data[p].shape[1]} != {cross_freq_len}"
                )

        f.close()


# ----------------------------------------------------------------------
# Phase 9 — VNA / S11 write contracts.
#
# Constraints:
#   1. VNA data is written when it arrives (synchronous in
#      record_vna_data, no buffering).
#   2. The file contains the DUT measurement (antenna/noise/load or
#      receiver) AND the OSL calibration data.
#   3. The file contains relevant metadata.
#   4. A hung VNA write must not block the corr-write path.
#
# The production VNA path (PandaClient.execute_vna at client.py:
# 381-382) bundles cal data INTO the data dict with a 'cal:' prefix
# rather than passing it via the explicit cal_data kwarg of
# write_s11_file. The first test below exercises that exact flow.
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "dut_keys,mode",
    [
        (["ant", "noise", "load"], "ant"),
        (["rec"], "rec"),
    ],
    ids=["ant_mode", "rec_mode"],
)
def test_vna_production_path_writes_dut_cal_and_metadata(dut_keys, mode):
    """Locks in the actual production VNA flow:
    PandaClient.execute_vna bundles cal:OSL data into the data dict
    with 'cal:' prefix and ships it through the redis stream as one
    flat dict. record_vna_data reads it and calls write_s11_file
    WITHOUT the explicit cal_data kwarg. The file format must split
    DUT vs cal correctly on read, and metadata must round-trip."""
    npoints = 100
    rng = np.random.default_rng(2026)

    # Build the data dict the way client.py:381-387 does:
    # DUT measurements + cal:open/cal:short/cal:load all in one dict.
    s11 = {}
    for k in dut_keys:
        s11[k] = rng.normal(size=npoints) + 1j * rng.normal(size=npoints)
    for cal_key in ("open", "short", "load"):
        s11[f"cal:{cal_key}"] = rng.normal(size=npoints) + 1j * rng.normal(
            size=npoints
        )

    # PandaClient.execute_vna stamps metadata_snapshot_unix right
    # before get_live_metadata() so downstream can sanity-check that
    # the metadata snapshot is contemporaneous with the VNA.
    snapshot_t = time.time()
    header = {
        **S11_HEADER,
        "mode": mode,
        "freqs": np.linspace(1e6, 250e6, npoints),
        "npoints": npoints,
        "metadata_snapshot_unix": snapshot_t,
    }
    metadata = {
        "imu_el": {**IMU_READING, "yaw": 0.5},
        "rfswitch": 7,
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        # Note: cal_data kwarg deliberately NOT passed — the cal
        # data is already embedded in s11 with cal: prefixes,
        # mirroring the production code path.
        io.write_s11_file(s11, header, metadata=metadata, save_dir=save_dir)

        files = glob.glob(str(save_dir / "*.h5"))
        assert len(files) == 1, f"expected 1 file, got: {files}"

        read_data, read_cal, read_header, read_meta = io.read_s11_file(
            files[0]
        )

        # DUT data is in the data section (no cal: prefix leaked).
        for k in dut_keys:
            assert k in read_data, f"DUT key {k!r} missing from read_data"
            np.testing.assert_array_equal(read_data[k], s11[k])
        for k in read_data.keys():
            assert not k.startswith("cal:"), (
                f"cal-prefixed key {k!r} leaked into read_data"
            )
        assert set(read_data.keys()) == set(dut_keys), (
            f"read_data keys {set(read_data)} != DUT keys {set(dut_keys)}"
        )

        # OSL cal data is in cal_data with the prefix stripped.
        for cal_key in ("open", "short", "load"):
            assert cal_key in read_cal, (
                f"cal key {cal_key!r} missing from read_cal"
            )
            np.testing.assert_array_equal(
                read_cal[cal_key], s11[f"cal:{cal_key}"]
            )
        assert set(read_cal.keys()) == {"open", "short", "load"}

        # Header preserved (mode, freqs, npoints, fstart, etc.).
        assert read_header["mode"] == mode
        assert read_header["fstart"] == header["fstart"]
        assert read_header["npoints"] == npoints
        np.testing.assert_array_equal(read_header["freqs"], header["freqs"])
        # metadata_snapshot_unix round-trips and is the value we set.
        assert read_header["metadata_snapshot_unix"] == pytest.approx(
            snapshot_t
        )

        # Metadata round-tripped including the nested IMU dict.
        assert read_meta["rfswitch"] == 7
        assert read_meta["imu_el"]["yaw"] == pytest.approx(0.5)


def test_corr_write_independent_of_hung_vna_write(monkeypatch):
    """A hung VNA write in a sibling thread must not block the corr
    write path. Locks in that the two paths share no Python-level
    synchronization — corr proceeds even when the VNA path is stuck.

    Note: this verifies Python-level independence (no shared locks,
    queues, or globals between corr and VNA writes). It does not
    exercise h5py's internal global mutex, which can serialize
    *file open* across threads in some builds. In production, real
    VNA writes are sub-second so any h5py serialization is bounded;
    a truly hung VNA file write is the failure mode this test
    simulates."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        vna_dir = save_dir / "vna"
        vna_dir.mkdir()

        ntimes = 3
        f = io.File(save_dir, ["0"], ntimes, HEADER)

        # Patch write_hdf5 to hang only on s11 paths. Corr writes
        # use temp filenames like /tmp/.../tmpXXX.h5.tmp (no "s11"),
        # so they fall through to the original.
        original_write_hdf5 = io.write_hdf5
        vna_unblock = threading.Event()
        vna_started = threading.Event()

        def hanging_write_hdf5(fname, *args, **kwargs):
            if "s11" in str(fname):
                vna_started.set()
                vna_unblock.wait(timeout=10)
            return original_write_hdf5(fname, *args, **kwargs)

        monkeypatch.setattr(io, "write_hdf5", hanging_write_hdf5)

        # Spawn the VNA write in a daemon thread; it will hang
        # inside hanging_write_hdf5 until vna_unblock is set.
        vna_data = generate_s11_data(npoints=100, cal=False)
        vna_thd = threading.Thread(
            target=io.write_s11_file,
            args=(vna_data, S11_HEADER),
            kwargs={"save_dir": vna_dir},
            daemon=True,
        )
        vna_thd.start()

        try:
            # Confirm the VNA write actually reached the hang point.
            assert vna_started.wait(timeout=2), (
                "VNA write didn't start — test setup is broken"
            )

            # Run a full corr buffer fill while the VNA write is
            # hung. This must not block.
            spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
            dtype = np.dtype(HEADER["dtype"])

            t_start = time.monotonic()
            for i in range(ntimes):
                d = {"0": np.full(spec_len, i + 1, dtype=dtype)}
                f.add_data(i + 1, 0.0, d)
            f._write_queue.join()
            elapsed = time.monotonic() - t_start

            # Corr file written despite the hung sibling VNA write.
            corr_files = glob.glob(str(save_dir / "corr_*.h5"))
            assert len(corr_files) == 1, (
                f"expected 1 corr file, got: {corr_files}"
            )

            # And the corr write completed quickly — it didn't sit
            # waiting on the hung VNA path.
            assert elapsed < 5.0, (
                f"corr write took {elapsed:.2f}s while VNA was hung; "
                f"likely blocked on shared state"
            )
        finally:
            vna_unblock.set()
            vna_thd.join(timeout=5)
            f.close()


# ----------------------------------------------------------------------
# Phase 11 — RF switch transition window.
#
# The pico reports the COMMANDED switch state synchronously when it
# receives a switch command, but the physical actuation takes ~200ms
# and the pico has no way to know when it actually finished. So when
# consecutive samples disagree on switch state, the data in the new
# sample (and possibly the next few, depending on integration time)
# could be contaminated by the transition. We flag a forward window
# of samples as UNKNOWN to cover the contamination — never mutate
# previously-written data, never go back to old files.
#
# Window = ceil(RFSWITCH_TRANSITION_WINDOW_S / integration_time)
# samples, with a minimum of 1.
# ----------------------------------------------------------------------


def _make_rfswitch_md(name):
    """Build a stream:rfswitch metadata dict with a single update
    reading at the given switch state name (e.g. ``"RFANT"``)."""
    return {
        "stream:rfswitch": [
            {
                "sensor_name": "rfswitch",
                "status": "update",
                "app_id": 5,
                "sw_state": PicoRFSwitch.rbin(PicoRFSwitch.path_str[name]),
                "sw_state_name": name,
            }
        ]
    }


def test_rfswitch_transition_within_buffer_flags_forward_window(caplog):
    """When two consecutive samples have different switch states,
    the new sample (and the next ceil(0.5/int_time) - 1 samples)
    are flagged UNKNOWN. The OLD sample is left untouched per the
    forward-only design."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ntimes = 10
        # Override int_time → 0.1s so the flagging window is
        # ceil(0.5/0.1) = 5 samples. HEADER's INTEGRATION_TIME (1.0s)
        # would give a window of 1, which doesn't exercise multi-sample
        # forward flagging.
        cfg = HEADER.copy()
        cfg["integration_time"] = 0.1
        f = io.File(tmpdir, ["0"], ntimes, cfg)

        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        d = {"0": np.full(spec_len, 1, dtype=dtype)}

        # Two samples in state 0 (no transition).
        f.add_data(1, 0.0, d, metadata=_make_rfswitch_md("RFANT"))
        f.add_data(2, 0.0, d, metadata=_make_rfswitch_md("RFANT"))
        # Now switch to state 1 — transition. Logged at INFO because
        # a scheduled switch is normal operation, not an anomaly.
        with caplog.at_level(logging.INFO, logger="eigsep_observing.io"):
            f.add_data(3, 0.0, d, metadata=_make_rfswitch_md("RFNOFF"))
        # Continue feeding state 1 samples — need enough to outlast
        # the 5-sample flagging window AND have a couple of clean
        # samples after. Total: 9 samples in a 10-slot buffer.
        for i in range(4, 10):
            f.add_data(i, 0.0, d, metadata=_make_rfswitch_md("RFNOFF"))

        # The first two samples (state RFANT) are unchanged.
        assert f.metadata["rfswitch"][0] == "RFANT"
        assert f.metadata["rfswitch"][1] == "RFANT"
        # Sample 3 (index 2) is the transition trigger — flagged.
        # Window = ceil(0.5 / 0.1) = 5, so indices 2, 3, 4, 5, 6 are
        # flagged UNKNOWN.
        for i in range(2, 7):
            assert f.metadata["rfswitch"][i] == "UNKNOWN", (
                f"index {i} expected UNKNOWN, got {f.metadata['rfswitch'][i]!r}"
            )
        # Indices 7 and 8 are back to the new state.
        assert f.metadata["rfswitch"][7] == "RFNOFF"
        assert f.metadata["rfswitch"][8] == "RFNOFF"

        # INFO log was emitted.
        infos = [r for r in caplog.records if r.levelno == logging.INFO]
        assert any(
            "transition detected" in r.getMessage()
            and "RFANT→RFNOFF" in r.getMessage()
            for r in infos
        ), f"expected transition INFO, got: {[r.getMessage() for r in infos]}"

        f.close()


def test_rfswitch_no_transition_no_flagging(caplog):
    """Consecutive samples with the same switch state must not
    trigger any flagging or transition logs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        f = io.File(tmpdir, ["0"], 10, HEADER)
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        d = {"0": np.full(spec_len, 1, dtype=dtype)}

        with caplog.at_level(logging.INFO, logger="eigsep_observing.io"):
            for i in range(5):
                f.add_data(i + 1, 0.0, d, metadata=_make_rfswitch_md("VNAS"))

        # All five samples carry the raw state, no UNKNOWN.
        for i in range(5):
            assert f.metadata["rfswitch"][i] == "VNAS"

        # No transition log at any level.
        assert not any(
            "transition detected" in r.getMessage() for r in caplog.records
        )

        f.close()


def test_rfswitch_transition_at_buffer_boundary_flags_only_new_buffer():
    """A transition that straddles a buffer boundary flags samples
    in the NEW buffer only — the previous buffer (already swapped
    to standby and being written) is never touched per the
    forward-only design. Locks in: never mutate previously-written
    data, never touch a previous file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = Path(tmpdir)
        ntimes = 3
        # Use 1.0s int_time so the flagging window is exactly 1
        # sample — keeps the buffer arithmetic simple.
        cfg = HEADER.copy()
        cfg["integration_time"] = 1.0
        f = io.File(save_dir, ["0"], ntimes, cfg)
        spec_len = io.data_shape(1, cfg["acc_bins"], cfg["nchan"])[1]
        dtype = np.dtype(cfg["dtype"])
        d = {"0": np.full(spec_len, 1, dtype=dtype)}

        # Fill the first buffer entirely with state 0. The 3rd
        # sample triggers corr_write → swap → reset.
        for i in range(ntimes):
            f.add_data(i + 1, 0.0, d, metadata=_make_rfswitch_md("RFANT"))
        f._write_queue.join()

        # The first file is on disk with all state RFANT.
        files_before = sorted(glob.glob(str(save_dir / "*.h5")))
        assert len(files_before) == 1
        _, _, first_meta_before = io.read_hdf5(files_before[0])
        for v in first_meta_before["rfswitch"]:
            assert v == "RFANT"

        # Now add the FIRST sample of the second buffer with state 1.
        # _prev_rfswitch_state survived the buffer swap, so this
        # triggers a cross-boundary transition detection. Window=1
        # at this int_time, so only this one sample is flagged.
        f.add_data(ntimes + 1, 0.0, d, metadata=_make_rfswitch_md("RFNOFF"))

        # Active buffer's first sample is UNKNOWN (forward flag).
        assert f.metadata["rfswitch"][0] == "UNKNOWN"

        # The first file on disk is UNCHANGED — never touched by
        # the cross-boundary detection.
        files_after = sorted(glob.glob(str(save_dir / "*.h5")))
        assert files_after == files_before
        _, _, first_meta_after = io.read_hdf5(files_after[0])
        # Same content as before the transition was detected.
        np.testing.assert_array_equal(
            first_meta_after["rfswitch"], first_meta_before["rfswitch"]
        )

        f.close()


def test_rfswitch_transition_window_scales_with_integration_time(caplog):
    """The number of samples flagged is ceil(0.5s / integration_time),
    minimum 1. Verify against several integration times."""
    cases = [
        (1.0, 1),  # ceil(0.5/1.0) = 1
        (0.5, 1),  # ceil(0.5/0.5) = 1
        (0.25, 2),  # ceil(0.5/0.25) = 2
        (0.1, 5),  # ceil(0.5/0.1) = 5
    ]
    for int_time, expected_n in cases:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = HEADER.copy()
            cfg["integration_time"] = int_time
            ntimes = expected_n + 5
            f = io.File(tmpdir, ["0"], ntimes, cfg)
            spec_len = io.data_shape(1, cfg["acc_bins"], cfg["nchan"])[1]
            dtype = np.dtype(cfg["dtype"])
            d = {"0": np.full(spec_len, 1, dtype=dtype)}

            # Anchor with state 0, then transition to state 1.
            f.add_data(1, 0.0, d, metadata=_make_rfswitch_md("RFANT"))
            for i in range(2, 2 + expected_n + 2):
                f.add_data(i, 0.0, d, metadata=_make_rfswitch_md("RFNOFF"))

            # Index 0 is state RFANT (anchor).
            assert f.metadata["rfswitch"][0] == "RFANT"
            # Indices 1..expected_n are flagged UNKNOWN.
            for i in range(1, 1 + expected_n):
                assert f.metadata["rfswitch"][i] == "UNKNOWN", (
                    f"int_time={int_time}: index {i} expected UNKNOWN, "
                    f"got {f.metadata['rfswitch'][i]!r}"
                )
            # The next sample (after the window) is back to state RFNOFF.
            assert f.metadata["rfswitch"][1 + expected_n] == "RFNOFF", (
                f"int_time={int_time}: window did not end at "
                f"expected index {1 + expected_n}"
            )

            f.close()


def test_rfswitch_missing_reading_mid_window_still_flagged():
    """If a sample inside the transition window has no rfswitch
    reading at all, it must still be flagged UNKNOWN — the corr
    data is contaminated regardless of whether the switch was
    reported on that integration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Override int_time → 0.1s so the window is 5 samples (see
        # test_rfswitch_transition_within_buffer_flags_forward_window).
        cfg = HEADER.copy()
        cfg["integration_time"] = 0.1
        f = io.File(tmpdir, ["0"], 10, cfg)
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        d = {"0": np.full(spec_len, 1, dtype=dtype)}

        # Anchor and trigger transition.
        f.add_data(1, 0.0, d, metadata=_make_rfswitch_md("RFANT"))
        f.add_data(2, 0.0, d, metadata=_make_rfswitch_md("RFNOFF"))
        # Now feed samples WITHOUT any rfswitch metadata at all
        # while still in the transition window.
        f.add_data(3, 0.0, d, metadata=None)
        f.add_data(4, 0.0, d, metadata=None)

        # Sample 0 = anchor state.
        assert f.metadata["rfswitch"][0] == "RFANT"
        # Samples 1, 2, 3 are inside the window (window=5 here).
        # Sample 1 was the trigger and has raw state RFNOFF forced to UNKNOWN.
        # Samples 2, 3 had no rfswitch reading but still get flagged.
        assert f.metadata["rfswitch"][1] == "UNKNOWN"
        assert f.metadata["rfswitch"][2] == "UNKNOWN"
        assert f.metadata["rfswitch"][3] == "UNKNOWN"

        f.close()


def test_rfswitch_consecutive_transitions_reset_window(caplog):
    """A second transition while still inside the first transition's
    window resets the window to the full count from the new
    transition. Detection compares against the last *valid raw*
    state, not the flagged UNKNOWN."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Override int_time → 0.1s so the window is 5 samples (see
        # test_rfswitch_transition_within_buffer_flags_forward_window).
        cfg = HEADER.copy()
        cfg["integration_time"] = 0.1
        f = io.File(tmpdir, ["0"], 20, cfg)
        spec_len = io.data_shape(1, HEADER["acc_bins"], HEADER["nchan"])[1]
        dtype = np.dtype(HEADER["dtype"])
        d = {"0": np.full(spec_len, 1, dtype=dtype)}

        # Anchor with state 0.
        f.add_data(1, 0.0, d, metadata=_make_rfswitch_md("RFANT"))
        # Transition to state 1 → window of 5.
        f.add_data(2, 0.0, d, metadata=_make_rfswitch_md("RFNOFF"))
        # Two samples into the window, transition to state 2 →
        # window resets to 5.
        f.add_data(3, 0.0, d, metadata=_make_rfswitch_md("RFNOFF"))
        f.add_data(4, 0.0, d, metadata=_make_rfswitch_md("RFNON"))
        # Now five more samples of state 2.
        for i in range(5, 10):
            f.add_data(i, 0.0, d, metadata=_make_rfswitch_md("RFNON"))

        # Index 0: anchor state RFANT.
        assert f.metadata["rfswitch"][0] == "RFANT"
        # Index 1, 2: first window (RFNOFF transition).
        assert f.metadata["rfswitch"][1] == "UNKNOWN"
        assert f.metadata["rfswitch"][2] == "UNKNOWN"
        # Index 3: second transition trigger (RFNON). UNKNOWN.
        # Window resets to 5 starting here, so indices 3-7 are
        # UNKNOWN.
        for i in range(3, 8):
            assert f.metadata["rfswitch"][i] == "UNKNOWN", (
                f"index {i} expected UNKNOWN (in reset window), "
                f"got {f.metadata['rfswitch'][i]!r}"
            )
        # Index 8: back to raw state RFNON.
        assert f.metadata["rfswitch"][8] == "RFNON"

        f.close()
