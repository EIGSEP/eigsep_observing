import datetime
import numpy as np
from pathlib import Path
import tempfile

from eigsep_observing import io

# header to use for testing, mimics EigsepFpga().header
HEADER = {
    "dtype": ("int32", ">"),
    "acc_bins": 2,
    "nchan": 1024,
    "fgp_file": "fpg_files/eigsep_fengine.fpg",
    "fpg_version": (0, 0),
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

S11_HEADER = {
    "fstart": 1e6,
    "fstop": 250e6,
    "npoints": 1000,
    "ifbw": 100,
    "power_dBm": 0,
    "freqs": np.linspace(1e6, 250e6, 1000),
    "mode": "ant",
}


def compare_dicts(dict1, dict2):
    """
    Compare two dictionaries containing numpy arrays for equality.
    """
    assert set(dict1) == set(dict2), "Dictionaries have different keys."
    for key in dict1:
        np.testing.assert_array_equal(
            dict1[key],
            dict2[key],
            err_msg=f"Arrays for key '{key}' are not equal.",
        )


def generate_data(reshape=True, return_time_freq=False):
    """
    Generate random data for the tests.

    Parameters
    ----------
    reshape : bool
        Use the `io.reshape_data` function to reshape the data.
    return_time_freq : bool
        Return the number of time and frequency bins in the data.

    Returns
    -------
    data : dict
        Dictionary containing the generated data.
    ntimes : int
        Number of time step in the data. Returned only if
        `return_time_freq' is True.
    nchan : int
        Number of frequency channels in the data. Returned only if
        `return_time_freq' is True.

    """
    rng = np.random.default_rng(1420)
    dtype = io.build_dtype("int32", ">")
    # need to use native dtype for the data generation
    native_dtype = io.build_dtype("int32", "=")
    data_min = np.iinfo(native_dtype).min
    data_max = np.iinfo(native_dtype).max
    ntimes = 60
    nchan = 1024
    autos = [str(i) for i in range(6)]
    cross = ["02", "04", "13", "15", "24", "35"]
    data = {}
    for k in autos:
        shape = io.data_shape(ntimes, 2, nchan)
        data[k] = rng.integers(
            0, high=data_max, size=shape, dtype=native_dtype
        )
    for k in cross:
        shape = io.data_shape(ntimes, 2, nchan, cross=True)
        data[k] = rng.integers(
            data_min, high=data_max, size=shape, dtype=native_dtype
        )
    # swap to specified dtype
    for k in data:
        data[k] = data[k].astype(dtype)
    if reshape:
        data = io.reshape_data(data)
    if return_time_freq:
        return data, ntimes, nchan
    return data


def generate_s11_data(cal=False):
    """
    Generate random S11 data for the tests.

    Parameters
    ----------
    cal : bool
        If True, generate calibration data as well.

    Returns
    -------
    data : dict
        Dictionary containing the generated S11 data.
    cal_data : dict or None
        Dictionary containing the generated calibration data. Only returned
        if ``cal'' is True.

    """
    rng = np.random.default_rng(1420)
    npoints = S11_HEADER["npoints"]
    data = {
        "ant": rng.normal(size=npoints) + 1j * rng.normal(size=npoints),
        "noise": rng.normal(size=npoints) + 1j * rng.normal(size=npoints),
    }
    if not cal:
        return data

    cal_data = {}
    for k in ["VNAO", "VNAS", "VNAL"]:
        cal_data[k] = rng.normal(size=npoints) + 1j * rng.normal(size=npoints)
    return data, cal_data


def test_build_dtype():
    # big endian 32-bit integer
    dt1 = np.dtype(">i4")
    dt2 = io.build_dtype("int32", ">")
    assert dt1 == dt2
    # little endian 32-bit integer
    dt1 = np.dtype("<i4")
    dt2 = io.build_dtype("int32", "<")
    assert dt1 == dt2
    # common types
    for typ in ["int", "float", "complex"]:
        for endian in ["<", ">", "="]:
            for byte in [4, 8]:
                if typ == "complex":
                    byte *= 2  # complex types have double the byte size
                dt1 = np.dtype(f"{endian}{typ[0]}{byte}")
                bits = byte * 8
                dt2 = io.build_dtype(f"{typ}{bits}", endian)
                assert dt1 == dt2


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


def test_to_remote_path():
    path = Path("/media/eigsep/T7/data")  # typical data path
    mnt_path = Path("/mnt/rpi")  # mount point
    remote_path = io.to_remote_path(path, mnt_path=mnt_path)
    assert remote_path == Path("/mnt/rpi/media/eigsep/T7/data")
    # relative path
    path = Path("data")
    remote_path = io.to_remote_path(path, mnt_path=mnt_path)
    here = Path.cwd().resolve()
    expected_path = f"/mnt/rpi/{str(here)}/data"
    assert remote_path == Path(expected_path)


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


def test_write_read_s11_file():
    data, cal_data = generate_s11_data(cal=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        # no filename, should create one automatically
        io.write_s11_file(data, S11_HEADER, fname=None, save_dir=tmpdir)
        # check that the file was created
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # might be off by a second, so we use glob to find the file
        assert len(list(Path(tmpdir).glob(f"s11_{now[:-2]}*.h5"))) == 1
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


def test_file():
    # test the File class
    temp_dir = tempfile.TemporaryDirectory()
    save_dir = Path(temp_dir.name)
    autos = [str(i) for i in range(6)]
    cross = ["02", "04", "13", "15", "24", "35"]
    pairs = autos + cross
    ntimes = 60
    test_file = io.File(save_dir, pairs, ntimes, HEADER, redis=None)

    # __init__
    assert test_file.save_dir.resolve() == save_dir.resolve()
    assert test_file.pairs == pairs
    assert test_file.ntimes == ntimes
    assert test_file.header == HEADER
    assert test_file.redis is None

    assert list(test_file.data.keys()) == pairs
    dtype = io.build_dtype(*HEADER["dtype"])
    for p in pairs:
        if len(p) == 1:
            shape = io.data_shape(ntimes, 2, 1024)
        else:
            shape = io.data_shape(ntimes, 2, 1024, cross=True)
        d = test_file.data[p]
        assert d.shape == shape
        assert d.dtype == dtype
        np.testing.assert_array_equal(d, np.zeros(shape, dtype=dtype))

    assert test_file._counter == 0

    # add_data
    data = generate_data(reshape=False)
    for i in range(ntimes - 1):
        to_add = {p: d[i] for p, d in data.items()}
        fname = test_file.add_data(to_add)
        assert fname is None  # None until the file is full
        assert test_file._counter == i + 1
        for p in pairs:
            assert np.array_equal(test_file.data[p][i], to_add[p])
    to_add = {p: d[-1] for p, d in data.items()}
    fname = test_file.add_data(to_add)
    assert fname is not None  # should return the filename
    # reset has been called
    assert test_file._counter == 0
    for p in pairs:
        if len(p) == 1:
            shape = io.data_shape(ntimes, 2, 1024)
        else:
            shape = io.data_shape(ntimes, 2, 1024, cross=True)
        d = test_file.data[p]
        assert d.shape == shape
        assert d.dtype == dtype
        np.testing.assert_array_equal(d, np.zeros(shape, dtype=dtype))

    # corr_write has been called by add_data
    assert Path(fname).exists()
    # check that the data is written correctly
    read_data, read_header, read_meta = io.read_hdf5(fname)
    compare_dicts(io.reshape_data(data, avg_even_odd=True), read_data)
    compare_dicts(HEADER, read_header)
    assert read_meta == {}

    temp_dir.cleanup()
