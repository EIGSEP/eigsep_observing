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
    "pam_atten": {0: 8, 1: 8, 2: 8},
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


def generate_data(reshape=True):
    """
    Generate random data for the tests.

    Parameters
    ----------
    reshape : bool
        Use the `io.reshape_data` function to reshape the data.

    Returns
    -------
    data : dict
        Dictionary containing the generated data.

    """
    rng = np.random.default_rng(1420)
    dtype = io.build_dtype("int32", ">")
    data_min = np.iinfo(dtype).min
    data_max = np.iinfo(dtype).max
    ntimes = 60
    nchan = 1024
    autos = [str(i) for i in range(6)]
    cross = ["02", "04", "13", "15", "24", "35"]
    data = {}
    for k in autos:
        shape = io.data_shape(ntimes, 2, nchan)
        data[k] = rng.integers(0, high=data_max, size=shape, dtype=dtype)
    for k in cross:
        shape = io.data_shape(ntimes, 2, nchan, cross=True)
        data[k] = rng.integers(
            data_min, high=data_max, size=shape, dtype=dtype
        )
    if reshape:
        data = io.reshape_data(data)
    return data


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
    for byte in [1, 2, 4, 8]:
        for typ in ["int", "float", "complex"]:
            for endian in ["<", ">", "="]:
                dt1 = np.dtype(f"{endian}{typ}{byte}")
                bits = byte * 8
                dt2 = io.build_dtype(f"{typ}{bits}", endian)
                assert dt1 == dt2


def test_reshape_data():
    raise NotImplementedError("Test for reshape_data is not implemented yet.")


def test_write_read_hdf5():
    data = generate_data(reshape=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        filename = Path(tmpdir) / "test.h5"
        io.write_hdf5(filename, data, HEADER)
        assert filename.exists()
        read_data, read_header, read_meta = io.read_hdf5(filename)
        assert read_data == data
        assert read_header == HEADER
        assert read_meta == {}

        # test with metadata
        io.write_hdf5(filename, data, HEADER, metadata=METADATA)
        read_data, read_header, read_meta = io.read_hdf5(filename)
        assert read_data == data
        assert read_header == HEADER
        assert read_meta == METADATA
