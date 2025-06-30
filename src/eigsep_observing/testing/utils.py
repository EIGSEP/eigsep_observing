import numpy as np

from .. import io


def compare_dicts(dict1, dict2):
    """
    Compare two dictionaries for equality.
    """
    assert set(dict1) == set(dict2), "Dictionaries have different keys."
    for key in dict1:
        np.testing.assert_array_equal(
            dict1[key],
            dict2[key],
            err_msg=f"Arrays for key '{key}' are not equal.",
        )


def generate_data(ntimes=60, raw=False, reshape=True, return_time_freq=False):
    """
    Generate random data for the tests.

    Parameters
    ----------
    ntimes : int
        Number of time steps in the data.
    raw : bool
        Return data as bytes.
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
    dtype = np.dtype(">i4")
    # need to use native dtype for the data generation
    native_dtype = np.dtype("=i4")
    data_min = np.iinfo(native_dtype).min
    data_max = np.iinfo(native_dtype).max
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
    if raw:
        data = {k: v.tobytes() for k, v in data.items()}
    if return_time_freq:
        return data, ntimes, nchan
    return data


def generate_s11_data(npoints=1000, cal=False):
    """
    Generate random S11 data for the tests.

    Parameters
    ----------
    npoints : int
        Number of points in the S11 data.
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
