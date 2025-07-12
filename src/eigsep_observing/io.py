from collections import defaultdict
import datetime
import h5py
import json
import logging
import numpy as np
from pathlib import Path

from eigsep_corr.utils import calc_times, calc_freqs_dfreq

logger = logging.getLogger(__name__)


def data_shape(ntimes, acc_bins, nchan, cross=False):
    """
    Expected shape of data array.

    Parameters
    ----------
    ntimes : int
        Number of integrations.
    acc_bins : int
        Number of accumulation bins. Usually 2 (for even/odd spectra).
    nchan : int
        Number of frequency channels.
    cross : bool
        Data represents cross-correlations. If True, the data has both
        real and imaginary parts. False means auto-correlations, which
        only have real parts.

    Returns
    -------
    tuple
        Shape of the data array as a tuple (ntimes, spec_len).

    """
    spec_len = acc_bins * nchan
    if cross:
        spec_len *= 2  # real and imaginary parts
    return (ntimes, spec_len)


def reshape_data(data, avg_even_odd=True):
    """
    Reshape data to the form (ntimes, nchan). From the SNAP, the
    even and odd spectra follow each other, here we split them
    and optionally average them. Moreover, cross-correlation data
    is explictly converted to complex numbers.

    Parameters
    ----------
    data : dict
        Dictionary of data arrays to be reshaped. Keys specify the
        correlation pairs.
    avg_even_odd : bool
        If True, average the even and odd spectra.

    Returns
    -------
    reshaped : dict
        Dictionary of reshaped data arrays.

    """
    reshaped = {}
    for p, arr in data.items():
        arr = np.atleast_2d(arr)  # ensure at least 2D if no times
        # place even/odd on last axis
        ntimes = arr.shape[0]
        arr = arr.reshape(ntimes, -1, 2, order="F")
        if avg_even_odd:
            arr = arr.mean(axis=2)
        if len(p) > 1:  # cross-correlation
            real = arr[:, ::2]
            imag = arr[:, 1::2]
            arr = real + 1j * imag
        reshaped[p] = arr
    return reshaped


def append_corr_header(header, acc_cnts, sync_times):
    """
    Append header for correlation files with useful computed
    quantities: times and frequencies.

    Parameters
    ----------
    header : dict
        Header dictionary for correlation file.
    acc_cnts : array_like
        Array of accumulation counts for each time step.
    sync_time : array_like
        Synchronization time for the measurements, used to calculate
        the times. This is when `acc_cnts` starts.

    Returns
    -------
    new_header : dict
        Updated header dictionary with additional computed quantities.

    Raises
    ------
    KeyError
        If the header does not contain the required keys.

    """
    times = calc_times(
        acc_cnts,
        header["integration_time"],
        sync_times,
    )
    freqs, dfreq = calc_freqs_dfreq(header["sample_rate"], header["nchan"])
    new_header = header.copy()
    new_header["times"] = times
    new_header["freqs"] = freqs
    new_header["dfreq"] = dfreq
    new_header["acc_cnt"] = acc_cnts
    return new_header


def _write_attr(grp, key, value):
    """
    Helper function to write attributes to an HDF5 group.

    Parameters
    ----------
    grp : h5py.Group
        HDF5 group to write the attribute to.
    key : str
        Name of the attribute.
    value : bool, int, float, str
        Value of the attribute. Must be a simple type (not a list or dict).

    Raises
    -------
    TypeError
        If the value is not a simple type (bool, int, float, str).

    """
    if isinstance(value, bool):
        grp.attrs[key] = np.bool_(value)
    elif isinstance(value, int):
        grp.attrs[key] = np.int64(value)
    elif isinstance(value, float):
        grp.attrs[key] = np.float64(value)
    elif isinstance(value, str):
        dtype = h5py.string_dtype(encoding="utf-8")
        grp.attrs.create(key, value, dtype=dtype)
    else:
        raise TypeError(f"Unsupported attribute type: {type(value)}")


def _write_dataset(grp, key, value):
    """
    Helper function to write a dataset to an HDF5 group.

    Parameters
    ----------
    grp : h5py.Group
        HDF5 group to write the dataset to.
    key : str
        Name of the dataset.
    value : np.ndarray or serializable object
        Object to be written as a dataset. If it is a numpy array,
        it is written directly. Otherwise it is serialized to JSON.

    """
    if isinstance(value, np.ndarray):
        # grp.create_dataset(key, data=value)
        # return
        data = json.dumps(value.tolist())
    else:
        data = json.dumps(value)

    grp.create_dataset(key, data=data)


def _write_header_item(grp, key, value):
    """
    Helper function to write an item to the header group in an HDF5 file.

    Parameters
    ----------
    grp : h5py.Group
        HDF5 group to write the item to.
    key : str
        Name of the item.
    value : most native Python types, see notes
        Value of the item.

    Notes
    -----
    Supported types include:
    - Simple types: bool, int, float, str, bytes
    - Arrays/lists: numpy arrays, lists, tuples (converted to arrays)
    - Dictionaries: nested dictionaries with simple types as values
    - Complex numbers: 0-dim numpy arrays (e.g., np.array(1.0+0j))
    - Path: converted to string
    - datetime.datetime: converted to string in ISO format
    - set: converted to a list

    Raises
    ------
    TypeError
        If the value is not a simple type or a small array/list/dict.

    """
    if isinstance(value, Path):
        value = str(value)
    if isinstance(value, datetime.datetime):
        value = value.isoformat()
    if isinstance(value, set):
        value = sorted(value)  # convert set to sorted list

    if isinstance(value, complex):
        _write_dataset(grp, key, np.complex128(value))
        return
    if isinstance(value, (bool, int, float, str)):
        _write_attr(grp, key, value)
        return
    if isinstance(value, (list, tuple, bytes, dict, np.ndarray)):
        _write_dataset(grp, key, value)
        return

    raise TypeError(f"Unsupported header type: {type(value)}")


def write_hdf5(fname, data, header, metadata=None):
    """
    Write data to an HDF5 file.

    Parameters
    ----------
    fname : str or Path
        Filename where the data will be written.
    data : dict
        Dictionary of data arrays to be written.
    header : dict
        Header information to be written to the file. This specifies
        static configuration, settings, etc. Values are expected to be
        primarily strings or numbers, but may also include small
        arrays, lists, or dictionaries. If a value is a dictionary,
        it must contain only simple types (strings, numbers).
    metadata : dict
        Additional metadata. Usually numpy arrays or lists, e.g.,
        sensor readings, timestamps, etc.

    Raises
    ------
    TypeError
        If the header contains unsupported types.

    """
    with h5py.File(fname, "w") as f:
        # data
        data_grp = f.create_group("data")
        for key, value in data.items():
            data_grp.create_dataset(key, data=value)
        # header
        header_grp = f.create_group("header")
        for key, value in header.items():
            _write_header_item(header_grp, key, value)
        # metadata
        if metadata is not None:
            metadata_grp = f.create_group("metadata")
            for key, value in metadata.items():
                _write_header_item(metadata_grp, key, value)


def read_hdf5(fname):
    """
    Read data from an HDF5 file.

    Parameters
    ----------
    fname : str or Path
        Filename from which to read the data.

    Returns
    -------
    data : dict
        Dictionary of data arrays read from the file.
    header : dict
        Header information read from the file.
    metadata : dict
        Metadata read from the file, if available.

    """
    with h5py.File(fname, "r") as f:
        data = {k: np.array(v) for k, v in f["data"].items()}
        # header
        header_grp = f["header"]
        header = {k: v for k, v in header_grp.attrs.items()}
        for name, obj in header_grp.items():
            if isinstance(obj, h5py.Group):
                header[name] = {k: v for k, v in obj.attrs.items()}
            else:
                header[name] = json.loads(obj[()])
        # metadata
        metadata = {}
        for name, obj in f.get("metadata", {}).items():
            if isinstance(obj, h5py.Group):
                metadata[name] = {k: v for k, v in obj.attrs.items()}
            else:
                metadata[name] = json.loads(obj[()])
    return data, header, metadata


def write_s11_file(
    data,
    header,
    metadata=None,
    cal_data=None,
    fname=None,
    save_dir=Path("."),
):
    """
    Write S11 measurement data to a file.

    Parameters
    ----------
    data : dict
        Dictionary containing S11 measurement data arrays. Keys specify
        the DUT, usually 'ant' or 'rec'.
    header : dict
        Static header information to be written, specifying the
        VNA settings.
    metadata : dict
        Additional metadata to be written to the file, such as
        sensor readings, timestamps, etc.
    cal_data : dict
        Dictionary containing calibration data arrays, with keys 'open',
        'short', and 'load'.
    fname : Path or str
        Filename where the data will be written. If not provided, a
        timestamped filename will be generated.
    save_dir : Path or str
        Directory where the data will be saved. Must be able to
        instantiate a Path object. Ignored if ``fname'' is an absolute path.

    """
    if fname is None:
        date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        mode = "ant" if "ant" in data else "rec"
        file_path = Path(save_dir) / f"{mode}s11_{date}.h5"
    else:
        fname = Path(fname)
        if not fname.is_absolute():
            file_path = Path(save_dir) / fname
        else:
            file_path = fname
    all_data = data.copy()
    if cal_data:
        for k, v in cal_data.items():
            key = f"cal:{k}"  # prefix calibration data keys
            all_data[key] = v
    write_hdf5(file_path, all_data, header, metadata=metadata)


def read_s11_file(fname):
    """
    Read S11 measurement data from a file.

    Parameters
    ----------
    fname : str or Path
        Filename from which to read the data.

    Returns
    -------
    data : dict
        Dictionary of S11 measurement data arrays read from the file.
    cal_data : dict
        Dictionary of calibration data arrays read from the file, with
        keys 'open', 'short', and 'load'. If no calibration data is
        available, this will be an empty dictionary.
    header : dict
        Header information read from the file.
    metadata : dict
        Metadata read from the file, if available.

    """
    data, header, metadata = read_hdf5(fname)
    # filter out calibration data keys
    cal_keys = [k for k in data.keys() if k.startswith("cal:")]
    cal_data = {}
    for k in cal_keys:
        cal_data[k[4:]] = data.pop(k)  # remove 'cal:' prefix
    return data, cal_data, header, metadata


class File:

    def __init__(self, save_dir, pairs, ntimes, cfg):
        """
        Initialize the File object for saving correlation data.

        Parameters
        ----------
        save_dir : Path or str
            Directory where the data will be saved. Must be able to
            instantiate a Path object.
        pairs : list
            List of correlation pairs to write.
        ntimes : int
            Number of time steps to accumulate per file.
        cfg : dict
            Observing configuration.

        """
        self.logger = logger
        self.save_dir = Path(save_dir)
        self.ntimes = ntimes
        self.pairs = pairs
        self.cfg = cfg
        self.set_header()

        acc_bins = cfg["acc_bins"]
        nchan = cfg["nchan"]
        dtype = np.dtype(cfg["dtype"])

        self.acc_cnts = np.zeros(self.ntimes)
        self.sync_times = np.zeros(self.ntimes)
        self.metadata = defaultdict(list)  # dynamic metadata
        self.data = {}
        for p in pairs:
            shape = data_shape(self.ntimes, acc_bins, nchan, cross=len(p) > 1)
            self.data[p] = np.zeros(shape, dtype=dtype)

        self.counter = 0

    def __len__(self):
        return self.counter

    def reset(self):
        """
        Reset the data arrays to zero.

        """
        self.metadata.clear()  # clear metadata
        for p in self.pairs:
            self.data[p].fill(0)
        self.acc_cnts.fill(0)
        self.sync_times.fill(0)
        self.counter = 0

    def set_header(self, header=None):
        """
        Set the header for the correlation file.

        Parameters
        ----------
        header : dict
            Header information to be written to the file. This specifies
            static configuration, settings, etc. Values are expected to be
            primarily strings or numbers, but may also include small
            arrays, lists, or dictionaries.

        """
        if header is None:
            self.header = {}
        else:
            self.header = header.copy()
        for key, val in self.cfg.items():
            if key not in self.header:
                self.header[key] = val

    def add_data(self, acc_cnt, sync_time, data, metadata=None):
        """
        Populate the data arrays with the given data. The data is expected
        to be of the dtype specified in the header.

        Parameters
        ----------
        acc_cnt : int
            Accumulation count.
        sync_time : float
            Synchronization time for the measurements, used to calculate
            the times. This is when `acc_cnt` starts.
        data : dict
            Dictionary of data arrays to be added for one time step.
        metadata : dict
            Dynamic metadata, such as sensor readings, timestamps, etc.

        """
        try:
            delta_cnt = acc_cnt - self._prev_cnt
        except AttributeError:  # first call
            delta_cnt = 1
        if delta_cnt > 1:  # fill with zeros
            self.add_data(
                self._prev_cnt + 1,
                sync_time,
                {p: np.zeros_like(self.data[p][0]) for p in self.pairs},
            )
        metadata = metadata or {}
        self.acc_cnts[self.counter] = acc_cnt
        self.sync_times[self.counter] = sync_time
        for p, d in data.items():
            arr = self.data[p]
            arr[self.counter] = d
        # process metadata
        for key in metadata:
            # value: list of dicts with keys data, status, cadence
            value = metadata[key]
            #md = self._avg_metadata(value)
            #self.metadata[key].append(md)
            self.metadata[key].append(value)
        self.counter += 1
        self._prev_cnt = acc_cnt
        if self.counter == self.ntimes:
            self.corr_write()

    def _avg_metadata(self, value):
        """
        Average the metadata value if needed.

        Parameters
        ----------
        value : list of dicts
            Output from `redis.get_metadata`. List of at least one dict
            with 'status' and 'app_id' keys and some number of data keys.

        Returns
        -------
        avg : dict
            Average value of the metadata where 'status' is not 'error'.

        """
        app_name = value[0]["sensor_name"]
        if app_name in ("temp_mon", "tempctrl"):  # need to handle A/B
            avgs = {}
            for subkey in ("A", "B"):
                keys = [k for k in value[0].keys() if k.startswith(subkey)]
                subval = [
                    {k[2:]: v[k] for k in keys if k in v} for v in value
                ]
                #keys.extend(["app_id", "sensor_name"])
                subavg = self._avg_metadata(subval)
                subavg["app_id"] = value[0]["app_id"]
                subavg["sensor_name"] = value[0]["sensor_name"]
                avgs[subkey] = subavg
            return avgs

        status_list = [v["status"] for v in value]
        if app_name == "rfswitch":
            state = [v["sw_state"] for v in value]
            if "error" in status_list or any(s != state[0] for s in state):
                return "SWITCHING"
            return state[0]  # all states are the same

        avg = {}  # avg metadata for this pico
        for data_key in value[0].keys():  # loop over the data keys
            if isinstance(value[0][data_key], str):
                # if string, just return the first one
                avg[data_key] = value[0][data_key]
                continue
            data = np.where(
                np.array(status_list) != "error",
                np.array([v[data_key] for v in value]),
                np.nan,
            )
            avg[data_key] = np.nanmean(data)
        return avg

    def corr_write(self, fname=None):
        """
        Write the data to a file.

        Parameters
        ----------
        fname : str, optional
            Filename where the data will be written. If not provided, a
            timestamped filename will be generated.

        """
        if fname is None:
            date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = self.save_dir / f"corr_{date}.h5"
        self.logger.info(f"Writing correlation data to {fname}")
        data = reshape_data(self.data, avg_even_odd=True)
        header = append_corr_header(
            self.header, self.acc_cnts, self.sync_times
        )
        write_hdf5(fname, data, header, metadata=self.metadata)
        self.reset()
