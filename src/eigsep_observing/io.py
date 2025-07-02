from collections import defaultdict
import datetime
import h5py
import json
import numpy as np
from pathlib import Path

from eigsep_corr.utils import calc_times, calc_freqs_dfreq


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


def append_corr_header(header, acc_cnts):
    """
    Append header for correlation files with useful computed
    quantities: times and frequencies.

    Parameters
    ----------
    header : dict
        Header dictionary for correlation file.
    acc_cnts : array_like
        Array of accumulation counts for each time step.

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
        acc_cnts, header["integration_time"], header["sync_time"]
    )
    freqs, dfreq = calc_freqs_dfreq(header["sample_rate"], header["nchan"])
    new_header = header.copy()
    new_header["times"] = times
    new_header["freqs"] = freqs
    new_header["dfreq"] = dfreq
    new_header["acc_cnts"] = acc_cnts
    return new_header

def process_metadata(metadata_entry):
    """
    Process metadata for correlation file header. Specfically ensures
    that each metadata key gives a single value per accumulation count.
    Averages calls where multiple values are provided for the same
    accumulation count, and fills NaN for missing values.

    Parameters
    ----------
    metadata_entry : dict
        Dictionary of a metadata entry. Keys are `data`, `status`, and
        `cadence`.

    Returns
    -------
    str, float or NaN
        Processed metadata value.

    """

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
        file_path = Path(save_dir) / f"s11_{date}.h5"
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

    def __init__(self, save_dir, pairs, ntimes, header):
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
        header : dict
            Header information to be written to the file.

        """
        self.save_dir = Path(save_dir)
        self.ntimes = ntimes
        self.pairs = pairs
        self.header = header

        acc_bins = header["acc_bins"]
        nchan = header["nchan"]
        dtype = np.dtype(header["dtype"])

        self.acc_cnts = np.zeros(self.ntimes)
        self.metadata = defaultdict(list)  # dynamic metadata
        self._metadata_keys = set()  # keys of metadata
        self._metadata_cadences = {}  # dict with cadence/fill count
        self.data = {}
        for p in pairs:
            shape = data_shape(self.ntimes, acc_bins, nchan, cross=len(p) > 1)
            self.data[p] = np.zeros(shape, dtype=dtype)

        self._counter = 0

    def __len__(self):
        return self._counter

    def reset(self):
        """
        Reset the data arrays to zero.

        """
        self.metadata.clear()  # clear metadata
        for p in self.pairs:
            self.data[p].fill(0)
        self.acc_cnts.fill(0)
        self._counter = 0

    def add_data(self, acc_cnt, data, metadata={}):
        """
        Populate the data arrays with the given data. The data is expected
        to be of the dtype specified in the header.

        Parameters
        ----------
        acc_cnt : int
            Accumulation count.
        data : dict
            Dictionary of data arrays to be added for one time step.
        metadata : dict
            Dynamic metadata, such as sensor readings, timestamps, etc.

        Returns
        -------
        str
            Filename where the data will be written, if the counter reaches
            the number of times. Otherwise, None.

        """
        self.acc_cnts[self._counter] = acc_cnt
        for p, d in data.items():
            arr = self.data[p]
            arr[self._counter] = d
        # process metadata
        self._metadata_keys |= metadata  # add new keys if any
        for key in self._metadata_keys:
            if key in metadata:
                value = metadata[key]
                self._metadata_cadences[key] = {
                    "cadence": value["cadence"], "fill_cnt": 0
                }
                md = process_metadata(value)
            else:  # in this case, metadata was added before
                self._metadata_cadences[key]["fill_cnt"] += 1
                md = self._fill_metadata()  # NaN or copy based on cnt
        for key, value in metadata.items():
            # XXX need to do some averaging here if multiple per time step
            # XXX if timeout then we need to add filler NaN
            # XXX is there a chance for no new data?
            # XXX answer to previous is yes, in that case - we copy previous
            # XXX value for number of acc_cnts corresponding to cfg[cadence]
            # XXX actually metadata ships with the cadence!
            # XXX then start filling NANs
            # also bear in mind we're getting a dict with data, status
            md = process_metadata(value)
            self.metadata[key].append(md)
        self._counter += 1
        if self._counter == self.ntimes:
            return self.corr_write()
        return None

    def corr_write(self, fname=None):
        """
        Write the data to a file.

        Parameters
        ----------
        fname : str, optional
            Filename where the data will be written. If not provided, a
            timestamped filename will be generated.

        Returns
        -------
        str
            Filename where the data was written.

        """
        if fname is None:
            date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = self.save_dir / f"corr_{date}.h5"
        data = reshape_data(self.data, avg_even_odd=True)
        header = append_corr_header(self.header, self.acc_cnts)
        write_hdf5(fname, data, header, metadata=self.metadata)
        self.reset()
        return fname
