import datetime
import h5py
import numpy as np
from pathlib import Path


def build_dtype(dtype, endian):
    """
    Build a NumPy dtype based on the given type and endianness.

    Parameters
    ----------
    dtype : str
        Data type to use, e.g., 'float32', 'complex64'.
    endian : str
        Endianness. Either '>' for big-endian or '<' for little-endian.

    Returns
    -------
    np.dtype
        The constructed NumPy dtype.

    """
    return np.dtype(dtype).newbyteorder(endian)


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


def to_remote_path(path, mnt_path=Path("/mnt/rpi")):
    """
    Convert a local path to a remote path. This ensures that data
    gathered on the client (LattePanda) is saved to the remote Raspbery
    Pi server.

    Parameters
    ----------
    path : str or Path
        Local path to be converted.
    mnt_path : str or Path
        Mount point for the remote server. Default is '/mnt/data'.

    Returns
    -------
    Path
        Converted remote path.

    """
    p = Path(path).resolve()
    mnt = Path(mnt_path)
    return mnt / p.relative_to("/")


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
            if isinstance(value, (str, int, float)):
                header_grp.attrs[key] = value
            elif isinstance(value, (list, tuple, np.ndarray)):
                arr = np.asarray(value)
                if arr.dtype.kind == "U":  # string type
                    header_grp.create_dataset(
                        key,
                        data=arr.astype(object),
                        dtype=h5py.string_dtype(encoding="utf-8"),
                    )
                else:
                    header_grp.create_dataset(key, data=arr)
            elif isinstance(value, dict):
                sub_grp = header_grp.create_group(key)
                for sub_key, sub_value in value.items():
                    sub_grp.attrs[sub_key] = sub_value
            else:
                raise TypeError(f"Unsupported header type: {type(value)}")
        # metadata
        if metadata is not None:
            metadata_grp = f.create_group("metadata")
            for key, value in metadata.items():
                arr = np.asarray(value)
                if arr.dtype.kind == "U":  # string type
                    metadata_grp.create_dataset(
                        key,
                        data=arr.astype(object),
                        dtype=h5py.string_dtype(encoding="utf-8"),
                    )
                else:
                    metadata_grp.create_dataset(key, data=arr)


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
                if obj.dtype.kind in ("S", "O"):  # string type
                    header[name] = obj.asstr()[()]
                else:
                    header[name] = obj[()]
        # metadata
        metadata = {}
        for k, v in f.get("metadata", {}).items():
            if v.dtype.kind in ("S", "O"):
                metadata[k] = v.asstr()[()]
            else:
                metadata[k] = v[()]
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

    def __init__(self, save_dir, pairs, ntimes, header, redis=None):
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
        redis : EigsepRedis
            Redis server to pull more header information from.

        """
        self.save_dir = Path(save_dir)
        self.ntimes = ntimes
        self.pairs = pairs
        self.header = header
        self.redis = redis

        acc_bins = header["acc_bins"]
        nchan = header["nchan"]
        dtype = build_dtype(*header["dtype"])

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
        for p in self.pairs:
            self.data[p].fill(0)
        self._counter = 0

    def add_data(self, data):
        """
        Populate the data arrays with the given data. The data is expected
        to be of the dtype specified in the header.

        Parameters
        ----------
        data : dict
            Dictionary of data arrays to be added for one time step.

        Returns
        -------
        str
            Filename where the data will be written, if the counter reaches
            the number of times. Otherwise, None.

        """
        for p, d in data.items():
            arr = self.data[p]
            arr[self._counter] = d
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
        if self.redis:
            metadata = self.redis.get_metadata()
        else:
            metadata = None
        data = reshape_data(self.data, avg_even_odd=True)
        write_hdf5(fname, data, self.header, metadata=metadata)
        self.reset()
        return fname
