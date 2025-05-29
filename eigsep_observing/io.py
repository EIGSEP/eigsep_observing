import datetime
import h5py
import json
import numpy as np
from pathlib import Path

from eigsep_corr import utils


# XXX from chatgpt, not implemented yet
def write_hdf5(filename, data_array, header: dict, streams: dict):
    """
    filename   : path to output .h5
    data_array : your main numpy array of shape (n_ints, n_bins, …)
    header     : {key: scalar or short list} to write as attrs
    streams    : {stream_name: List[ (ts, v1, v2, …), … ] }
    """
    raise NotImplementedError("This function is not implemented yet.")
    with h5py.File(filename, "w") as f:
        # 1) main dataset
        ds = f.create_dataset(
            "rf_data", data=data_array, compression="gzip", chunks=True
        )

        # 2) write header attrs
        for key, val in header.items():
            # h5py can handle ints, floats, strings, and small lists/arrays
            # If val is a list/tuple, turn into numpy array
            if isinstance(val, (list, tuple)):
                val = np.array(val)
            ds.attrs[key] = val

        # 3) create a metadata group for your time-series
        meta_grp = f.create_group("metadata")

        for name, records in streams.items():
            # records: List of (ts, v1, v2, …)
            if not records:
                continue

            # Convert to a structured NumPy array:
            # e.g. streams["accel"] = [(ts1, ax1, ay1, az1), ...]
            first = records[0]
            nfields = len(first)
            # Build dtype: first field 'ts' as float (seconds since epoch),
            # others 'v0','v1',...
            dt = np.dtype(
                [("ts", "f8")] + [(f"v{i}", "f8") for i in range(1, nfields)]
            )
            arr = np.empty(len(records), dtype=dt)
            for i, rec in enumerate(records):
                arr[i] = rec  # tuple will map to fields
            # Write as a dataset
            meta_grp.create_dataset(
                name,
                data=arr,
                compression="gzip",
                maxshape=(None,),  # allow appending if desired
                chunks=True,
            )


def data_shape(ntimes, acc_bins, nchan, cross=False):
    """
    Expected shape of data array.

    """
    spec_len = acc_bins * nchan
    if cross:
        spec_len *= 2  # real and imaginary parts
    return (ntimes, spec_len)


def write_file(fname, data, header):
    """
    Write correlation data to a file.

    Parameters
    ----------
    fname : str
        Filename where the data will be written.
    data : dict
        Dictionary of data arrays to be written.
    header : dict
        Header information to be written to the file.

    """
    raise NotImplementedError("This function is not implemented yet.")


def write_s11_file(
    data, cal_data=None, fname=None, save_dir=Path("."), header=None
):
    """
    Write S11 measurement data to a file.

    SAVE data which is a dict of one/two numpy arrays.
    AND SAVE OSL if available (cal_data).

    Need the orientation of the box in the header.

    Parameters
    ----------
    data : dict
        Dictionary containing S11 measurement data arrays.
    cal_data : dict
        Dictionary containing calibration data arrays, with keys 'open',
        'short', and 'load'.
    fname : Path or str
        Filename where the data will be written. If not provided, a
        timestamped filename will be generated.
    save_dir : Path or str
        Directory where the data will be saved. Must be able to
        instantiate a Path object. Ignored if ``fname'' is an absolute path.
    header : dict
        File header information to be written.

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
     # XXX here
     # write_hdf5()
     raise NotImplementedError("This function is not implemented yet.")


class File:

    def __init__(self, save_dir, pairs, ntimes, header, redis=None):
        """
        Initialize the File object for saving correlation data.

        Parameters
        ----------
        save_dir : str
            Directory where the data will be saved.
        pairs : list
            List of correlation pairs to write.
        ntimes : int
            Number of time steps to accumulate per file.
        header : dict
            Header information to be written to the file.
        redis : EigsepRedis
            Redis server to pull more header information from.

        """
        self.save_dir = save_dir
        self.ntimes = ntimes
        self.pairs = pairs
        self.header = header
        self.redis = redis

        acc_bins = header["acc_bins"]
        nchan = header["nchan"]
        dtype = header["dtype"]

        self.header["redis"] = None  # placeholder, set in corr_write
        self.data = {}
        for p in pairs:
            shape = data_shape(ntimes, acc_bins, nchan, cross=len(p) > 1)
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
        self.header["redis"] = None
        self._counter = 0

    def add_data(self, data):
        """
        Populate the data arrays with the given data.

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
            fname = Path(self.save_dir) / f"corr_{date}.h5"
        if self.redis is not None:
            self.header["redis"] = self.redis.get_header()
        write_file(fname, self.data, self.header)
        self.reset()
        return fname

class S11File:

    def __init__(self, vna, redis=None):
        """
        Initialize the S11File object for saving S11 measurement data.

        Parameters
        ----------
        vna : cmt_vna.VNA
            VNA object to pull data from.
        redis : EigsepRedis, optional
            Redis server to pull more header information from.

        """
        self.vna = vna
        self.redis = redis
