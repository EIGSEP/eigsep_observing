import datetime
import hp5y
import json
import numpy as np
from pathlib import Path

from eigsep_corr import utils


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
    Write data to a file.

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


class File:

    def __init__(self, save_dir, pairs, ntimes, header):
        """
        Initialize the File object.

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

        """
        self.save_dir = save_dir
        self.ntimes = ntimes
        self.pairs = pairs
        self.header = header

        acc_bins = header["acc_bins"]
        nchan = header["nchan"]
        dtype = header["dtype"]

        self.data = {}
        for p in pairs:
            shape = data_shape(ntimes, acc_bins, nchan, cross=len(p) > 1)
            self.data[p] = np.zeros(shape, dtype=dtype)

        self.acc_cnt = np.zeros(ntimes, dtype=dtype)
        self.obs_mode = np.empty(ntimes, dtype=str)
        self._counter = 0

    def __len__(self):
        return self._counter

    def reset(self):
        """
        Reset the data arrays to zero.

        """
        for p in self.pairs:
            self.data[p].fill(0)
        self.acc_cnt.fill(0)
        self._counter = 0

    def add_data(self, data, cnt, mode="sky"):
        """
        Populate the data arrays with the given data.

        Parameters
        ----------
        data : dict
            Dictionary of data arrays to be added for one time step.
        cnt : int
            Current accumulation count.
        mode : str
            Observing mode, either ``sky'', ``noise'', or ``load''. Added
            to the header.

        Returns
        -------
        str
            Filename where the data will be written, if the counter reaches
            the number of times. Otherwise, None.

        """
        for p, d in data.items():
            arr = self.data[p]
            arr[self._counter] = d
            self.acc_cnt[self._counter] = cnt
            self.obs_mode[self._counter] = mode
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
        # XXX
        # need to write acc cnt and obs mode to the file
        # XXX
        :q
        :q
        if fname is None:
            date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = Path(self.save_dir) / f"corr_{date}.h5"
        write_file(fname, self.data, self.header)
        self.reset()
        return fname
