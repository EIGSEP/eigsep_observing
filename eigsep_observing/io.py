import datetime
import json
import os
import struct
import numpy as np
from eigsep_corr import utils

HEADER_LEN_BYTES = 8
HEADER_LEN_DTYPE = '>Q'
DEFAULT_NTIMES = 60
DEFAULT_HEADER = {
    "dtype": ("int32", ">"),  # data type, endianess of data
    "infochan": 2,  # number of frequency channels used to track acc_cnt
    "nchan": 1024,  # number of frequency channels
    "acc_bins": 2,  # number of accumulation bins per integration
    "fpg_file": "eigsep_fengine_1g_v2_3_2024-07-08_1858.fpg",
    "fpg_version": 0x2003,
    "sample_rate": int(500e6),  # in Hz
    "gain": 4,  # gain of ADC
    "corr_acc_len": 2**28,  # number of samples to accumulate
    "corr_scalar": 2**9,  # 2^9 = 1, using 8 bits after binary point
    "pol01_delay": 0,  # delay in sample clocks of inputs 0/1
    "pol23_delay": 0,  # delay in sample clocks of inputs 2/3
    "pol45_delay": 0,  # delay in sample clocks of inputs 4/5
    "pam_atten": {0: (8, 8), 1: (8, 8), 2: (8, 8)},  # PAM attenuations
    "fft_shift": 0x0055,
    "pairs": ['0', '1', '2', '3', '4', '5',
              '02', '04', '24', '13', '15', '35'],
    "acc_cnt": np.arange(DEFAULT_NTIMES),
    "sync_time": 0.0,
}


def build_dtype(dtype, endian):
    return np.dtype(dtype).newbyteorder(endian)


def unpack_raw_header(buf, header_size=None):
    if header_size is None:
        header_size = len(buf)
    else:
        buf = buf[:header_size]
    header = json.loads(buf)  # trim trailing nulls
    dt = build_dtype(*header['dtype'])
    data_start = header_size + HEADER_LEN_BYTES + (8 - (header_size % 8))
    header['header_size'] = header_size
    header['data_start'] = data_start
    header['pam_atten'] = {int(k): v for k, v in header['pam_atten'].items()}
    header['acc_cnt'] = np.array(header['acc_cnt'], dtype=dt)
    return header


def pack_raw_header(header):
    dt = build_dtype(*header['dtype'])
    # filter to official header keys
    header = {k: v for k, v in header.items() if k in DEFAULT_HEADER}
    header['acc_cnt'] = np.array(header['acc_cnt'], dtype=dt).tolist()
    buf = json.dumps(header)
    return buf


def _read_header_size(fh):
    """Read size of header from first word in file."""
    fh.seek(0, 0)  # go to beginning of file
    return struct.unpack(HEADER_LEN_DTYPE, fh.read(HEADER_LEN_BYTES))[0]


def _read_raw_header(fh):
    header_size = _read_header_size(fh)  # leaves us after ``header size''
    header = unpack_raw_header(fh.read(header_size).decode('utf-8'))
    return header


def _write_raw_header(fh, header):
    buf = pack_raw_header(header).encode('utf-8')
    header_size = len(buf)
    fh.write(struct.pack(HEADER_LEN_DTYPE, header_size))
    fh.write(buf)
    fh.write((8 - (header_size % 8)) * b'\x00')  # pad with trailing nulls


def read_header(filename):
    with open(filename, 'rb') as fh:
        h = _read_raw_header(fh)
    # augment raw header with useful calculated values
    h['filename'] = filename
    h['filesize'] = filesize = os.path.getsize(filename)
    intlen = calc_pair_offsets(h['pairs'], h['acc_bins'], h['nchan'], h['dtype'])[-1]
    h['nspec'] = (filesize - h['data_start']) // intlen
    assert h['nspec'] == len(h['acc_cnt']), "Check file size matches integration cnts"
    h['freqs'], h['dfreq'] = utils.calc_freqs_dfreq(h['sample_rate'], h['nchan'])
    h['inttime'] = inttime = utils.calc_inttime(
        h['sample_rate'], h['corr_acc_len'], acc_bins=h["acc_bins"]
    )
    h['times'] = utils.calc_times(h['acc_cnt'], inttime, h['sync_time'])
    return h


def calc_pair_offsets(pairs, acc_bins, nchan, dtype):
    dt = build_dtype(*dtype)
    nspec = np.array([0] + [1 if len(p) == 1 else 2 for p in pairs])
    offsets = np.cumsum(nspec)
    return offsets * dt.itemsize * acc_bins * nchan


def unpack_raw_data(buf, pair, acc_bins=2, nchan=1024, dtype=('int32', '>')):
    dt = build_dtype(*dtype)
    if type(buf) is not bytes:
        buf = b''.join(buf)
    data = np.frombuffer(buf, dtype=dt)
    if len(pair) == 1:
        data.shape = (-1, acc_bins, nchan, 1)  # auto
    else:
        data.shape = (-1, acc_bins, nchan, 2)  # cross (real/imag last axis)
    return data


def pack_raw_data(data, dtype=('int32', '>')):
    dt = build_dtype(*dtype)
    buf = data.astype(dt).tobytes()
    return buf


def unpack_data(fh_buf, h, nspec=-1, skip=0):
    pair_offs = calc_pair_offsets(
        h['pairs'], h['acc_bins'], h['nchan'], h['dtype']
    )
    integration_len = pair_offs[-1]
    if type(fh_buf) is bytes:
        buf = fh_buf
    else:
        fh = fh_buf
        start = h['data_start'] + skip * integration_len
        fh.seek(start, 0)
        if nspec < 0:
            buf = fh.read()
        else:
            buf = fh.read(nspec * integration_len)
    data = {
        p: [
            buf[b+pair_offs[i]:b+pair_offs[i+1]]
            for b in range(0, len(buf), integration_len)
        ] for i, p in enumerate(h['pairs'])
    }
    data = {
        p: unpack_raw_data(v, p, h['acc_bins'], h['nchan'], h['dtype'])
        for p, v in data.items()
    }
    return data


def pack_data(data, h):
    ntimes = list(data.values())[0].shape[0]
    buf = [
        pack_raw_data(data[p][i], dtype=h['dtype'])
        for i in range(ntimes)
        for p in h['pairs']
    ]
    buf = b''.join(buf)
    return buf


def pack_corr_data(dict_list, h):
    """
    For use with a list of dicts of binary data read straight from correlator.
    """
    buf = b''.join([d[k] for d in dict_list for k in h['pairs']])
    return buf


def read_file(filename, header=None, nspec=-1, skip=0):
    if header is None:
        header = read_header(filename)
    with open(filename, 'rb') as fh:
        data = unpack_data(fh, header, nspec=nspec, skip=skip)
    return header, data


def write_file(filename, header, data):
    """
    Write binary data to file.

    Parameters
    ----------
    filename : str
    header : dict
    data : dict

    """
    with open(filename, "wb") as fh:
        _write_raw_header(fh, header)
        if type(data) is bytes:
            fh.write(data)
        else:
            fh.write(pack_data(data, header))


class File:

    def __init__(self, save_dir, ntimes=DEFAULT_NTIMES, header=DEFAULT_HEADER):
        self.save_dir = save_dir
        self.ntimes = ntimes
        self.buffer = {}
        self.header = header
        self.header["acc_cnt"] = []

    def __len__(self):
        return len(self.buffer)

    def reset(self):
        """
        Clear buffer and reset header.
        """
        self.buffer = {}
        self.header["acc_cnt"] = []

    def add_data(self, data, acc_cnt):
        self.buffer[acc_cnt] = data
        self.header["acc_cnt"].append(acc_cnt)
        if len(self) == self.ntimes:
            return self.corr_write()
        else:
            return None

    def corr_write(self, fname=None):
        if fname is None:
            date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = os.path.join(self.save_dir, f"{date}.eig")
        buf = [self.buffer[acc_cnt] for acc_cnt in self.header['acc_cnt']]
        packed_data = pack_corr_data(buf, self.header)
        write_file(fname, self.header, packed_data)
        self.reset()
        return fname
