from collections import defaultdict
import datetime
import h5py
import json
import logging
import numpy as np
import os
import queue
import tempfile
import threading
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
    value : np.ndarray, np.generic, or serializable object
        Object to be written as a dataset. Numeric numpy arrays and
        scalars are written natively. Everything else is serialized
        to JSON.

    """
    # np.generic covers numpy scalars (e.g. np.float64, np.int32)
    if isinstance(value, (np.ndarray, np.generic)):
        if value.dtype.kind in ("f", "i", "u", "c", "b"):
            grp.create_dataset(key, data=value)
            return
        # non-numeric (strings, objects): JSON-encode
        data = json.dumps(value.tolist())
    else:
        data = json.dumps(value)
    grp.create_dataset(key, data=data)


def _read_dataset(obj):
    """
    Read an HDF5 dataset, handling both native arrays and
    JSON-encoded data (for backward compatibility).

    Parameters
    ----------
    obj : h5py.Dataset
        HDF5 dataset to read.

    Returns
    -------
    data : np.ndarray, scalar, list, dict, or str
        The dataset contents.

    """
    data = obj[()]
    if isinstance(data, (bytes, str)):
        return json.loads(data)
    return data


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
                header[name] = _read_dataset(obj)
        # metadata
        metadata = {}
        for name, obj in f.get("metadata", {}).items():
            if isinstance(obj, h5py.Group):
                metadata[name] = {k: v for k, v in obj.attrs.items()}
            else:
                metadata[name] = _read_dataset(obj)
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


# Sensor schemas: field name -> expected Python type.
# The type is what the field should be when the sensor is healthy.
# None values are always allowed (sensor error may null out fields).
_IMU_SCHEMA = {
    "sensor_name": str,
    "status": str,
    "app_id": int,
    "quat_i": float,
    "quat_j": float,
    "quat_k": float,
    "quat_real": float,
    "accel_x": float,
    "accel_y": float,
    "accel_z": float,
    "lin_accel_x": float,
    "lin_accel_y": float,
    "lin_accel_z": float,
    "gyro_x": float,
    "gyro_y": float,
    "gyro_z": float,
    "mag_x": float,
    "mag_y": float,
    "mag_z": float,
    "calibrated": bool,
    "accel_cal": int,
    "mag_cal": int,
}

SENSOR_SCHEMAS = {
    "imu_panda": _IMU_SCHEMA,
    "imu_antenna": _IMU_SCHEMA,
    "temp_mon": {
        "sensor_name": str,
        "app_id": int,
        "A_status": str,
        "A_temp": float,
        "A_timestamp": float,
        "B_status": str,
        "B_temp": float,
        "B_timestamp": float,
    },
    "tempctrl": {
        "sensor_name": str,
        "app_id": int,
        "watchdog_tripped": bool,
        "watchdog_timeout_ms": int,
        "A_status": str,
        "A_T_now": float,
        "A_timestamp": float,
        "A_T_target": float,
        "A_drive_level": float,
        "A_enabled": bool,
        "A_active": bool,
        "A_int_disabled": bool,
        "A_hysteresis": float,
        "A_clamp": float,
        "B_status": str,
        "B_T_now": float,
        "B_timestamp": float,
        "B_T_target": float,
        "B_drive_level": float,
        "B_enabled": bool,
        "B_active": bool,
        "B_int_disabled": bool,
        "B_hysteresis": float,
        "B_clamp": float,
    },
    "rfswitch": {
        "sensor_name": str,
        "status": str,
        "app_id": int,
        "sw_state": int,
    },
    "lidar": {
        "sensor_name": str,
        "status": str,
        "app_id": int,
        "distance_m": float,
    },
}


def _validate_metadata(entry, schema):
    """
    Validate a single metadata dict against its schema.

    Parameters
    ----------
    entry : dict
        A single sensor reading.
    schema : dict
        Mapping of field name to expected Python type.

    Returns
    -------
    violations : list of str
        Human-readable descriptions of contract violations.
        Empty list means the entry is valid.

    """
    violations = []
    schema_keys = set(schema)
    entry_keys = set(entry)
    missing = schema_keys - entry_keys
    extra = entry_keys - schema_keys
    if missing:
        violations.append(f"missing keys: {sorted(missing)}")
    if extra:
        violations.append(f"extra keys: {sorted(extra)}")
    for key in schema_keys & entry_keys:
        val = entry[key]
        if val is None:
            continue
        expected = schema[key]
        if expected is float:
            ok = isinstance(val, (int, float)) and not isinstance(val, bool)
        elif expected is int:
            ok = isinstance(val, int) and not isinstance(val, bool)
        else:
            ok = isinstance(val, expected)
        if not ok:
            violations.append(
                f"key '{key}': expected {expected.__name__}, "
                f"got {type(val).__name__}"
            )
    return violations


def avg_metadata(value):
    """
    Average metadata readings down to one entry per sample.

    Parameters
    ----------
    value : list of dicts
        Output from ``redis.get_metadata``. List of at least one
        dict with 'status' and 'app_id' keys and data keys.

    Returns
    -------
    avg : dict or str or None
        Averaged metadata. For rfswitch, returns the switch state
        string or ``"UNKNOWN"`` if the state changed.

    """
    if not value or not isinstance(value[0], dict):
        return None

    app_name = value[0].get("sensor_name", "")
    schema = SENSOR_SCHEMAS.get(app_name)

    if schema is not None:
        for i, entry in enumerate(value):
            violations = _validate_metadata(entry, schema)
            if violations:
                logger.warning(
                    "Metadata contract violation in '%s' (entry %d): %s",
                    app_name,
                    i,
                    "; ".join(violations),
                )
    else:
        logger.warning(
            "No schema for sensor '%s'; skipping validation",
            app_name,
        )

    if app_name in ("temp_mon", "tempctrl"):
        return _avg_temp_metadata(value, app_name, schema)

    if app_name == "rfswitch":
        return _avg_rfswitch_metadata(value)

    # generic sensor (e.g. IMU, lidar)
    return _avg_sensor_values(value, schema)


def _avg_temp_metadata(value, app_name, schema):
    """
    Average temp_mon / tempctrl metadata, handling A/B channels.

    """
    avgs = {
        "app_id": value[0].get("app_id"),
        "sensor_name": app_name,
    }
    # Build a sub-schema for each channel by stripping the prefix.
    for subkey in ("A", "B"):
        prefix = f"{subkey}_"
        if schema is not None:
            keys = [k for k in schema if k.startswith(prefix)]
            sub_schema = {k[len(prefix) :]: schema[k] for k in keys}
        else:
            keys = [k for k in value[0] if k.startswith(prefix)]
            sub_schema = None
        if not keys:
            continue
        subvals = []
        for v in value:
            sub = {}
            for k in keys:
                if k in v:
                    sub[k[len(prefix) :]] = v[k]
            if sub:
                sub.setdefault("status", v.get(f"{subkey}_status"))
                subvals.append(sub)
        if subvals:
            avgs[subkey] = _avg_sensor_values(subvals, sub_schema)
    return avgs


def _avg_rfswitch_metadata(value):
    """
    Average rfswitch metadata.  Returns the switch state if
    constant, or ``"UNKNOWN"`` if it changed or errored.

    """
    status_list = [v.get("status") for v in value]
    states = [v.get("sw_state") for v in value]
    if "error" in status_list:
        return "UNKNOWN"
    unique = set(s for s in states if s is not None)
    if len(unique) > 1:
        return "UNKNOWN"
    return states[0] if states else None


def _avg_sensor_values(value, schema=None):
    """
    Average numeric sensor values, keep first string/bool value.

    When *schema* is provided, it determines which keys to process
    and whether each key is numeric (``float``/``int``) or
    categorical (``str``/``bool``).  Without a schema, falls back
    to type-sniffing from the first non-None value.

    """
    if not value:
        return None

    avg = {}
    status_list = [v.get("status", "update") for v in value]

    # Determine the set of keys to iterate over.
    if schema is not None:
        all_keys = list(schema)
    else:
        all_keys = list(value[0])

    for data_key in all_keys:
        # Determine whether this field is numeric.
        if schema is not None:
            is_numeric = schema[data_key] in (float, int)
        else:
            # Fallback: sniff type from first non-None value.
            first_val = None
            for v in value:
                first_val = v.get(data_key)
                if first_val is not None:
                    break
            is_numeric = isinstance(first_val, (int, float)) and (
                not isinstance(first_val, bool)
            )

        if not is_numeric:
            avg[data_key] = value[0].get(data_key)
            continue

        try:
            raw = []
            for v, st in zip(value, status_list):
                val = v.get(data_key)
                if (
                    st != "error"
                    and val is not None
                    and isinstance(val, (int, float))
                    and not isinstance(val, bool)
                ):
                    raw.append(val)
            avg[data_key] = float(np.mean(raw)) if raw else None
        except Exception as e:
            logger.warning("Could not average key '%s': %s", data_key, e)
            avg[data_key] = None
    return avg


class File:
    def __init__(self, save_dir, pairs, ntimes, cfg):
        """
        Initialize the File object for saving correlation data.
        Uses a double-buffered async writer so that HDF5 I/O never
        blocks the data-reading loop.

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

        # active buffer
        self.acc_cnts = np.zeros(self.ntimes)
        self.sync_times = np.zeros(self.ntimes)
        self.metadata = defaultdict(list)
        self.data = {}
        for p in pairs:
            shape = data_shape(self.ntimes, acc_bins, nchan, cross=len(p) > 1)
            self.data[p] = np.zeros(shape, dtype=dtype)

        # standby buffer
        self._standby_acc_cnts = np.zeros(self.ntimes)
        self._standby_sync_times = np.zeros(self.ntimes)
        self._standby_metadata = defaultdict(list)
        self._standby_data = {}
        for p in pairs:
            shape = data_shape(self.ntimes, acc_bins, nchan, cross=len(p) > 1)
            self._standby_data[p] = np.zeros(shape, dtype=dtype)

        self.counter = 0

        # async writer
        self._write_queue = queue.Queue(maxsize=1)
        self._write_error = None  # set by writer thread on failure
        self._standby_ready = threading.Event()
        self._standby_ready.set()
        self._writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True
        )
        self._writer_thread.start()

    def __len__(self):
        return self.counter

    def reset(self):
        """
        Reset the active data arrays to zero.

        """
        self.metadata.clear()
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
            Expected format from ``get_metadata()``:
            ``{stream_name: [list_of_dicts]}``.

        """
        if acc_cnt is None or data is None:
            self.logger.warning("Received None for acc_cnt or data, skipping.")
            return
        try:
            delta_cnt = acc_cnt - self._prev_cnt
        except AttributeError:  # first call
            delta_cnt = 1

        # iterative gap-fill with zeros (avoids recursion for large gaps)
        if delta_cnt > 1:
            zero_data = {p: np.zeros_like(self.data[p][0]) for p in self.pairs}
            base_cnt = self._prev_cnt
            for i in range(1, delta_cnt):
                self._insert_sample(base_cnt + i, sync_time, zero_data)

        # Process metadata from get_metadata (stream format:
        # {stream_name: [list_of_dicts]}).  Each list contains all
        # readings since the last call; we average them down to one
        # entry per sample to resample onto the correlator cadence.
        # Strip the "stream:" prefix — it's a Redis artifact — and
        # split temp sensors' A/B channels into separate entries.
        processed_md = {}
        metadata = metadata or {}
        for key in metadata:
            value = metadata[key]
            if not (isinstance(value, list) and len(value) > 0):
                self.logger.warning(
                    f"Unexpected metadata for '{key}': {value!r}"
                )
                continue
            # strip stream prefix
            name = key.removeprefix("stream:")
            averaged = avg_metadata(value)
            if isinstance(averaged, dict) and "A" in averaged:
                # temp sensor: split A/B into separate flat entries
                for ch in ("A", "B"):
                    if ch in averaged:
                        processed_md[f"{name}_{ch.lower()}"] = averaged[ch]
            else:
                processed_md[name] = averaged
        self._insert_sample(acc_cnt, sync_time, data, processed_md)

    def _insert_sample(
        self, acc_cnt, sync_time, sample_data, sample_metadata=None
    ):
        """
        Insert one sample into the active buffer, flushing to disk
        when the buffer is full.

        Parameters
        ----------
        acc_cnt : int
            Accumulation count for this sample.
        sync_time : float
            Synchronization time.
        sample_data : dict
            One spectrum per correlation pair.
        sample_metadata : dict, optional
            Pre-processed metadata: ``{key: scalar_value}``.
            Keys absent from the active metadata are back-filled
            with ``None``; active keys absent from
            *sample_metadata* get ``None`` appended.

        """
        sample_metadata = sample_metadata or {}
        self.acc_cnts[self.counter] = acc_cnt
        self.sync_times[self.counter] = sync_time
        for p in self.pairs:
            self.data[p][self.counter] = sample_data[p]
        # pad new keys so indices align 1:1 with samples
        for key in sample_metadata:
            if key not in self.metadata:
                self.metadata[key] = [None] * self.counter
            self.metadata[key].append(sample_metadata[key])
        # pad missing keys with None for 1:1 correspondence
        for key in self.metadata:
            if key not in sample_metadata:
                self.metadata[key].append(None)
        self.counter += 1
        self._prev_cnt = acc_cnt
        if self.counter == self.ntimes:
            self.corr_write()

    # ----------- double-buffered async writer -----------

    def _swap_buffers(self):
        """O(1) reference swap between active and standby buffers."""
        self.data, self._standby_data = (
            self._standby_data,
            self.data,
        )
        self.acc_cnts, self._standby_acc_cnts = (
            self._standby_acc_cnts,
            self.acc_cnts,
        )
        self.sync_times, self._standby_sync_times = (
            self._standby_sync_times,
            self.sync_times,
        )
        self.metadata, self._standby_metadata = (
            self._standby_metadata,
            self.metadata,
        )

    def _writer_loop(self):
        """Background thread that dequeues write jobs."""
        while True:
            job = self._write_queue.get()
            if job is None:  # shutdown signal
                self._standby_ready.set()
                self._write_queue.task_done()
                break
            (
                fname,
                data,
                acc_cnts,
                sync_times,
                metadata,
                counter,
                header,
            ) = job
            try:
                self._do_write(
                    fname,
                    data,
                    acc_cnts,
                    sync_times,
                    metadata,
                    counter,
                    header,
                )
            except Exception as e:
                self.logger.error(f"Failed to write {fname}: {e}")
                self._write_error = e
            finally:
                self._standby_ready.set()
                self._write_queue.task_done()

    def _do_write(
        self, fname, data, acc_cnts, sync_times, metadata, counter, header
    ):
        """
        Atomic write: write to a temp file, then rename. This prevents
        a crash mid-write from leaving a corrupted .h5 file — either
        the complete file exists or it doesn't (rename is atomic on
        POSIX).

        """
        if fname is None:
            date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = self.save_dir / f"corr_{date}.h5"
        self.logger.info(f"Writing correlation data to {fname}")

        # slice to counter so short final files don't include trailing zeros
        data = {p: d[:counter] for p, d in data.items()}
        acc_cnts = acc_cnts[:counter]
        sync_times = sync_times[:counter]
        metadata = {k: v[:counter] for k, v in metadata.items()}

        reshaped = reshape_data(data, avg_even_odd=True)
        full_header = append_corr_header(header, acc_cnts, sync_times)

        fd, tmp_path = tempfile.mkstemp(dir=self.save_dir, suffix=".h5.tmp")
        os.close(fd)
        try:
            write_hdf5(tmp_path, reshaped, full_header, metadata=metadata)
            os.rename(tmp_path, fname)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def corr_write(self, fname=None):
        """
        Enqueue the current buffer for async writing, swap to the
        standby buffer, and reset.

        Parameters
        ----------
        fname : str, optional
            Filename where the data will be written. If not provided, a
            timestamped filename will be generated.

        """
        if self.counter == 0:
            return

        if self._write_error is not None:
            self.logger.error(
                f"Previous write failed: {self._write_error}. "
                "Data from that buffer was lost."
            )
            self._write_error = None

        # pad any short metadata lists to match counter
        for key in self.metadata:
            while len(self.metadata[key]) < self.counter:
                self.metadata[key].append(None)

        # wait for writer to finish with standby buffer
        self._standby_ready.wait()
        self._standby_ready.clear()

        # package job with current buffer references
        job = (
            fname,
            self.data,
            self.acc_cnts,
            self.sync_times,
            self.metadata,
            self.counter,
            self.header.copy(),
        )

        # swap buffers and reset active
        self._swap_buffers()
        self.reset()

        # enqueue for async write
        self._write_queue.put(job)

    def close(self):
        """
        Shut down the writer thread and wait for pending writes.

        """
        self._write_queue.put(None)
        self._writer_thread.join(timeout=30)
        if self._writer_thread.is_alive():
            self.logger.error("Writer thread did not shut down within timeout")
