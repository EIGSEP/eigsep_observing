from collections import defaultdict
import datetime
import h5py
import json
import logging
import math
import numpy as np
import os
import queue
import tempfile
import threading
import time
from pathlib import Path

from .linear_range import (
    LinearRangeError,
    load_linear_range,
    validate_operating_point,
)
from .utils import calc_times, calc_freqs_dfreq

logger = logging.getLogger(__name__)

# Conservative window for the RF switch actuation + pico cadence.
# The physical switch takes ~200ms to actuate, and the pico reports
# its commanded state on a ~200ms cadence. The 500ms includes the
# 200ms actuation, the up-to-200ms pico cadence delay before the
# new state is reported, and ~100ms safety margin.
RFSWITCH_TRANSITION_WINDOW_S = 0.5


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


def reshape_data(data, acc_bins=2, avg_even_odd=True):
    """
    Reshape raw correlator spectra to the file/consumer layout.

    The reshape depends on how many accumulation bins the running
    firmware emits per integration (``acc_bins``), which the producer
    derives from the SNAP firmware version and stamps on the corr
    header / config (see :class:`eigsep_observing.fpga.EigsepFpga`):

    - ``acc_bins == 2`` (firmware < 2.4): even and odd spectra follow
      each other in the raw buffer. They are split onto a trailing
      length-2 axis (Fortran order: first half even, second half odd).
      When ``avg_even_odd=True`` (the production/file-write path) the
      pair is averaged with banker's rounding (``np.rint``,
      round-half-to-even) and returned as **int32**; when ``False`` the
      even/odd axis is preserved and crosses are returned as complex128.
      The float64 intermediate in ``mean()`` is exact for int32 inputs
      (sum of two int32 values ≤ 2^32, within float64's 2^53
      exact-integer range). The ±0.5 LSB rounding error is ~5 orders of
      magnitude below the radiometric noise floor for typical EIGSEP
      integration depths.

    - ``acc_bins == 1`` (firmware ≥ 2.4): a single spectrum per
      integration — no even/odd jackknifing, so there is nothing to
      split or average. ``avg_even_odd`` is ignored.

    Both modes return the same consumer-facing layout:

    - Auto-correlations: ``(ntimes, nchan)`` int32.
    - Cross-correlations: ``(ntimes, nchan, 2)`` int32 (``[..., 0]``
      real, ``[..., 1]`` imaginary) — except the legacy
      ``acc_bins == 2, avg_even_odd=False`` path, which returns
      complex128.

    Parameters
    ----------
    data : dict
        Dictionary of data arrays to be reshaped. Keys specify the
        correlation pairs.
    acc_bins : int
        Accumulation bins per integration (2 for even/odd firmware,
        1 for single-spectrum firmware).
    avg_even_odd : bool
        If True, average the even and odd spectra. Only meaningful when
        ``acc_bins == 2``.

    Returns
    -------
    reshaped : dict
        Dictionary of reshaped data arrays.

    """
    reshaped = {}
    for p, arr in data.items():
        arr = np.atleast_2d(arr)  # ensure at least 2D if no times
        ntimes = arr.shape[0]
        if acc_bins == 2:
            # place even/odd on last axis
            arr = arr.reshape(ntimes, -1, 2, order="F")
            if avg_even_odd:
                # Unbiased integer average via banker's rounding.
                # mean(axis=2) goes through float64, which is exact for
                # int32 inputs (sum ≤ 2^32 < 2^53). rint uses
                # round-half-to-even (no systematic bias on crosses).
                arr = np.rint(arr.mean(axis=2)).astype(np.int32)
            if len(p) > 1:  # cross-correlation
                real = arr[:, ::2]
                imag = arr[:, 1::2]
                if avg_even_odd:
                    arr = np.stack([real, imag], axis=-1)
                else:
                    arr = real + 1j * imag
        else:
            # Single spectrum per integration (no even/odd). Nothing to
            # average; autos pass through, crosses only split the
            # interleaved real/imag onto a trailing length-2 axis.
            arr = arr.astype(np.int32)
            if len(p) > 1:  # cross-correlation
                real = arr[:, ::2]
                imag = arr[:, 1::2]
                arr = np.stack([real, imag], axis=-1)
        reshaped[p] = arr
    return reshaped


def effective_input_to_ant(wiring, adc_mux_sel):
    """Effective ``{input_str: antenna}`` map given the wiring and mux.

    The correlator emits a fixed set of products keyed by digital input
    position; ``adc_mux_sel`` (a 3-bit int) decides which physical
    antenna actually feeds each odd input position. ``bit0`` routes
    input 0's antenna into input 1, ``bit1`` routes input 2 into input
    3, ``bit2`` routes input 4 into input 5; a clear bit leaves the odd
    input on its own wired antenna.

    Even inputs (0, 2, 4) always map to their wired antenna. The result
    is sparse: an odd input that is open (un-wired) and not filled by a
    copy — or a copy whose source input is itself un-wired — is omitted,
    so a downstream label renderer falls back to the raw digital key
    rather than inventing an antenna. ``wiring`` is the hardware manifest
    (``{"ants": {name: {"snap": {"input": n}}}}``); ``None``/empty -> {}.
    """
    base = {}
    for ant, spec in ((wiring or {}).get("ants") or {}).items():
        snap = (spec or {}).get("snap") or {}
        inp = snap.get("input")
        if inp is not None:
            base[int(inp)] = ant
    out = {}
    for p in range(6):
        if p % 2 == 1 and (adc_mux_sel >> (p // 2)) & 1:
            src = p - 1  # the even input this odd input copies
            if src in base:
                out[str(p)] = base[src]
        elif p in base:
            out[str(p)] = base[p]
    return out


def pair_label(pair, input_to_ant):
    """Physical-antenna label for a digital corr pair, or ``None``.

    The raw digital pair is suffixed (``"primA / primB [02]"``,
    ``"primA [0]"``) so that mux-induced redundant baselines — the same
    physical pair measured on two input positions — stay distinguishable
    in a legend. Returns ``None`` if any input in the pair is unmapped,
    letting the caller fall back to the raw key. Pairs longer than two
    characters are not valid corr keys and also return ``None``.
    """
    if len(pair) == 1:
        a = input_to_ant.get(pair)
        return None if a is None else f"{a} [{pair}]"
    if len(pair) == 2:
        a = input_to_ant.get(pair[0])
        b = input_to_ant.get(pair[1])
        if a is None or b is None:
            return None
        return f"{a} / {b} [{pair}]"
    return None


def corr_pair_labels(header, pairs):
    """Map each digital corr pair to its physical-antenna label.

    Sources the effective input->antenna map from ``header`` —
    preferring the producer-written ``input_to_ant`` field, falling back
    to ``effective_input_to_ant(wiring, adc_mux_sel)`` for files that
    predate the field. Returns ``{pair: label_or_None}``.
    """
    header = header or {}
    input_to_ant = header.get("input_to_ant")
    if input_to_ant is not None:
        input_to_ant = {str(k): v for k, v in input_to_ant.items()}
    else:
        input_to_ant = effective_input_to_ant(
            header.get("wiring"), header.get("adc_mux_sel", 0)
        )
    return {p: pair_label(p, input_to_ant) for p in pairs}


def append_corr_header(header, acc_cnts, sync_times):
    """
    Append header for correlation files with useful computed
    quantities: times, frequencies, and (when configured via
    ``linear_range_file``) the per-channel linear-range bounds.

    Each computed field is wrapped in a try/except so that a missing
    or malformed header field cannot prevent the corr data from being
    written. On failure, the field is omitted from the output and an
    ERROR is logged. Producers must fix the header to restore the
    field.

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
        Computed fields may be missing if the source header was
        malformed (the failure is logged at ERROR level).

    """
    new_header = header.copy()
    new_header["acc_cnt"] = acc_cnts
    try:
        new_header["times"] = calc_times(
            acc_cnts,
            header["integration_time"],
            sync_times,
        )
    except (KeyError, TypeError, ValueError) as e:
        logger.error(
            f"Header contract violation: cannot compute 'times' "
            f"({type(e).__name__}: {e}). 'times' will be missing "
            f"from the file. Producer must be fixed."
        )
    try:
        freqs, dfreq = calc_freqs_dfreq(header["sample_rate"], header["nchan"])
        new_header["freqs"] = freqs
        new_header["dfreq"] = dfreq
    except (KeyError, TypeError, ValueError) as e:
        logger.error(
            f"Header contract violation: cannot compute 'freqs' "
            f"({type(e).__name__}: {e}). 'freqs' and 'dfreq' will be "
            f"missing from the file. Producer must be fixed."
        )
    # Per-channel linear-range bounds (raw corr counts), from the
    # packaged calibration product named by ``linear_range_file`` in
    # corr_config.yaml (which reaches this header via the set_header
    # cfg merge). Bounds measured at a different operating point are
    # junk, so a mismatch omits them — fix the config or re-measure.
    lr_file = header.get("linear_range_file")
    if lr_file:
        try:
            product = load_linear_range(lr_file)
        except LinearRangeError as e:
            logger.error(
                f"Linear-range contract violation: {e}. "
                f"'linear_range_min'/'linear_range_max' will be "
                f"missing from the file. Fix 'linear_range_file' in "
                f"corr_config or regenerate the product."
            )
        else:
            mismatches = validate_operating_point(product["header"], header)
            if mismatches:
                logger.error(
                    f"Linear-range operating-point mismatch for "
                    f"{lr_file!r}: {'; '.join(mismatches)}. "
                    f"'linear_range_min'/'linear_range_max' will be "
                    f"missing from the file. Re-measure the product "
                    f"at this operating point or fix corr_config."
                )
            else:
                new_header["linear_range_min"] = product["linear_min"]
                new_header["linear_range_max"] = product["linear_max"]
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
    value : bool, int, float, str (Python or numpy scalar)
        Value of the attribute. Must be a simple type (not a list or
        dict). Numpy scalars (np.bool_, np.integer, np.floating) are
        accepted and stored as their canonical numpy types so a header
        built with numpy values is layout-equivalent to one built with
        Python natives.

    Raises
    -------
    TypeError
        If the value is not a simple type.

    """
    # bool MUST be checked before int — Python bool is a subclass of
    # int, and we want True/False stored as bool, not int.
    if isinstance(value, (bool, np.bool_)):
        grp.attrs[key] = np.bool_(value)
    elif isinstance(value, (int, np.integer)):
        grp.attrs[key] = np.int64(value)
    elif isinstance(value, (float, np.floating)):
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
    if isinstance(
        value,
        (bool, int, float, str, np.bool_, np.integer, np.floating),
    ):
        _write_attr(grp, key, value)
        return
    if isinstance(value, (list, tuple, bytes, dict, np.ndarray)):
        _write_dataset(grp, key, value)
        return

    raise TypeError(f"Unsupported header type: {type(value)}")


def write_hdf5(fname, data, header, metadata=None):
    """
    Write data to an HDF5 file.

    Corr data is sacred: the ``data`` group is written first, and
    every header/metadata item is wrapped in a per-key safety net.
    A single bad header or metadata field is logged at ERROR level
    and skipped — the corr data is always preserved.

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

    """
    with h5py.File(fname, "w") as f:
        # data — written first so corr data is always in the file even
        # if every other write fails.
        data_grp = f.create_group("data")
        for key, value in data.items():
            data_grp.create_dataset(key, data=value)
        # header — per-key safety net: a contract violation on one
        # field must not prevent the rest of the header from being
        # written.
        header_grp = f.create_group("header")
        for key, value in header.items():
            try:
                _write_header_item(header_grp, key, value)
            except (TypeError, ValueError) as e:
                logger.error(
                    f"Header contract violation: failed to write "
                    f"key '{key}' ({type(e).__name__}: {e}). "
                    f"Skipping this field. Producer must be fixed."
                )
        # metadata — same per-key safety net.
        if metadata is not None:
            metadata_grp = f.create_group("metadata")
            for key, value in metadata.items():
                try:
                    _write_header_item(metadata_grp, key, value)
                except (TypeError, ValueError) as e:
                    logger.error(
                        f"Metadata contract violation: failed to "
                        f"write key '{key}' ({type(e).__name__}: "
                        f"{e}). Skipping this field. Producer must "
                        f"be fixed."
                    )


def write_metadata_hdf5(fname, metadata):
    """
    Write standalone metadata streams to HDF5.

    Produces the *same* on-disk shape as the ``metadata`` group of a corr
    file (see :func:`write_hdf5`): each stream's value — a list of
    per-sample dicts — is JSON-encoded under a top-level ``metadata``
    group via the shared :func:`_write_header_item` path, so a ``None``
    field survives as JSON ``null`` rather than a typed zero/empty
    sentinel.

    Used by ``scripts/record_metadata.py`` to save pico metadata when no
    correlator loop is running (the corr-side
    ``EigObserver.record_corr_data`` path is unavailable). Read it back
    with :func:`read_metadata_hdf5`.

    Parameters
    ----------
    fname : str or Path
        Output filename.
    metadata : dict
        ``{stream_name: [sample_dict, ...]}``. Each sample dict is one
        Redis stream entry (raw payload plus a folded-in ``_ts_unix``).
        A per-stream safety net mirrors :func:`write_hdf5`: a contract
        violation on one stream is logged at ERROR and skipped.
    """
    with h5py.File(fname, "w") as f:
        metadata_grp = f.create_group("metadata")
        for key, value in metadata.items():
            try:
                _write_header_item(metadata_grp, key, value)
            except (TypeError, ValueError) as e:
                logger.error(
                    f"Metadata contract violation: failed to write key "
                    f"'{key}' ({type(e).__name__}: {e}). Skipping this "
                    f"stream. Producer must be fixed."
                )


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
        data = {}
        for k, v in f["data"].items():
            arr = np.array(v)
            # Reconstruct complex from int32 (re, im) storage.
            # Old files store crosses as complex128 (returned as-is).
            if arr.ndim >= 2 and arr.shape[-1] == 2 and arr.dtype.kind == "i":
                arr = arr[..., 0].astype(np.float64) + 1j * arr[..., 1].astype(
                    np.float64
                )
            data[k] = arr
        # header
        header_grp = f["header"]
        header = {k: v for k, v in header_grp.attrs.items()}
        for name, obj in header_grp.items():
            if isinstance(obj, h5py.Group):
                header[name] = {k: v for k, v in obj.attrs.items()}
            else:
                header[name] = _read_dataset(obj)
        # metadata — like the header, this group can carry both
        # attrs (scalar metadata stored via _write_attr) and
        # datasets/subgroups (lists, dicts, arrays). Read both,
        # mirroring the header read above.
        metadata = {}
        if "metadata" in f:
            meta_grp = f["metadata"]
            for k, v in meta_grp.attrs.items():
                metadata[k] = v
            for name, obj in meta_grp.items():
                if isinstance(obj, h5py.Group):
                    metadata[name] = {k: v for k, v in obj.attrs.items()}
                else:
                    metadata[name] = _read_dataset(obj)
    return data, header, metadata


def read_metadata_hdf5(fname):
    """
    Read a metadata file written by :func:`write_metadata_hdf5` (e.g. by
    ``scripts/record_metadata.py``).

    Returns ``{stream_name: [sample_dict, ...]}`` — the same shape and
    None-faithful JSON decoding :func:`read_hdf5` returns for a corr
    file's ``metadata`` group, so a dropped reading reads back as ``None``
    rather than a zero/empty sentinel. Each recorder sample dict carries a
    folded-in ``_ts_unix`` (float unix seconds) for joining across streams,
    e.g. ``np.array([d["_ts_unix"] for d in rows])``.

    Parameters
    ----------
    fname : str or Path
        Path to a ``metadata_*.h5`` file produced by the recorder.

    Returns
    -------
    dict
        ``{stream_name: [sample_dict, ...]}``; empty if the file holds no
        ``metadata`` group.
    """
    out = {}
    with h5py.File(fname, "r") as f:
        if "metadata" not in f:
            return out
        for name, obj in f["metadata"].items():
            out[name] = _read_dataset(obj)
    return out


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
        date = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d_%H%M%SZ"
        )
        mode = "ant" if "ant" in data else "rec"
        file_path = Path(save_dir) / f"{mode}s11_{date}.h5"
        # Disambiguate same-second collisions on auto-generated
        # names. Mirrors the corr-file disambiguation in
        # File._do_write.
        suffix = 1
        while file_path.exists():
            file_path = Path(save_dir) / f"{mode}s11_{date}-{suffix}.h5"
            suffix += 1
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


# Required keys for the corr-write path. Validation logs ERROR per
# violation but does NOT raise — the script keeps running so corr
# data continues to flow, and the writer path is hardened to skip
# fields that depend on missing or malformed header values.
#
# Unit conventions (NOT enforced by the type-only schema; producers
# must follow these or downstream computations will be wrong):
#   - sample_rate is in MHz (matches corr_config.yaml)
#   - integration_time is in seconds
#   - sync_time (when present) is a Unix timestamp in seconds
CORR_HEADER_SCHEMA = {
    "acc_bins": int,
    "avg_even_odd": bool,
    "nchan": int,
    "dtype": str,  # must also be parseable by np.dtype
    "integration_time": float,
    "sample_rate": float,
    "wiring": dict,
}


def _validate_corr_header(header):
    """
    Validate a correlation header against ``CORR_HEADER_SCHEMA``.

    Parameters
    ----------
    header : dict
        Header to validate.

    Returns
    -------
    violations : list of str
        Human-readable contract violations. Empty list means valid.

    """
    violations = []
    for key, expected in CORR_HEADER_SCHEMA.items():
        if key not in header:
            violations.append(f"missing key '{key}'")
            continue
        val = header[key]
        if expected is bool:
            # np.bool_ from h5py round-trip is not a subclass of
            # Python bool in all numpy versions.
            ok = isinstance(val, (bool, np.bool_))
        elif expected is float:
            ok = isinstance(
                val, (int, float, np.integer, np.floating)
            ) and not isinstance(val, bool)
        elif expected is int:
            ok = isinstance(val, (int, np.integer)) and not isinstance(
                val, bool
            )
        else:
            ok = isinstance(val, expected)
        if not ok:
            violations.append(
                f"key '{key}': expected {expected.__name__}, got "
                f"{type(val).__name__}"
            )
    # dtype must additionally be parseable by numpy
    if isinstance(header.get("dtype"), str):
        try:
            np.dtype(header["dtype"])
        except TypeError as e:
            violations.append(
                f"key 'dtype': cannot parse '{header['dtype']}' as "
                f"numpy dtype ({e})"
            )
    return violations


# Sensor schemas: field name -> expected Python type.
# The type is what the field should be when the sensor is healthy.
# None values are always allowed (sensor error may null out fields).
#
# Type → averaging policy in _avg_sensor_values:
#   float → np.mean over non-error samples (the "real" averaging path)
#   int   → min over non-error samples (no-op for the invariant
#           constants), except _MAX_REDUCED_FIELDS → max (worst-case
#           for reject counters)
#   bool  → any over non-error samples (worst-case for fault flags)
#   str   → first value if unanimous, else "UNKNOWN"
# See _avg_sensor_values for details and the rationale per type.
#
# IMU schemas reflect the BNO085 UART RVC mode introduced in picohost
# 1.0.0: only yaw/pitch/roll orientation and acceleration are reported.
# The two physical IMU picos share the same derived field name but
# different sign semantics: imu_el (panda elevation, app_id 3) adds
# gravity-derived signed elevation; imu_az (antenna azimuth turntable,
# app_id 6) adds |theta| elevation only — the accel/yaw azimuth blend
# was retired in picohost 4.3 (azimuth is owned by potmon; accel-az is
# degenerate at level). All derived fields are float->float (mean
# reduction); all are None when uncalibrated.
_IMU_BASE = {
    "sensor_name": str,
    "status": str,
    "app_id": int,
    "yaw": float,
    "pitch": float,
    "roll": float,
    "accel_x": float,
    "accel_y": float,
    "accel_z": float,
}

# imu_el (panda elevation, app_id 3): gravity-derived signed elevation.
_IMU_EL_SCHEMA = {**_IMU_BASE, "el_deg": float}

# imu_az (antenna azimuth turntable, app_id 6): |theta| elevation only.
_IMU_AZ_SCHEMA = {**_IMU_BASE, "el_deg": float}

# tempctrl publishes two flat streams (one per Peltier channel), each
# matching this schema. The producer is
# `picohost.base.PicoPeltier._peltier_redis_handler`, which fans the
# firmware's combined tick into two `writer.add(...)` calls, stripping
# the `LNA_`/`LOAD_` prefix and duplicating the device-wide
# `watchdog_tripped` / `watchdog_timeout_ms` fields into both streams.
# With per-stream `status`, both streams flow through the generic
# `_avg_sensor_values` reduction like every other sensor.
#
# A channel descoped via the firmware `installed` flag
# (tempctrl_settings.{LNA,LOAD}.installed: false) publishes NO stream
# at all — clean absence (no corr-file column, no staleness warnings),
# never a sentinel or a permanent error stream. Both schemas stay
# registered here regardless: the module may return, and the fan-out
# never puts an `installed` field into the published per-channel shape.
_PELTIER_SCHEMA = {
    "sensor_name": str,
    "status": str,
    "app_id": int,
    "watchdog_tripped": bool,
    "watchdog_timeout_ms": int,
    "T_now": float,
    # Analog-thermistor diagnostics added by pico-firmware #109: the Pico
    # reports the raw ADC voltage at the divider node and the derived
    # thermistor resistance alongside the converted temperature. Scalar
    # floats, so they reduce via the standard float->mean path.
    "voltage": float,
    "resistance": float,
    "timestamp": float,
    "T_target": float,
    "drive_level": float,
    "enabled": bool,
    "active": bool,
    # Sticky control latches (pico-firmware feat/tempctrl-status-redesign):
    # sensor_tripped = rate-guard latch (garbage burst), stall_tripped =
    # drive moved nothing for a full window, runaway_tripped = temperature
    # moved against the drive (mis-wire/lost-heatsink signature). All gate
    # drive and are cleared by a *_enable=true host ack; none affect the
    # per-stream `status`, which is data-validity only — a latched channel
    # with a recovered sensor keeps contributing real values here. `any`
    # reduction = worst-case fault flag, as designed.
    "sensor_tripped": bool,
    "stall_tripped": bool,
    "runaway_tripped": bool,
    # Rate-guard reject counter (pico-firmware #150): the rate guard is
    # control-only, so rejected-but-plausible conversions are still
    # reported (status="update", real values) with this per-channel
    # counter as the cross-check marker. It increments once per
    # consecutive rejected sample and resets to 0 on an accepted one,
    # so any nonzero value inside an integration means at least one
    # averaged-in sample was rate-guard-rejected. Reduces via max —
    # see _MAX_REDUCED_FIELDS; the default int min would wash a
    # mid-integration burst back to 0 and delete the marker.
    "sensor_rejects": int,
    # Asymmetric-clamp safety setting (False forbids drive<0). Reduces
    # via `any` like every other bool config field; a mid-integration
    # toggle is an operator action and rare enough that surfacing
    # disagreement via the existing invariant-throttle is acceptable.
    "cooling_enabled": bool,
    "hysteresis": float,
    "clamp": float,
    "Kp": float,
    "Ki": float,
    "integral": float,
}

# `potmon` (potentiometer monitor): the producer is `PotMonEmulator` +
# `PicoPotentiometer._pot_redis_handler`, which augments the raw
# voltages with calibration slope/intercept and the derived angle.
# All published fields are scalar per the picohost scalar-only contract
# (see `picohost.base.redis_handler`); the cal parameters are flattened
# into per-component scalars rather than emitted as a `[m, b]` list.
# Fields are `None` for an uncalibrated stream — `_validate_metadata`
# short-circuits None, and `_avg_sensor_values`'s float reduction
# filters None survivors, so an uncalibrated stream averages cleanly
# to None for the cal/angle fields.
# `pot_az_near_rail` (picohost >= 3.12) is derived from the raw voltage
# (within POT_NEAR_RAIL_V = 0.2 V of an ADC rail), not from the cal, so
# it is a real bool even on an uncalibrated stream; it is `None` only
# when the voltage itself is missing. A railed pot reports a steady,
# plausible voltage indistinguishable from a parked antenna, so this
# flag is the stream-level tell that the absolute azimuth reference is
# at risk. The bool→`any` reduction is the fault-flag worst case: an
# integration where any sample was near a rail is flagged.
# The producer composition also carries the SP1 failsafe termination
# fields added by `_pot_redis_handler` from the firmware's `sp1_term`
# (picohost >= 4.4) — see the schema entries below.
SENSOR_SCHEMAS = {
    "imu_el": _IMU_EL_SCHEMA,
    "imu_az": _IMU_AZ_SCHEMA,
    "tempctrl_lna": _PELTIER_SCHEMA,
    "tempctrl_load": _PELTIER_SCHEMA,
    "potmon": {
        "sensor_name": str,
        "status": str,
        "app_id": int,
        "pot_az_voltage": float,
        "pot_az_angle": float,
        "pot_az_cal_slope": float,
        "pot_az_cal_intercept": float,
        "pot_az_near_rail": bool,
        # SP1 failsafe termination (picohost >= 4.4): raw pin level on
        # the potmon pico's GPIO 27 (0 = SHORT cap, the failsafe;
        # 1 = OPEN) plus the host-derived name. The str field is the
        # primary consumer surface: its first-if-unanimous-else-
        # "UNKNOWN" reduction flags a mid-integration flip exactly like
        # the rfswitch state string. The int rides along raw (additive
        # handler contract); its min reduction is not meaningful across
        # a flip — use the name. NOT an invariant field: it legitimately
        # changes when the observing cycle toggles the termination.
        "sp1_term": int,
        "sp1_term_name": str,
    },
    "rfswitch": {
        "sensor_name": str,
        "status": str,
        "app_id": int,
        "sw_state": int,
        "sw_state_name": str,
    },
    # `rfswitch_therm`: three PCB thermistors on the RF switch board,
    # fanned out of the switch-state line by
    # PicoRFSwitch._rfswitch_redis_handler (the system_current pattern) so
    # the categorical rfswitch stream stays pure. Like system_current it is
    # a derived stream with no `app_id`. `volt_therm<i>` is the raw ADC-pin
    # voltage (0-3.3V, referenced to the RP2040's internal ADC full-scale;
    # the external divider is a 5V pullup, so there is no 3.3V in the
    # sensor harness). `temp_therm<i>` is the host-side datasheet-Beta
    # conversion in degrees C, `None` when the channel is dead/shorted or
    # ADC-saturated (below ~8.5C the 5V divider drives the pin past the 3.3V
    # ADC ceiling). All six floats reduce via the standard float->mean path
    # and land in the corr file (PCB temperature affects the cal network);
    # None short-circuits in _validate_metadata / _avg_sensor_values.
    "rfswitch_therm": {
        "sensor_name": str,
        "status": str,
        "volt_therm0": float,
        "volt_therm1": float,
        "volt_therm2": float,
        "temp_therm0": float,
        "temp_therm1": float,
        "temp_therm2": float,
    },
    "lidar": {
        "sensor_name": str,
        "status": str,
        "app_id": int,
        "distance_m": float,
    },
    # `system_current`: whole-system current draw, fanned out from the
    # lidar Pico's ACS724 by picohost's PicoLidar._lidar_redis_handler.
    # Like `adc_stats`, it is a derived stream with no `app_id` (not a
    # 1:1 pico app). `status` is producer-fixed to "update" (the ADC read
    # is decoupled from lidar's I2C result). `current_a` is the meaningful
    # value (amps); `current_voltage` is the raw ADC-pin voltage diagnostic.
    # `current_cal_slope` (A/V) and `current_cal_intercept` (A) are the
    # measured two-point cal projected to amps-vs-volts
    # (current_a == slope*current_voltage + intercept), flattened to scalars
    # like potmon's pot_az_cal_slope/intercept. All four floats reduce via
    # the standard float->mean path; all are None for an uncalibrated stream
    # (no nominal fallback in picohost >= 3.11), and _validate_metadata /
    # _avg_sensor_values short-circuit None cleanly.
    "system_current": {
        "sensor_name": str,
        "status": str,
        "current_voltage": float,
        "current_a": float,
        "current_cal_slope": float,
        "current_cal_intercept": float,
    },
    # Motor positions are stepper counts. The C firmware emits them
    # as ints, but `PicoMotor._motor_redis_handler` coerces to float
    # at the Redis-publish boundary so they go through the float→mean
    # reduction rather than the int→min "invariant" reduction —
    # positions legitimately change within an integration during a
    # scan, so the integration row should record the mean position.
    # See picohost's _motor_redis_handler for the producer-side cast.
    # `boot_id` (picohost 3.7.0) is the firmware's random per-boot
    # constant, the host-side detector for "pico rebooted, step
    # counters reset". It stays int (not coerced by the producer
    # handler) and is in _INVARIANT_FIELDS: a mid-integration
    # disagreement means the pico power-cycled inside the window —
    # exactly the loud-ERROR producer event the invariant path exists
    # for.
    "motor": {
        "sensor_name": str,
        "status": str,
        "app_id": int,
        "boot_id": int,
        "az_pos": float,
        "az_target_pos": float,
        "el_pos": float,
        "el_target_pos": float,
    },
    # `adc_stats` is produced by the SNAP-side correlator (not a pico),
    # published on the SNAP transport via a second ``MetadataWriter``
    # that lives in ``EigsepFpga``. One entry is emitted per
    # ``adc_snapshot_period_s`` tick: ``_publish_adc_stats`` reduces
    # the raw snapshot frames grabbed for the adc_snapshot stream to
    # mean, power, and RMS for each of the 12 ADC cores (6 SNAP
    # inputs × 2 interleaved cores). The stats are software-derived
    # because the flashed bitstreams carry no ``input_rms_*``
    # hardware accumulators (2026-07-09 eigsep-backend diagnosis).
    # At this cadence nearly every integration row carries no
    # adc_stats sample (gap-filled None) — expected; the publish
    # period must stay below the stream-freshness ``max_age_s``
    # (30 s) or every zero-entry drain near the end of a period
    # would log a spurious stale warning. Field names are
    # ``input{N}_core{C}_{stat}``
    # where ``N`` is the snap-input index 0..5 (same label the corr
    # file uses for auto-correlations) and ``C`` is the interleaved
    # ADC core 0/1 — the even/odd sample split of the snapshot,
    # matching the 2N/2N+1 core convention of the retired register
    # block. Keeping cores split preserves the
    # interleaved-imbalance diagnostic while making each field trivially
    # joinable to the corr data it describes. Envelope is minimal: no
    # ``app_id`` (not a picohost app) and no invariant fields beyond
    # ``sensor_name``. The 36 floats go through the standard float→mean
    # reduction, so an integration row that captured a sample carries the
    # per-core RMS averaged over the integration — visible to offline
    # data analysts as a flagging / quality trace alongside the corr
    # data.
    "adc_stats": {
        "sensor_name": str,
        "status": str,
        **{
            f"input{n}_core{c}_{stat}": float
            for n in range(6)
            for c in range(2)
            for stat in ("mean", "power", "rms")
        },
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
            # Strict float check matches `_avg_sensor_values`, which
            # rejects ints at reduction time. If the validator were
            # lenient (`isinstance(val, (int, float))`) an int-emitting
            # producer would pass validation silently and then be
            # dropped to None by the float reducer — silent data loss.
            # Keep them aligned so contract drift surfaces as a
            # WARNING here, not a None at write time.
            ok = isinstance(val, float) and not isinstance(val, bool)
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


# VNA S11 producer contract. ``PandaClient.measure_s11`` publishes to
# the VNA stream the VNA's own ``header`` dict (fstart, fstop, npoints,
# ifbw, power_dBm, freqs) plus two fields added at publish time:
# ``mode`` (``"ant"`` or ``"rec"``) and ``metadata_snapshot_unix``
# (Unix seconds, the snapshot-capture wallclock). Downstream tools and
# file headers rely on this exact shape. ``freqs`` is a numpy array,
# checked separately from the scalar-typed fields.
#
# Unit conventions (NOT enforced by the type-only schema; producer
# must follow these):
#   - fstart, fstop in Hz (see cmt_vna.setup default units)
#   - ifbw in Hz
#   - power_dBm in dBm
#   - metadata_snapshot_unix is Unix seconds (time.time() semantics)
VNA_S11_HEADER_SCHEMA = {
    "fstart": float,
    "fstop": float,
    "npoints": int,
    "ifbw": float,
    "power_dBm": float,
    "mode": str,
    "metadata_snapshot_unix": float,
}

# OSL calibration standards measured by ``measure_OSL`` and prefixed
# with ``"cal:"`` by ``measure_s11`` before publication. The prefix is
# the disk-format convention also used by ``write_s11_file`` /
# ``read_s11_file``. Sourced once here so a producer rename shows up as
# a single-point contract break.
VNA_S11_CAL_KEYS = frozenset({"cal:VNAO", "cal:VNAS", "cal:VNAL"})

# Per-mode required DUT keys on the VNA payload (in addition to the cal
# keys above). The ``"ant"`` mode measures antenna, load, noise, the
# ambient load (``amb``, path VNAAMB) and the Spare-1 cable twice —
# ``sp1_short`` and ``sp1_open``, path VNASP1 with the failsafe
# termination driven to SHORT then OPEN via the potmon pico (see
# ``switch_connections`` in obs_config for what is physically
# connected); ``"rec"`` measures the receiver. These come from
# ``VNA.measure_ant`` / ``VNA.measure_dut`` / ``VNA.measure_rec`` and
# must not regress without an explicit update here — downstream
# consumers key on these names.
VNA_S11_MODE_DATA_KEYS = {
    "ant": frozenset({"ant", "load", "noise", "amb", "sp1_short", "sp1_open"}),
    "rec": frozenset({"rec"}),
}


def _validate_vna_s11_header(header):
    """
    Validate a VNA S11 header against ``VNA_S11_HEADER_SCHEMA`` plus
    the ``freqs`` numpy-array field.

    Parameters
    ----------
    header : dict
        Header dict produced by ``PandaClient.measure_s11`` (VNA
        header plus ``mode`` and ``metadata_snapshot_unix``).

    Returns
    -------
    violations : list of str
        Human-readable contract violations. Empty list means valid.
    """
    violations = []
    for key, expected in VNA_S11_HEADER_SCHEMA.items():
        if key not in header:
            violations.append(f"missing key '{key}'")
            continue
        val = header[key]
        if expected is float:
            ok = isinstance(
                val, (int, float, np.integer, np.floating)
            ) and not isinstance(val, bool)
        elif expected is int:
            ok = isinstance(val, (int, np.integer)) and not isinstance(
                val, bool
            )
        else:
            ok = isinstance(val, expected)
        if not ok:
            violations.append(
                f"key '{key}': expected {expected.__name__}, got "
                f"{type(val).__name__}"
            )
    if "mode" in header and header["mode"] not in VNA_S11_MODE_DATA_KEYS:
        violations.append(
            f"key 'mode': expected one of "
            f"{sorted(VNA_S11_MODE_DATA_KEYS)}, got {header['mode']!r}"
        )
    if "freqs" not in header:
        violations.append("missing key 'freqs'")
    else:
        freqs = header["freqs"]
        # Accept ndarray (producer side) or list (post-JSON-roundtrip
        # through VnaWriter). Either way, ensure it's non-empty and
        # length-matches npoints when both are present.
        if isinstance(freqs, np.ndarray):
            n = freqs.size
        elif isinstance(freqs, list):
            n = len(freqs)
        else:
            violations.append(
                f"key 'freqs': expected ndarray or list, got "
                f"{type(freqs).__name__}"
            )
            n = None
        if n == 0:
            violations.append("key 'freqs': empty array")
        npoints = header.get("npoints")
        if n is not None and isinstance(npoints, int) and n != npoints:
            violations.append(
                f"key 'freqs': length {n} does not match npoints {npoints}"
            )
    return violations


def _validate_vna_s11_data(data, mode):
    """
    Validate a VNA S11 data dict against the per-mode contract.

    Parameters
    ----------
    data : dict
        Output of ``PandaClient.measure_s11``: per-DUT complex arrays
        plus the ``cal:*`` OSL calibration arrays.
    mode : str
        ``"ant"`` or ``"rec"``.

    Returns
    -------
    violations : list of str
        Human-readable contract violations. Empty list means valid.
    """
    violations = []
    if mode not in VNA_S11_MODE_DATA_KEYS:
        violations.append(
            f"unknown mode {mode!r}; expected one of "
            f"{sorted(VNA_S11_MODE_DATA_KEYS)}"
        )
        return violations
    expected_dut = VNA_S11_MODE_DATA_KEYS[mode]
    required = expected_dut | VNA_S11_CAL_KEYS
    missing = required - set(data)
    extra = set(data) - required
    if missing:
        violations.append(f"missing keys: {sorted(missing)}")
    if extra:
        violations.append(f"extra keys: {sorted(extra)}")
    for key in required & set(data):
        arr = data[key]
        if not isinstance(arr, np.ndarray):
            violations.append(
                f"key '{key}': expected np.ndarray, got {type(arr).__name__}"
            )
            continue
        if arr.dtype.kind != "c":
            violations.append(
                f"key '{key}': expected complex dtype, got {arr.dtype}"
            )
        if arr.ndim != 1 or arr.size == 0:
            violations.append(
                f"key '{key}': expected non-empty 1-D array, got shape "
                f"{arr.shape}"
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
                joined = "; ".join(violations)
                logger.warning(
                    f"Metadata contract violation in '{app_name}' "
                    f"(entry {i}): {joined}"
                )
    else:
        logger.warning(
            f"No schema for sensor '{app_name}'; skipping validation"
        )

    if app_name == "rfswitch":
        return _avg_rfswitch_metadata(value)

    # generic sensor (e.g. IMU, lidar, tempctrl_lna, tempctrl_load)
    return _avg_sensor_values(value, schema, app_name=app_name)


def _avg_rfswitch_metadata(value):
    """
    Average rfswitch metadata. Returns the switch state *name*
    (``"RFANT"``, ``"VNAO"``, ...) if constant across the integration,
    or ``"UNKNOWN"`` if it changed or errored. Reads the human-readable
    ``sw_state_name`` that picohost v3's rfswitch redis handler
    publishes — no int-to-name reverse mapping is needed on the
    consumer side.

    """
    status_list = [v.get("status") for v in value]
    states = [v.get("sw_state_name") for v in value]
    if "error" in status_list:
        return "UNKNOWN"
    unique = set(s for s in states if s is not None)
    if len(unique) > 1:
        return "UNKNOWN"
    return states[0] if states else None


# ----------------------------------------------------------------------
# Categorical-disagreement detection for invariant fields.
#
# A handful of fields in SENSOR_SCHEMAS should be constant within a
# single integration: a Pico's app_id is hardcoded in firmware, a
# stream's sensor_name is fixed, the tempctrl watchdog timeout is config,
# and the motor's boot_id is constant for the life of a boot. If two
# readings inside one integration disagree, what it means depends on the
# field: for sensor_name / app_id / watchdog_timeout_ms something is
# wrong upstream (Pico misconfiguration, stream cross-talk, memory
# corruption), whereas a boot_id disagreement is a legitimate hardware
# event — the Pico power-cycled inside the window and its step counters
# reset. `_log_invariant_disagreement` is field-aware so the operator
# gets the right diagnosis either way.
#
# We log this as ERROR (not WARNING) in both cases because it always
# wants attention — a producer bug must be fixed, and a mid-integration
# power-cycle means the motor lost its zero. We throttle per (stream,
# field) at 60s so a persistent disagreement doesn't drown the log file:
# the corr loop runs at ~4 Hz, so an unthrottled log of every
# disagreement could emit ~14k events/hour for a chronic case.
#
# Non-invariant fields that legitimately change inside an integration
# (the tempctrl `watchdog_tripped` fault flag, the
# `LNA_enabled`/`LOAD_enabled`/`LNA_active`/`LOAD_active` mode flags,
# the `LNA_status`/`LOAD_status` strings) are NOT logged — the per-type
# reduction in _avg_sensor_values already encodes the disagreement in
# the saved value (`any` for bools, `"UNKNOWN"` for strings) so
# downstream can detect the issue from the file alone. Note that
# rfswitch's raw `sw_state` int and human-readable `sw_state_name` are
# both handled by _avg_rfswitch_metadata, not _avg_sensor_values. Most
# int fields reaching _avg_sensor_values are either in _INVARIANT_FIELDS
# (where `min` is a no-op-on-agreement safety net behind the invariant
# ERROR log path) or in _MAX_REDUCED_FIELDS (legitimately-varying
# worst-case counters, where the saved `max` encodes the disagreement
# silently, like bool `any`). A third category is a raw int that rides
# along a paired name/str field which is the actual consumer surface —
# e.g. potmon's `sp1_term` (raw GPIO level), whose `sp1_term_name` is
# what downstream reads; such an int can stay out of both sets, taking
# the plain `min` default with the disagreement left for the str
# field's own "UNKNOWN" reduction to surface. A future schema int that
# fits none of the three gets `min` with the disagreement silently
# captured rather than logged — decide which category it belongs in
# when adding it.
# ----------------------------------------------------------------------
_INVARIANT_FIELDS = frozenset(
    {"sensor_name", "app_id", "watchdog_timeout_ms", "boot_id"}
)

# Int fields that reduce via max() instead of min(). These are the
# legitimately-varying ints: worst-case counters where an integration
# that saw even one nonzero sample must stay identifiable after
# reduction. The producer resets such counters to 0 on every good
# sample, so `min` would wash a mid-integration burst back to 0 and
# delete the marker from the corr record. Disagreement is silent by
# design — the saved max already encodes it, exactly like bool `any`.
# Today: tempctrl's per-channel rate-guard reject counter (issue #207,
# pico-firmware #150).
_MAX_REDUCED_FIELDS = frozenset({"sensor_rejects"})
_INVARIANT_LOG_THROTTLE_S = 60.0
_last_invariant_log = {}  # {(app_name, field): unix_timestamp}


def _log_invariant_disagreement(app_name, field, observed):
    """Throttled ERROR log for an invariant-field disagreement.

    Logs at most once per (``app_name``, ``field``) per
    ``_INVARIANT_LOG_THROTTLE_S`` seconds. The throttle state is
    module-level so it survives across calls; tests that need to
    exercise the throttle can clear ``_last_invariant_log``.
    """
    key = (app_name, field)
    now = time.time()
    last = _last_invariant_log.get(key, 0.0)
    if now - last < _INVARIANT_LOG_THROTTLE_S:
        return
    _last_invariant_log[key] = now
    observed_repr = sorted(set(observed), key=str)
    if field == "boot_id":
        # A boot_id change within one integration is a real hardware
        # event, not a producer bug: the Pico power-cycled inside the
        # ~0.25 s window and its firmware drew a fresh per-boot id (the
        # motor step counters reset with it). Still ERROR — it always
        # wants attention — but the diagnosis points at the hardware.
        diagnosis = (
            "the Pico power-cycled mid-integration and re-seeded its "
            "per-boot id (motor step counters reset). This is a hardware "
            "event, not a producer bug."
        )
    else:
        diagnosis = (
            "This should never happen; check the producer for "
            "misconfiguration or stream cross-talk."
        )
    logger.error(
        f"Invariant metadata field '{field}' for stream '{app_name}' "
        f"disagreed within an integration: observed {observed_repr}. "
        f"{diagnosis}"
    )


def _avg_sensor_values(value, schema=None, *, app_name=""):
    """
    Reduce per-integration sensor readings to one entry, per type.

    See the "Metadata averaging: per-type reduction policy" section in
    CLAUDE.md for the canonical rationale and downstream-consumer guide.
    The summary below is the local cheat sheet for code edits.

    Reduction policy by schema type (Design C):

    ====  ====================================================  =========================
    type  reduction                                              filter errored?
    ====  ====================================================  =========================
    float ``np.mean`` over surviving samples                    yes
    int   ``min`` over surviving samples (worst-case); ``max``  yes
          for :data:`_MAX_REDUCED_FIELDS` (reject counters)
    bool  ``any`` over surviving samples (fault-flag worst-case)yes
    str   first value if unanimous, else ``"UNKNOWN"``          yes
    ====  ====================================================  =========================

    Plus, **before** the per-key loop, the integration's own ``status``
    field collapses to ``"error"`` if *any* sample errored. This is the
    integration-level fault flag downstream uses to mark suspect rows.
    Without this, an integration where 9 of 10 samples errored would
    silently average over the survivor and the file would say
    ``status: "update"`` for the row.

    For ``int``/``str`` fields in :data:`_INVARIANT_FIELDS`
    (``sensor_name``, ``app_id``, ``watchdog_timeout_ms``, ``boot_id``),
    a disagreement also emits a throttled ERROR log via
    :func:`_log_invariant_disagreement`. That log is field-aware: for the
    true producer-side invariants the message points at misconfiguration,
    while a ``boot_id`` disagreement is reported as the hardware event it
    is (a Pico power-cycle mid-integration). ``app_name`` is the stream
    name used as log context.

    Without a *schema*, falls back to type-sniffing from the first
    non-None value, using the same per-type reductions.

    Returns ``None`` if *value* is empty.
    """
    if not value:
        return None

    status_list = [v.get("status", "update") for v in value]
    any_error = any(s == "error" for s in status_list)

    # Step 1: collapse status before the per-key loop. Doing it here
    # means downstream can read a single per-row status flag instead
    # of inspecting every numeric field for None to infer that errors
    # happened. The first-value fallback handles the all-update case.
    avg = {}
    if "status" in (schema if schema is not None else value[0]):
        avg["status"] = "error" if any_error else value[0].get("status")

    all_keys = list(schema) if schema is not None else list(value[0])

    for data_key in all_keys:
        if data_key == "status":
            continue  # already collapsed above

        # Resolve the field's type, either from the schema or by
        # sniffing the first non-None value. bool is checked before
        # int because Python bool subclasses int.
        if schema is not None:
            typ = schema[data_key]
        else:
            first_val = None
            for v in value:
                first_val = v.get(data_key)
                if first_val is not None:
                    break
            if isinstance(first_val, bool):
                typ = bool
            elif isinstance(first_val, float):
                typ = float
            elif isinstance(first_val, int):
                typ = int
            elif isinstance(first_val, str):
                typ = str
            else:
                typ = None  # unknown — fall through to first-value default

        # Collect non-error, non-None values for this key. All four
        # reductions filter errored samples for consistency: an errored
        # reading's value is not trustworthy, and the status collapse
        # above already encodes "this integration had errors" so the
        # signal isn't lost.
        survivors = [
            v.get(data_key)
            for v, st in zip(value, status_list)
            if st != "error" and v.get(data_key) is not None
        ]

        if typ is float:
            # Strict isinstance(float) check rejects ints/bools that
            # might sneak through a producer contract violation.
            floats = [s for s in survivors if isinstance(s, float)]
            try:
                avg[data_key] = float(np.mean(floats)) if floats else None
            except Exception as e:
                logger.warning(f"Could not average key '{data_key}': {e}")
                avg[data_key] = None
        elif typ is int:
            ints = [
                s
                for s in survivors
                if isinstance(s, int) and not isinstance(s, bool)
            ]
            if not ints:
                avg[data_key] = None
            else:
                if data_key in _INVARIANT_FIELDS and len(set(ints)) > 1:
                    _log_invariant_disagreement(app_name, data_key, ints)
                if data_key in _MAX_REDUCED_FIELDS:
                    avg[data_key] = max(ints)
                else:
                    avg[data_key] = min(ints)
        elif typ is bool:
            bools = [s for s in survivors if isinstance(s, bool)]
            avg[data_key] = any(bools) if bools else None
        elif typ is str:
            strs = [s for s in survivors if isinstance(s, str)]
            if not strs:
                avg[data_key] = None
            elif len(set(strs)) == 1:
                avg[data_key] = strs[0]
            else:
                if data_key in _INVARIANT_FIELDS:
                    _log_invariant_disagreement(app_name, data_key, strs)
                avg[data_key] = "UNKNOWN"
        else:
            # Unknown type (schemaless and first value was None or an
            # unsupported type). Preserve the historical fallback of
            # carrying value[0] forward.
            avg[data_key] = value[0].get(data_key)

    return avg


class File:
    def __init__(
        self,
        save_dir,
        pairs,
        ntimes,
        cfg,
        writer_timeout=30.0,
        on_write=None,
    ):
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
        writer_timeout : float
            Maximum seconds ``corr_write`` will wait for the writer
            thread to release the standby buffer before dropping the
            active buffer with a loud ERROR. Bounds the worst-case
            behavior on a stuck writer (slow disk, NFS stall, etc.):
            corr data is sacred, but staying alive to capture future
            data is more important than blocking forever to save the
            current buffer. Default 30s — well above a normal HDF5
            write of one buffer (sub-second) and well below the
            shortest realistic buffer cadence.
        on_write : callable or None
            Optional ``on_write(path, mtime_unix)`` callback invoked
            from the writer thread after a successful ``os.rename``.
            Used by ``EigObserver`` to publish the live-status
            file-write heartbeat to Redis so a dashboard on a
            different host can see new files land without needing
            access to ``save_dir`` on disk. Exceptions raised by the
            callback are caught and logged at ERROR — corr data is
            sacred, a flaky heartbeat must not corrupt the writer.

        """
        self.logger = logger
        self.save_dir = Path(save_dir)
        self.ntimes = ntimes
        self.pairs = pairs
        self.cfg = cfg
        self._writer_timeout = writer_timeout
        self._on_write = on_write
        self._dropped_buffers = 0
        # RF switch transition tracking — see Phase 11 in
        # add_data. Forward-only: never mutates previously-written
        # samples. Both fields persist across buffer swaps and
        # writer drops since they live on File, not the buffer.
        self._prev_rfswitch_state = None
        self._rfswitch_unknown_remaining = 0
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

        Validates the merged header against ``CORR_HEADER_SCHEMA`` and
        logs ERROR per violation. Does NOT raise: corr data is sacred,
        and a header bug must not stop the script. The writer path is
        hardened to skip fields that depend on missing or malformed
        values, so producers see loud logs but data continues to flow.

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
        violations = _validate_corr_header(self.header)
        for v in violations:
            self.logger.error(
                f"Header contract violation: {v}. Producer must be "
                f"fixed; affected fields will be missing from "
                f"written files."
            )

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
        if data is None:
            self.logger.error(
                "SNAP contract violation: data is None, dropping "
                "sample. Producer must be fixed."
            )
            return
        if acc_cnt is None:
            # Keep the sample so corr data is preserved; mark the
            # acc_cnt slot as NaN so downstream can detect that this
            # row's timestamp is unknown. _prev_cnt becomes NaN too,
            # which means gap detection across this sample is lost
            # until the next valid acc_cnt re-anchors the sequence.
            self.logger.error(
                "SNAP contract violation: acc_cnt is None, storing "
                "NaN and saving sample anyway. Producer must be "
                "fixed."
            )
            acc_cnt = float("nan")
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
                self.logger.error(
                    f"Producer contract violation: metadata for "
                    f"stream '{key}' must be a non-empty list, got "
                    f"{value!r}. Dropping this stream for this sample."
                )
                continue
            # Per-stream safety net: corr data is sacred. A producer
            # contract violation that escapes avg_metadata must never
            # block the corr-data write. Log at ERROR so the producer
            # gets fixed; drop only this stream's metadata for this
            # sample and fall through to _insert_sample.
            try:
                # strip stream prefix
                name = key.removeprefix("stream:")
                processed_md[name] = avg_metadata(value)
            except Exception as e:
                self.logger.error(
                    f"Metadata contract violation processing stream "
                    f"'{key}': {e}. Producer must be fixed; dropping "
                    f"this stream's metadata for this sample."
                )

        # RF switch transition detection (Phase 11). The pico
        # reports the *commanded* switch state synchronously when
        # it receives a switch command, but the physical actuation
        # takes ~200ms and the pico has no way to know when it
        # finished. Detect transitions by comparing consecutive
        # samples' raw rfswitch states; on a change, flag a forward
        # window of samples as UNKNOWN to cover the contamination.
        # Forward-only — never mutates previously-written samples.
        new_rfswitch = processed_md.get("rfswitch")
        if (
            new_rfswitch not in (None, "UNKNOWN")
            and self._prev_rfswitch_state not in (None, "UNKNOWN")
            and new_rfswitch != self._prev_rfswitch_state
        ):
            try:
                int_time = float(self.header["integration_time"])
                n_to_flag = max(
                    1,
                    math.ceil(RFSWITCH_TRANSITION_WINDOW_S / int_time),
                )
            except (KeyError, TypeError, ValueError):
                n_to_flag = 2  # safe default for typical 0.25s int
            self._rfswitch_unknown_remaining = n_to_flag
            self.logger.info(
                f"RF switch transition detected: "
                f"{self._prev_rfswitch_state}→{new_rfswitch}. "
                f"Flagging next {n_to_flag} sample(s) as UNKNOWN to "
                f"cover the ~{int(RFSWITCH_TRANSITION_WINDOW_S * 1000)}ms "
                f"actuation+cadence window."
            )
        # Update prev only when we saw a real raw state — UNKNOWN
        # and None do not advance the comparison anchor.
        if new_rfswitch not in (None, "UNKNOWN"):
            self._prev_rfswitch_state = new_rfswitch
        # Apply the forward flag if we're inside a transition
        # window. This overrides any raw state in processed_md, and
        # is also applied when the sample carried no rfswitch
        # reading at all — the corr data is contaminated regardless
        # of whether we got a switch reading.
        if self._rfswitch_unknown_remaining > 0:
            processed_md["rfswitch"] = "UNKNOWN"
            self._rfswitch_unknown_remaining -= 1

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
        # Per-pair safety net: a SNAP contract violation on one pair
        # (missing pair, half-spectrum, wrong dtype) must not cost us
        # the other pairs in the same sample. Skip the bad pair —
        # its slot stays at zero from buffer init/reset, which is
        # visually distinguishable from real data downstream — and
        # keep going. Half-spectra are not partially saved (the
        # ValueError catches them and the pair is dropped wholesale).
        for p in self.pairs:
            try:
                self.data[p][self.counter] = sample_data[p]
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(
                    f"SNAP contract violation: cannot write pair "
                    f"'{p}' at sample {self.counter} "
                    f"({type(e).__name__}: {e}). Zeroing slot. "
                    f"Producer must be fixed."
                )
                # Belt-and-suspenders: enforce the zero-on-drop
                # contract at the use site so it does not depend on
                # __init__/reset having previously zeroed the slot.
                self.data[p][self.counter] = 0
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

        The rename is deliberately *outside* the cleanup ``try/except``:
        if ``write_hdf5`` succeeds but ``os.rename`` then raises (e.g.
        a transient NFS / filesystem error), we let the exception
        propagate without deleting the temp file. The just-written data
        is preserved on disk as ``corr_*.h5.tmp`` for an operator to
        recover by hand. Corr data is sacred — never destroy a
        successful write because of a downstream filesystem hiccup.

        """
        if fname is None:
            date = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%d_%H%M%SZ"
            )
            fname = self.save_dir / f"corr_{date}.h5"
            # Disambiguate if a file with the same second-resolution
            # timestamp already exists. In production, file_time is
            # 60-240s so this almost never triggers; the loop is
            # bounded by the number of writes per second (typically
            # zero) and each iteration is a single stat() call (~10
            # μs). An explicit fname (passed by the caller) is left
            # alone — that's the existing API contract.
            suffix = 1
            while fname.exists():
                fname = self.save_dir / f"corr_{date}-{suffix}.h5"
                suffix += 1
        self.logger.info(f"Writing correlation data to {fname}")

        # slice to counter so short final files don't include trailing zeros
        data = {p: d[:counter] for p, d in data.items()}
        acc_cnts = acc_cnts[:counter]
        sync_times = sync_times[:counter]
        metadata = {k: v[:counter] for k, v in metadata.items()}

        reshaped = reshape_data(
            data,
            acc_bins=header.get("acc_bins", 2),
            avg_even_odd=header.get("avg_even_odd", True),
        )
        full_header = append_corr_header(header, acc_cnts, sync_times)

        fd, tmp_path = tempfile.mkstemp(dir=self.save_dir, suffix=".h5.tmp")
        os.close(fd)
        try:
            write_hdf5(tmp_path, reshaped, full_header, metadata=metadata)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
        os.rename(tmp_path, fname)
        if self._on_write is not None:
            try:
                self._on_write(fname, time.time())
            except Exception as exc:
                self.logger.error(
                    f"on_write callback raised for {fname}: {exc}. "
                    "File is already on disk; heartbeat will be stale "
                    "until the next successful write."
                )

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

        # Bounded wait for the writer to release the standby buffer.
        # If the writer is stuck (slow disk, NFS stall, etc.), drop
        # the active buffer rather than block forever — corr data is
        # sacred, but staying alive to capture future data is more
        # important than blocking the data loop indefinitely. The
        # script keeps running and resumes normal writes once the
        # writer unblocks.
        if not self._standby_ready.wait(timeout=self._writer_timeout):
            self._dropped_buffers += 1
            self.logger.error(
                f"Writer thread blocked for >{self._writer_timeout}s; "
                f"dropping buffer of {self.counter} samples (total "
                f"dropped: {self._dropped_buffers}). Script continues; "
                f"resolve the underlying I/O issue."
            )
            self.reset()
            return
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
        Flush any pending data, shut down the writer thread, and
        surface final-state errors.

        Calls ``corr_write`` first if the active buffer is non-empty,
        so a caller can simply call ``close()`` at the end of a run
        without remembering to flush manually. The flush goes through
        the normal writer path (bounded wait, drop-on-timeout, full
        ERROR logging) — corr data is sacred but ``close()`` must
        remain bounded.

        """
        if self.counter > 0:
            self.corr_write()
        self._write_queue.put(None)
        self._writer_thread.join(timeout=30)
        if self._writer_thread.is_alive():
            self.logger.error("Writer thread did not shut down within timeout")
        if self._write_error is not None:
            self.logger.error(
                f"Pending write error at shutdown: {self._write_error}. "
                f"Final buffer data was lost."
            )
            self._write_error = None
        if self._dropped_buffers > 0:
            self.logger.error(
                f"Total buffers dropped due to writer hang: "
                f"{self._dropped_buffers}. Investigate I/O performance."
            )
