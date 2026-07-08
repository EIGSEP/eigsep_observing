"""
Per-channel correlator linear-range calibration product.

The product is an npz file produced by
``scripts/linearity_test/fit_linearity.py`` from a manual noise-source
attenuation sweep. It holds per-channel minimum and maximum raw corr
counts inside which the SNAP signal chain was measured to be linear
(NaN where the fit is degenerate, e.g. above the anti-aliasing LPF
cutoff), plus the full corr header of the measurement as the
operating-point provenance.

Two consumers share this module:

- ``io.append_corr_header`` injects the bounds into corr file headers
  as native float datasets at file-write time.
- The live-status aggregator draws them as dashed reference curves on
  the corr spectrum plot.

Both consumers must validate the product's operating point against the
live corr header (:func:`validate_operating_point`) and **omit** the
bounds on mismatch — bounds measured at a different operating point
(gain, FFT shift, accumulation length, ...) are junk data. Failures
raise :class:`LinearRangeError` here; consumers log at ERROR and move
on, so a bad product can never block corr data.
"""

import functools
import json
import logging
from pathlib import Path

import numpy as np

from .utils import get_data_path

logger = logging.getLogger(__name__)

# Corr-header fields that pin the operating point the product was
# measured at. If any of these differ between the product and the live
# header, the counts-space bounds do not transfer.
OPERATING_POINT_FIELDS = (
    "nchan",
    "sample_rate",
    "adc_gain",
    "fft_shift",
    "corr_scalar",
    "corr_acc_len",
    "acc_bins",
    "avg_even_odd",
    "fpg_version",
)

# npz keys every product file must carry. Per-input provenance keys
# (``linear_min_{p}``, ``slope_{p}``, ...) are optional and discovered
# by prefix scan.
_REQUIRED_KEYS = (
    "freqs",
    "linear_min",
    "linear_max",
    "header_json",
    "threshold_db",
    "smooth_window",
    "created_unix",
    "source_file",
)


class LinearRangeError(ValueError):
    """A linear-range product file is missing, unreadable, or malformed."""


def save_linear_range(
    path,
    *,
    freqs,
    linear_min,
    linear_max,
    header,
    threshold_db,
    smooth_window,
    created_unix,
    source_file,
    per_input=None,
):
    """
    Write a linear-range product npz.

    The single writer of the product schema — used by
    ``fit_linearity.py`` and by test fixtures, so fixtures match the
    production format by construction.

    Parameters
    ----------
    path : str or Path
        Output file path.
    freqs : array_like
        Frequency axis in Hz, shape (nchan,).
    linear_min, linear_max : array_like
        Per-channel bounds in raw corr counts, shape (nchan,),
        NaN where the fit was degenerate.
    header : dict
        Corr header of the measurement (JSON-native), the
        operating-point provenance.
    threshold_db : float
        Deviation-from-fit tolerance used to define the linear region.
    smooth_window : int
        Median-filter window (channels) applied across frequency.
    created_unix : float
        Product creation time (unix seconds).
    source_file : str
        Name of the sweep npz the product was fit from.
    per_input : dict, optional
        Per-input provenance, ``{input: {field: (nchan,) array}}`` with
        fields ``linear_min``, ``linear_max``, ``slope``, ``intercept``.

    """
    freqs = np.asarray(freqs, dtype=np.float64)
    linear_min = np.asarray(linear_min, dtype=np.float64)
    linear_max = np.asarray(linear_max, dtype=np.float64)
    if not (freqs.shape == linear_min.shape == linear_max.shape):
        raise LinearRangeError(
            f"shape mismatch: freqs {freqs.shape}, "
            f"linear_min {linear_min.shape}, linear_max {linear_max.shape}"
        )
    flat = {}
    for p, fields in (per_input or {}).items():
        for field, arr in fields.items():
            flat[f"{field}_{p}"] = np.asarray(arr, dtype=np.float64)
    np.savez(
        path,
        freqs=freqs,
        linear_min=linear_min,
        linear_max=linear_max,
        header_json=json.dumps(header),
        threshold_db=float(threshold_db),
        smooth_window=int(smooth_window),
        created_unix=float(created_unix),
        source_file=str(source_file),
        **flat,
    )


def load_linear_range(fname):
    """
    Load a linear-range product npz.

    Non-absolute names resolve against the packaged
    ``eigsep_observing/data/`` directory, mirroring the ``fpg_file``
    convention. Results are cached per resolved path (product files
    are immutable, versioned artifacts); the returned arrays are
    marked read-only so one consumer cannot corrupt another's view.

    Returns
    -------
    dict
        Keys ``freqs``, ``linear_min``, ``linear_max`` (read-only
        float64 arrays), ``header`` (dict — the measurement's corr
        header), ``threshold_db``, ``smooth_window``, ``created_unix``,
        ``source_file``, and ``per_input``
        (``{input: {field: array}}``).

    Raises
    ------
    LinearRangeError
        If the file is missing, unreadable, or violates the product
        schema.

    """
    path = Path(fname)
    if not path.is_absolute():
        path = Path(get_data_path(fname))
    return _load_cached(str(path))


@functools.lru_cache(maxsize=8)
def _load_cached(path):
    try:
        with np.load(path, allow_pickle=False) as npz:
            return _parse_product(npz, path)
    except LinearRangeError:
        raise
    except Exception as e:
        # np.load failure modes span OSError, ValueError, and
        # zipfile.BadZipFile; collapse them into the module's typed
        # contract error so consumers catch exactly one thing.
        raise LinearRangeError(
            f"cannot load linear-range product {path!r} "
            f"({type(e).__name__}: {e})"
        ) from e


def _parse_product(npz, path):
    missing = [k for k in _REQUIRED_KEYS if k not in npz.files]
    if missing:
        raise LinearRangeError(
            f"linear-range product {path!r} is missing required "
            f"keys: {missing}"
        )
    product = {
        "freqs": npz["freqs"].astype(np.float64),
        "linear_min": npz["linear_min"].astype(np.float64),
        "linear_max": npz["linear_max"].astype(np.float64),
        "header": json.loads(str(npz["header_json"])),
        "threshold_db": float(npz["threshold_db"]),
        "smooth_window": int(npz["smooth_window"]),
        "created_unix": float(npz["created_unix"]),
        "source_file": str(npz["source_file"]),
    }
    nchan = product["freqs"].shape
    for key in ("linear_min", "linear_max"):
        if product[key].shape != nchan:
            raise LinearRangeError(
                f"linear-range product {path!r}: {key} shape "
                f"{product[key].shape} != freqs shape {nchan}"
            )
    per_input = {}
    prefixes = ("linear_min_", "linear_max_", "slope_", "intercept_")
    for key in npz.files:
        for prefix in prefixes:
            if key.startswith(prefix):
                p = key[len(prefix) :]
                field = prefix[:-1]
                per_input.setdefault(p, {})[field] = npz[key].astype(
                    np.float64
                )
                break
    product["per_input"] = per_input
    for arr in (
        product["freqs"],
        product["linear_min"],
        product["linear_max"],
    ):
        arr.setflags(write=False)
    for fields in per_input.values():
        for arr in fields.values():
            arr.setflags(write=False)
    return product


def validate_operating_point(product_header, live_header):
    """
    Compare the product's operating point against a live corr header.

    Parameters
    ----------
    product_header : dict
        The ``header`` entry of a loaded product.
    live_header : dict
        The current corr header (from ``CorrConfigStore.get_header``
        or the merged file header).

    Returns
    -------
    list of str
        One human-readable entry per mismatched
        ``OPERATING_POINT_FIELDS`` field; empty means the product is
        valid for this header. A field missing on either side counts
        as a mismatch — silence is not agreement.

    """
    mismatches = []
    for name in OPERATING_POINT_FIELDS:
        prod = product_header.get(name)
        live = live_header.get(name)
        # JSON round-trips tuples to lists; normalize before comparing.
        if isinstance(prod, tuple):
            prod = list(prod)
        if isinstance(live, tuple):
            live = list(live)
        if prod != live:
            mismatches.append(f"{name}: product={prod!r} live={live!r}")
    return mismatches
