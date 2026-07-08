"""Tests for the linear-range calibration product (linear_range.py).

Product fixtures are written with ``save_linear_range`` — the same
writer ``fit_linearity.py`` uses — so they match the production file
schema by construction.
"""

import numpy as np
import pytest

from conftest import HEADER
from eigsep_observing import linear_range

NCHAN = HEADER["nchan"]


def _product_arrays():
    """(freqs, linear_min, linear_max) shaped like a real fit output.

    The top ~10% of channels are NaN-masked, mirroring the band above
    the 225 MHz LPF cutoff where the noise source injects no power and
    the per-channel fit is degenerate.
    """
    freqs = np.arange(NCHAN) * (500e6 / 2 / NCHAN)
    linear_min = np.full(NCHAN, 1e5)
    linear_max = np.full(NCHAN, 5e8)
    linear_min[-NCHAN // 10 :] = np.nan
    linear_max[-NCHAN // 10 :] = np.nan
    return freqs, linear_min, linear_max


def _write_product(path, header=HEADER, **overrides):
    freqs, linear_min, linear_max = _product_arrays()
    kwargs = dict(
        freqs=freqs,
        linear_min=linear_min,
        linear_max=linear_max,
        header=header,
        threshold_db=1.0,
        smooth_window=17,
        created_unix=1751000000.0,
        source_file="linearity_corr.npz",
        per_input={
            "0": {
                "linear_min": linear_min,
                "linear_max": linear_max,
                "slope": np.full(NCHAN, 0.1),
                "intercept": np.full(NCHAN, 7.0),
            },
        },
    )
    kwargs.update(overrides)
    linear_range.save_linear_range(path, **kwargs)
    return path


def test_save_load_round_trip(tmp_path):
    path = _write_product(tmp_path / "product.npz")
    product = linear_range.load_linear_range(str(path))
    freqs, linear_min, linear_max = _product_arrays()
    np.testing.assert_array_equal(product["freqs"], freqs)
    np.testing.assert_array_equal(product["linear_min"], linear_min)
    np.testing.assert_array_equal(product["linear_max"], linear_max)
    assert product["header"] == HEADER
    assert product["threshold_db"] == 1.0
    assert product["smooth_window"] == 17
    assert product["created_unix"] == 1751000000.0
    assert product["source_file"] == "linearity_corr.npz"
    assert set(product["per_input"]) == {"0"}
    per = product["per_input"]["0"]
    assert set(per) == {"linear_min", "linear_max", "slope", "intercept"}
    np.testing.assert_array_equal(per["slope"], np.full(NCHAN, 0.1))


def test_loaded_arrays_are_read_only(tmp_path):
    path = _write_product(tmp_path / "product.npz")
    product = linear_range.load_linear_range(str(path))
    with pytest.raises(ValueError):
        product["linear_min"][0] = 0.0


def test_load_is_cached_per_path(tmp_path):
    path = _write_product(tmp_path / "product.npz")
    first = linear_range.load_linear_range(str(path))
    second = linear_range.load_linear_range(str(path))
    assert first is second


def test_missing_file_raises_typed_error(tmp_path):
    with pytest.raises(linear_range.LinearRangeError, match="cannot load"):
        linear_range.load_linear_range(str(tmp_path / "nope.npz"))


def test_missing_keys_raise_typed_error(tmp_path):
    path = tmp_path / "malformed.npz"
    freqs, linear_min, _ = _product_arrays()
    np.savez(path, freqs=freqs, linear_min=linear_min)
    with pytest.raises(
        linear_range.LinearRangeError, match="missing required"
    ):
        linear_range.load_linear_range(str(path))


def test_bound_shape_mismatch_rejected_at_save(tmp_path):
    with pytest.raises(linear_range.LinearRangeError, match="shape"):
        _write_product(
            tmp_path / "bad.npz", linear_min=np.full(NCHAN // 2, 1e5)
        )


def test_relative_name_resolves_against_data_dir(tmp_path, monkeypatch):
    _write_product(tmp_path / "relative.npz")
    monkeypatch.setattr(
        linear_range, "get_data_path", lambda fname: tmp_path / fname
    )
    product = linear_range.load_linear_range("relative.npz")
    assert product["header"] == HEADER


def test_validate_operating_point_identical_headers():
    assert linear_range.validate_operating_point(HEADER, HEADER) == []


@pytest.mark.parametrize("field", linear_range.OPERATING_POINT_FIELDS)
def test_validate_operating_point_flags_each_field(field):
    live = dict(HEADER)
    live[field] = "DIFFERENT"
    mismatches = linear_range.validate_operating_point(HEADER, live)
    assert len(mismatches) == 1
    assert mismatches[0].startswith(f"{field}:")


def test_validate_operating_point_missing_field_is_mismatch():
    live = dict(HEADER)
    del live["adc_gain"]
    mismatches = linear_range.validate_operating_point(HEADER, live)
    assert len(mismatches) == 1
    assert "adc_gain" in mismatches[0]


def test_validate_operating_point_tuple_list_equivalent():
    """JSON round-trips tuples to lists; the comparison must not flag
    fpg_version=[2, 4] vs fpg_version=(2, 4) as a mismatch."""
    live = dict(HEADER, fpg_version=tuple(HEADER["fpg_version"]))
    assert linear_range.validate_operating_point(HEADER, live) == []


def test_extra_live_header_fields_ignored():
    """Non-operating-point fields (sync_time, run_tag, ...) may differ
    freely between measurement and deployment."""
    live = dict(HEADER, sync_time=0.0, run_tag="manual-session")
    assert linear_range.validate_operating_point(HEADER, live) == []
