"""Tests for the EIGSEP ideal-OSL VNA calibration helpers.

The cal is quick-look — operator-visible S11 in dB on the live
dashboard and the first-order trace in bring-up HDF5 files. Field
deployment uses generic SMA caps as OSL standards; the math here
assumes ideal reflection coefficients (+1 / -1 / 0) and relies on
lab post-processing for precise correction (see
``vna_calibration.py``).

Round-trip pattern: synthesize a known network and a known DUT, embed
the DUT through the network, plus measure the OSL standards through
the same network, then run the calibration and assert we recover the
DUT trace within tight tolerance.
"""

from __future__ import annotations

import numpy as np
import pytest
from cmt_vna.calkit import embed_sparams

from eigsep_observing.vna_calibration import (
    VnaCache,
    calibrate_s11,
)


# Smaller than the production sweep (npoints=1000 in
# config/dummy_config.yaml) — the math here is shape-agnostic, so a
# short axis exercises the same code path with cheaper tests.
NFREQ = 64


def _trivial_ideal_osl():
    """OSL measurements for an *ideal* (no-error) VNA.

    With these standards the cal should reduce to the identity: any
    DUT trace should pass through ``calibrate_s11`` unchanged.
    """
    cal_o = np.ones(NFREQ, dtype=complex)
    cal_s = -np.ones(NFREQ, dtype=complex)
    cal_l = np.zeros(NFREQ, dtype=complex)
    return cal_o, cal_s, cal_l


def test_calibrate_s11_ideal_osl_is_identity():
    """If the OSL standards arrived ideal, calibration must be a no-op.

    Sanity check that pins the math: a system whose error network is
    truly identity has nothing to de-embed.
    """
    raw = np.linspace(0.1, 0.5, NFREQ).astype(complex) * np.exp(
        1j * np.linspace(0, np.pi / 2, NFREQ)
    )
    cal_o, cal_s, cal_l = _trivial_ideal_osl()

    out = calibrate_s11(raw, cal_o, cal_s, cal_l)

    np.testing.assert_allclose(out, raw, atol=1e-12)


def test_calibrate_s11_recovers_dut_through_known_network():
    """Round-trip: embed a known DUT through a known error network,
    embed the ideal standards through the same network, calibrate.

    The de-embed is just the inverse of the embed; with the OSL
    standards measured through the same path, we should recover the
    intrinsic DUT to numerical precision.
    """
    # A non-trivial 3-vector S-parameter set per frequency, varying
    # across the band so any frequency-axis-mixup would surface as a
    # mismatch rather than a constant offset.
    f_norm = np.linspace(0, 1, NFREQ)
    s11_e = (0.05 + 0.03j) * (1 + 0.1 * f_norm)
    s12s21_e = (0.9 + 0.05j) * (1 - 0.05 * f_norm)
    s22_e = (-0.02 + 0.04j) * (1 + 0.2 * f_norm)
    sparams = np.stack([s11_e, s12s21_e, s22_e])  # (3, NFREQ)

    # Ideal standards at the antenna plane.
    gamma_open = np.ones(NFREQ, dtype=complex)
    gamma_short = -np.ones(NFREQ, dtype=complex)
    gamma_load = np.zeros(NFREQ, dtype=complex)

    # What the VNA would have measured for each: embedded through the
    # error network.
    cal_o = embed_sparams(sparams, gamma_open)
    cal_s = embed_sparams(sparams, gamma_short)
    cal_l = embed_sparams(sparams, gamma_load)

    # A known DUT at the antenna plane, also embedded through the
    # same network.
    dut_true = 0.3 * np.exp(1j * np.pi * f_norm)
    dut_meas = embed_sparams(sparams, dut_true)

    out = calibrate_s11(dut_meas, cal_o, cal_s, cal_l)

    np.testing.assert_allclose(out, dut_true, atol=1e-10)


def test_calibrate_s11_rejects_shape_mismatch():
    """Shape contract is enforced loudly. A producer-side bug that
    publishes mismatched arrays must surface as a ValueError, not a
    silent broadcast or a numpy-level cryptic error."""
    raw = np.zeros(NFREQ, dtype=complex)
    cal_o = np.zeros(NFREQ, dtype=complex)
    cal_s = np.zeros(NFREQ, dtype=complex)
    cal_l = np.zeros(NFREQ - 1, dtype=complex)  # wrong length

    with pytest.raises(ValueError, match="must share length"):
        calibrate_s11(raw, cal_o, cal_s, cal_l)


def test_calibrate_s11_accepts_lists():
    """The route hands cmtvna numpy arrays today, but the math should
    accept anything ``np.asarray`` can swallow so a future caller
    (e.g. a JSON-roundtripped fixture) doesn't have to pre-cast."""
    raw = [0.1 + 0j] * NFREQ
    cal_o = [1.0 + 0j] * NFREQ
    cal_s = [-1.0 + 0j] * NFREQ
    cal_l = [0.0 + 0j] * NFREQ

    out = calibrate_s11(raw, cal_o, cal_s, cal_l)

    assert out.shape == (NFREQ,)
    np.testing.assert_allclose(out, np.asarray(raw, dtype=complex), atol=1e-12)


def test_vna_cache_is_frozen():
    """VnaCache is the type the aggregator hands to the route handler
    under the snapshot lock. Freezing it makes accidental mutation
    surface as a TypeError instead of a hard-to-debug data-race."""
    cache = VnaCache(
        freqs=np.array([1.0]),
        raw_s11=np.array([0.1 + 0j]),
        cal_o=np.array([1.0 + 0j]),
        cal_s=np.array([-1.0 + 0j]),
        cal_l=np.array([0.0 + 0j]),
        received_unix=123.0,
        metadata_snapshot_unix=120.0,
    )
    with pytest.raises(AttributeError):
        cache.received_unix = 999.0  # type: ignore[misc]
