"""Tests for the live-status first-order Y-factor calibration helpers.

The cal is display-only — operator-visible spectra in Kelvin on the
live dashboard. The math itself is a pure-Python module so it can be
unit-tested in isolation, without aggregator / Flask scaffolding.

Round-trip pattern: synthesize ``P_off``, ``P_on``, and a fictitious
sky/source spectrum from a *known* ``(G, T_rx, T_LOAD, T_ENR)``, run
the cal, and assert we recover the inputs to within a tight tolerance.
This is the inverse of the contract the field deployment cares about,
which is the right shape for a contract test.
"""

from __future__ import annotations

import numpy as np
import pytest

from eigsep_observing.live_status.calibration import (
    apply_calibration_auto,
    apply_calibration_cross_mag,
    compute_gain_trx,
)


NCHAN = 32  # tiny — these tests don't care about realism, just shapes


def _synthesize(
    *,
    gain: np.ndarray,
    t_rx: np.ndarray,
    t_load: float,
    t_enr_k: float,
):
    """Build a (P_on, P_off) pair consistent with the cal model.

    P_off = G * (T_LOAD + T_rx)
    P_on  = G * (T_LOAD + T_rx + T_ENR)
    """
    p_off = gain * (t_load + t_rx)
    p_on = gain * (t_load + t_rx + t_enr_k)
    return p_on, p_off


def test_compute_gain_trx_recovers_inputs_for_well_posed_case():
    """Round-trip: feed the model with known (G, T_rx) and recover them.

    Hand-picked numbers (G=1e6 arb/K, T_rx=80 K, T_LOAD=290 K,
    T_ENR=1500 K) so the assert can run with a tight relative tolerance
    and any drift in the math jumps out immediately.
    """
    gain_in = np.full(NCHAN, 1e6)
    t_rx_in = np.full(NCHAN, 80.0)
    t_load = 290.0
    t_enr_k = 1500.0
    p_on, p_off = _synthesize(
        gain=gain_in, t_rx=t_rx_in, t_load=t_load, t_enr_k=t_enr_k
    )

    gain, t_rx = compute_gain_trx(p_on, p_off, t_load, t_enr_k)

    np.testing.assert_allclose(gain, gain_in, rtol=1e-12)
    np.testing.assert_allclose(t_rx, t_rx_in, rtol=1e-12)


def test_compute_gain_trx_handles_p_on_equals_p_off_with_nan():
    """If P_on == P_off, the gain estimate divides by zero — propagate
    NaN per channel rather than crashing or returning ±inf.

    This is the "stale on/off" failure mode: a producer pushed the same
    integration twice (or the diode never fired). Calibrated mode is
    going to fall back to raw at the route level, but the math itself
    still has to be safe so the route can inspect ``calibration_meta``
    rather than catching exceptions.
    """
    p = np.full(NCHAN, 1e9)
    gain, t_rx = compute_gain_trx(p, p, t_load=290.0, t_enr_k=1500.0)
    assert np.all(np.isnan(gain))
    assert np.all(np.isnan(t_rx))


def test_compute_gain_trx_marks_negative_diff_as_nan():
    """If P_on < P_off (noise diode dimmer than the load — physically
    impossible, so a producer/wiring bug), gain would be negative and
    the cal would invert the spectrum. Mark those channels NaN so the
    downstream apply step skips them and the operator sees a hole, not
    nonsense.
    """
    p_off = np.full(NCHAN, 2e9)
    p_on = np.full(NCHAN, 1e9)
    gain, t_rx = compute_gain_trx(p_on, p_off, t_load=290.0, t_enr_k=1500.0)
    assert np.all(np.isnan(gain))
    assert np.all(np.isnan(t_rx))


def test_apply_calibration_auto_recovers_t_sky():
    """End-to-end Y-factor: cal an RFANT auto and recover T_sky."""
    gain = np.full(NCHAN, 1e6)
    t_rx = np.full(NCHAN, 80.0)
    t_sky = np.full(NCHAN, 250.0)
    p_ant = gain * (t_sky + t_rx)

    t_in = apply_calibration_auto(p_ant, gain, t_rx)

    np.testing.assert_allclose(t_in, t_sky, rtol=1e-12)


def test_apply_calibration_auto_on_rfnoff_returns_t_load():
    """Sanity-check baked into the cal: an RFNOFF spectrum, calibrated
    with the same on/off pair it came from, must return T_LOAD. This is
    the operator-visible "is the calibration sane" check on the
    dashboard."""
    gain = np.full(NCHAN, 1e6)
    t_rx = np.full(NCHAN, 80.0)
    t_load = 290.0
    p_off = gain * (t_load + t_rx)

    t_in = apply_calibration_auto(p_off, gain, t_rx)

    np.testing.assert_allclose(t_in, t_load, rtol=1e-12)


def test_apply_calibration_auto_propagates_nan_gain():
    """A NaN gain channel must yield NaN in the calibrated output, not
    +/-inf or a silent zero. This is what shows the operator a hole in
    the spectrum where the cal is unreliable."""
    gain = np.full(NCHAN, 1e6)
    gain[5] = np.nan
    t_rx = np.full(NCHAN, 80.0)
    p = np.full(NCHAN, 1e9)

    t_in = apply_calibration_auto(p, gain, t_rx)

    assert np.isnan(t_in[5])
    # Other channels still finite.
    assert np.all(np.isfinite(np.delete(t_in, 5)))


def test_apply_calibration_cross_mag_divides_by_gain():
    """Cross magnitudes are scaled by 1/G to put them in K-equivalent
    units. Phase is the JSON's separate field and is not touched by the
    cal at all (the live-status route handles that).
    """
    gain = np.full(NCHAN, 1e6)
    raw_mag = np.full(NCHAN, 5e6)

    cal_mag = apply_calibration_cross_mag(raw_mag, gain)

    np.testing.assert_allclose(cal_mag, 5.0, rtol=1e-12)


def test_apply_calibration_cross_mag_propagates_nan_gain():
    gain = np.full(NCHAN, 1e6)
    gain[3] = np.nan
    raw_mag = np.full(NCHAN, 5e6)

    cal_mag = apply_calibration_cross_mag(raw_mag, gain)

    assert np.isnan(cal_mag[3])
    assert np.all(np.isfinite(np.delete(cal_mag, 3)))


def test_compute_gain_trx_accepts_lists():
    """Inputs come from JSON / Python-list shapes in the live-status
    plumbing (the aggregator caches arrays, but the route handler
    massages them). Accept anything ``np.asarray`` can swallow rather
    than forcing the caller to pre-cast.
    """
    p_off = [1e9] * NCHAN
    p_on = [2.5e9] * NCHAN
    gain, t_rx = compute_gain_trx(p_on, p_off, t_load=290.0, t_enr_k=1500.0)
    assert gain.shape == (NCHAN,)
    assert t_rx.shape == (NCHAN,)
    assert np.all(np.isfinite(gain))


@pytest.mark.parametrize("t_enr_k", [0.0, -1500.0])
def test_compute_gain_trx_invalid_t_enr_raises(t_enr_k):
    """A non-positive ``T_ENR`` is a config bug — refuse to run rather
    than silently produce garbage. The route falls back to raw on
    ``ValueError`` so the dashboard keeps painting; the producer is
    fixed by the operator.
    """
    p = np.full(NCHAN, 1e9)
    with pytest.raises(ValueError, match="t_enr_k"):
        compute_gain_trx(p, p * 0.5, t_load=290.0, t_enr_k=t_enr_k)
