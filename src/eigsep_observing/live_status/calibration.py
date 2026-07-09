"""First-order Y-factor calibration for the live-status dashboard.

Display-only. The "real" calibration (lab-measured ENR, polynomial
bandpass corrections, etc.) lives elsewhere; this module exists so an
operator looking at the live dashboard can flip a toggle and see
spectra in Kelvin, with the built-in sanity check that ``RFAMB``
calibrates to ``T_amb`` (ambient load) and ``RFNON`` to
``T_hot = T_ns + T_ENR`` (noise-source pad ambient plus the
attenuated diode excess). ``RFNOFF`` is still measured every cycle
and lands near ``T_ns`` (the pad ambient, diode off) as an offline
cross-check.

Pure numpy. No Redis, no Flask, no aggregator — the route handler in
``app.py`` and the cache in ``aggregator.py`` are the only callers.
"""

from __future__ import annotations

from typing import Tuple

import numpy as np


def compute_gain_trx(
    p_on,
    p_amb,
    t_hot_k: float,
    t_cold_k: float,
) -> Tuple[np.ndarray, np.ndarray]:
    """Y-factor solve for ``(G(ν), T_rx(ν))``.

    ``G(ν)   = (P_on - P_amb) / (T_hot - T_cold)``
    ``T_rx(ν) = P_amb / G(ν) - T_cold``

    ``t_hot_k`` is the RFNON input temperature (the noise-source pad's
    physical temperature plus the attenuated diode excess) and
    ``t_cold_k`` the RFAMB ambient-load temperature. Both references
    are measured independently — no shared-base-temperature assumption.

    Channels with ``P_on <= P_amb`` (which would give a non-positive
    gain — physically a wiring or producer bug, not a real signal) are
    set to ``NaN`` so callers see a hole in the spectrum rather than
    inverted nonsense.
    """
    if not (t_hot_k > t_cold_k):
        raise ValueError(
            f"t_hot_k ({t_hot_k!r}) must exceed t_cold_k ({t_cold_k!r}); "
            "a hot reference at or below ambient is a calibration/"
            "configuration bug — fix the caller's settings, don't paper "
            "over it"
        )
    p_on = np.asarray(p_on, dtype=np.float64)
    p_amb = np.asarray(p_amb, dtype=np.float64)
    diff = p_on - p_amb
    with np.errstate(divide="ignore", invalid="ignore"):
        gain_raw = diff / (float(t_hot_k) - float(t_cold_k))
        gain = np.where(gain_raw > 0, gain_raw, np.nan)
        t_rx = p_amb / gain - float(t_cold_k)
    return gain, t_rx


def apply_calibration_auto(p, gain, t_rx) -> np.ndarray:
    """Auto-correlation power → input temperature.

    ``T_in(ν) = P(ν) / G(ν) - T_rx(ν)``

    NaN-in / NaN-out per channel.
    """
    p = np.asarray(p, dtype=np.float64)
    gain = np.asarray(gain, dtype=np.float64)
    t_rx = np.asarray(t_rx, dtype=np.float64)
    with np.errstate(invalid="ignore", divide="ignore"):
        return p / gain - t_rx


def apply_calibration_cross_mag(mag, gain) -> np.ndarray:
    """Cross-correlation magnitude → K-equivalent units.

    Single-receiver, common-gain assumption: ``|V_cal| = |V| / G``.
    Phase is unaffected by an amplitude-only cal and is left to the
    caller.
    """
    mag = np.asarray(mag, dtype=np.float64)
    gain = np.asarray(gain, dtype=np.float64)
    with np.errstate(invalid="ignore", divide="ignore"):
        return mag / gain
