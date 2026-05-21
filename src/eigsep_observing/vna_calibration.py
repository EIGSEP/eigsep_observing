"""Field-grade OSL calibration for the EIGSEP VNA path.

The deployed system uses generic SMA open / short / load caps as
calibration standards, not the metrology-grade S911T calkit that
:class:`cmt_vna.calkit.S911T` models. We therefore assume **ideal**
reflection coefficients — ``+1`` open, ``-1`` short, ``0`` load — and
accept the systematic error that introduces. Online displays and
bring-up artifacts are quick-look outputs; lab post-processing on
the saved ``.h5`` files re-applies a precise calkit model when
needed.

Pure numpy / cmt_vna primitives. No Redis, no Flask, no aggregator.
Callers: the live-status route handler in ``live_status/app.py``,
the cache in ``live_status/aggregator.py``, and
``eigsep_observing.vna.save_vna_manual_h5`` (the local-HDF5 path of
``scripts/vna_manual.py``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
from cmt_vna.calkit import de_embed_sparams, network_sparams


@dataclass(frozen=True)
class VnaCache:
    """Most recent VNA payload of one mode (``"ant"`` or ``"rec"``).

    Holds exactly what the route handler needs to (a) calibrate the
    measurement on demand and (b) display its age. Stored as a frozen
    dataclass so the aggregator's snapshot lock can hand a reference to
    a Flask handler without copying — the arrays themselves are never
    mutated in place after a write.
    """

    freqs: np.ndarray  # (Nfreq,) float, Hz
    raw_s11: np.ndarray  # (Nfreq,) complex, the DUT trace
    cal_o: np.ndarray  # (Nfreq,) complex, raw OSL Open
    cal_s: np.ndarray  # (Nfreq,) complex, raw OSL Short
    cal_l: np.ndarray  # (Nfreq,) complex, raw OSL Load
    received_unix: float  # wallclock at which this entry was drained
    metadata_snapshot_unix: Optional[float]  # producer's trigger time


def calibrate_s11(
    raw_s11: np.ndarray,
    cal_o: np.ndarray,
    cal_s: np.ndarray,
    cal_l: np.ndarray,
) -> np.ndarray:
    """Apply ideal-OSL calibration to a raw DUT S11 trace.

    Builds the one-port error model from the measured OSL standards
    against ideal ``[+1, -1, 0]`` reference reflections, then de-embeds
    the network from ``raw_s11``. All input arrays must share the same
    ``(Nfreq,)`` shape.

    Returns the calibrated complex S11 at the OSL reference plane.
    """
    raw = np.asarray(raw_s11, dtype=complex)
    open_ = np.asarray(cal_o, dtype=complex)
    short = np.asarray(cal_s, dtype=complex)
    load = np.asarray(cal_l, dtype=complex)

    if not (raw.ndim == open_.ndim == short.ndim == load.ndim == 1):
        raise ValueError(
            "calibrate_s11: raw_s11 and OSL standards must all be 1-D; "
            "got "
            f"raw ndim={raw.ndim}, shape={raw.shape}, "
            f"O ndim={open_.ndim}, shape={open_.shape}, "
            f"S ndim={short.ndim}, shape={short.shape}, "
            f"L ndim={load.ndim}, shape={load.shape}"
        )

    n = raw.shape[0]
    if not (open_.shape == short.shape == load.shape == (n,)):
        raise ValueError(
            "calibrate_s11: raw_s11 and OSL standards must share "
            "length; got "
            f"raw={raw.shape}, O={open_.shape}, "
            f"S={short.shape}, L={load.shape}"
        )
    gamma_true = np.stack(
        [
            np.ones(n, dtype=complex),
            -np.ones(n, dtype=complex),
            np.zeros(n, dtype=complex),
        ]
    )
    gamma_meas = np.stack([open_, short, load])
    sprms = network_sparams(gamma_true, gamma_meas)
    return de_embed_sparams(sprms, raw)
