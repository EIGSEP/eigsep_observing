"""Redundant elevation estimate from the two IMUs.

imu_el (on the box, azimuth-invariant) reports SIGNED elevation over the
full range and is the primary. imu_az (on the turntable) reports only |θ|
(azimuth-sensitive ⇒ sign-degenerate) and is a magnitude-only failover when
imu_el is dead. The two are NEVER averaged — they are different quantities;
their disagreement (|imu_el| vs imu_az |θ|) is a fault signal.
"""

from collections import namedtuple

import redis

ElEstimate = namedtuple("ElEstimate", "el_deg magnitude_only source")


def _get(reader, key):
    try:
        return (reader.get(key) or {}).get("el_deg")
    except (KeyError, redis.exceptions.ConnectionError):
        return None


def read_el_estimate(snapshot_reader, *, logger=None, crosscheck_tol_deg=5.0):
    """Return an :class:`ElEstimate` from the two IMU streams.

    Parameters
    ----------
    snapshot_reader : MetadataSnapshotReader (or duck-type with .get)
        Provides ``imu_el`` and ``imu_az`` snapshot dicts.
    logger : logging.Logger or None
        When supplied, a WARNING is emitted if both IMUs are present but
        their magnitudes disagree by more than *crosscheck_tol_deg*.
    crosscheck_tol_deg : float
        Tolerance in degrees for the cross-check (default 5.0).

    Returns
    -------
    ElEstimate
        Named tuple ``(el_deg, magnitude_only, source)``.
        *source* is ``"imu_el"`` (signed primary), ``"imu_az"``
        (magnitude-only failover), or ``"none"`` (both absent /
        connection error).  A ``redis.exceptions.ConnectionError`` from
        the reader also yields ``ElEstimate(None, False, "none")``.
    """
    el = _get(snapshot_reader, "imu_el")
    az = _get(snapshot_reader, "imu_az")
    if el is not None:
        if (
            az is not None
            and logger is not None
            and abs(abs(el) - az) > crosscheck_tol_deg
        ):
            logger.warning(
                "IMU elevation readings disagree: imu_el=%.1f deg, "
                "imu_az|θ|=%.1f deg (>%.1f) — possible IMU fault.",
                el,
                az,
                crosscheck_tol_deg,
            )
        return ElEstimate(el, False, "imu_el")
    if az is not None:
        return ElEstimate(az, True, "imu_az")
    return ElEstimate(None, False, "none")
