"""Antenna pointing: per-sensor az/el consensus for the live-status panel.

Pure computation (no Flask/Redis). Turns the /api/metadata per-sensor payload
into az/el degrees for each sensor that reports them, plus a median consensus
and the max-min spread. The spread is the live drift/stall signal; the
consensus is display-only (the authoritative weighted estimate is a
post-processing concern, not a live decision).
"""

import numpy as np


def _val(metadata, sensor, field):
    entry = metadata.get(sensor)
    if not entry:
        return None
    v = (entry.get("value") or {}).get(field)
    return float(v) if isinstance(v, (int, float)) else None


def _reduce(values):
    pts = {k: v for k, v in values.items() if v is not None}
    if not pts:
        return {}
    out = dict(pts)
    nums = list(pts.values())
    out["consensus"] = float(np.median(nums))
    out["spread"] = float(max(nums) - min(nums)) if len(nums) >= 2 else None
    return out


def compute_orientation(metadata, steps_to_deg):
    """Per-sensor az/el in degrees + median consensus + spread.

    metadata: the /api/metadata per-sensor dict ({sensor: {"value": {...}}}).
    steps_to_deg: callable mapping motor steps -> degrees (single source of
    truth = the firmware geometry). Sensors absent or non-numeric are omitted.
    """
    az_pos = _val(metadata, "motor", "az_pos")
    el_pos = _val(metadata, "motor", "el_pos")
    az = {
        "motor": steps_to_deg(az_pos) if az_pos is not None else None,
        "potmon": _val(metadata, "potmon", "pot_az_angle"),
        "imu_az": _val(metadata, "imu_az", "az_deg"),
    }
    el = {
        "motor": steps_to_deg(el_pos) if el_pos is not None else None,
        "imu_az": _val(metadata, "imu_az", "el_deg"),
        "imu_el": _val(metadata, "imu_el", "el_deg"),
    }
    return {"az": _reduce(az), "el": _reduce(el)}
