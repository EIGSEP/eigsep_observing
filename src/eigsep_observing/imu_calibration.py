"""Read-only consumer of picohost's ``imu_calibration`` Redis key.

The sixth single-key JSON K/V module on :mod:`_redis_json_kv`, but unlike
the other five (run_tag, obs_config_owner, file_heartbeat, snap_reinit,
corr_health) it has no publish path: picohost's ``calibrate-imu`` /
:class:`picohost.buses.ImuCalStore` owns the write. ``eigsep_observing``
only reads it, at corr/VNA file-start, to embed the IMU mount calibration
in file headers for offline recovery (parity with the pot cal that already
rides the metadata stream). See issue #176.

The stored blob is a JSON object with optional ``imu_el`` / ``imu_az``
mount-calibration sections, a ``metadata`` block, and an ``upload_time``
(unix float, injected by ``Transport.upload_dict``). It is embedded
verbatim — the consumer does not validate or transform the section shape,
so picohost remains free to evolve it.
"""

from __future__ import annotations

import logging

from picohost.keys import IMU_CAL_KEY

from ._redis_json_kv import read_json

logger = logging.getLogger(__name__)

__all__ = ["IMU_CAL_KEY", "read_calibration", "upload_unix"]


def read_calibration(transport) -> dict:
    """Latest ``imu_calibration`` blob, or ``{}`` on any failure.

    A missing key, transport error, malformed JSON, or a non-dict payload
    all resolve to ``{}``. ``{}`` means "no IMU cal recoverable from this
    file" — panda unreachable at file-open, or never calibrated. The read
    never raises, so it is safe in the corr-sacred / VNA-best-effort write
    paths.
    """
    out = read_json(
        transport,
        IMU_CAL_KEY,
        label="imu_calibration",
        logger=logger,
        parse=lambda payload: payload if isinstance(payload, dict) else None,
    )
    return out if isinstance(out, dict) else {}


def upload_unix(blob: dict) -> float:
    """Producer publish time from ``blob["upload_time"]``, else ``0.0``.

    Surfaced as the flat ``imu_calibration_upload_unix`` header field so
    offline consumers can staleness-check without digging into the nested
    blob; ``0.0`` mirrors the ``{}`` sentinel ("no recoverable cal").
    """
    try:
        return float(blob.get("upload_time"))
    except (TypeError, ValueError):
        return 0.0
