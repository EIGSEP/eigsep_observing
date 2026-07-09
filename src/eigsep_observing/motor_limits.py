"""Redis K/V for rig-wide motor safe-travel limits.

Single source of truth read by EVERY MotorClient (autonomous observer and
bring-up scripts alike), so the travel-limit guard is rig-wide rather than
per-process. Set/inspected by scripts/set_motor_limits.py. Follows the
shared _redis_json_kv sibling pattern (run_tag, corr_health, ...). Not on the
metadata bus; NOT obs_config (so the scripts/CLAUDE.md "no ConfigStore
upload" rule is not in play).
"""

import logging

from ._redis_json_kv import publish_json, read_json

logger = logging.getLogger(__name__)

_KEY = "motor_limits"


def publish_motor_limits(
    transport,
    *,
    az_limits_deg,
    el_limits_deg,
    pot_az_v_limits,
    imu_el_limits_deg,
):
    publish_json(
        transport,
        _KEY,
        {
            "az_limits_deg": az_limits_deg,
            "el_limits_deg": el_limits_deg,
            "pot_az_v_limits": pot_az_v_limits,
            "imu_el_limits_deg": imu_el_limits_deg,
        },
    )


def read_motor_limits(transport):
    return read_json(
        transport,
        _KEY,
        label="motor_limits",
        logger=logger,
        parse=lambda payload: payload,
    )
