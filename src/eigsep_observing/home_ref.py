"""Redis K/V for the operational motor home reference.

Written by field_zero when it sets zero; read by MotorHomer to know where
to return. Raw, drift-free, power-cycle-stable values: az home as the pot
voltage v0 (slope-independent), el home as the signed imu_el elevation.
Follows the shared _redis_json_kv sibling pattern (run_tag, corr_health,
snap_reinit, file_heartbeat) — not on the metadata bus, not a *Writer/
*Reader class.
"""

import logging

from ._redis_json_kv import publish_json, read_json

logger = logging.getLogger(__name__)

_KEY = "home_ref"


def publish_home_ref(transport, *, pot_az_voltage_v0, imu_el_deg_home):
    publish_json(
        transport,
        _KEY,
        {
            "pot_az_voltage_v0": pot_az_voltage_v0,
            "imu_el_deg_home": imu_el_deg_home,
        },
    )


def read_home_ref(transport):
    return read_json(
        transport,
        _KEY,
        label="home_ref",
        logger=logger,
        parse=lambda payload: payload,
    )
