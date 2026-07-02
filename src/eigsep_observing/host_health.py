"""Raspberry Pi host vitals K/V for the live-status dashboard.

Published by the ``eigsep-host-health`` console script (an always-on
systemd service on each pi), consumed by ``LiveStatusAggregator``. One
field today:

- ``temp_c``: the SoC temperature from
  ``/sys/class/thermal/thermal_zone0/temp`` (millidegrees on both the
  Pi 4B and the Pi 5). ``None`` when the thermal zone could not be
  read — the publish still happens so a fresh ``published_unix``
  distinguishes "sensor read failed" from "publisher is down".

Each pi runs its own Redis server (backend pi at ``rpi_ip``, panda pi
at ``panda_ip``) and the publisher writes to its *local* Redis, so a
single key constant suffices: the transport the aggregator reads it
through identifies which pi the reading belongs to. ``hostname`` rides
along as provenance for the operator.

This is deliberately a plain K/V, *not* a metadata-bus stream: the
metadata bus is drained by the corr loop into the HDF5 file path,
which would pull in a ``SENSOR_SCHEMAS`` entry, a producer-contract
emulator, and the averaging pipeline for a diagnostic no offline
consumer needs. The live dashboard is the only consumer, so it rides
the same small-K/V pattern as ``corr_health`` / ``file_heartbeat``.
The single key is overwritten on every publish — consumers only ever
care about the most recent write.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

from ._redis_json_kv import publish_json, read_json

logger = logging.getLogger(__name__)

HOST_HEALTH_KEY = "eigsep:host_health"

# Present and equivalent on both pis: the Pi 4B and Pi 5 each expose
# the SoC temperature as thermal_zone0 (type ``cpu-thermal``).
THERMAL_ZONE_PATH = Path("/sys/class/thermal/thermal_zone0/temp")

_EMPTY = {
    "hostname": None,
    "temp_c": None,
    "published_unix": None,
    "seconds_since_publish": None,
}


def read_cpu_temp_c(path: Path = THERMAL_ZONE_PATH) -> Optional[float]:
    """Read the SoC temperature in Celsius from the sysfs thermal zone.

    Returns ``None`` (with a WARNING) when the zone is missing or
    unparseable — e.g. running on a non-pi development host. Callers
    publish the ``None`` as-is so the dashboard shows "unknown value,
    live publisher" rather than a stale tile.
    """
    try:
        return int(path.read_text().strip()) / 1000.0
    except (OSError, ValueError) as exc:
        logger.warning("failed to read CPU temperature from %s: %s", path, exc)
        return None


def publish(
    transport,
    *,
    temp_c: Optional[float],
    hostname: str,
    now: Optional[float] = None,
) -> None:
    """Stamp the host vitals snapshot into Redis.

    Any exception is logged at WARNING and swallowed (the
    ``file_heartbeat`` policy): the publisher is an always-on service
    and its loop is already the retry path — a Redis restart costs a
    few dashboard ticks, not a service crash.
    """
    payload = {
        "hostname": str(hostname),
        "temp_c": float(temp_c) if temp_c is not None else None,
        "published_unix": time.time() if now is None else now,
    }
    try:
        publish_json(transport, HOST_HEALTH_KEY, payload)
    except Exception as exc:
        logger.warning("failed to publish host health: %s", exc)


def _parse(obj) -> dict:
    temp = obj["temp_c"]
    return {
        "hostname": str(obj["hostname"]),
        "temp_c": float(temp) if temp is not None else None,
        "published_unix": float(obj["published_unix"]),
    }


def read(transport, *, now: Optional[float] = None) -> dict:
    """Fetch the latest snapshot with a derived ``seconds_since_publish``.

    A missing key, a Redis transport error, or a malformed payload all
    resolve to the empty-sentinel dict — the dashboard renders a grey
    "unknown" tile rather than failing.
    """
    t_now = time.time() if now is None else now
    out = read_json(
        transport,
        HOST_HEALTH_KEY,
        label="host health",
        logger=logger,
        parse=_parse,
    )
    if out is None:
        return dict(_EMPTY)
    out["seconds_since_publish"] = max(0.0, t_now - out["published_unix"])
    return out
