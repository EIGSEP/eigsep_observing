"""Corr-loop health diagnostic K/V.

Published by ``EigsepFpga._publish_corr_health`` (on the throttled
``diagnostics_period_s`` thread) on the SNAP-side ``Transport``,
consumed by ``LiveStatusAggregator``. Two fields:

- ``dropped_integrations``: cumulative count of integrations the host
  failed to read before the FPGA overwrote the BRAM, since the observe
  loop started. Monotonic non-decreasing.
- ``readout_time_ms``: the most recent ``read_data`` wall-time in
  milliseconds (``None`` until the first readout completes). Watch this
  approach the integration time as ``corr_acc_len`` is lowered — the
  headroom between them is the drop budget.

This is deliberately a plain K/V, *not* a metadata-bus stream: anything
on the metadata bus is drained by the corr loop into the HDF5 file
path, which would pull in a ``SENSOR_SCHEMAS`` entry, a
producer-contract emulator, and the averaging pipeline. The file
already records every drop losslessly — each row carries its
``acc_cnt``, so a dropped integration is a visible gap offline. The
live dashboard is the only consumer that needs this surface, so it
rides the same small-K/V pattern as ``file_heartbeat`` /
``snap_reinit``. The single key is overwritten on every publish —
consumers only ever care about the most recent write.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from ._redis_json_kv import publish_json, read_json

logger = logging.getLogger(__name__)

CORR_HEALTH_KEY = "eigsep:corr_health"

_EMPTY = {
    "dropped_integrations": None,
    "readout_time_ms": None,
    "published_unix": None,
    "seconds_since_publish": None,
}


def _parse(obj) -> dict:
    readout = obj["readout_time_ms"]
    return {
        "dropped_integrations": int(obj["dropped_integrations"]),
        "readout_time_ms": float(readout) if readout is not None else None,
        "published_unix": float(obj["published_unix"]),
    }


def publish(
    transport,
    *,
    dropped_integrations: int,
    readout_time_ms: Optional[float],
    now: Optional[float] = None,
) -> None:
    """Stamp the corr-loop health snapshot into Redis.

    Unlike ``file_heartbeat.publish`` this does NOT swallow transport
    errors: it is called at the diagnostics cadence (~1 Hz), so a dead
    Redis would warn-spam the journal. The caller (``EigsepFpga``) owns
    the failure policy — disable-on-first-failure with a single ERROR.
    """
    payload = {
        "dropped_integrations": int(dropped_integrations),
        "readout_time_ms": (
            float(readout_time_ms) if readout_time_ms is not None else None
        ),
        "published_unix": time.time() if now is None else now,
    }
    publish_json(transport, CORR_HEALTH_KEY, payload)


def read(transport, *, now: Optional[float] = None) -> dict:
    """Fetch the latest snapshot with a derived ``seconds_since_publish``.

    A missing key, a Redis transport error, or a malformed payload all
    resolve to the empty-sentinel dict — the dashboard renders the bare
    corr-loop tile (no drop/readout suffixes) rather than failing.
    """
    t_now = time.time() if now is None else now
    out = read_json(
        transport,
        CORR_HEALTH_KEY,
        label="corr health",
        logger=logger,
        parse=_parse,
    )
    if out is None:
        return dict(_EMPTY)
    out["seconds_since_publish"] = max(0.0, t_now - out["published_unix"])
    return out
