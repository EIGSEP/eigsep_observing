"""Cross-process heartbeat for SNAP ``--reinit`` events.

Published by ``eigsep_observing.scripts.fpga_init`` (the
``eigsep-fpga-init`` console script) after a successful ``--reinit``
init, consumed by ``LiveStatusAggregator``. Mirrors
:mod:`eigsep_observing.file_heartbeat`: a single Redis key holds a
small JSON blob, overwritten on each publish — consumers only ever
care about the latest count and timestamp.

Operationally this surfaces SNAP recovery activity on the live-status
dashboard. The supervisor (``deploy/systemd/eigsep-observe.service``)
restarts ``eigsep-fpga-init --reinit -p`` on hardware failure; each
successful re-init bumps the counter, so the operator can see at a
glance whether the SNAP has been thermal-cycling without checking
``journalctl``.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from ._redis_json_kv import publish_json, read_json

logger = logging.getLogger(__name__)

REINIT_KEY = "eigsep:snap_reinit"

_EMPTY = {
    "count": None,
    "last_reinit_unix": None,
    "seconds_since_reinit": None,
}


def _parse(obj) -> dict:
    return {
        "count": int(obj["count"]),
        "last_reinit_unix": float(obj["last_reinit_unix"]),
    }


def publish(transport) -> None:
    """Bump the reinit count and stamp ``now`` as ``last_reinit_unix``.

    Read-modify-write on a single JSON blob. Any exception is logged
    at WARNING and swallowed: the heartbeat is observability, not
    correctness — losing a count must not block a successful init.

    Production has a single writer (the systemd-managed
    ``eigsep-fpga-init``), so the read-modify-write is not racy in
    practice; concurrent publishers would only lose a count or two.
    """
    try:
        count = read_json(
            transport,
            REINIT_KEY,
            label="snap reinit heartbeat",
            logger=logger,
            parse=lambda obj: int(obj.get("count", 0)),
        )
        if count is None:
            count = 0  # absent or malformed payload: reset the count
        publish_json(
            transport,
            REINIT_KEY,
            {"count": count + 1, "last_reinit_unix": time.time()},
        )
    except Exception as exc:
        logger.warning("failed to publish snap reinit heartbeat: %s", exc)


def read(transport, *, now: Optional[float] = None) -> dict:
    """Fetch the latest reinit count and derived age.

    Mirrors :func:`eigsep_observing.file_heartbeat.read`: a missing
    key, transport error, or malformed payload all resolve to the
    empty-sentinel dict. The dashboard classifies that as "unknown"
    rather than failing.
    """
    t_now = time.time() if now is None else now
    out = read_json(
        transport,
        REINIT_KEY,
        label="snap reinit heartbeat",
        logger=logger,
        parse=_parse,
    )
    if out is None:
        return dict(_EMPTY)
    return {
        "count": out["count"],
        "last_reinit_unix": out["last_reinit_unix"],
        "seconds_since_reinit": max(0.0, t_now - out["last_reinit_unix"]),
    }
