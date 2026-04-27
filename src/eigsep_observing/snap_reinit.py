"""Cross-process heartbeat for SNAP ``--reinit`` events.

Published by ``scripts/fpga_init.py`` after a successful ``--reinit``
init, consumed by ``LiveStatusAggregator``. Mirrors
:mod:`eigsep_observing.file_heartbeat`: a single Redis key holds a
small JSON blob, overwritten on each publish — consumers only ever
care about the latest count and timestamp.

Operationally this surfaces SNAP recovery activity on the live-status
dashboard. The supervisor (``deploy/systemd/eigsep-observe.service``)
restarts ``fpga_init.py --reinit -p`` on hardware failure; each
successful re-init bumps the counter, so the operator can see at a
glance whether the SNAP has been thermal-cycling without checking
``journalctl``.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

REINIT_KEY = "eigsep:snap_reinit"

_EMPTY = {
    "count": None,
    "last_reinit_unix": None,
    "seconds_since_reinit": None,
}


def publish(transport) -> None:
    """Bump the reinit count and stamp ``now`` as ``last_reinit_unix``.

    Read-modify-write on a single JSON blob. Any exception is logged
    at WARNING and swallowed: the heartbeat is observability, not
    correctness — losing a count must not block a successful init.

    Production has a single writer (the systemd-managed
    ``fpga_init.py``), so the read-modify-write is not racy in
    practice; concurrent publishers would only lose a count or two.
    """
    try:
        raw = transport.get_raw(REINIT_KEY)
        count = 0
        if raw is not None:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            try:
                count = int(json.loads(raw).get("count", 0))
            except (
                ValueError,
                TypeError,
                KeyError,
                json.JSONDecodeError,
            ) as exc:
                logger.warning(
                    "malformed snap reinit payload %r: %s; resetting count",
                    raw,
                    exc,
                )
                count = 0
        payload = json.dumps(
            {"count": count + 1, "last_reinit_unix": time.time()}
        )
        transport.add_raw(REINIT_KEY, payload)
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
    try:
        raw = transport.get_raw(REINIT_KEY)
    except Exception as exc:
        logger.warning("failed to read snap reinit heartbeat: %s", exc)
        return dict(_EMPTY)
    if raw is None:
        return dict(_EMPTY)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        obj = json.loads(raw)
        count = int(obj["count"])
        last_unix = float(obj["last_reinit_unix"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        logger.warning("malformed snap reinit payload %r: %s", raw, exc)
        return dict(_EMPTY)
    return {
        "count": count,
        "last_reinit_unix": last_unix,
        "seconds_since_reinit": max(0.0, t_now - last_unix),
    }
