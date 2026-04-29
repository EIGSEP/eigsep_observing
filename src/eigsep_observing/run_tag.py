"""Cross-process tag identifying which panda script is driving the run.

Published by the panda-side entry-points
(``scripts/panda_observe.py``, ``scripts/no_switch_observation.py``,
``scripts/vna_position_sweep.py``) when they start, cleared in their
``finally`` blocks. Consumed by ``EigObserver.record_corr_data`` (corr
file headers) and ``PandaClient.measure_s11`` (VNA file headers) so
the active script's identity is captured per-file for offline
provenance.

Mirrors :mod:`eigsep_observing.file_heartbeat` and
:mod:`eigsep_observing.snap_reinit`: a single Redis key holds a small
JSON blob, overwritten on each publish — consumers only ever care
about the most recent value. ``clear`` overwrites with a null payload
rather than deleting the key (Transport doesn't expose ``delete``).

Steady-state runs publish ``"panda_observe"`` so downstream's
``run_tag`` field is uniformly populated by exactly one of the three
scripts that own the panda. A ``None`` then signals a misconfiguration
(broken publish, panda not running, transport unavailable) rather than
the default for normal operations.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

RUN_TAG_KEY = "eigsep:run_tag"

_EMPTY = {
    "run_tag": None,
    "run_started_at_unix": None,
}


def publish(transport, tag: str, started_unix: Optional[float] = None) -> None:
    """Stamp the active script's tag into Redis.

    Any exception is logged at WARNING and swallowed. ``run_tag`` is
    provenance, not correctness — losing it must never block the
    script's main work.
    """
    if started_unix is None:
        t = time.time()
    else:
        try:
            t = float(started_unix)
        except (ValueError, TypeError) as exc:
            t = 0.0
            logger.warning(
                f"invalid started_unix {started_unix!r} for run_tag {tag!r}: "
                f"{exc}; using 0.0."
            )
    try:
        payload = json.dumps({"run_tag": str(tag), "run_started_at_unix": t})
        transport.add_raw(RUN_TAG_KEY, payload)
    except Exception as exc:
        logger.warning("failed to publish run_tag: %s", exc)


def clear(transport) -> None:
    """Overwrite the run_tag key with a null payload.

    Transport doesn't expose ``delete``, so ``clear`` writes the
    same shape as ``read``'s empty sentinel. ``read`` handles both
    "key absent" and "null payload" identically.
    """
    payload = json.dumps(dict(_EMPTY))
    try:
        transport.add_raw(RUN_TAG_KEY, payload)
    except Exception as exc:
        logger.warning("failed to clear run_tag: %s", exc)


def read(transport) -> dict:
    """Fetch the latest run_tag.

    A missing key, transport error, malformed payload, or null
    payload all resolve to the empty-sentinel dict ``{"run_tag":
    None, "run_started_at_unix": None}``. Consumers should treat
    ``run_tag is None`` as a misconfiguration signal, not the
    steady-state default.
    """
    try:
        raw = transport.get_raw(RUN_TAG_KEY)
    except Exception as exc:
        logger.warning("failed to read run_tag: %s", exc)
        return dict(_EMPTY)
    if raw is None:
        return dict(_EMPTY)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        obj = json.loads(raw)
        tag = obj["run_tag"]
        started = obj["run_started_at_unix"]
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        logger.warning("malformed run_tag payload %r: %s", raw, exc)
        return dict(_EMPTY)
    if tag is None and started is None:
        return dict(_EMPTY)
    if tag is None or started is None:
        logger.warning(
            "partial run_tag payload (%r, %r); returning empty sentinel",
            tag,
            started,
        )
        return dict(_EMPTY)
    return {"run_tag": str(tag), "run_started_at_unix": float(started)}
