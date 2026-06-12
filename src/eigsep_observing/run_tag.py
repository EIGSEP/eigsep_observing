"""Cross-process tag identifying which panda script is driving the run.

Published by the panda-side entry-points
(``eigsep-panda`` console script,
``src/eigsep_observing/scripts/no_switch_observation.py``,
``src/eigsep_observing/scripts/vna_position_sweep.py``) when they
start, cleared in their ``finally`` blocks. Consumed by ``EigObserver.record_corr_data`` (corr
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

import logging
import time
from contextlib import contextmanager
from typing import Optional

from ._redis_json_kv import publish_json, read_json

logger = logging.getLogger(__name__)

RUN_TAG_KEY = "eigsep:run_tag"

_EMPTY = {
    "run_tag": None,
    "run_started_at_unix": None,
}


def _parse(obj) -> dict:
    tag = obj["run_tag"]
    started = obj["run_started_at_unix"]
    return {
        "run_tag": str(tag) if tag is not None else None,
        "run_started_at_unix": float(started) if started is not None else None,
    }


def publish(transport, tag: str, started_unix: Optional[float] = None) -> None:
    """Stamp the active script's tag into Redis.

    Logs a WARNING if an existing non-empty tag with a different name
    is being overwritten — catches the "two driver scripts started
    concurrently" race that ``session``'s pre-publish refuse check
    can lose. The publish still proceeds (provenance is best-effort);
    the WARNING is the second-line audit trail.

    Any exception is logged at WARNING and swallowed. ``run_tag`` is
    provenance, not correctness — losing it must never block the
    script's main work.
    """
    existing = read(transport)
    if existing["run_tag"] is not None and existing["run_tag"] != str(tag):
        logger.warning(
            "run_tag %r is overwriting existing %r — two driver scripts "
            "may be running concurrently.",
            str(tag),
            existing["run_tag"],
        )
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
        payload = {"run_tag": str(tag), "run_started_at_unix": t}
        publish_json(transport, RUN_TAG_KEY, payload)
    except Exception as exc:
        logger.warning("failed to publish run_tag: %s", exc)


def clear(transport) -> None:
    """Overwrite the run_tag key with a null payload.

    Transport doesn't expose ``delete``, so ``clear`` writes the
    same shape as ``read``'s empty sentinel. ``read`` handles both
    "key absent" and "null payload" identically.
    """
    try:
        publish_json(transport, RUN_TAG_KEY, dict(_EMPTY))
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
    out = read_json(
        transport, RUN_TAG_KEY, label="run_tag", logger=logger, parse=_parse
    )
    if out is None:
        return dict(_EMPTY)
    tag = out["run_tag"]
    started = out["run_started_at_unix"]
    if tag is None and started is None:
        return dict(_EMPTY)
    if tag is None or started is None:
        logger.warning(
            "partial run_tag payload (%r, %r); returning empty sentinel",
            tag,
            started,
        )
        return dict(_EMPTY)
    return out


@contextmanager
def session(transport, tag: str):
    """Publish ``tag`` for the duration of the with-block.

    Refuses to enter if a different non-empty tag is already published
    (raises ``RuntimeError``). On exit, clears only if our tag is still
    the active one — a later script that overwrote it (in violation of
    the refuse-on-conflict policy) is not trampled.

    The pre-publish refuse check is best-effort: there is no atomic
    compare-and-set between ``read`` and ``publish``, so two scripts
    starting in the same millisecond can both pass the check. The
    publish-time overwrite WARNING in :func:`publish` surfaces the
    collision in that case.
    """
    existing = read(transport)
    if existing["run_tag"] not in (None, str(tag)):
        raise RuntimeError(
            f"Another driver script is already publishing run_tag="
            f"{existing['run_tag']!r}; refusing to start {str(tag)!r}. "
            f"Stop the other script first."
        )
    publish(transport, tag)
    try:
        yield
    finally:
        current = read(transport)
        if current["run_tag"] == str(tag):
            clear(transport)
