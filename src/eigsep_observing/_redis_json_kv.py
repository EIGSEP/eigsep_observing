"""Shared publish/read shape for the single-key Redis JSON K/V modules.

Six sibling modules ride this shape — :mod:`~eigsep_observing.run_tag`,
:mod:`~eigsep_observing.obs_config_owner`,
:mod:`~eigsep_observing.file_heartbeat`,
:mod:`~eigsep_observing.snap_reinit`,
:mod:`~eigsep_observing.corr_health`,
:mod:`~eigsep_observing.imu_calibration` (read-only; picohost writes it):
a single Redis key holds a small JSON blob, overwritten on each publish
via ``transport.add_raw`` / ``transport.get_raw`` — consumers only ever
care about the most recent value. This module is the single home for the
serialize/deserialize path so a parsing fix propagates to all siblings at
once (issue #149).

What stays per-module: the ``*_KEY`` constant, the ``_EMPTY`` sentinel,
lifecycle semantics (``run_tag``'s ``clear``/``session``,
``obs_config_owner``'s deliberate lack of ``clear``, ``snap_reinit``'s
read-modify-write counter), derived fields (``seconds_since_*``), and
publish failure policy — :func:`publish_json` always raises, and each
sibling decides whether to swallow+WARN (four do) or propagate to the
caller (``corr_health``).
"""

from __future__ import annotations

import json
from typing import Any, Callable, Optional


def publish_json(transport, key: str, payload: dict) -> None:
    """Serialize ``payload`` and overwrite ``key``.

    Raises on serialization or transport error — the caller owns the
    failure policy (swallow+WARN for provenance/heartbeat keys, raise
    for ``corr_health`` whose caller disables on first failure).
    """
    transport.add_raw(key, json.dumps(payload))


def read_json(
    transport,
    key: str,
    *,
    label: str,
    logger,
    parse: Callable[[Any], Any],
) -> Optional[Any]:
    """Fetch ``key``, decode, ``json.loads``, and apply ``parse``.

    Returns ``parse``'s result, or ``None`` for every failure mode so
    each sibling maps it to its own ``_EMPTY`` sentinel: missing key
    (silent), transport error (``"failed to read {label}"`` WARNING),
    malformed JSON or a ``parse`` raising ``ValueError`` / ``TypeError``
    / ``KeyError`` (``"malformed {label} payload"`` WARNING with the
    raw payload).

    ``parse`` must do all field extraction and type coercion — running
    it inside the try-block is what guarantees a junk payload degrades
    to the sentinel instead of raising out of the sibling's ``read``.
    Warnings go to the caller's ``logger`` so per-module log
    provenance survives the extraction.
    """
    try:
        raw = transport.get_raw(key)
    except Exception as exc:
        logger.warning("failed to read %s: %s", label, exc)
        return None
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        return parse(json.loads(raw))
    except (ValueError, TypeError, KeyError) as exc:
        logger.warning("malformed %s payload %r: %s", label, raw, exc)
        return None
