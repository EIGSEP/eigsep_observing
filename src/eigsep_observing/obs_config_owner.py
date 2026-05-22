"""Persistent record of who last uploaded the panda obs_config.

Mirrors :mod:`eigsep_observing.run_tag` in shape — a single Redis key
holds a small JSON blob, overwritten on each publish — but with one
critical lifecycle difference: there is no ``clear``. The owner record
represents "the last script that legitimately uploaded an obs_config",
and the cfg it uploaded persists in Redis after the script exits. A
``clear`` on exit would falsely declare the cfg unowned even though
its content is still authoritative.

Published by the three uploader scripts (``eigsep-panda``,
``scripts/no_switch_observation.py``, ``scripts/vna_position_sweep.py``)
immediately after their ``ConfigStore.upload(cfg)`` call. Consumed by
``EigObserver._with_header_overlays`` and ``PandaClient.measure_s11``
so corr- and VNA-file headers carry the cfg provenance alongside the
cfg block itself.

Downstream trust checks:

- ``header["obs_config_owner"] != "UNKNOWN"`` — necessary condition:
  someone with authority uploaded the cfg at some point.
- ``header["run_tag"] == header["obs_config_owner"]`` — stronger
  condition: the active driver is the cfg owner, so the cfg block in
  the same header reflects what is running right now (not a
  bring-up script's transient view).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

OBS_CONFIG_OWNER_KEY = "eigsep:obs_config_owner"

_EMPTY = {
    "owner": None,
    "uploaded_at_unix": None,
}


def publish_owner(
    transport, owner: str, *, uploaded_at_unix: Optional[float] = None
) -> None:
    """Stamp the cfg uploader's identity into Redis.

    Any exception is logged at WARNING and swallowed. The owner record
    is provenance, not correctness — losing it must never block the
    script's main work.
    """
    if uploaded_at_unix is None:
        t = time.time()
    else:
        try:
            t = float(uploaded_at_unix)
        except (ValueError, TypeError) as exc:
            t = 0.0
            logger.warning(
                f"invalid uploaded_at_unix {uploaded_at_unix!r} for "
                f"obs_config_owner {owner!r}: {exc}; using 0.0."
            )
    try:
        payload = json.dumps({"owner": str(owner), "uploaded_at_unix": t})
        transport.add_raw(OBS_CONFIG_OWNER_KEY, payload)
    except Exception as exc:
        logger.warning("failed to publish obs_config_owner: %s", exc)


def read_owner(transport) -> dict:
    """Fetch the latest cfg-owner record.

    A missing key, transport error, malformed payload, or null payload
    all resolve to the empty-sentinel dict ``{"owner": None,
    "uploaded_at_unix": None}``. Consumers should treat ``owner is
    None`` as a misconfiguration signal (no authorized uploader has
    seeded Redis yet), not the steady-state default.
    """
    try:
        raw = transport.get_raw(OBS_CONFIG_OWNER_KEY)
    except Exception as exc:
        logger.warning("failed to read obs_config_owner: %s", exc)
        return dict(_EMPTY)
    if raw is None:
        return dict(_EMPTY)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        obj = json.loads(raw)
        owner = obj["owner"]
        uploaded = obj["uploaded_at_unix"]
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        logger.warning("malformed obs_config_owner payload %r: %s", raw, exc)
        return dict(_EMPTY)
    if owner is None and uploaded is None:
        return dict(_EMPTY)
    if owner is None or uploaded is None:
        logger.warning(
            "partial obs_config_owner payload (%r, %r); "
            "returning empty sentinel",
            owner,
            uploaded,
        )
        return dict(_EMPTY)
    return {"owner": str(owner), "uploaded_at_unix": float(uploaded)}
