"""Cross-process heartbeat for corr file writes.

Published by ``EigObserver.record_corr_data`` on the SNAP-side
``Transport`` after each successful HDF5 rename, consumed by
``LiveStatusAggregator``. The live-status dashboard runs on a separate
computer wired into the field network switch (same as the existing
liveplotter), so a filesystem probe on ``corr_save_dir`` would not
see the writer's disk. Redis is the shared surface both sides already
talk to.

This is a small K/V heartbeat, not a per-bus stream, so it lives
directly on ``Transport.add_raw`` / ``Transport.get_raw`` rather than
as a new writer/reader class. The single key is overwritten on every
publish — consumers only ever care about the most recent write.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)

FILE_HEARTBEAT_KEY = "eigsep:last_corr_file_write"

_EMPTY = {
    "newest_h5_path": None,
    "mtime_unix": None,
    "seconds_since_write": None,
}


def publish(transport, path: Union[str, Path], mtime_unix: float) -> None:
    """Stamp a file-write heartbeat into Redis.

    Any exception is logged at WARNING and swallowed so a transient
    Redis issue never blocks the corr-write path. The file has already
    landed on disk by the time this runs; losing a heartbeat only
    costs a dashboard tile for one cycle.
    """
    payload = json.dumps({"path": str(path), "mtime_unix": float(mtime_unix)})
    try:
        transport.add_raw(FILE_HEARTBEAT_KEY, payload)
    except Exception as exc:
        logger.warning("failed to publish file heartbeat: %s", exc)


def read(transport, *, now: Optional[float] = None) -> dict:
    """Fetch the latest heartbeat with a derived ``seconds_since_write``.

    Returns the same ``{newest_h5_path, mtime_unix, seconds_since_write}``
    shape as the legacy filesystem probe so the aggregator and
    front-end don't need to change. A missing key, a Redis transport
    error, or a malformed payload all resolve to the empty-sentinel
    dict — the dashboard classifies that as "unknown" rather than
    failing.
    """
    t_now = time.time() if now is None else now
    try:
        raw = transport.get_raw(FILE_HEARTBEAT_KEY)
    except Exception as exc:
        logger.warning("failed to read file heartbeat: %s", exc)
        return dict(_EMPTY)
    if raw is None:
        return dict(_EMPTY)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        obj = json.loads(raw)
        path = obj["path"]
        mtime = float(obj["mtime_unix"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        logger.warning("malformed file heartbeat payload %r: %s", raw, exc)
        return dict(_EMPTY)
    return {
        "newest_h5_path": path,
        "mtime_unix": mtime,
        "seconds_since_write": max(0.0, t_now - mtime),
    }
