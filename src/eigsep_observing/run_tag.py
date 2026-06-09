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

``session`` auto-reclaims a stale tag whose holder is *provably dead*.
An unclean shutdown (power loss, ``SIGKILL``, a hard reboot mid-run)
skips the ``finally`` clear, stranding a tag that would otherwise make
the next driver script refuse to start forever. Because every publisher
is a panda-side script sharing the panda's localhost Redis, the holder
and the script checking the lock are always co-located, so the staleness
check is a local liveness probe: a recorded ``boot_id`` differing from
the current one (the machine rebooted since the holder started) or a
recorded ``pid`` that no longer exists both prove the holder is gone, and
``session`` overwrites the stale tag with a WARNING instead of refusing.
The probe is conservative — a holder on another host, or one that cannot
otherwise be probed, is assumed alive and the refusal stands.
``scripts/clear_run_tag.py`` is the manual fallback for those
unverifiable cases.
"""

from __future__ import annotations

import logging
import os
import socket
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
def _boot_id() -> Optional[str]:
    """Return this machine's boot id, or ``None`` if unavailable.

    On Linux ``/proc/sys/kernel/random/boot_id`` is a UUID regenerated on
    every boot. A recorded boot_id that differs from the current one
    proves the recording process did not survive a reboot — and that PID
    reuse after the reboot makes a PID probe untrustworthy, so the boot_id
    comparison takes precedence. Returns ``None`` on platforms without the
    file (the liveness check then leans on the PID probe alone).
    """
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip()
    except OSError:
        return None


def _pid_alive(pid) -> bool:
    """True if a process with ``pid`` currently exists on this machine."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists but owned by another user
    except OSError:
        return False
    return True


def _holder_is_dead(transport) -> bool:
    """True if the current run_tag holder is provably dead.

    Reads the raw payload for the holder's ``pid`` / ``hostname`` /
    ``boot_id`` and applies a conservative liveness probe. Returns
    ``False`` whenever liveness cannot be established — a holder we cannot
    probe (different host, missing metadata, unreadable payload) is
    assumed alive so a live lock is never stolen. Callers invoke this only
    after :func:`read` has reported a different, non-empty holder.
    """
    try:
        raw = transport.get_raw(RUN_TAG_KEY)
    except Exception:
        return False
    if raw is None:
        return False
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        obj = json.loads(raw)
    except (ValueError, json.JSONDecodeError):
        return False
    if not isinstance(obj, dict):
        return False
    if obj.get("hostname") != socket.gethostname():
        return False
    boot = obj.get("boot_id")
    my_boot = _boot_id()
    if boot is not None and my_boot is not None and boot != my_boot:
        return True
    pid = obj.get("pid")
    if pid is None:
        return False
    return not _pid_alive(pid)


def publish(transport, tag: str, started_unix: Optional[float] = None) -> None:
    """Stamp the active script's tag into Redis.

    Logs a WARNING if an existing non-empty tag with a different name —
    whose holder is still *alive* — is being overwritten, catching the
    "two driver scripts started concurrently" race that ``session``'s
    pre-publish refuse check can lose. Overwriting a provably-dead holder
    is a deliberate stale-lock reclaim (``session`` logs that separately),
    so it does not trip this warning. The publish still proceeds either
    way (provenance is best-effort); the WARNING is the second-line audit
    trail.

    Any exception is logged at WARNING and swallowed. ``run_tag`` is
    provenance, not correctness — losing it must never block the
    script's main work.
    """
    existing = read(transport)
    if (
        existing["run_tag"] is not None
        and existing["run_tag"] != str(tag)
        and not _holder_is_dead(transport)
    ):
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
        payload = {
            "run_tag": str(tag),
            "run_started_at_unix": t,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "boot_id": _boot_id(),
        }
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

    If a different non-empty tag is already published, refuses to enter
    (raises ``RuntimeError``) unless that holder is *provably dead*
    (:func:`_holder_is_dead`) — a tag stranded by an unclean shutdown is
    reclaimed with a WARNING instead. On exit, clears only if our tag is
    still the active one — a later script that overwrote it (in violation
    of the refuse-on-conflict policy) is not trampled.

    The pre-publish refuse check is best-effort: there is no atomic
    compare-and-set between ``read`` and ``publish``, so two scripts
    starting in the same millisecond can both pass the check. The
    publish-time overwrite WARNING in :func:`publish` surfaces the
    collision in that case.
    """
    existing = read(transport)
    held = existing["run_tag"]
    if held not in (None, str(tag)):
        if _holder_is_dead(transport):
            logger.warning(
                "Reclaiming stale run_tag=%r left by an unclean shutdown "
                "(holder process is gone); starting %r.",
                held,
                str(tag),
            )
        else:
            raise RuntimeError(
                f"Another driver script is already publishing run_tag="
                f"{held!r}; refusing to start {str(tag)!r}. Stop the other "
                f"script first, or clear a stale lock with "
                f"scripts/clear_run_tag.py."
            )
    publish(transport, tag)
    try:
        yield
    finally:
        current = read(transport)
        if current["run_tag"] == str(tag):
            clear(transport)
