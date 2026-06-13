r"""
Inspect and clear a stale ``run_tag`` lock.

A driver script (``motor_manual.py``, ``motor_control.py``,
``record_vna.py``, ...) stamps ``eigsep:run_tag`` into Redis for the
duration of its run via :func:`eigsep_observing.run_tag.session`, and
clears it in a ``finally`` block on exit. An unclean shutdown -- power
loss, ``SIGKILL``, a hard reboot mid-run -- skips that ``finally``,
leaving a stale tag that makes the *next* driver script refuse to start::

    RuntimeError: Another driver script is already publishing
    run_tag='motor_manual'; refusing to start 'motor_control'.
    Stop the other script first.

``run_tag.session`` already auto-reclaims a stale tag whose holder is
*provably dead* (its process is gone or the machine rebooted since it
started), so the common crash-on-the-same-panda case recovers with no
operator action. This tool is the manual fallback for the cases
``session`` deliberately will not reclaim -- a holder it cannot probe
(e.g. one recorded on a different host) -- and for an operator who simply
wants to inspect or force-clear the tag.

It shows the currently-held tag (and how long ago it started), then
clears it on confirmation -- the documented one-liner recovery so an
operator never has to hand-write a ``python -c`` snippet in the field.

Before clearing, confirm no driver script is *actually* running -- the
lock exists to stop two scripts driving the same hardware at once, and it
cannot tell "crashed and left a tag" apart from "still running"::

    ps aux | grep -E "_manual\.py|motor_control|record_" | grep -v grep

If something shows up, stop *that* script instead of clearing the lock;
the lock is doing its job. If nothing shows up, the tag is stale -- clear
it and restart your script.
"""

from argparse import ArgumentParser
import logging
import time

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import build_transport_bare
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


def _describe(state):
    """Human-readable summary of a ``run_tag.read`` result."""
    tag = state["run_tag"]
    if tag is None:
        return "run_tag is empty -- nothing to clear."
    started = state["run_started_at_unix"]
    try:
        age = time.time() - float(started)
        age_str = f"started {age:.0f}s ago"
    except (TypeError, ValueError):
        age_str = "unknown start time"
    return f"run_tag is held by {tag!r} ({age_str})."


def main(transport, args):
    state = run_tag.read(transport)
    logger.info(_describe(state))
    if state["run_tag"] is None:
        return

    if not args.yes:
        prompt = f"Clear stale run_tag={state['run_tag']!r}? [y/N] "
        if input(prompt).strip().lower() not in ("y", "yes"):
            logger.info("Aborted; run_tag left untouched.")
            return

    run_tag.clear(transport)
    after = run_tag.read(transport)
    if after["run_tag"] is None:
        logger.info("Cleared. You can start your driver script now.")
    else:
        logger.error(
            "Clear did not take effect; run_tag still %r. "
            "Is another script re-publishing it right now?",
            after["run_tag"],
        )


def _parse_args():
    parser = ArgumentParser(
        description="Inspect and clear a stale run_tag lock left by a "
        "crashed/killed driver script."
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt (non-interactive clear).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    transport = build_transport_bare(False)
    main(transport, args)
