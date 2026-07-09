"""
Az/el scan script.

Runs a beam scan through ``PicoManager`` via Redis — no direct serial
to the motor pico, so the manager service stays up throughout. Log
start/end boundaries to the shared status stream so the ground
observer sees when a scan is active.

The scan grid is configured per axis from the command line via
``--az_start/--az_stop/--az_step`` and the ``--el_*`` equivalents
(degrees). Bounds are inclusive of the stop endpoint, so e.g.
``--az_start 0 --az_stop 180 --az_step 5`` sweeps 0..180 in 5 deg steps.

``--axis`` selects which axis is swept: ``both`` (default) runs the
full 2-D grid, while ``az`` or ``el`` sweep a single axis and hold the
other fixed at ``--el`` / ``--az`` respectively (default 0). In a
single-axis scan the held axis's ``--*_start/stop/step`` flags are
ignored.
"""

from argparse import ArgumentParser
import logging
import time

import numpy as np
from eigsep_redis import StatusWriter
from picohost.proxy import PicoProxy

from eigsep_observing import MotorClient, run_tag
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport,
    require_pico,
)
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


def _axis_range(start, stop, step):
    """Build one scan axis from CLI bounds.

    Grid points run from ``start`` to ``stop`` spaced by ``step``
    degrees, *including* the ``stop`` endpoint when it lands on the step
    grid (so ``0 -> 180`` reaches 180). A ``stop`` that doesn't land on
    the grid stops at the last point ``<= stop`` rather than
    overshooting the mechanical boundary. Direction is always ascending;
    the serpentine traversal in ``MotorClient.scan`` handles sweep
    direction, so an inverted ``stop < start`` is a user error.
    """
    if step <= 0:
        raise ValueError(f"step must be positive, got {step}")
    if stop < start:
        raise ValueError(f"stop ({stop}) must be >= start ({start})")
    # +step/2 epsilon so an on-grid stop survives float drift.
    return np.arange(start, stop + step / 2.0, step)


def _build_grid(args):
    """Resolve ``(az_range, el_range, el_first)`` from CLI args.

    For ``--axis both`` the full 2-D grid is built from both sets of
    bounds and ``--el_first`` is honored. For a single-axis scan the
    swept axis is built from its ``--<axis>_start/stop/step`` bounds
    while the other axis collapses to a single hold point (``--el`` for
    an az scan, ``--az`` for an el scan). ``el_first`` is then forced so
    the swept axis is the inner (fast) loop, which preserves the
    ``pause_s`` sweep semantics; the held axis's bound/step flags are
    ignored. Bad bounds still raise from ``_axis_range`` before any
    hardware is touched.
    """
    if args.axis == "az":
        az_range = _axis_range(args.az_start, args.az_stop, args.az_step)
        el_range = np.array([args.el], dtype=float)
        return az_range, el_range, False
    if args.axis == "el":
        el_range = _axis_range(args.el_start, args.el_stop, args.el_step)
        az_range = np.array([args.az], dtype=float)
        return az_range, el_range, True
    az_range = _axis_range(args.az_start, args.az_stop, args.az_step)
    el_range = _axis_range(args.el_start, args.el_stop, args.el_step)
    return az_range, el_range, args.el_first


def _describe_grid(args, az_range, el_range):
    """One-line human summary of the resolved scan grid for the log."""
    az = (
        f"az {args.az_start:g}..{args.az_stop:g} step {args.az_step:g} "
        f"({len(az_range)} pts)"
    )
    el = (
        f"el {args.el_start:g}..{args.el_stop:g} step {args.el_step:g} "
        f"({len(el_range)} pts)"
    )
    if args.axis == "az":
        return f"{az}, el held at {args.el:g}"
    if args.axis == "el":
        return f"{el}, az held at {args.az:g}"
    return f"{az}, {el}"


def _prompt_go_home(motor):
    """On a Ctrl-C interrupt, offer to drive back to step (0, 0).

    Defaults to *No* so an interrupt leaves the motors halted in place
    (the safe abort) unless the operator explicitly opts in. A
    non-interactive stdin (``EOFError``) or a second Ctrl-C
    (``KeyboardInterrupt``) is treated as No. When confirmed, the
    open-loop ``MotorClient.home`` primitive drives back to the scan
    origin (step 0,0 — the counter zero the scan pattern is relative
    to, not the cal-defined home); a Ctrl-C during that move aborts it.
    """
    try:
        answer = input("Go to scan origin (0,0)? [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        logger.info("Leaving motors in place.")
        return
    if answer not in ("y", "yes"):
        logger.info("Leaving motors in place.")
        return
    logger.info("Returning to home (0, 0)...")
    try:
        motor.home()
    except KeyboardInterrupt:
        logger.info("Home move aborted.")
    except (TimeoutError, RuntimeError) as exc:
        logger.error("Home move failed: %s", exc)


def main(transport, args):
    # Build (and validate) the grid before touching hardware so bad
    # bounds fail fast without opening a run_tag session.
    az_range, el_range, el_first = _build_grid(args)
    require_pico(PicoProxy("motor", transport, source="motor_scan"))
    with run_tag.session(transport, "motor_scan"):
        status = StatusWriter(transport)
        motor = MotorClient(transport)

        started = time.monotonic()
        status.send("motor_scan started")
        logger.info("motor_scan started")
        logger.info("Scan grid: %s", _describe_grid(args, az_range, el_range))

        try:
            motor.set_delay()
            motor.halt()
            motor.scan(
                az_range_deg=az_range,
                el_range_deg=el_range,
                el_first=el_first,
                repeat_count=args.count,
                pause_s=args.pause_s,
                sleep_between=args.sleep_s,
            )
        except KeyboardInterrupt:
            logger.info("Scan interrupted by user")
            _prompt_go_home(motor)
        except (TimeoutError, RuntimeError) as exc:
            logger.error("Motor scan aborted: %s", exc)
        finally:
            motor.halt()
            elapsed = time.monotonic() - started
            msg = f"motor_scan ended (duration={elapsed:.1f}s)"
            status.send(msg)
            logger.info(msg)


def _parse_args():
    parser = ArgumentParser(description="Run az/el motor scan via PicoManager")
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient",
    )
    add_redis_args(parser)
    parser.add_argument(
        "--axis",
        choices=("az", "el", "both"),
        default="both",
        help=(
            "Which axis to sweep: 'az' (elevation held at --el), 'el' "
            "(azimuth held at --az), or 'both' (default, full 2-D grid)."
        ),
    )
    parser.add_argument(
        "--el_first",
        action="store_true",
        help="Scan az as outer loop (el is the fast axis); default is az "
        "fast. Ignored unless --axis both.",
    )
    parser.add_argument(
        "--az_start",
        type=float,
        default=-180.0,
        help="Azimuth scan start in degrees (default: -180).",
    )
    parser.add_argument(
        "--az_stop",
        type=float,
        default=180.0,
        help="Azimuth scan stop in degrees, inclusive (default: 180).",
    )
    parser.add_argument(
        "--az_step",
        type=float,
        default=5.0,
        help="Azimuth grid step size in degrees (default: 5).",
    )
    parser.add_argument(
        "--az",
        type=float,
        default=0.0,
        help="Azimuth hold position in degrees when --axis el (default: 0).",
    )
    parser.add_argument(
        "--el_start",
        type=float,
        default=-180.0,
        help="Elevation scan start in degrees (default: -180).",
    )
    parser.add_argument(
        "--el_stop",
        type=float,
        default=180.0,
        help="Elevation scan stop in degrees, inclusive (default: 180).",
    )
    parser.add_argument(
        "--el_step",
        type=float,
        default=5.0,
        help="Elevation grid step size in degrees (default: 5).",
    )
    parser.add_argument(
        "--el",
        type=float,
        default=0.0,
        help="Elevation hold position in degrees when --axis az (default: 0).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of full-grid passes (default: run until Ctrl-C).",
    )
    parser.add_argument(
        "--pause_s",
        type=float,
        default=None,
        help="Seconds to pause at each grid point.",
    )
    parser.add_argument(
        "--sleep_s",
        type=float,
        default=None,
        help="Seconds to sleep between passes (with --count).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    main(transport, args)
