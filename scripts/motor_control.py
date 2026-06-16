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


def _prompt_go_home(motor):
    """On a Ctrl-C interrupt, offer to drive back to (0, 0).

    Defaults to *No* so an interrupt leaves the motors halted in place
    (the safe abort) unless the operator explicitly opts in. A
    non-interactive stdin (``EOFError``) or a second Ctrl-C
    (``KeyboardInterrupt``) is treated as No. When confirmed, the same
    ``MotorClient.home`` primitive ``motor_manual.py`` uses drives the
    return; a Ctrl-C during that move aborts it.
    """
    try:
        answer = input("Go home (0,0)? [y/N] ").strip().lower()
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
    az_range = _axis_range(args.az_start, args.az_stop, args.az_step)
    el_range = _axis_range(args.el_start, args.el_stop, args.el_step)
    require_pico(PicoProxy("motor", transport, source="motor_control"))
    with run_tag.session(transport, "motor_control"):
        status = StatusWriter(transport)
        motor = MotorClient(transport)

        started = time.monotonic()
        status.send("motor_control started")
        logger.info("motor_control started")
        logger.info(
            "Scan grid: az %g..%g step %g (%d pts), "
            "el %g..%g step %g (%d pts)",
            args.az_start,
            args.az_stop,
            args.az_step,
            len(az_range),
            args.el_start,
            args.el_stop,
            args.el_step,
            len(el_range),
        )

        try:
            motor.set_delay()
            motor.halt()
            motor.scan(
                az_range_deg=az_range,
                el_range_deg=el_range,
                el_first=args.el_first,
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
            msg = f"motor_control ended (duration={elapsed:.1f}s)"
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
        "--el_first",
        action="store_true",
        help="Scan az as outer loop (el is the fast axis); default is az fast.",
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
