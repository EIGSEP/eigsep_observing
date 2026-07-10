"""
Az/el scan script.

Runs a beam scan through ``PicoManager`` via Redis — no direct serial
to the motor pico, so the manager service stays up throughout. Log
start/end boundaries to the shared status stream so the ground
observer sees when a scan is active.

Three modes:

* Default (neither hold flag): the full 2-D serpentine grid via
  :meth:`MotorClient.scan` — homes to step (0, 0) before the first
  pass and after completion; ``--el_first`` picks the fast axis.
* ``--el E``: single-axis azimuth sweep. One move takes elevation to
  ``E``; azimuth then sweeps ``--az_start`` to ``--az_stop``, in the
  same direction on every pass (each pass first repositions to the
  start — expect the angle to run backwards during that leg). No
  homing: when done the rig parks in place, elevation still at ``E``.
* ``--az A``: the mirrored single-axis elevation sweep at fixed
  azimuth ``A``.

The sweep bounds are configured per axis via
``--az_start/--az_stop/--az_step`` and the ``--el_*`` equivalents
(degrees). Bounds are inclusive of the stop endpoint, so e.g.
``--az_start 0 --az_stop 180 --az_step 5`` sweeps 0..180 in 5 deg
steps. With ``--pause_s`` the sweep steps through every grid point and
dwells at each; without it each pass is a single continuous
start-to-stop move. In a single-axis mode the held axis's
``--*_start/stop/step`` flags are ignored.
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
    the caller sets sweep direction (serpentine in ``MotorClient.scan``
    for the 2-D grid, fixed start -> stop in the single-axis sweep), so
    an inverted ``stop < start`` is a user error.
    """
    if step <= 0:
        raise ValueError(f"step must be positive, got {step}")
    if stop < start:
        raise ValueError(f"stop ({stop}) must be >= start ({start})")
    # +step/2 epsilon so an on-grid stop survives float drift.
    return np.arange(start, stop + step / 2.0, step)


def _resolve_plan(args):
    """Validate CLI flags and build the scan plan before any hardware
    is touched.

    Returns ``(mode, az_range, el_range)`` where ``mode`` is ``"az"``
    (azimuth sweep at fixed ``--el``), ``"el"`` (elevation sweep at
    fixed ``--az``), or ``"both"`` (full 2-D grid, neither hold flag
    given). In a single-axis mode the held axis's range is ``None`` —
    its ``--*_start/stop/step`` bounds are ignored. Passing both
    ``--az`` and ``--el`` is ambiguous and raises; bad bounds raise
    from ``_axis_range``.
    """
    if args.az is not None and args.el is not None:
        raise ValueError(
            "--az and --el are mutually exclusive: pass --el to sweep "
            "azimuth at that elevation, --az to sweep elevation at "
            "that azimuth, or neither for the full 2-D grid."
        )
    if args.el is not None:
        az_range = _axis_range(args.az_start, args.az_stop, args.az_step)
        return "az", az_range, None
    if args.az is not None:
        el_range = _axis_range(args.el_start, args.el_stop, args.el_step)
        return "el", None, el_range
    az_range = _axis_range(args.az_start, args.az_stop, args.az_step)
    el_range = _axis_range(args.el_start, args.el_stop, args.el_step)
    return "both", az_range, el_range


def _describe_plan(mode, args, az_range, el_range):
    """One-line human summary of the resolved scan for the log."""
    if mode == "az":
        return (
            f"az sweep {args.az_start:g}..{args.az_stop:g} "
            f"({len(az_range)} pts) at el {args.el:g}"
        )
    if mode == "el":
        return (
            f"el sweep {args.el_start:g}..{args.el_stop:g} "
            f"({len(el_range)} pts) at az {args.az:g}"
        )
    return (
        f"az {args.az_start:g}..{args.az_stop:g} step {args.az_step:g} "
        f"({len(az_range)} pts), "
        f"el {args.el_start:g}..{args.el_stop:g} step {args.el_step:g} "
        f"({len(el_range)} pts)"
    )


def _single_axis_sweep(motor, args, sweep_axis, sweep_range):
    """Hold one axis, sweep the other; park at the hold when done.

    One move takes the held axis to its ``--el`` / ``--az`` hold angle;
    the sweep axis then runs start -> stop, in the same direction on
    every pass (each pass begins by repositioning to the sweep start).
    Without ``--pause_s`` each pass is a single continuous move; with
    it the sweep dwells at every grid point. Unlike
    :meth:`MotorClient.scan` there is no homing before or after: on
    completion the rig parks in place, held axis still at its hold
    angle. Every move routes through :meth:`MotorClient.move_to`, so
    the travel-limit guard and sensor fence apply.
    """
    if sweep_axis == "az":
        hold_axis, hold_deg = "el", float(args.el)
    else:
        hold_axis, hold_deg = "az", float(args.az)

    logger.info("Moving %s to hold angle %g deg", hold_axis, hold_deg)
    motor.move_to(**{f"{hold_axis}_deg": hold_deg})

    start, stop = float(sweep_range[0]), float(sweep_range[-1])
    npass = 0
    while args.count is None or npass < args.count:
        if args.pause_s is None:
            logger.info("%s: repositioning to %g deg", sweep_axis, start)
            motor.move_to(**{f"{sweep_axis}_deg": start})
            logger.info("%s: sweeping %g -> %g deg", sweep_axis, start, stop)
            motor.move_to(**{f"{sweep_axis}_deg": stop})
        else:
            logger.info(
                "%s: stepping %g -> %g deg, %g s dwell per point",
                sweep_axis,
                start,
                stop,
                args.pause_s,
            )
            for val in sweep_range:
                motor.move_to(**{f"{sweep_axis}_deg": float(val)})
                time.sleep(args.pause_s)
        npass += 1
        if args.sleep_s is not None and (
            args.count is None or npass < args.count
        ):
            logger.info("Sleeping %g s between passes", args.sleep_s)
            time.sleep(args.sleep_s)
    logger.info("Sweep done; parked with %s at %g deg", hold_axis, hold_deg)


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
    # Resolve (and validate) the plan before touching hardware so bad
    # flags fail fast without opening a run_tag session.
    mode, az_range, el_range = _resolve_plan(args)
    require_pico(PicoProxy("motor", transport, source="motor_scan"))
    with run_tag.session(transport, "motor_scan"):
        status = StatusWriter(transport)
        motor = MotorClient(transport)

        started = time.monotonic()
        status.send("motor_scan started")
        logger.info("motor_scan started")
        logger.info(
            "Scan plan: %s", _describe_plan(mode, args, az_range, el_range)
        )

        try:
            motor.set_delay()
            motor.halt()
            if mode == "az":
                _single_axis_sweep(motor, args, "az", az_range)
            elif mode == "el":
                _single_axis_sweep(motor, args, "el", el_range)
            else:
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
        "--el_first",
        action="store_true",
        help="Scan az as outer loop (el is the fast axis); default is az "
        "fast. Only used for the default 2-D grid.",
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
        default=None,
        help="Hold azimuth at this angle (degrees) and sweep elevation "
        "only. Mutually exclusive with --el.",
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
        default=None,
        help="Hold elevation at this angle (degrees) and sweep azimuth "
        "only. Mutually exclusive with --az.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of passes over the grid or sweep (default: run "
        "until Ctrl-C).",
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
