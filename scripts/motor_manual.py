"""
Interactive motor jogging and scan-origin UI.

Jog the motors into the desired scan origin, then Enter to begin
zeroing the step counters. Zeroing is two-step: Enter arms a
confirmation and 'y' commits it, so an accidental Enter can't redefine
the origin. After zeroing, ``motor_scan.py`` treats the current
physical position as ``(0, 0)``.

Zeroing here defines a *scan origin* (an arbitrary lab/scan pattern
reference), which is distinct from *home*: home is defined by the pot
calibration (az where the calibrated pot reads 0°, el at IMU-level)
and is not operator-adjustable — 'h' drives there via the homer (az:
one pot-referenced corrective jog under the divergence guard; el:
closed-loop convergence) and re-trues the step counters on success. Use
field_zero for the guided home-and-zero flow.

Controls:
    u / d  - jog elevation up / down
    l / r  - jog azimuth left / right
    + / -  - increase / decrease jog step size
    g      - goto absolute azimuth (prompts for degrees); with
             --az-pot-verify, corrects az slip against the pot
    h      - go home on both axes: pot 0 deg az, IMU-level el (any key
             cancels; requires a pot calibration)
    a      - go home in azimuth only (pot 0 deg; el never moves and
             keeps its step counter)
    e      - go home in elevation only (IMU-level; az never moves and
             keeps its step counter; needs no pot calibration)
    Enter  - arm zero confirmation (scan origin at current pose)
    y      - confirm and zero (after Enter); any other key cancels
    q      - quit without zeroing
"""

from argparse import ArgumentParser
import curses
import logging

from picohost.proxy import PicoProxy

from eigsep_observing import MotorZeroer, run_tag
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport,
    require_pico,
)
from eigsep_observing.motor_client import MotorLimitError
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


def _goto_notice(target, result):
    """One-line summary of a goto-az verify outcome for the curses UI."""
    if result is None:
        return f"goto az {target:.1f}: sent (verify off)"
    if result.degraded:
        return f"goto az {target:.1f}: pot unavailable (open-loop)"
    if result.converged:
        return (
            f"goto az {target:.1f}: OK, {result.iters} jog(s), "
            f"resid {result.residual_deg:.1f} deg"
        )
    return (
        f"goto az {target:.1f}: SLIP resid {result.residual_deg:.1f} deg "
        f"after {result.iters} jog(s)"
    )


def _do_goto(screen, zeroer):
    """Prompt for a target az and run a verified goto; set notice."""
    curses.echo()
    screen.timeout(-1)
    screen.addstr(11, 0, "goto az (deg): ")
    screen.clrtoeol()
    screen.refresh()
    try:
        raw = screen.getstr(11, 15, 8).decode().strip()
    except Exception:
        raw = ""
    finally:
        curses.noecho()
        screen.timeout(100)
    if not raw:
        zeroer.notice = "goto cancelled"
        return
    try:
        target = float(raw)
    except ValueError:
        zeroer.notice = f"goto: bad az {raw!r}"
        return
    try:
        result = zeroer.goto_az(target)
    except (MotorLimitError, RuntimeError, TimeoutError) as exc:
        zeroer.notice = f"goto failed: {exc}"
        return
    zeroer.notice = _goto_notice(target, result)


def _render(screen, zeroer, deg):
    az_str, el_str, connected = zeroer.status_text()
    screen.clear()
    screen.addstr(0, 0, "=== Motor Zeroing ===")
    screen.addstr(2, 0, f"Jog step: {deg:.1f} deg")
    if connected:
        screen.addstr(3, 0, f"AZ pos: {az_str}")
        screen.addstr(4, 0, f"EL pos: {el_str}")
    else:
        screen.addstr(3, 0, "AZ pos: DISCONNECTED (waiting for reconnect)")
        screen.addstr(4, 0, "EL pos: ---")
    screen.addstr(6, 0, "u/d = jog EL | l/r = jog AZ")
    screen.addstr(7, 0, "+/- = change step size | g = goto az (deg)")
    screen.addstr(
        8,
        0,
        "h = home both | a = home AZ | e = home EL (pot 0 / level)",
    )
    screen.addstr(
        9, 0, "Enter = zero scan origin (asks to confirm) | q = quit"
    )
    if zeroer.is_homing:
        axes = "+".join(a.upper() for a in zeroer.homing_axes)
        screen.addstr(
            10,
            0,
            f">>> HOMING {axes} to cal home... press any key to cancel <<<",
        )
    elif zeroer.pending_zero:
        screen.addstr(
            10,
            0,
            ">>> ZERO HERE? 'y' to confirm, any other key to cancel <<<",
        )
    if zeroer.notice:
        width = screen.getmaxyx()[1]
        screen.addstr(12, 0, f"! {zeroer.notice}"[: width - 1])
    screen.refresh()


def _build_zeroer(transport, args):
    """Build a ``MotorZeroer``, honouring ``args.override_limits`` and
    ``args.az_step0_fallback``."""
    return MotorZeroer(
        transport,
        enforce_limits=not args.override_limits,
        az_step0_fallback=args.az_step0_fallback,
        az_pot_verify=args.az_pot_verify,
    )


def _curses_main(screen, transport, args):
    curses.noecho()
    screen.timeout(100)

    zeroer = _build_zeroer(transport, args)
    zeroer.set_delay()
    zeroer.halt()
    deg = args.deg
    zeroed = False

    try:
        while True:
            _render(screen, zeroer, deg)
            ch = screen.getch()
            if ch == ord("g") and not zeroer.is_homing and zeroer.is_available:
                _do_goto(screen, zeroer)
                continue
            deg, should_exit, was_zeroed = zeroer.handle_key(ch, deg)
            if should_exit:
                zeroed = was_zeroed
                break
    except KeyboardInterrupt:
        pass
    finally:
        zeroer.cancel_home()  # stop any in-flight background home
        zeroer.halt()

    if zeroed:
        logger.info("Step counters zeroed at the current pose (scan origin).")
    else:
        logger.info("Exited without zeroing.")


def _parse_args():
    parser = ArgumentParser(
        description="Jog motors to home position and zero step counters"
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient",
    )
    add_redis_args(parser)
    parser.add_argument(
        "--deg",
        type=float,
        default=1.0,
        help="Initial jog step size in degrees (default: 1.0)",
    )
    parser.add_argument(
        "--override-limits",
        action="store_true",
        help=(
            "Disable travel limits for this session"
            " (recovery from out-of-window)."
        ),
    )
    parser.add_argument(
        "--az-step0-fallback",
        action="store_true",
        help=(
            "If the potmon is not publishing, still park az at step 0"
            " open-loop when homing (default: skip az — home is"
            " pot-referenced and the pot fence is inert without it)."
        ),
    )
    parser.add_argument(
        "--az-pot-verify",
        action="store_true",
        help=(
            "Enable closed-loop az pot-verify for the 'g' goto command "
            "(detect + correct az slip against the pot)."
        ),
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    require_pico(PicoProxy("motor", transport, source="motor_manual"))
    if args.override_limits:
        logger.warning(
            "Travel limits DISABLED for this session"
            " (--override-limits) — recovery mode."
        )
    with run_tag.session(transport, "motor_manual"):
        curses.wrapper(_curses_main, transport, args)


if __name__ == "__main__":
    main()
