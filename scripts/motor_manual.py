"""
Interactive motor zeroing UI.

Jog the motors into the desired home position, then Enter to begin
zeroing the step counters. Zeroing is two-step: Enter arms a
confirmation and 'y' commits it, so an accidental Enter can't redefine
home. After zeroing, ``motor_scan.py`` treats the current physical
position as ``(0, 0)``.

Controls:
    u / d  - jog elevation up / down
    l / r  - jog azimuth left / right
    + / -  - increase / decrease jog step size
    h      - go home: drive both axes to step 0 (any key cancels)
    Enter  - arm zero confirmation
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
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


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
    screen.addstr(7, 0, "+/- = change step size | h = go home (0,0)")
    screen.addstr(8, 0, "Enter = zero (asks to confirm) | q = quit")
    if zeroer.is_homing:
        screen.addstr(
            10,
            0,
            ">>> HOMING to (0,0)... press any key to cancel <<<",
        )
    elif zeroer.pending_zero:
        screen.addstr(
            10,
            0,
            ">>> ZERO HERE? 'y' to confirm, any other key to cancel <<<",
        )
    screen.refresh()


def _build_zeroer(transport, args):
    """Build a ``MotorZeroer``, honouring ``args.override_limits``."""
    return MotorZeroer(transport, enforce_limits=not args.override_limits)


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
        logger.info("Step counters zeroed. Motors are at home (0, 0).")
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
