"""
Interactive motor zeroing UI.

Jog the motors into the desired home position, then Enter to zero
the step counters. After zeroing, ``motor_control.py`` treats the
current physical position as ``(0, 0)``.

Controls:
    u / d  - jog elevation up / down
    l / r  - jog azimuth left / right
    + / -  - increase / decrease jog step size
    Enter  - zero step counters and exit
    q      - quit without zeroing
"""

from argparse import ArgumentParser
import curses
import logging

from eigsep_redis import Transport

from eigsep_observing import MotorZeroer
from eigsep_observing.testing import DummyPandaClient  # noqa: F401
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


def _build_transport(dummy):
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        transport = Transport(host="localhost", port=6380)
        transport.reset()
        transport._dummy_client = DummyPandaClient(transport=transport)
        return transport
    return Transport(host="localhost", port=6379)


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
    screen.addstr(7, 0, "+/- = change step size")
    screen.addstr(8, 0, "Enter = zero and exit | q = quit")
    screen.refresh()


def _curses_main(screen, transport, args):
    curses.noecho()
    screen.nodelay(False)

    zeroer = MotorZeroer(transport)
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
    parser.add_argument(
        "--deg",
        type=float,
        default=1.0,
        help="Initial jog step size in degrees (default: 1.0)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    transport = _build_transport(args.dummy)
    curses.wrapper(_curses_main, transport, args)
