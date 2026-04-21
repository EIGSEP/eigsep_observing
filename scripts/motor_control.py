"""
Az/el scan script.

Runs a beam scan through ``PicoManager`` via Redis — no direct serial
to the motor pico, so the manager service stays up throughout. Log
start/end boundaries to the shared status stream so the ground
observer sees when a scan is active.
"""

from argparse import ArgumentParser
import logging
import time

import numpy as np
from eigsep_redis import StatusWriter, Transport

from eigsep_observing import MotorScanner
from eigsep_observing.testing import DummyPandaClient  # noqa: F401 (for --dummy)
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


def _build_transport(dummy):
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        transport = Transport(host="localhost", port=6380)
        transport.reset()
        # Spin up an embedded PicoManager on the fake redis so
        # PicoProxy finds the motor. Keep the client alive via the
        # `_dummy_client` attribute so it isn't garbage-collected.
        transport._dummy_client = DummyPandaClient(transport=transport)
        return transport
    return Transport(host="localhost", port=6379)


def main(transport, args):
    status = StatusWriter(transport)
    scanner = MotorScanner(transport)

    started = time.monotonic()
    status.send("motor_control started")
    logger.info("motor_control started")

    try:
        scanner.set_delay()
        scanner.halt()
        scanner.scan(
            az_range_deg=np.linspace(-180.0, 180.0, 10),
            el_range_deg=np.linspace(-180.0, 180.0, 10),
            el_first=args.el_first,
            repeat_count=args.count,
            pause_s=args.pause_s,
            sleep_between=args.sleep_s,
        )
    except KeyboardInterrupt:
        logger.info("Scan interrupted by user")
    except (TimeoutError, RuntimeError) as exc:
        logger.error("Motor scan aborted: %s", exc)
    finally:
        scanner.halt()
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
    parser.add_argument(
        "--el_first",
        action="store_true",
        help="Scan az as outer loop (el is the fast axis); default is az fast.",
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
    transport = _build_transport(args.dummy)
    main(transport, args)
