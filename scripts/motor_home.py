"""Closed-loop return-to-home. Standalone active driver (claims run_tag);
talks to hardware only via MotorClient/PicoProxy; requires pico-manager.

Drives the antenna back to the home set by field_zero, using pot-voltage
(az) + IMU (el) feedback, settling between moves. Re-zeros the step counter
on convergence. Run after a motor_scan / motor_manual session to re-
establish a known position.
"""

import logging
from argparse import ArgumentParser

from eigsep_observing import MotorHomer, run_tag
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport,
    require_pico,
)
from eigsep_observing.home_ref import read_home_ref
from eigsep_observing.utils import configure_eig_logger

configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


def run(transport, *, dry_run=False):
    if read_home_ref(transport) is None:
        raise SystemExit(
            "No home reference; run field_zero to set home first."
        )
    homer = MotorHomer(transport, source="motor_home")
    require_pico(homer.motor_client._proxy)
    if dry_run:
        pot_v, el_est = homer._read_sensors()
        ref = read_home_ref(transport)
        logger.info(
            "Dry run: pot=%s V (home %.3f), el=%s (%s)",
            pot_v,
            ref["pot_az_voltage_v0"],
            el_est.el_deg,
            el_est.source,
        )
        return
    with run_tag.session(transport, "motor_home"):
        result = homer.home()
        logger.info("Home result: %s", result)


def main():
    p = ArgumentParser(description="Closed-loop return-to-home")
    p.add_argument("--dummy", action="store_true")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report residuals vs home without moving.",
    )
    add_redis_args(p)
    args = p.parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    run(transport, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
