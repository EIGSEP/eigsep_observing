"""Return-to-home (az: single pot-referenced correction; el: closed-loop).
Standalone active driver (claims run_tag); talks to hardware only via
MotorClient/PicoProxy; requires pico-manager.

Drives the antenna to the cal-defined home — az where the calibrated pot
reads 0° (v_home = -b/m from PotCalStore), el at IMU-level — az takes one
guarded corrective jog off a noise-averaged pot read; el converges
closed-loop on the IMU. Re-zeros each axis' step counter on success. Run
after a motor_scan / motor_manual session to re-establish a known position.
"""

import logging
from argparse import ArgumentParser

from eigsep_observing import MotorHomer, run_tag
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport,
    require_pico,
)
from eigsep_observing.utils import configure_eig_logger

configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


def run(transport, *, dry_run=False, override_limits=False):
    if override_limits:
        logger.warning(
            "Travel limits DISABLED for this session"
            " (--override-limits) — recovery mode."
        )
    homer = MotorHomer(
        transport,
        source="motor_home",
        enforce_limits=not override_limits,
    )
    try:
        v_home = homer.az_home_voltage()
    except RuntimeError as exc:
        raise SystemExit(str(exc))
    require_pico(homer.motor_client._proxy)
    if dry_run:
        pot_v = homer._read_pot_once()
        el_est = homer._read_el()
        logger.info(
            "Dry run: pot=%s V (home %.3f), el=%s (%s)",
            pot_v,
            v_home,
            el_est.el_deg,
            el_est.source,
        )
        return
    with run_tag.session(transport, "motor_home"):
        result = homer.home()
        logger.info("Home result: %s", result)


def main():
    p = ArgumentParser(
        description=(
            "Return-to-home (az: single pot-referenced correction; "
            "el: closed-loop)"
        )
    )
    p.add_argument("--dummy", action="store_true")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report residuals vs home without moving.",
    )
    p.add_argument(
        "--override-limits",
        action="store_true",
        help=(
            "Disable travel limits for this session"
            " (recovery from out-of-window)."
        ),
    )
    add_redis_args(p)
    args = p.parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    run(transport, dry_run=args.dry_run, override_limits=args.override_limits)


if __name__ == "__main__":
    main()
