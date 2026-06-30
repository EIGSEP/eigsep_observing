"""Set or inspect the rig-wide motor safe-travel limits (MotorLimitStore).

Operator admin tool. Writes a dedicated Redis K/V (NOT obs_config), read by
every MotorClient — autonomous observer and bring-up scripts alike — so the
travel-limit guard is rig-wide. Run once per rig (and after re-surveying the
safe pot-voltage / IMU-el endpoints). Drives no motor; does not claim
run_tag.

  set_motor_limits.py --show
  set_motor_limits.py --az-limits -180 180 --el-limits -30 30 \\
      --pot-az-v 0.2 3.1 --imu-el -30 30
  set_motor_limits.py --el-limits -30 30 --no-pot-fence --no-imu-fence
"""

import logging
from argparse import ArgumentParser

from eigsep_observing._scripts_util import add_redis_args, build_transport
from eigsep_observing.motor_limits import (
    publish_motor_limits,
    read_motor_limits,
)
from eigsep_observing.utils import configure_eig_logger

configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


def publish_from_args(transport, *, az_limits, el_limits, pot_az_v, imu_el):
    publish_motor_limits(
        transport,
        az_limits_deg=az_limits,
        el_limits_deg=el_limits,
        pot_az_v_limits=pot_az_v,
        imu_el_limits_deg=imu_el,
    )


def run(transport, args):
    if args.show:
        current = read_motor_limits(transport)
        logger.info("Current motor limits: %s", current or "unset")
        return
    pot_az_v = None if args.no_pot_fence else args.pot_az_v
    imu_el = None if args.no_imu_fence else args.imu_el
    publish_from_args(
        transport,
        az_limits=args.az_limits,
        el_limits=args.el_limits,
        pot_az_v=pot_az_v,
        imu_el=imu_el,
    )
    logger.info(
        "Published motor limits: az=%s el=%s pot_v=%s imu_el=%s",
        args.az_limits,
        args.el_limits,
        pot_az_v,
        imu_el,
    )


def main():
    p = ArgumentParser(description="Set/inspect rig-wide motor limits")
    p.add_argument("--dummy", action="store_true")
    p.add_argument("--show", action="store_true")
    p.add_argument("--az-limits", type=float, nargs=2, default=[-180.0, 180.0])
    p.add_argument("--el-limits", type=float, nargs=2, default=[-180.0, 180.0])
    p.add_argument("--pot-az-v", type=float, nargs=2, default=None)
    p.add_argument("--no-pot-fence", action="store_true")
    p.add_argument("--imu-el", type=float, nargs=2, default=None)
    p.add_argument("--no-imu-fence", action="store_true")
    add_redis_args(p)
    args = p.parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    run(transport, args)


if __name__ == "__main__":
    main()
