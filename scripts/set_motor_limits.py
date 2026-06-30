"""Set or inspect the rig-wide motor safe-travel limits (MotorLimitStore).

Operator admin tool. Writes a dedicated Redis K/V (NOT obs_config), read by
every MotorClient — autonomous observer and bring-up scripts alike — so the
travel-limit guard is rig-wide. Run once per rig (and after re-surveying the
safe pot-voltage / IMU-el endpoints). Drives no motor; does not claim
run_tag.

A *set* merges with the existing limits — only the windows you specify
change; unspecified fields keep their stored values. Use
``--no-pot-fence`` / ``--no-imu-fence`` to explicitly disable those
fences (sets the field to None).

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
        msg = f"Current motor limits: {current if current else 'unset'}"
        logger.info(msg)
        print(msg)
        return
    existing = read_motor_limits(transport) or {}

    def _resolve(window_arg, no_fence, key, default):
        if no_fence:
            return None
        if window_arg is not None:
            return window_arg
        return existing.get(key, default)

    az = _resolve(args.az_limits, False, "az_limits_deg", [-180.0, 180.0])
    el = _resolve(args.el_limits, False, "el_limits_deg", [-180.0, 180.0])
    pot = _resolve(args.pot_az_v, args.no_pot_fence, "pot_az_v_limits", None)
    imu = _resolve(args.imu_el, args.no_imu_fence, "imu_el_limits_deg", None)
    publish_motor_limits(
        transport,
        az_limits_deg=az,
        el_limits_deg=el,
        pot_az_v_limits=pot,
        imu_el_limits_deg=imu,
    )
    msg = f"Published motor limits: az={az} el={el} pot_v={pot} imu_el={imu}"
    logger.info(msg)
    print(msg)


def main():
    p = ArgumentParser(description="Set/inspect rig-wide motor limits")
    p.add_argument("--dummy", action="store_true")
    p.add_argument("--show", action="store_true")
    p.add_argument("--az-limits", type=float, nargs=2, default=None)
    p.add_argument("--el-limits", type=float, nargs=2, default=None)
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
