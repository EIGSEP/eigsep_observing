import logging
from eigsep_observing.utils import configure_eig_logger

logger = configure_eig_logger(level=logging.DEBUG)

import argparse  # noqa: E402
from eigsep_corr.config import load_config  # noqa: E402
from eigsep_corr.fpga import add_args  # noqa: E402
from eigsep_observing import EigsepFpga  # noqa: E402
from eigsep_observing.utils import get_config_path  # noqa: E402
from eigsep_observing.testing import DummyEigsepFpga  # noqa: E402

parser = argparse.ArgumentParser(
    description="Snap observing with Eigsep FPGA",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
default_config_file = get_config_path("corr_config.yaml")
add_args(parser, default_config_file=default_config_file)
args = parser.parse_args()
cfg = load_config(args.config_file)

if args.force_program:
    program = "force"
else:
    program = args.program

if args.dummy_mode:
    logger.warning("Running in DUMMY mode.")
    fpga = DummyEigsepFpga(cfg=cfg, program=program)
else:
    snap_ip = cfg["snap_ip"]
    logger.info(f"Connecting to Eigsep correlator at {snap_ip}.")
    fpga = EigsepFpga(cfg=cfg, program=program)

# initialize SNAP
fpga.initialize(
    initialize_adc=args.initialize_adc,
    initialize_fpga=args.initialize_fpga,
    sync=args.sync,
)

# validate config and upload to redis
fpga.upload_config(validate=True)

# start observing
logger.info("Starting observation.")
try:
    fpga.observe(pairs=None, timeout=10)
except KeyboardInterrupt:
    logger.info("Observation interrupted by user.")
finally:
    logger.info("Stopping observation.")
