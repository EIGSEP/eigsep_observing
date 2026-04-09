import logging
from eigsep_observing.utils import configure_eig_logger

logger = configure_eig_logger(level=logging.INFO)

import argparse  # noqa: E402
from threading import Thread  # noqa: E402

import IPython  # noqa: E402
from eigsep_observing import EigsepFpga  # noqa: E402
from eigsep_observing.testing import DummyEigsepFpga  # noqa: E402
from eigsep_observing.utils import get_config_path, load_config  # noqa: E402

parser = argparse.ArgumentParser(
    description="Snap observing with Eigsep FPGA",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-p",
    dest="program",
    action="store_true",
    default=False,
    help="Program Eigsep correlator.",
)
parser.add_argument(
    "-P",
    dest="force_program",
    action="store_true",
    default=False,
    help="Force program Eigsep correlator.",
)
parser.add_argument(
    "-a",
    dest="initialize_adc",
    action="store_true",
    default=False,
    help="Initialize ADCs.",
)
parser.add_argument(
    "-f",
    dest="initialize_fpga",
    action="store_true",
    default=False,
    help="Initialize Eigsep correlator.",
)
parser.add_argument(
    "-s",
    dest="sync",
    action="store_true",
    default=False,
    help="Sync Eigsep correlator.",
)
parser.add_argument(
    "--config_file",
    dest="config_file",
    default=str(get_config_path("corr_config.yaml")),
    help="Configuration file for Eigsep Fpga.",
)
parser.add_argument(
    "--dummy",
    dest="dummy_mode",
    action="store_true",
    default=False,
    help="Run with a dummy SNAP interface",
)
parser.add_argument(
    "-i",
    "--interactive",
    action="store_true",
    default=False,
    help="Drop into an IPython shell with live access to the fpga object.",
)
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
if args.interactive:
    thd = Thread(
        target=fpga.observe,
        kwargs={"pairs": None, "timeout": 10},
        daemon=True,
    )
    thd.start()
    try:
        IPython.embed(
            banner1=(
                "EIGSEP interactive shell. The `fpga` object is live.\n"
                "Type `exit` or Ctrl-D to stop observing and exit."
            ),
        )
    finally:
        fpga.end_observing()
        thd.join(timeout=5)
        logger.info("Observing done.")
else:
    try:
        fpga.observe(pairs=None, timeout=10)
    except KeyboardInterrupt:
        logger.info("Observation interrupted by user.")
    finally:
        fpga.end_observing()
        logger.info("Observing done.")
