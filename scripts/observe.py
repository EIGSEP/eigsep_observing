"""
Observing script and filewriter for Eigsep. Reads configuration from
Redis (uploaded by PandaClient) and writes correlation and VNA data to
disk. The Panda runs autonomously — start it before running this script.
"""

import argparse
import logging
from pathlib import Path
import threading
import time
import yaml

from eigsep_observing import EigObserver, EigsepObsRedis
from eigsep_observing.testing import DummyEigObserver
from eigsep_observing.utils import configure_eig_logger, get_config_path

# logger with rotating file handler
logger = logging.getLogger("__name__")
configure_eig_logger(level=logging.INFO)

# command line arguments
parser = argparse.ArgumentParser(
    description="Eigsep Observer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--cfg_file",
    dest="cfg_file",
    type=Path,
    default=get_config_path("obs_config.yaml"),
    help=(
        "Configuration file for the observer. Used for IP addresses "
        "and observer-side settings (save directories, ntimes). "
        "The Panda reads its own config from Redis."
    ),
)
parser.add_argument(
    "--snap",
    dest="use_snap",
    action=argparse.BooleanOptionalAction,
    default=True,
    help="Read correlation data from RPi Redis in box.",
)
parser.add_argument(
    "--panda",
    dest="use_panda",
    action=argparse.BooleanOptionalAction,
    default=True,
    help="Connect to LattePanda in box.",
)
parser.add_argument(
    "--dummy",
    action="store_true",
    help="Run in dummy mode, using mock Redis instances.",
)

args = parser.parse_args()
if args.dummy:
    logger.warning(
        "Running in DUMMY mode, using mock Redis instances. "
        "No actual data will be recorded."
    )
    redis_port = 6380  # test port for mock Redis
    args.cfg_file = get_config_path("dummy_config.yaml")
else:
    redis_port = 6379

with open(args.cfg_file, "r") as f:
    cfg = yaml.safe_load(f)
rpi_ip = cfg["rpi_ip"]
panda_ip = cfg["panda_ip"]


# initialize the Redis instances
if args.use_snap:
    logger.info(f"Connecting to RPi Redis instance at {rpi_ip}.")
    redis_snap = EigsepObsRedis(host=rpi_ip, port=redis_port)
else:
    logger.warning("Not connecting to RPi Redis instance.")
    redis_snap = None
if args.use_panda:
    logger.info(f"Connecting to LattePanda at {panda_ip}.")
    redis_panda = EigsepObsRedis(host=panda_ip, port=redis_port)
else:
    logger.warning("Not connecting to LattePanda")
    redis_panda = None

if args.use_panda:
    logger.info("Waiting for Panda config in Redis.")
    while True:
        try:
            redis_panda.config.get()
            break
        except ValueError:
            logger.info("Panda config not yet available, retrying...")
            time.sleep(1.0)

if args.dummy:
    observer = DummyEigObserver(redis_snap=redis_snap, redis_panda=redis_panda)
else:
    observer = EigObserver(redis_snap=redis_snap, redis_panda=redis_panda)

thds = {}
thds["status"] = observer.status_thread
# set up file writing: corr_thd for correlation data, panda_thd for s11
if args.use_snap:
    corr_thd = threading.Thread(
        target=observer.record_corr_data,
        args=(cfg["corr_save_dir"],),
        kwargs={"ntimes": cfg["corr_ntimes"], "timeout": 10},
    )
    thds["corr"] = corr_thd
    logger.info("Starting correlation file writing thread.")
    corr_thd.start()

# set up VNA file writing — use panda config from Redis
if args.use_panda and observer.cfg.get("use_vna", False):
    logger.info(f"panda connected: {observer.panda_connected}")
    vna_thd = threading.Thread(
        target=observer.record_vna_data,
        args=(observer.cfg["vna_save_dir"],),
    )
    thds["vna"] = vna_thd
    logger.info("Starting VNA file writing thread.")
    vna_thd.start()


try:
    observer.stop_event.wait()  # wait until stop event
except KeyboardInterrupt:
    logger.info("Keyboard interrupt received, stopping observer.")
finally:
    observer.stop_event.set()
    for name in thds:
        logger.info(f"Stopping thread: {name}")
        thds[name].join()
        logger.info(f"Thread {name} stopped.")
    logger.info("All threads stopped. Exiting observer.")

if args.dummy:
    # reset Redis instances
    if args.use_snap:
        redis_snap.reset()
    if args.use_panda:
        redis_panda.reset()
