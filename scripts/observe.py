"""
Main observing script for Eigsep, using SNAP correlator, a VNA, Dicke
switching, and motor rotations. This script runs on the main single
board computer, currently a Raspberry Pi 4 on the ground.

The script runs indefinitely until interrupted, allowing for continuous
observations. Exceptions raised by motors or switches may cause these
threads to exit, but the main observer thread will continue running.
If file writing is enabled and the recording thread exits, the main
thread exits. This way, intterupted file writing can't go unnoticed.
"""

import argparse
import logging
from pathlib import Path
import threading

from eigsep_corr.utils import get_config_path
from eigsep_observing import EigObserver, EigsepRedis
from eigsep_observing.utils import configure_eig_logger

# logger with rotating file handler
logger = logging.getLogger("__name__")
configure_eig_logger(level=logging.DEBUG)

# command line arguments
parser = argparse.ArgumentParser(
    description="Eigsep Observer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-s",
    dest="use_switches",
    action="store_true",
    default=False,
    help="Enable Dicke switching.",
)
parser.add_argument(
    "-v",
    "--vna",
    dest="use_vna",
    action="store_true",
    default=False,
    help="Do VNA measurements.",
)
parser.add_argument(
    "-r",
    dest="rotate_motors",
    action="store_true",
    default=False,
    help="Enable motor rotations.",
)
parser.add_argument(
    "-w",
    dest="write_files",
    action="store_true",
    default=False,
    help="Write data files to disk.",
)
parser.add_argument(
    "--cfg_file",
    dest="cfg_file",
    type=Path,
    default=get_config_path("obs_config.yaml"),
    help="Configuration file for the observer.",
)
parser.add_argument(
    "--panda_ip",
    dest="panda_ip",
    type=str,
    default="10.10.10.12",
    help="IP address of the Panda board.",
)

args = parser.parse_args()

# initialize the Redis instances
redis_snap = EigsepRedis(host="localhost", port=6379)
redis_panda = EigsepRedis(host=args.panda_ip, port=6379)

# upload the configuration file to the Redis instances
redis_snap.upload_config(args.cfg_file, from_file=True)
redis_panda.upload_config(args.cfg_file, from_file=True)

observer = EigObserver(redis_snap=redis_snap, redis_panda=redis_panda)
thds = {}

# set up file writing
if args.write_files:
    record_thd = threading.Thread(
        target=observer.record_corr_data,
        kwargs={"pairs": None, "timeout": 10},
    )
    thds["snap"] = record_thd
    logger.info("Starting file writing thread.")
    record_thd.start()

# set up dicke switching
if args.use_switches:
    switch_thd = threading.Thread(target=observer.do_swiching)
    thds["switches"] = switch_thd
    logger.info("Starting switch thread.")
    switch_thd.start()

# set up VNA measurements
if args.use_vna:
    vna_thd = threading.Thread(target=observer.observe_vna)
    thds["vna"] = vna_thd
    logger.info("Starting VNA measurement thread.")
    vna_thd.start()

# set up motor rotations if requested
if args.rotate_motors:
    motor_thd = threading.Thread(target=observer.rotate_motors)
    thds["motors"] = motor_thd
    logger.info("Starting motor rotation thread.")
    motor_thd.start()

try:
    if "snap" in thds:
        thds["snap"].join()  # stops blocking if recording thread exits
    else:
        threading.Event().wait()  # wait forever
except KeyboardInterrupt:
    logger.info("Keyboard interrupt received, stopping observer.")
finally:
    for name in thds:
        logger.info(f"Stopping thread: {name}")
        observer.stop_events[name].set()
        thds[name].join()
    logger.info("All threads stopped. Exiting observer.")
