"""
Observing script and filewriter for Eigsep. Uploads a config to the Panda,
which then runs autonomously. Reads correlation data from the RPi Redis
and metadata from the Panda Redis.
"""

import argparse
import logging
from pathlib import Path
import threading

from eigsep_corr.config import load_config
from eigsep_observing import EigObserver, EigsepRedis
from eigsep_observing.testing import DummyEigObserver
from eigsep_observing.utils import configure_eig_logger, get_config_path

# logger with rotating file handler
logger = logging.getLogger("__name__")
configure_eig_logger(level=logging.DEBUG)

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
    help="Configuration file for the observer.",
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

cfg = load_config(args.cfg_file, compute_inttime=False)
rpi_ip = cfg["rpi_ip"]
panda_ip = cfg["panda_ip"]


# initialize the Redis instances
logger.info(f"Connecting to RPi Redis instance at {rpi_ip}.")
redis_snap = EigsepRedis(host=rpi_ip, port=redis_port)
if args.use_panda:
    logger.info(f"Connecting to LattePanda at {panda_ip}.")
    redis_panda = EigsepRedis(host=panda_ip, port=redis_port)

    # upload the configuration file to the Redis instances
    redis_panda.upload_config(cfg, from_file=False)
else:
    logger.info("Not connecting to LattePanda, using RPi Redis only.")
    redis_panda = None

if args.dummy:
    observer = DummyEigObserver(redis_snap=redis_snap, redis_panda=redis_panda)
else:
    observer = EigObserver(redis_snap=redis_snap, redis_panda=redis_panda)

if args.use_panda:
    observer.reprogram_panda(force=True)

thds = {}
# set up file writing: corr_thd for correlation data, panda_thd for s11
corr_thd = threading.Thread(
    target=observer.record_corr_data,
    args=(cfg["corr_save_dir"],),
    kwargs={"ntimes": cfg["corr_ntimes"], "timeout": 10},
)
thds["corr"] = corr_thd
logger.info("Starting correlation file writing thread.")
corr_thd.start()

# set up VNA measurements
if cfg["use_vna"]:
    # vna_thd = threading.Thread(target=observer.observe_vna)
    vna_thd = threading.Thread(
        target=observer.record_vna_data,
        args=(cfg["vna_save_dir"],),
    )
    thds["vna"] = vna_thd
    logger.info("Starting VNA measurement thread.")
    vna_thd.start()


try:
    for t in thds.values():
        t.join()  # blocks forever until the thread is done
except KeyboardInterrupt:
    logger.info("Keyboard interrupt received, stopping observer.")
finally:
    observer.stop_event.set()
    for name in thds:
        logger.info(f"Stopping thread: {name}")
        thds[name].join()
    logger.info("All threads stopped. Exiting observer.")

if args.dummy:
    # reset Redis instances
    redis_snap.reset()
    if args.use_panda:
        redis_panda.reset()
