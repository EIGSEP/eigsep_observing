from argparse import ArgumentParser
import logging
from threading import Thread

from eigsep_observing import EigsepRedis, PandaClient
from eigsep_observing.testing import DummyPandaClient
from eigsep_observing.utils import configure_eig_logger

# logger with rotating file handler
logger = logging.getLogger("__name__")
configure_eig_logger(level=logging.INFO)

parser = ArgumentParser(description="Panda observing client")
parser.add_argument(
    "--dummy", action="store_true", help="Run in dummy mode (no hardware)"
)
args = parser.parse_args()


if args.dummy:
    logger.warning("Running in DUMMY mode, no hardware will be used.")
    redis = EigsepRedis(host="localhost", port=6380)
    redis.reset()  # reset test redis database
    client = DummyPandaClient(redis)
else:
    redis = EigsepRedis(host="localhost", port=6379)
    client = PandaClient(redis)

logger.info(f"Client configuration: {client.cfg}")
thds = {}
# switches
if client.cfg["use_switches"]:
    switch_thd = Thread(target=client.switch_loop)
    thds["switch"] = switch_thd
    logger.info("Starting switch thread")
    switch_thd.start()

# VNA
if client.cfg["use_vna"]:
    vna_thd = Thread(target=client.vna_loop)
    thds["vna"] = vna_thd
    logger.info("Starting VNA thread")
    vna_thd.start()

# ctrl
ctrl_thd = Thread(target=client.ctrl_loop, daemon=True)
thds["ctrl"] = ctrl_thd
logger.info("Starting control thread")
ctrl_thd.start()

try:
    for t in thds.values():
        t.join()
except KeyboardInterrupt:
    logger.info("Keyboard interrupt received, stopping threads")
finally:
    client.stop_client.set()
    for name, t in thds.items():
        if name == "ctrl":
            continue  # ctrl thread is daemon, no need to join
        logger.info(f"Joining thread {name}")
        t.join()
        logger.info(f"Thread {name} joined")
    logger.info("All threads joined, exiting.")
