from argparse import ArgumentParser
import logging
from threading import Thread

from eigsep_redis import Transport

from eigsep_observing import PandaClient
from eigsep_observing.testing import DummyPandaClient
from eigsep_observing.utils import configure_eig_logger

# logger with rotating file handler
configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = ArgumentParser(description="Panda observing client")
parser.add_argument(
    "--dummy", action="store_true", help="Run in dummy mode (no hardware)"
)
args = parser.parse_args()


if args.dummy:
    logger.warning("Running in DUMMY mode, no hardware will be used.")
    transport = Transport(host="localhost", port=6380)
    transport.reset()  # reset test redis database
    client = DummyPandaClient(transport=transport)
else:
    transport = Transport(host="localhost", port=6379)
    client = PandaClient(transport)

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

try:
    client.stop_client.wait()  # wait until stop signal is set
except KeyboardInterrupt:
    logger.info("Keyboard interrupt received, stopping threads")
finally:
    client.stop()
    for name, t in thds.items():
        logger.info(f"Joining thread {name}")
        t.join()
        logger.info(f"Thread {name} joined")
    logger.info("All threads joined, exiting.")
