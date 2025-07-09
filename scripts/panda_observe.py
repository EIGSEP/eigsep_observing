import logging
from threading import Thread

from eigsep_observing import EigsepRedis, PandaClient
from eigsep_observing.utils import configure_eig_logger

# logger with rotating file handler
logger = logging.getLogger("__name__")
configure_eig_logger(level=logging.DEBUG)

redis = EigsepRedis(host="localhost", port=6379)
client = PandaClient(redis)

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
ctrl_thd = Thread(target=client.ctrl_loop)
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
        logger.info(f"Joining thread {name}")
        t.join()
    logger.info("All threads joined, exiting.")
