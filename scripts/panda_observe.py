import logging

from eigsep_observing import EigsepRedis, PandaClient
from eigsep_observing.utils import configure_eig_logger

# logger with rotating file handler
logger = logging.getLogger("__name__")
configure_eig_logger(level=logging.DEBUG)

redis = EigsepRedis(host="localhost", port=6379)
client = PandaClient(redis)

# main loop, runs indefinitely
while True:
    try:
        client.read_ctrl()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, exiting.")
        break

client.stop_client.set()
logger.info("Closed connection to Panda.")
