import logging

from eigsep_observing import EigsepRedis, PandaClient

LOG_LEVEL = logging.INFO
PI_IP = "10.10.10.10"
INIT_TIMEOUT = 60  # seconds to wait for init commands from Redis
INIT_TRIES = 3  # number of tries to initialize the client

logger = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL)

redis = EigsepRedis(host=PI_IP)
client = PandaClient(redis, logger=logger)
tries = 0
while True:
    try:
        client.read_init_commands(timeout=INIT_TIMEOUT)
        break
    except TimeoutError:
        tries += 1
        logger.warning(f"Try {tries}/{INIT_TRIES} failed.")
        if tries >= INIT_TRIES:
            logger.error("Failed to initialize PandaClient.")
            raise
        logger.info("Retrying.")

# main loop, runs indefinitely
client.read_ctrl()
