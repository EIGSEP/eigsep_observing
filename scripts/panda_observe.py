import logging

from eigsep_observing import EigsepRedis, PandaClient

LOG_LEVEL = logging.INFO
PI_IP = "10.10.10.10"
INIT_TIMEOUT = 60  # seconds to wait for init commands from Redis

logger = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL)

redis = EigsepRedis(host=PI_IP)
client = PandaClient(redis, logger=logger)
client.read_init_commands(timeout=INIT_TIMEOUT)

# main loop, runs indefinitely
client.read_ctrl()
