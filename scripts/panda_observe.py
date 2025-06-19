from eigsep_observing import EigsepRedis, PandaClient

redis = EigsepRedis()
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
