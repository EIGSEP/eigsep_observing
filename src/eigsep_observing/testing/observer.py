import logging

from .. import EigObserver, utils

logger = logging.getLogger(__name__)

CORR_CFG_PATH = utils.get_config_path("corr_config.yaml")
CFG_PATH = utils.get_config_path("dummy_config.yaml")


class DummyEigObserver(EigObserver):

    def __init__(self, redis_snap=None, redis_panda=None):
        """
        Override constrcutor to use dummy configs.
        """
        # upload corr config to redis, parent class will read it
        if redis_snap is not None:
            redis_snap.upload_corr_config(CORR_CFG_PATH, from_file=True)
        if redis_panda is not None:
            redis_panda.upload_config(CFG_PATH, from_file=True)
        # call parent constructor
        super().__init__(redis_snap=redis_snap, redis_panda=redis_panda)
