import logging

import yaml

from .. import EigObserver, utils

logger = logging.getLogger(__name__)

CORR_CFG_PATH = utils.get_config_path("corr_config.yaml")
CFG_PATH = utils.get_config_path("dummy_config.yaml")


class DummyEigObserver(EigObserver):
    def __init__(self, redis_snap=None, redis_panda=None):
        """
        Override constructor to use dummy configs.
        """
        # upload corr config to redis, parent class will read it
        if redis_snap is not None:
            redis_snap.corr_config.upload(utils.load_config(CORR_CFG_PATH))
        if redis_panda is not None:
            with open(CFG_PATH, "r") as f:
                redis_panda.config.upload(yaml.safe_load(f))
        # call parent constructor
        super().__init__(redis_snap=redis_snap, redis_panda=redis_panda)
