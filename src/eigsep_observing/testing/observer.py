import logging

import yaml
from eigsep_redis import ConfigStore

from .. import EigObserver, utils
from ..corr import CorrConfigStore

logger = logging.getLogger(__name__)

CORR_CFG_PATH = utils.get_config_path("corr_config.yaml")
CFG_PATH = utils.get_config_path("dummy_config.yaml")


class DummyEigObserver(EigObserver):
    def __init__(self, transport_snap=None, transport_panda=None):
        """
        Override constructor to pre-seed the dummy configs on each
        transport so the parent constructor's ``.get()`` calls find
        them.
        """
        if transport_snap is not None:
            CorrConfigStore(transport_snap).upload(
                utils.load_config(CORR_CFG_PATH)
            )
        if transport_panda is not None:
            with open(CFG_PATH, "r") as f:
                ConfigStore(transport_panda).upload(yaml.safe_load(f))
        super().__init__(
            transport_snap=transport_snap,
            transport_panda=transport_panda,
        )
