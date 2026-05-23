import logging

import yaml
from eigsep_redis import ConfigStore

from .. import EigObserver, utils
from ..corr import CorrConfigStore

logger = logging.getLogger(__name__)

CORR_CFG_PATH = utils.get_config_path("corr_config.yaml")
CFG_PATH = utils.get_config_path("dummy_config.yaml")


class DummyEigObserver(EigObserver):
    def __init__(self, transport_snap, transport_panda):
        """
        Pre-seed the dummy configs on each transport so the SNAP-side
        ``CorrConfigStore.get()`` succeeds during parent construction
        and ``_with_header_overlays`` finds a populated panda
        ``ConfigStore`` when tests exercise the overlay path.

        Both transports are required, mirroring ``EigObserver``'s
        contract after the opportunistic-panda refactor.
        """
        CorrConfigStore(transport_snap).upload(
            utils.load_config(CORR_CFG_PATH)
        )
        with open(CFG_PATH, "r") as f:
            ConfigStore(transport_panda).upload(yaml.safe_load(f))
        super().__init__(
            transport_snap=transport_snap,
            transport_panda=transport_panda,
        )
