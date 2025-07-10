from cmt_vna.testing import DummyVNA
from eigsep_corr.config import load_config
import picohost

from .. import PandaClient

default_cfg_file = (
    "/home/christian/Documents/research/eigsep/eigsep_observing/src/"
    "eigsep_observing/config/dummy_config.yaml"
)
default_cfg = load_config(default_cfg_file, compute_inttime=False)


class DummyPandaClient(PandaClient):
    """
    Mock up of PandaClient for testing purposes, that uses dummy
    implementations of the VNA and PicoHost.
    """

    # override pico classes with dummies
    PICO_CLASSES = {
        "imu": picohost.testing.DummyPicoDevice,
        "therm": picohost.testing.DummyPicoDevice,
        "peltier": picohost.testing.DummyPicoPeltier,
        "lidar": picohost.testing.DummyPicoDevice,
        "switch": picohost.testing.DummyPicoRFSwitch,
        "motor": picohost.testing.DummyPicoMotor,
    }

    def __init__(self, redis, default_cfg=default_cfg):
        """
        Override the default config.
        """
        super().__init__(redis, default_cfg=default_cfg)

    def init_VNA(self):
        """
        Override the VNA initialization to use a dummy VNA.
        """
        self.vna = DummyVNA(
            ip=self.cfg["vna_ip"],
            port=self.cfg["vna_port"],
            timeout=self.cfg["vna_timeout"],
            save_dir=self.cfg["vna_save_dir"],
            switch_network=self.switch_nw,
        )
        kwargs = self.cfg["vna_settings"].copy()
        kwargs["power_dBm"] = kwargs["power_dBm"]["ant"]
        self.vna.setup(**kwargs)
        self.redis.r.sadd("ctrl_commands", "VNA")
