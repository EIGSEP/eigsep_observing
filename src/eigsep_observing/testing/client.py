from functools import partial

import yaml
from cmt_vna.testing import DummyVNA
import picohost

from .. import PandaClient

default_cfg_file = (
    "/home/christian/Documents/research/eigsep/eigsep_observing/src/"
    "eigsep_observing/config/dummy_config.yaml"
)


# Picohost ships a single ``DummyPicoIMU`` class whose ``EMULATOR_CLASS``
# instantiates ``ImuEmulator`` with the default ``app_id=3``. The default
# emits ``sensor_name="imu_el"``, which means two ``DummyPicoIMU`` instances
# both publish to the same Redis key — the panda would only ever see one
# IMU. The dummy bus needs distinct ``imu_el`` and ``imu_az`` streams to
# match the real two-pico hardware (firmware ``APP_IMU_EL`` = 3 and
# ``APP_IMU_AZ`` = 6, see picohost 1.0.0). We work around this in
# eigsep_observing by subclassing ``DummyPicoIMU`` and rebinding
# ``EMULATOR_CLASS`` to a ``partial`` that pins the desired ``app_id``.
# ``partial`` is used (rather than a plain function) to avoid the
# descriptor binding that would otherwise turn ``self.EMULATOR_CLASS``
# into a bound method and inject ``self`` as the first arg to
# ``ImuEmulator``. If picohost later grows native ``DummyPicoImuEl`` /
# ``DummyPicoImuAz`` classes, this shim can be deleted.
class _DummyPicoImuEl(picohost.testing.DummyPicoIMU):
    EMULATOR_CLASS = partial(picohost.testing.ImuEmulator, app_id=3)


class _DummyPicoImuAz(picohost.testing.DummyPicoIMU):
    EMULATOR_CLASS = partial(picohost.testing.ImuEmulator, app_id=6)


class DummyPandaClient(PandaClient):
    """
    Mock up of PandaClient for testing purposes, that uses dummy
    implementations of the VNA and PicoHost.
    """

    # override pico classes with emulator-backed dummies
    PICO_CLASSES = {
        "imu_el": _DummyPicoImuEl,
        "imu_az": _DummyPicoImuAz,
        "potmon": picohost.testing.DummyPicoPotentiometer,
        "tempctrl": picohost.testing.DummyPicoPeltier,
        "lidar": picohost.testing.DummyPicoLidar,
        "rfswitch": picohost.testing.DummyPicoRFSwitch,
        "motor": picohost.testing.DummyPicoMotor,
    }

    def __init__(self, redis, default_cfg=None):
        """
        Override the default config.
        """
        if default_cfg is None:
            try:
                with open(default_cfg_file, "r") as f:
                    default_cfg = yaml.safe_load(f)
            except FileNotFoundError:
                default_cfg = {}
        super().__init__(redis, default_cfg=default_cfg)

    def get_pico_config(self, fname, app_mapping):
        """
        Override the pico config loading to use the default dummy config.
        """
        pico_cfg = {
            "motor": "dummy",
            "tempctrl": "dummy",
            "potmon": "dummy",
            "imu_el": "dummy",
            "imu_az": "dummy",
            "lidar": "dummy",
            "rfswitch": "dummy",
        }
        return pico_cfg

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
