from functools import partial

import yaml
from cmt_vna.testing import DummyVNA
import picohost.testing
from picohost.manager import PicoManager

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


# Map device names to dummy picohost classes for the embedded manager.
DUMMY_PICO_CLASSES = {
    "imu_el": _DummyPicoImuEl,
    "imu_az": _DummyPicoImuAz,
    "potmon": picohost.testing.DummyPicoPotentiometer,
    "tempctrl": picohost.testing.DummyPicoPeltier,
    "lidar": picohost.testing.DummyPicoLidar,
    "rfswitch": picohost.testing.DummyPicoRFSwitch,
    "motor": picohost.testing.DummyPicoMotor,
}


class DummyPandaClient(PandaClient):
    """
    Test PandaClient backed by an in-process PicoManager.

    Starts a PicoManager with emulator-backed DummyPico* devices on
    the same (fake)redis instance before ``super().__init__`` runs, so
    that the proxy objects built inside PandaClient.__init__ find
    their devices already registered.
    """

    def __init__(self, redis, default_cfg=None):
        if default_cfg is None:
            try:
                with open(default_cfg_file, "r") as f:
                    default_cfg = yaml.safe_load(f)
            except FileNotFoundError:
                default_cfg = {}
        # Start the embedded manager BEFORE super().__init__ so that
        # PicoProxy.is_available is True when the parent constructor
        # builds sw_proxy.
        self._manager = self._start_dummy_manager(redis)
        super().__init__(redis, default_cfg=default_cfg)

    def _start_dummy_manager(self, redis):
        """Create and start a PicoManager with dummy devices."""
        mgr = PicoManager(redis)
        for name, cls in DUMMY_PICO_CLASSES.items():
            pico = cls("/dev/dummy", eig_redis=redis, name=name)
            mgr.picos[name] = pico
            redis.r.sadd("picos", name)
        mgr.start()
        return mgr

    def init_VNA(self):
        """
        Override the VNA initialization to use a dummy VNA.
        """
        self.vna = DummyVNA(
            ip=self.cfg["vna_ip"],
            port=self.cfg["vna_port"],
            timeout=self.cfg["vna_timeout"],
            save_dir=self.cfg["vna_save_dir"],
            switch_fn=self._switch_to,
        )
        kwargs = self.cfg["vna_settings"].copy()
        kwargs["power_dBm"] = kwargs["power_dBm"]["ant"]
        self.vna.setup(**kwargs)

    def stop(self, timeout=5.0):
        """Stop client loops, then the embedded PicoManager."""
        super().stop(timeout=timeout)
        if hasattr(self, "_manager") and self._manager:
            self._manager.stop()
            self._manager = None
