from functools import partial

import yaml
from cmt_vna.testing import DummyVNA
from eigsep_redis import ConfigStore, HeartbeatWriter, MetadataWriter
from eigsep_redis.testing import DummyTransport
import picohost.testing
from picohost.keys import pico_heartbeat_name
from picohost.manager import HEARTBEAT_TTL, PicoManager

from .. import PandaClient
from ..utils import get_config_path

_dummy_cfg_file = get_config_path("dummy_config.yaml")


# Picohost ships a single ``DummyPicoIMU`` class whose ``EMULATOR_CLASS``
# instantiates ``ImuEmulator`` with the default ``app_id=3``. The default
# emits ``sensor_name="imu_el"``, which means two ``DummyPicoIMU`` instances
# both publish to the same Redis key — the panda would only ever see one
# IMU. The dummy bus needs distinct ``imu_el`` and ``imu_az`` streams to
# match the real two-pico hardware (firmware ``APP_IMU_EL`` = 3 and
# ``APP_IMU_AZ`` = 6, see picohost 1.0.0). We work around this in
# eigsep_observing by subclassing ``DummyPicoIMU`` and rebinding
# ``EMULATOR_CLASS`` to a ``partial`` that pins the desired ``app_id``.
# The ``partial`` is wrapped in ``staticmethod`` because Python 3.14 made
# ``functools.partial`` a method descriptor: without ``staticmethod``,
# accessing ``self.EMULATOR_CLASS`` would auto-bind ``self`` as the first
# positional arg to ``ImuEmulator``, colliding with the pinned ``app_id``
# keyword and raising ``TypeError: got multiple values for argument
# 'app_id'``. ``staticmethod.__get__`` returns the wrapped object unbound,
# restoring pre-3.14 behaviour. If picohost later grows native
# ``DummyPicoImuEl`` / ``DummyPicoImuAz`` classes, this shim can be deleted.
class _DummyPicoImuEl(picohost.testing.DummyPicoIMU):
    EMULATOR_CLASS = staticmethod(
        partial(picohost.testing.ImuEmulator, app_id=3)
    )


class _DummyPicoImuAz(picohost.testing.DummyPicoIMU):
    EMULATOR_CLASS = staticmethod(
        partial(picohost.testing.ImuEmulator, app_id=6)
    )


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


def start_dummy_pico_manager(transport):
    """Create and start a :class:`PicoManager` populated with dummy devices.

    The manager and each device share ``transport`` so producers
    (picohost) and consumers (PandaClient or any bring-up script) talk
    to the same (fake)redis. Each device gets its own
    :class:`MetadataWriter` — the new picohost device API (picohost
    1.0.0+) routes status publication through ``metadata_writer``
    instead of the retired ``eig_redis`` composition shim. Per-device
    :class:`HeartbeatWriter` entries mirror
    ``PicoManager._register_devices`` so ``PicoProxy`` availability
    checks succeed immediately after start.

    Returns the started manager; callers are responsible for calling
    ``manager.stop()`` on teardown.
    """
    mgr = PicoManager(transport)
    writer = MetadataWriter(transport)
    for name, cls in DUMMY_PICO_CLASSES.items():
        pico = cls("/dev/dummy", metadata_writer=writer, name=name)
        mgr.picos[name] = pico
        hb = HeartbeatWriter(transport, name=pico_heartbeat_name(name))
        hb.set(ex=HEARTBEAT_TTL, alive=True)
        mgr._heartbeats[name] = hb
        transport.r.sadd("picos", name)
    mgr.start()
    return mgr


class DummyPandaClient(PandaClient):
    """
    Test PandaClient backed by an in-process PicoManager.

    Starts a PicoManager with emulator-backed DummyPico* devices on
    the same (fake)redis instance before ``super().__init__`` runs, so
    that the proxy objects built inside PandaClient.__init__ find
    their devices already registered.
    """

    def __init__(self, transport=None, *, cfg=None):
        if transport is None:
            transport = DummyTransport()
        if cfg is None:
            # Mirror the parent's cfg=None semantics: when Redis has a
            # cfg, forward cfg=None so the parent's ``_get_cfg`` reads
            # it and emits the canonical "Using config from Redis" log.
            # Only fall back to the packaged dummy yaml when Redis is
            # empty — testing convenience that lets
            # ``DummyPandaClient()`` work standalone without forcing
            # every test to upload a cfg first. The fallback is
            # in-memory only (no Redis upload), so it does not pollute
            # the cfg-owner provenance.
            try:
                ConfigStore(transport).get()
                forwarded_cfg = None
            except ValueError:
                try:
                    with open(_dummy_cfg_file, "r") as f:
                        forwarded_cfg = yaml.safe_load(f)
                except FileNotFoundError:
                    forwarded_cfg = {}
        else:
            forwarded_cfg = cfg
        # Start the embedded manager BEFORE super().__init__ so that
        # PicoProxy.is_available is True when the parent constructor
        # builds sw_proxy.
        self._manager = start_dummy_pico_manager(transport)
        super().__init__(transport, cfg=forwarded_cfg)

    def init_VNA(self):
        """
        Override the VNA initialization to use a dummy VNA.
        """
        self.vna = DummyVNA(
            ip=self.cfg["vna_ip"],
            port=self.cfg["vna_port"],
            timeout=self.cfg["vna_timeout"],
            switch_fn=self._switch,
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
