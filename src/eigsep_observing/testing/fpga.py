import logging
import time
from collections import defaultdict
from math import floor

from eigsep_redis.testing import DummyTransport

from ..fpga import EigsepFpga, default_config
from .utils import generate_data  # noqa: F401  (re-exported for tests)

logger = logging.getLogger(__name__)


class DummyFpga:
    """
    In-memory stand-in for ``casperfpga.CasperFpga``.

    Implements the register-level interface that ``blocks.Block`` and
    its subclasses rely on (``read_int``/``read_uint``/``write_int``/
    ``read``/``write``/``blindwrite``/``listdev``), backed by a plain
    dict. Unknown registers default to 0 so real blocks can read
    power-on state without ``KeyError``. ``corr_acc_cnt`` is a free-
    running wallclock-driven counter (advances at ``cnt_period``
    seconds per tick from instance construction), matching the
    behavior of a correlator that's been synced.

    The constructor accepts the production calling convention
    (``CasperFpga(snap_ip, transport=...)``) so it can slot into
    ``EigsepFpga._make_fpga`` overrides without signature juggling.
    """

    def __init__(self, snap_ip=None, transport=None, **kwargs):
        self.snap_ip = snap_ip
        self.transport = transport
        self.sync_time = time.time()
        self.cnt_period = kwargs.pop("cnt_period", 2**28 / (500 * 1e6))
        self.regs = defaultdict(int)
        self.regs["version_version"] = 0x20003
        self.regs["corr_acc_len"] = kwargs.get("corr_acc_len", 67108864)
        self.regs["corr_scalar"] = kwargs.get("corr_scalar", 512)

    def upload_to_ram_and_program(self, fpg_file, force=False):
        pass

    def write_int(self, reg, val, word_offset=0, **kwargs):
        logger.debug(f"Writing {val} to {reg}")
        self.regs[reg] = val

    def read_int(self, reg, word_offset=0, **kwargs):
        if reg == "corr_acc_cnt":
            acc_cnt = (time.time() - self.sync_time) / self.cnt_period
            return int(floor(acc_cnt))
        return self.regs[reg]

    read_uint = read_int

    def read(self, reg, nbytes, **kwargs):
        return b"\x12" * nbytes

    def write(self, reg, val, offset=0, **kwargs):
        pass

    def blindwrite(self, reg, val, **kwargs):
        pass

    def listdev(self):
        return list(self.regs.keys())


class DummyAdcAdc:
    def selectInput(self, inp):
        pass


class DummyAdc:
    """
    Stand-in for ``casperfpga.snapadc.SnapAdc``. Matches the subset of
    the SnapAdc interface that ``EigsepFpga.initialize_adc`` exercises.
    """

    def __init__(self, fpga, num_chans=2, resolution=8, ref=None):
        self.fpga = fpga
        self.num_chans = num_chans
        self.resolution = resolution
        self.ref = ref

    def init(self, sample_rate=500):
        self.adc = DummyAdcAdc()

    def alignLineClock(self):
        return []

    def alignFrameClock(self):
        return []

    def rampTest(self):
        return []

    def selectADC(self):
        pass

    def set_gain(self, gain):
        pass


class DummyPam:
    """
    Stand-in for ``blocks.Pam``. The real Pam talks to an I2C bus via
    ``casperfpga.i2c``, which cannot run against ``DummyFpga`` — hence
    the substitute. Keeps the same call surface ``EigsepFpga``
    exercises in ``initialize_pams`` / ``get_pam_atten`` /
    ``set_pam_atten``.
    """

    def __init__(self, host, name=None, logger=None):
        self.host = host
        self.name = name
        self.attenuation = (0, 0)

    def initialize(self, verify=False):
        pass

    def set_attenuation(self, att_e, att_n, verify=True):
        self.attenuation = (att_e, att_n)
        if verify:
            assert (att_e, att_n) == self.get_attenuation()

    def get_attenuation(self):
        return self.attenuation


class DummyEigsepFpga(EigsepFpga):
    """
    Hardware-free ``EigsepFpga`` for testing.

    Inherits the production ``__init__`` verbatim — the only
    substitutions happen at the factory-method boundary:

    - ``_make_fpga`` → ``DummyFpga`` (in-memory register backend)
    - ``_make_adc``  → ``DummyAdc``  (SnapAdc substitute)
    - ``_make_pam``  → ``DummyPam``  (I2C-free PAM substitute)

    The real ``Sync`` / ``NoiseGen`` / ``Input`` / ``Pfb`` blocks run
    unchanged against ``DummyFpga``, so production register-write
    sequences are actually exercised by tests. A fakeredis-backed
    ``DummyTransport`` is built in place of the real ``Transport`` —
    tests can also pass one explicitly to share state across
    instances.

    After ``super().__init__()`` the dummy primes a few registers to
    reflect a freshly-configured FPGA (pfb_ctrl bits that match
    ``cfg["fft_shift"]``). Without this, ``validate_config`` would
    fail on any test that calls it, because power-on defaults are
    zero and real ``Pfb.get_fft_shift`` reads the actual bits.
    """

    def __init__(self, cfg=default_config, transport=None, program=False):
        if transport is None:
            transport = DummyTransport()
        super().__init__(cfg=cfg, transport=transport, program=program)
        # Seed pfb_ctrl with cfg fft_shift bits via the real Pfb so
        # header/validate_config report the expected value.
        self.pfb.set_fft_shift(self.cfg["fft_shift"])

    def _make_fpga(self):
        corr_acc_len = self.cfg["corr_acc_len"]
        sample_rate = self.cfg["sample_rate"]
        cnt_period = corr_acc_len / (sample_rate * 1e6)
        return DummyFpga(
            snap_ip=self.cfg["snap_ip"],
            cnt_period=cnt_period,
            corr_acc_len=corr_acc_len,
            corr_scalar=self.cfg["corr_scalar"],
        )

    def _make_adc(self, ref):
        return DummyAdc(self.fpga, ref=ref)

    def _make_pam(self, num):
        return DummyPam(self.fpga, f"i2c_ant{num}")
