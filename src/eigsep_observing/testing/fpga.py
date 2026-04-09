import logging
import time
from math import floor

from ..fpga import EigsepFpga, default_config
from .eig_redis import DummyEigsepRedis

logger = logging.getLogger(__name__)


class DummyBlock:
    def __init__(self, fpga, *args, **kwargs):
        self.fpga = fpga

    def init(self, *args, **kwargs):
        pass

    def initialize(self, *args, **kwargs):
        pass

    def __getattribute__(self, attr):
        try:
            return object.__getattribute__(self, attr)
        except AttributeError:
            return self

    def __call__(self, *args, **kwargs):
        return None


class DummyFpga(DummyBlock):
    def __init__(self, **kwargs):
        self.sync_time = None
        self.cnt_period = kwargs.pop("cnt_period", 2**28 / (500 * 1e6))
        self.regs = {}
        self.regs["version_version"] = 0x20003
        self.regs["corr_acc_len"] = kwargs.get("corr_acc_len", 67108864)
        self.regs["corr_scalar"] = kwargs.get("corr_scalar", 512)
        self.regs["fft_shift"] = kwargs.get("fft_shift", 0x0FF)
        self.regs["pfb_pol01_delay"] = 0
        self.regs["pfb_pol23_delay"] = 0
        self.regs["pfb_pol45_delay"] = 0

    def upload_to_ram_and_program(self, fpg_file, force=False):
        pass

    def write_int(self, reg, val):
        logger.debug(f"Writing {val} to {reg}")
        self.regs[reg] = val

    def read_int(self, reg):
        if reg == "corr_acc_cnt":
            if self.sync_time is None:
                return 0
            acc_cnt = (time.time() - self.sync_time) / self.cnt_period
            acc_cnt = int(floor(acc_cnt))
            return acc_cnt
        else:
            return self.regs[reg]

    read_uint = read_int

    def read(self, reg, nbytes):
        return b"\x12" * nbytes

    def write(self, reg, val, offset=0, **kwargs):
        pass

    def blindwrite(self, reg, val, **kwargs):
        pass


class DummyAdcAdc:
    def selectInput(self, inp):
        pass


class DummyAdc(DummyBlock):
    def __init__(self, fpga, num_chans=2, resolution=8, ref=None):
        super().__init__(fpga)

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


class DummyPfb(DummyBlock):
    def set_fft_shift(self, fft_shift):
        self.fpga.write_int("fft_shift", fft_shift)

    def get_fft_shift(self):
        return self.fpga.read_int("fft_shift")


class DummyPam(DummyBlock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attenuation = (0, 0)

    def initialize(self):
        pass

    def set_attenuation(self, att_e, att_n, verify=True):
        self.attenuation = (att_e, att_n)
        if verify:
            assert (att_e, att_n) == self.get_attenuation()

    def get_attenuation(self):
        return self.attenuation


class DummySync(DummyBlock):
    def set_delay(self, delay):
        pass

    def arm_sync(self):
        pass

    def arm_noise(self):
        pass

    def sw_sync(self):
        self.fpga.sync_time = time.time()


class DummyNoise(DummyBlock):
    def set_seed(self, stream=None, seed=0):
        pass


class DummyInput(DummyBlock):
    def use_noise(self, stream=None):
        pass

    def use_adc(self, stream=None):
        pass


class DummyEigsepFpga(EigsepFpga):
    """
    Hardware-free EigsepFpga for testing.

    Replaces the casperfpga-backed FPGA object and register blocks with
    in-memory Dummy* implementations, and uses DummyEigsepRedis (backed
    by fakeredis) instead of a real Redis server.
    """

    def __init__(self, cfg=default_config, program=False):
        self.logger = logger
        self.cfg = cfg
        self.pairs = cfg["pairs"]

        self.fpg_file = self.cfg["fpg_file"]
        corr_acc_len = self.cfg["corr_acc_len"]
        sample_rate = self.cfg["sample_rate"]
        cnt_period = corr_acc_len / (sample_rate * 1e6)
        self.fpga = DummyFpga(
            snap_ip=self.cfg["snap_ip"],
            transport=None,
            cnt_period=cnt_period,
            corr_acc_len=corr_acc_len,
            corr_scalar=self.cfg["corr_scalar"],
            fft_shift=self.cfg["fft_shift"],
        )
        if program:
            force = program == "force"
            self.fpga.upload_to_ram_and_program(self.fpg_file, force=force)

        if cfg["use_ref"]:
            ref = 10
        else:
            ref = None

        self.logger.debug("Adding dummy blocks to FPGA")
        self.adc = DummyAdc(self.fpga, ref=ref)
        self.sync = DummySync(self.fpga)
        self.noise = DummyNoise(self.fpga)
        self.inp = DummyInput(self.fpga)
        self.pfb = DummyPfb(self.fpga)
        self.blocks = [self.sync, self.noise, self.inp, self.pfb]

        self.autos = [p for p in self.pairs if len(p) == 1]
        self.crosses = [p for p in self.pairs if len(p) == 2]
        self.pams = []

        self.logger.debug("Initializing dummy Redis")
        redis_cfg = self.cfg.get("redis", {})
        host = redis_cfg.get("host", "localhost")
        port = redis_cfg.get("port", 6379)
        self.redis = DummyEigsepRedis(host=host, port=port)

        self.adc_initialized = False
        self.pams_initialized = False
        self.is_synchronized = False

    def initialize_pams(self):
        """Initialize dummy PAMs using DummyPam objects."""
        self.pams = [DummyPam(self.fpga) for _ in range(3)]
        self.blocks.extend(self.pams)
        self.pams_initialized = True
        for ant in self.cfg["rf_chain"]["ants"]:
            atten = self.cfg["rf_chain"]["ants"][ant]["pam"]["atten"]
            self.set_pam_atten(ant, atten)
