"""
Module for interfacing to a 6-antpol xx/yy correlator for EIGSEP.
This is nominally uses a 4-tap, 2048 real sample (1024 ch) PFB,
with a direct correlation and vector accumulation of 2048 samples,
producing odd/even data sets that are jackknifed every spectrum
(which is faster than ideal: the PFB correlates adjacent spectra).

The important bit widths are:
(ADC) 8_7 (PFB_FIR) 18_17 (FFT) 18_17 (CORR) 18_17 (SCALAR) 18_7 (VACC) 32_7
Affecting the signal level are the FFT_SHIFT (0b00001010101) and the
CORR_SCALAR (18_8).
"""

from copy import deepcopy
import logging
import time
from pathlib import Path
from queue import Queue
from threading import Event, Thread

import numpy as np

from .blocks import Input, NoiseGen, Pam, Pfb, Sync
from .eig_redis import EigsepObsRedis
from .utils import (
    calc_inttime,
    get_config_path,
    get_data_path,
    load_config,
)

USE_CASPERFPGA = True
try:
    import casperfpga
    from casperfpga.transport_tapcp import TapcpTransport
except ImportError:
    USE_CASPERFPGA = False
    TapcpTransport = None

logger = logging.getLogger(__name__)
if not USE_CASPERFPGA:
    logger.warning("Running without casperfpga installed")

default_config_file = get_config_path("corr_config.yaml")
default_config = load_config(default_config_file)


def _cfg_diff_summary(disk_cfg, redis_cfg):
    """
    Produce a human-readable summary of differences between two
    configuration dicts. Used by ``assert_config_matches_redis`` to
    make "re-run with --reinit" errors debuggable.

    Recurses into nested dicts and reports dotted paths; non-dict
    leaves are compared with ``!=``.
    """
    lines = []

    def _walk(a, b, path):
        for key in sorted(set(a) | set(b)):
            sub = f"{path}.{key}" if path else key
            if key not in a:
                lines.append(f"  {sub}: <missing on disk> vs {b[key]!r}")
            elif key not in b:
                lines.append(f"  {sub}: {a[key]!r} vs <missing in Redis>")
            elif isinstance(a[key], dict) and isinstance(b[key], dict):
                _walk(a[key], b[key], sub)
            elif a[key] != b[key]:
                lines.append(
                    f"  {sub}: {a[key]!r} (disk) vs {b[key]!r} (Redis)"
                )

    _walk(disk_cfg, redis_cfg, "")
    return "\n".join(lines) if lines else "  <identical>"


class EigsepFpga:
    def __init__(self, cfg=default_config, program=False):
        """
        Class for interfacing with the SNAP board.

        Parameters
        ----------
        cfg : dict
            Configuration dictionary. See `config/corr_config.yaml` for
            details.
        program : bool or str
            Whether to program the SNAP with the fpg file. Options are
            True (program if fpg_file is different from the one in
            flash), False (do not program), 'force' (always program).

        """
        self.logger = logger
        self.logger.debug("Initializing EigsepFpga")
        cfg = deepcopy(cfg)
        self.cfg = cfg
        self.pairs = cfg["pairs"]
        self.autos = [p for p in self.pairs if len(p) == 1]
        self.crosses = [p for p in self.pairs if len(p) == 2]

        # redis instance
        rcfg = self.cfg["redis"]
        self.redis = self._create_redis(rcfg["host"], rcfg["port"])

        fpg_file = Path(self.cfg["fpg_file"])
        if not fpg_file.is_absolute():
            self.fpg_file = str(get_data_path(fname=fpg_file).resolve())
        else:
            self.fpg_file = str(fpg_file.resolve())
        self.cfg["fpg_file"] = self.fpg_file

        self.fpga = self._make_fpga()
        if program:
            force = program == "force"
            self.fpga.upload_to_ram_and_program(self.fpg_file, force=force)

        if cfg["use_ref"]:
            ref = 10
        else:
            ref = None

        # blocks
        self.adc = self._make_adc(ref=ref)
        self.sync = Sync(self.fpga, "sync")
        self.noise = NoiseGen(self.fpga, "noise", nstreams=6)
        self.inp = Input(self.fpga, "input", nstreams=12)
        self.pfb = Pfb(self.fpga, "pfb")
        self.blocks = [self.sync, self.noise, self.inp, self.pfb]

        self.adc_initialized = False
        self.pams_initialized = False
        self.is_synchronized = False

    @staticmethod
    def _create_redis(host: str, port: int) -> EigsepObsRedis:
        """
        Create an EigsepObsRedis instance.

        Parameters
        ----------
        host : str
            The hostname for the Redis server.
        port : int
            The port number for the Redis server.

        Returns
        -------
        EigsepObsRedis
            An instance of EigsepObsRedis connected to the specified
            Redis server.

        """
        return EigsepObsRedis(host=host, port=port)

    def _make_fpga(self):
        """
        Construct the underlying CasperFpga object. Hookable so tests
        can substitute an in-memory register backend without patching
        casperfpga itself (which may not be importable in dev envs).
        """
        return casperfpga.CasperFpga(
            self.cfg["snap_ip"], transport=TapcpTransport
        )

    def _make_adc(self, ref):
        """
        Construct the SNAP ADC wrapper. Hookable for tests; see
        `_make_fpga`.
        """
        return casperfpga.snapadc.SnapAdc(
            self.fpga, num_chans=2, resolution=8, ref=ref
        )

    def _make_pam(self, num):
        """
        Construct a PAM block. Hookable for tests because the real
        Pam driver talks to an I2C bus via casperfpga and cannot run
        against an in-memory fpga backend.
        """
        return Pam(self.fpga, f"i2c_ant{num}")

    @property
    def version(self):
        val = self.fpga.read_uint("version_version")
        major = val >> 16
        minor = val & 0xFFFF
        return [major, minor]

    @property
    def header(self):
        """
        Generate a file header. This is a copy of the configuration
        dictionary, but replaced with the actual values from the
        SNAP when available.

        Returns
        -------
        dict
            Dictionary with keys as configuration parameters and values
            as the actual values from the SNAP board.

        """
        if self.adc_initialized:
            sample_rate = self.adc.sample_rate / 1e6  # in MHz
            adc_gain = self.adc.gain
        else:
            sample_rate = self.cfg["sample_rate"]
            adc_gain = self.cfg["adc_gain"]
        rf_chain = self.cfg["rf_chain"].copy()
        if self.pams_initialized:  # update PAM attenuation
            for ant in rf_chain["ants"]:
                try:
                    atten = self.get_pam_atten(ant)
                except OSError as e:
                    self.logger.error(
                        f"Error getting PAM attenuation for {ant}: {e}"
                    )
                    self.pams_initialized = False
                    break
                rf_chain["ants"][ant]["pam"]["atten"] = atten
        if self.is_synchronized:
            sync_time = self.sync_time
        else:
            sync_time = 0

        corr_acc_len = self.fpga.read_uint("corr_acc_len")
        acc_bins = self.cfg["acc_bins"]
        t_int = calc_inttime(
            sample_rate * 1e6,  # in Hz
            corr_acc_len,
            acc_bins=acc_bins,
        )
        m = {
            "snap_ip": self.cfg["snap_ip"],
            "fpg_file": str(self.fpg_file),
            "fpg_version": self.version,
            "sample_rate": sample_rate,
            "nchan": self.cfg["nchan"],
            "use_ref": self.cfg["use_ref"],
            "use_noise": self.cfg["use_noise"],
            "adc_gain": adc_gain,
            "fft_shift": self.pfb.get_fft_shift(),
            "corr_acc_len": corr_acc_len,
            "corr_scalar": self.fpga.read_uint("corr_scalar"),
            "corr_word": self.cfg["corr_word"],
            "acc_bins": acc_bins,
            "avg_even_odd": self.cfg["avg_even_odd"],
            "dtype": self.cfg["dtype"],
            "pol_delay": {
                "01": self.fpga.read_uint("pfb_pol01_delay"),
                "23": self.fpga.read_uint("pfb_pol23_delay"),
                "45": self.fpga.read_uint("pfb_pol45_delay"),
            },
            "redis": self.cfg["redis"],
            "sync_time": sync_time,
            "integration_time": t_int,
            "rf_chain": rf_chain,
        }
        return m

    @property
    def antennas(self):
        """
        Get the list of antennas from the configuration, with their
        digital input numbers.

        Returns
        -------
        dict
            Dictionary with antenna names as keys and their digital
            input numbers as values.

        """
        return {
            name: ant["snap"]["input"]
            for name, ant in self.cfg["rf_chain"]["ants"].items()
        }

    def validate_config(self):
        """
        Ensure that the configuration in `self.cfg` matches the
        actual hardware setup, from `self.header`.

        Raises
        ------
        RuntimeError
            If the configuration does not match the hardware setup.

        """
        fails = []
        for key, value in self.header.items():
            cfg_value = self.cfg.get(key)
            if key == "sync_time":
                continue  # sync_time is not in cfg
            if key in ("pam_atten", "pol_delay"):
                if set(value.keys()) != set(cfg_value.keys()):
                    fails.append(key)
                elif any(value[k] != cfg_value[k] for k in value.keys()):
                    fails.append(key)
            elif value != cfg_value:
                fails.append(key)
        if len(fails) > 0:
            raise RuntimeError(
                "Configuration does not match hardware setup: "
                + ", ".join(fails)
            )

    def assert_config_matches_redis(self) -> None:
        """
        Attach-path assertion: ``self.cfg`` must match the config
        already in Redis.

        The attach path (``fpga_init.py`` without ``--reinit``) does
        not re-initialize the hardware, so ``validate_config`` — which
        compares ``self.cfg`` against ``self.header`` — is vacuous for
        fields that the header echoes from ``self.cfg`` when a block's
        ``_initialized`` flag is False (ADC ``sample_rate`` /
        ``adc_gain``, PAM ``atten``). On attach, the cfg already in
        Redis is the authoritative record of the running hardware's
        intended configuration (written by the prior reinit run), so
        the safe check is "the cfg I'm about to attach with matches
        the cfg already in Redis."

        Raises
        ------
        RuntimeError
            If there is no config in Redis (cold boot — caller should
            run with ``--reinit``), or if ``self.cfg`` differs from
            the Redis-resident cfg (yaml was edited without a matching
            reinit; either revert the yaml or re-run with ``--reinit``
            to apply).
        """
        try:
            redis_cfg = self.redis.corr_config.get_config()
        except ValueError:
            raise RuntimeError(
                "No corr config in Redis; cannot attach. "
                "Run with --reinit to initialize from this config."
            )
        # Transport injects ``upload_time`` on every publish; it's not
        # part of the config contract and always differs.
        redis_cfg.pop("upload_time", None)
        if self.cfg != redis_cfg:
            diff = _cfg_diff_summary(self.cfg, redis_cfg)
            raise RuntimeError(
                "Config on disk differs from Redis-resident config:\n"
                f"{diff}\n"
                "Re-run with --reinit to apply the new config, or "
                "revert the yaml to match the running hardware."
            )

    def upload_config(self, validate: bool = True) -> None:
        """
        Upload the configuration to Redis.

        Parameters
        ----------
        validate : bool, optional
            Whether to validate the configuration with hardware
            before uploading.

        Raises
        -------
        RuntimeError
            If 'validate' is True and the configuration does not match
            the hardware configuration.

        """
        if validate:
            try:
                self.validate_config()
            except RuntimeError as e:
                self.logger.error(f"Configuration validation failed: {e}")
                raise RuntimeError("Configuration validation failed") from e
        self.logger.debug("Uploading configuration to Redis.")
        self.redis.corr_config.upload_config(self.cfg, from_file=False)

    def initialize(
        self,
        initialize_adc=True,
        initialize_fpga=True,
        sync=True,
    ):
        """
        Initialize the Eigsep correlator.

        Parameters
        ----------
        initialize_adc : bool
            Initialize the ADCs.
        initialize_fpga : bool
            Initialize the FPGA.
        sync : bool
            Synchronize the correlator clock.

        Raises
        ------
        ValueError
            If ``initialize_fpga=True`` and ``sync=False``. Rewriting
            the correlator registers (``corr_acc_len``, etc.) restarts
            the accumulation counter, so any previously-published
            ``sync_time`` is stale — the hardware must be re-synced or
            ``acc_cnt`` cannot be converted to a valid wallclock.

        Notes
        -----
        This is a convenience method that calls the methods
            - `initialize_adc`
            - `initialize_fpga`
            - `set_input`
            - `synchronize`
        in the specified order with their default parameters.

        """
        if initialize_fpga and not sync:
            raise ValueError(
                "initialize_fpga=True requires sync=True: re-initializing "
                "the FPGA registers resets acc_cnt and invalidates the "
                "previous sync_time."
            )
        if initialize_adc:
            self.logger.debug("Initializing ADCs.")
            self.initialize_adc()
        if initialize_fpga:
            self.logger.debug("Initializing FPGA.")
            self.initialize_fpga()
        self.set_input()
        if sync:
            self.logger.debug("Synchronizing correlator clock.")
            self.synchronize()
        # Explicit re-publish: initialize() is the outer state-change
        # boundary. synchronize() also publishes, so on sync=True this
        # is a redundant (but harmless, freshest-wins) upload; on
        # sync=False / ADC-only paths this is the only publish that
        # reflects the freshly-initialized state.
        self.redis.corr_config.upload_header(self.header)

    def rehydrate_sync_from_header(self):
        """
        Restore ``sync_time`` / ``is_synchronized`` from the corr header.

        Intended for the attach path — reconnecting to a SNAP that is
        already running and synced from a previous process. Without
        this, ``self.sync_time=0`` on startup and ``CorrWriter.add``
        would drop every integration until the next ``synchronize()``
        call (see :mod:`eigsep_observing.corr`). The header is the
        authoritative persisted record of the last sync: it's
        published on every state-changing call (``initialize``,
        ``synchronize``, ``set_pam_atten*``, ``set_pol_delay``) and
        persists in Redis until overwritten, so it should be present
        whenever a SNAP has been initialized and synced.

        Returns
        -------
        bool
            ``True`` if a valid ``sync_time`` was found on the header
            and restored. ``False`` if the header is missing or
            ``sync_time`` is falsy (cold boot — the caller should
            synchronize explicitly).
        """
        try:
            header = self.redis.corr_config.get_header()
        except ValueError:
            self.logger.warning(
                "No corr header in Redis; cannot rehydrate sync_time. "
                "This is expected on a cold boot — run with --reinit."
            )
            return False
        sync_time = header.get("sync_time", 0)
        if not sync_time:
            self.logger.warning(
                "Corr header present but sync_time=0; not rehydrating."
            )
            return False
        self.sync_time = sync_time
        self.is_synchronized = True
        self.logger.info(f"Rehydrated sync_time={sync_time} from corr header.")
        return True

    def _run_adc_test(self, test, n_tries):
        """
        Run a test and retry if it fails.

        Parameters
        ----------
        test : callable
            The test to run. Must return a list of failed tests.
        n_tries : int
            Number of attempts at each test before giving up.

        Raises
        ------
        RuntimeError
            If the tests do not pass after n_tries attempts.

        """
        fails = test()
        tries = 1
        while len(fails) > 0:
            self.logger.warning(f" {test.__name__} failed on: " + str(fails))
            fails = test()
            tries += 1
            if tries > n_tries:
                raise RuntimeError(f"test failed after {tries} tries")

    def initialize_adc(self, n_tries=10):
        """
        Initialize the ADC. Aligns the clock and data lanes, and runs a
        ramp test.

        Parameters
        ----------
        n_tries : int
            Number of attempts at each test before giving up. Default 10.

        Raises
        ------
        RuntimeError
            If the tests do not pass after n_tries attempts.

        Notes
        -----
        This is called by `initialize` but can be called separately.

        """
        sample_rate = self.cfg["sample_rate"]
        gain = self.cfg["adc_gain"]

        self.logger.info("Initializing ADCs")
        self.adc.init(sample_rate=sample_rate)

        self._run_adc_test(self.adc.alignLineClock, n_tries=n_tries)
        self._run_adc_test(self.adc.alignFrameClock, n_tries=n_tries)
        self._run_adc_test(self.adc.rampTest, n_tries=n_tries)

        self.adc.selectADC()
        self.adc.adc.selectInput([1, 1, 3, 3])
        self.adc.set_gain(gain)

        self.adc.sample_rate = int(sample_rate * 1e6)  # in Hz
        self.adc.gain = gain
        self.adc_initialized = True

    def initialize_fpga(self, verify=False):
        """
        Initialize the correlator.

        Notes
        -----
        This is called by `initialize` but can be called separately.

        """
        fft_shift = self.cfg["fft_shift"]
        corr_acc_len = self.cfg["corr_acc_len"]
        corr_scalar = self.cfg["corr_scalar"]
        pol_delay = self.cfg["pol_delay"]

        for blk in self.blocks:
            blk.initialize()
        try:
            self.initialize_pams()
        except OSError:
            self.logger.error("Couldn't initialize PAMs.")
            pass
        self.logger.info(f"Setting FFT_SHIFT: {fft_shift}")
        self.pfb.set_fft_shift(fft_shift)
        self.logger.info(f"Setting CORR_ACC_LEN: {corr_acc_len}")
        self.fpga.write_int("corr_acc_len", corr_acc_len)
        self.logger.info(f"Setting CORR_SCALAR: {corr_scalar}")
        self.fpga.write_int("corr_scalar", corr_scalar)
        if verify:
            assert self.fpga.read_uint("corr_acc_len") == corr_acc_len
            assert self.fpga.read_uint("corr_scalar") == corr_scalar
        self.set_pol_delay(pol_delay, verify=verify)

    def set_input(self):
        """
        Point the input mux at noise or ADC based on ``cfg["use_noise"]``.

        This only configures the mux and the noise seed; it does not arm
        the sync / noise generator or issue any ``sw_sync`` pulses. The
        arming happens in :meth:`synchronize`, which branches on the
        same config flag so that noise mode arms the noise generator and
        ADC mode arms the external sync path.

        Notes
        -----
        Called by :meth:`initialize`. Switching inputs post-init changes
        the data product (digital noise vs sky) and should be done by
        re-running ``initialize`` with the updated config, not by
        calling this directly.
        """
        self.noise.set_seed(stream=None, seed=0)
        if self.cfg["use_noise"]:
            self.logger.warning("Switching to noise input.")
            self.inp.use_noise(stream=None)
        else:
            self.logger.info("Switching to ADC input.")
            self.inp.use_adc(stream=None)

    def set_pol_delay(self, delay, verify=False):
        """
        Delay one or more input channels. The same delay is applied to
        both polarizations, so it can be set for 01, 23, and 45.

        Parameters
        ----------
        delay : dict
            Keys are "01", "23", and "45". Values (int) are the delay in
            clock cycles. Max 1024 (2^10).

        Notes
        -----
        This is called by `initialize_fpga`. Can be called separately
        to change the delay after initialization.

        """
        for key in ["01", "23", "45"]:
            dly = delay.get(key, 0)
            self.logger.info(f"Setting POL{key}_DELAY: {dly}")
            self.fpga.write_int(f"pfb_pol{key}_delay", dly)
            if verify:
                assert self.fpga.read_uint(f"pfb_pol{key}_delay") == dly
        self.redis.corr_config.upload_header(self.header)

    def initialize_pams(self):
        """
        Initialize the PAMs.

        Notes
        -----
        This is called by `initialize_fpga`.

        """
        self.pams = []
        for num in range(3):
            pam = self._make_pam(num)
            pam.initialize()
            self.pams.append(pam)
        self.blocks.extend(self.pams)
        self.pams_initialized = True

        for ant in self.cfg["rf_chain"]["ants"]:
            atten = self.cfg["rf_chain"]["ants"][ant]["pam"]["atten"]
            self.logger.info(f"Setting PAM attenuation for {ant} to {atten}")
            self.set_pam_atten(ant, atten)

    def set_pam_atten(self, ant, attenuation):
        """
        Set the attenuation for the PAMs.

        Parameters
        ----------
        ant : str
            Antenna identifier. See `self.antennas` for valid values.
        attenuation : int
            Attenuation value in dB to set for the PAM. Must be 0-15.

        Raises
        ------
        RuntimeError
            If PAMs are not initialized.

        Notes
        -----
        This is called by `initialize_pams`. Can be called separately
        to change the attenuation after initialization.

        """
        if not self.pams_initialized:
            raise RuntimeError("PAMs not initialized.")
        num = self.cfg["rf_chain"]["ants"][ant]["pam"]["num"]
        pam = self.pams[num]
        atten_e, atten_n = pam.get_attenuation()
        atten = {"E": atten_e, "N": atten_n}
        update_pol = self.cfg["rf_chain"]["ants"][ant]["pam"]["pol"]
        atten[update_pol] = attenuation
        pam.set_attenuation(atten["E"], atten["N"], verify=True)
        self.redis.corr_config.upload_header(self.header)

    def get_pam_atten(self, ant):
        """
        Get the attenuation for a PAM.

        Parameters
        ----------
        ant : str
            Antenna identifier. See `self.antennas` for valid values.

        Returns
        -------
        int
            Attenuation value in dB for the specified antenna.

        """
        if not self.pams_initialized:
            raise RuntimeError("PAMs not initialized.")
        num = self.cfg["rf_chain"]["ants"][ant]["pam"]["num"]
        pam = self.pams[num]
        atten_e, atten_n = pam.get_attenuation()
        atten = {"E": atten_e, "N": atten_n}
        pol = self.cfg["rf_chain"]["ants"][ant]["pam"]["pol"]
        return atten[pol]

    def set_pam_atten_all(self, attenuation):
        """
        Set the attenuation for all PAMs.

        Parameters
        ----------
        attenuation : int
            Attenuation value in dB to set for all PAMs. Must be 0-15.

        Raises
        ------
        RuntimeError
            If PAMs are not initialized.

        """
        if not self.pams_initialized:
            raise RuntimeError("PAMs not initialized.")
        for pam in self.pams:
            pam.set_attenuation(attenuation, attenuation, verify=True)
        self.redis.corr_config.upload_header(self.header)

    def synchronize(self, delay=0):
        """
        Synchronize the correlator clock and publish sync time to Redis.

        Branches on ``cfg["use_noise"]``: noise mode arms the noise
        generator; ADC mode arms the external sync path after setting
        the pulse-to-trigger delay. Both paths then issue three
        ``sw_sync`` pulses, record ``sync_time``, and upload the fresh
        header so ``CorrWriter.add`` stops gating.

        Parameters
        ----------
        delay : int
            Delay in FPGA clock ticks between arrival of an external
            sync pulse and the issuing of an internal trigger. Only
            used in ADC mode.

        """
        if self.cfg["use_noise"]:
            self.sync.arm_noise()
        else:
            self.sync.set_delay(delay)
            self.sync.arm_sync()
        for _ in range(3):
            self.sync.sw_sync()
        sync_time = time.time()  # not an int unless 1PPS is provided
        self.logger.info(f"Synchronized at {sync_time}.")
        self.sync_time = sync_time
        self.is_synchronized = True
        # update header in redis with fresh sync time
        self.redis.corr_config.upload_header(self.header)

    def unpack_data(self, data):
        """
        Unpack raw correlation data into numpy arrays.

        Parameters
        ----------
        data : dict
            Dictionary with keys as the input pairs and values as raw
            correlation data in bytes.

        Returns
        -------
        dict
            Dictionary with keys as the input pairs and values as numpy
            arrays of unpacked correlation data.

        """
        dt = self.cfg["dtype"]
        return {k: np.frombuffer(v, dtype=dt) for k, v in data.items()}

    def _read_spec(self, spec_type, i, unpack):
        """
        Read a single spectrum from the FPGA. This is a helper method
        for read_auto and read_cross and should not be called directly.

        Parameters
        ----------
        spec_type : str
            The type of spectrum to read, either 'auto' or 'cross'.
        i : str, list of str, or None
            The identifier of the spectrum to read, e.g. '0', '02'. If
            None, reads all spectra of the specified type.
        unpack : bool
            Whether to unpack the data into numpy arrays. Default is
            False, which returns raw bytes.

        Returns
        -------
        spec : bytes or numpy array
            The spectrum data. If unpack is True, returns a numpy array
            of integers, otherwise returns raw bytes.

        """
        if i is None:
            if spec_type == "auto":
                i = self.autos
            else:
                i = self.crosses
        elif isinstance(i, str):
            i = [i]
        if len(i) == 0:
            return {}
        # total number of bytes to read, factor of 2 is for odd/even
        nbytes = self.cfg["corr_word"] * 2 * self.cfg["nchan"]
        if spec_type == "cross":
            nbytes *= 2  # real/imag for cross correlations
        spec = {}
        for k in i:
            key = f"corr_{spec_type}_{k}_dout"
            data = self.fpga.read(key, nbytes)
            spec[k] = data
        if unpack:
            spec = self.unpack_data(spec)
        return spec

    def read_auto(self, i=None, unpack=False):
        """
        Read the i'th (counting from 0) autocorrelation spectrum.

        Parameters
        ----------
        i : str
            Which autocorrelation to read. Default is None, which reads
            all autocorrelations.
        unpack : bool
            Whether to unpack the data into numpy arrays. Default is
            False, which returns raw bytes.

        Returns
        -------
        spec : dict
            Dictionary with keys as autocorrelation identifiers and
            values as the corresponding spectra. If unpack is True,
            values are numpy arrays of integers.

        Notes
        -----
        The first half of the data is the 'even' integration, and the
        second half is the 'odd' integration.

        """
        return self._read_spec("auto", i, unpack)

    def read_cross(self, ij=None, unpack=False):
        """
        Read the cross-correlation spectrum between inputs i and j.

        Parameters
        ----------
        ij : str
            Which cross-correlation to read. Default is None, which
            reads all cross-correlations.
        unpack : bool
            Whether to unpack the data into numpy arrays. Default is
            False, which returns raw bytes.

        Returns
        -------
        spec : dict
            Dictionary with keys as autocorrelation identifiers and
            values as the corresponding spectra. If unpack is True,
            values are numpy arrays of integers.

        Notes
        -----
        The first half of the data is the 'even' integration, and the
        second half is the 'odd' integration. Real and imaginary parts
        are interleaved, with every other sample being the real part
        and the following sample being the imaginary part.

        """
        return self._read_spec("cross", ij, unpack)

    def read_data(self, pairs=None, unpack=False):
        """
        Read even/odd spectra for correlations specified in pairs.

        Parameters
        ----------
        pairs : str, list of str or None
            List of pairs to read. If None, reads all pairs. If a
            string, reads the single pair specified.
        unpack : bool
            Whether to unpack the data into numpy arrays. Default is
            False, which returns raw bytes.

        Returns
        -------
        data : dict
            Dictionary with keys as correlation identifiers and values
            as the corresponding spectra. If unpack is True, values are
            numpy arrays of integers.

        """
        data = {}
        if pairs is None:
            pairs = self.pairs
        elif type(pairs) is str:
            pairs = [pairs]
        data = self.read_auto([p for p in pairs if len(p) == 1], unpack=unpack)
        data.update(
            self.read_cross([p for p in pairs if len(p) != 1], unpack=unpack)
        )
        return data

    def _read_integrations(self, pairs, timeout=10):
        """
        Read integrated correlations from the SNAP board.

        Parameters
        ----------
        pairs : list
            List of pairs to read.
        timeout : float
            Number of seconds to wait for a new integration before
            timing out.

        """
        cnt = self.fpga.read_int("corr_acc_cnt")
        t = time.time()

        while time.time() < t + timeout and not self.event.is_set():
            new_cnt = self.fpga.read_int("corr_acc_cnt")
            if new_cnt == cnt:
                time.sleep(0.01)
                continue
            if new_cnt > cnt + 1:
                self.logger.warning(
                    f"Missed {new_cnt - cnt - 1} integration(s)."
                )
            cnt = new_cnt
            self.logger.info(f"Reading acc_cnt={cnt}")
            data = self.read_data(pairs=pairs, unpack=False)
            if cnt != self.fpga.read_int("corr_acc_cnt"):
                self.logger.error(
                    f"Read of acc_cnt={cnt} FAILED to complete before "
                    "next integration."
                )
            self.queue.put({"data": data, "cnt": cnt})
            t = time.time()

    def end_observing(self):
        try:
            self.event.set()
            self.queue.put(None)  # signals end of observing
        except AttributeError:
            self.logger.error("Observation not started or already ended.")

    def observe(self, pairs=None, timeout=10):
        """
        Read correlator data and stream it to Redis.

        Parameters
        ----------
        pairs : list of str
            List of correlation pairs to read. If None, all pairs are
            read and streamed.
        timeout : int
            Timeout in seconds for reading data from the correlator.

        Raises
        -------
        TimeoutError
            If the read operation times out.

        """
        self.queue = Queue(maxsize=0)
        self.event = Event()
        self.upload_config(validate=True)
        t_int = self.header["integration_time"]
        self.logger.info(f"Integration time is {t_int} seconds.")
        if pairs is None:
            pairs = self.pairs
        self.logger.info(f"Starting observation for pairs: {pairs}.")

        thd = Thread(
            target=self._read_integrations,
            args=(pairs,),
            kwargs={"timeout": timeout},
        )
        thd.start()

        while not self.event.is_set() or not self.queue.empty():
            d = self.queue.get()
            if d is None:
                if self.event.is_set():
                    self.logger.info("End of queue, processing finished.")
                    break
                else:
                    continue
            data = d["data"]
            cnt = d["cnt"]
            sync_time = self.sync_time if self.is_synchronized else 0
            self.redis.corr.add(data, cnt, sync_time, dtype=self.cfg["dtype"])
