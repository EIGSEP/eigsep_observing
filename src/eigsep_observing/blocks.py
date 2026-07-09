import logging
import numpy as np
import struct
import time

try:
    from casperfpga import i2c
    from casperfpga import i2c_gpio
    from casperfpga import i2c_volt
    from casperfpga import i2c_eeprom
    from casperfpga import i2c_sn
except ImportError:
    logging.warning("Running without casperfpga installed")

# There are so many I2C warnings that a new level is defined
# to filter them out
I2CWARNING = logging.INFO - 5
logging.addLevelName("I2CWARNING", I2CWARNING)

HIST_BINS = np.arange(-128, 128)  # for histogram of ADC inputs
ERROR_VALUE = -1  # default value for status reports if comms are broken
ERROR_STRING = (
    "UNKNOWN"  # default string for status reports if comms are broken
)


# Block Classes
class Block:
    def __init__(self, host, name, logger=None):
        self.host = host  # casperfpga object
        # One logger per host. Multiple blocks share the same logger.
        # Multiple hosts should *not* share the same logger,
        # since we can multithread over hosts.
        if logger is None:
            logger = logging.getLogger(__name__)
        self.logger = logger
        self.name = name
        if (name is None) or (name == ""):
            self.prefix = ""
        else:
            self.prefix = name + "_"

    def _prefix_log(self, msg):
        """
        Take a log message, and prefix it with "<block> - ".
        Eg, take "Argh, I'm broken" and replace it with
        "eq_tvg - Argh I'm broken"
        """
        prefix = "%s - " % self.name
        return prefix + msg

    def _debug(self, msg, *args, **kwargs):
        self.logger.debug(self._prefix_log(msg), *args, **kwargs)

    def _info(self, msg, *args, **kwargs):
        self.logger.info(self._prefix_log(msg), *args, **kwargs)

    def _warning(self, msg, *args, **kwargs):
        self.logger.warning(self._prefix_log(msg), *args, **kwargs)

    def _error(self, msg, *args, **kwargs):
        self.logger.error(self._prefix_log(msg), *args, **kwargs)

    def _critical(self, msg, *args, **kwargs):
        self.logger.critical(self._prefix_log(msg), *args, **kwargs)

    def _exception(self, msg, *args, **kwargs):
        self.logger.exception(self._prefix_log(msg), *args, **kwargs)

    def initialize(self, verify=False):
        """
        Individual blocks should override this
        method to configure themselves appropriately
        """
        pass

    def listdev(self):
        """
        return a list of all register names within
        the block.
        """
        devs = self.host.listdev()
        return [
            x[len(self.prefix) :] for x in devs if x.startswith(self.prefix)
        ]

    def read_int(self, reg, word_offset=0, **kwargs):
        return self.host.read_int(
            self.prefix + reg, word_offset=word_offset, **kwargs
        )

    def write_int(self, reg, val, word_offset=0, **kwargs):
        self.host.write_int(
            self.prefix + reg, val, word_offset=word_offset, **kwargs
        )

    def read_uint(self, reg, word_offset=0, **kwargs):
        return self.host.read_uint(
            self.prefix + reg, word_offset=word_offset, **kwargs
        )

    def write_uint(self, reg, val, word_offset=0, **kwargs):
        self.host.write_int(
            self.prefix + reg, val, word_offset=word_offset, **kwargs
        )

    def read(self, reg, nbytes, **kwargs):
        return self.host.read(self.prefix + reg, nbytes, **kwargs)

    def write(self, reg, val, offset=0, **kwargs):
        self.host.write(self.prefix + reg, val, offset=offset, **kwargs)

    def blindwrite(self, reg, val, **kwargs):
        self.host.blindwrite(self.prefix + reg, val, **kwargs)

    def set_reg_bits(self, reg, val, start, width=1):
        orig_val = self.read_uint(reg)
        masked = orig_val & (0xFFFFFFFF - ((2**width - 1) << start))
        new_val = masked + (val << start)
        self.write_uint(reg, new_val)

    def get_reg_bits(self, reg, start, width=1):
        val = self.read_uint(reg)
        val = val >> start
        val &= 2**width - 1
        return val


class Sync(Block):
    def __init__(self, host, name, logger=None):
        super().__init__(host, name, logger=logger)
        self.OFFSET_ARM_SYNC = 0
        self.OFFSET_ARM_NOISE = 1
        self.OFFSET_SW_SYNC = 4

    def uptime(self):
        """
        Returns uptime in seconds, assumes 250 MHz FPGA clock
        """
        try:
            return self.read_uint("uptime")
        except RuntimeError:
            return ERROR_VALUE

    def set_delay(self, delay):
        """
        Set the delay, in FPGA clock ticks, between the arrival of an external
        sync pulse and the issuing of an internal trigger.
        inputs:
            delay (integer) : Number of FPGA clocks delay
        """
        self.write_int("sync_delay", delay)

    def period(self):
        """
        Returns period of sync in pulses, in FPGA clock ticks
        """
        return self.read_uint("period")

    def change_period(self, period):
        """
        Change the period of the sync pulse
        """
        # self.host.write_int('timebase_sync_period', period)
        self._info("Changed sync period to %.2f" % period)

    def count(self):
        """
        Returns Number of external sync pulses received.
        """
        return self.read_uint("count")

    def wait_for_sync(self):
        """
        Block until a sync has been received.
        """
        c = self.count()
        while self.count() == c:
            time.sleep(0.05)

    def arm_sync(self):
        """
        Arm sync pulse generator.
        """
        for i in [0, 1, 0]:
            self.set_reg_bits("arm", i, self.OFFSET_ARM_SYNC)

    def arm_noise(self):
        """
        Arm noise generator resets
        """
        for i in [0, 1, 0]:
            self.set_reg_bits("arm", i, self.OFFSET_ARM_NOISE)

    def sw_sync(self):
        """
        Issue a software sync pulse
        """
        for i in [0, 1, 0]:
            self.set_reg_bits("arm", i, self.OFFSET_SW_SYNC)

    def initialize(self, verify=False):
        """
        Initialize this block. Set sync period to 0.
        """
        self.write_int("arm", 0)
        # self.change_period(2**16 * 9*7*6*5*3)
        self.change_period(0)
        if verify:
            assert self.read_int("arm") == 0


class NoiseGen(Block):
    def __init__(self, host, name, nstreams=6, logger=None):
        super().__init__(host, name, logger=logger)
        self.nstreams = nstreams

    def set_seed(self, stream=None, seed=0, verify=True):
        """
        Set the seed of the noise generator for a given stream. Six 8-b
        seeds are stored in two 32-b registers from LSB to MSB. Of these,
        only seeds 0, 2, and 4 are used, going to the 3 LFSRs that serve
        inputs 0-1, 2-3, and 4-5 respectively.

        Inputs:
            stream (int): Which stream to switch. If None, switch all.
            seed (int): int 0-255 for seeding digital noise generator.
        """
        if stream is None:
            for stm in range(self.nstreams):
                self.set_seed(stream=stm, seed=seed, verify=verify)
        else:
            assert stream < self.nstreams
            assert seed < 256
            regname = "seed_%d" % (stream // 4)
            self.set_reg_bits(regname, seed, 8 * (stream % 4), 8)
            if verify:
                assert seed == self.get_seed(stream)

    def get_seed(self, stream=None):
        """
        Get the seed of the noise generator for a given stream.

        Inputs:
            stream (int): Which stream to switch. If None, switch all.
        """
        if stream is None:
            return [self.get_seed(stm) for stm in range(self.nstreams)]
        assert stream < self.nstreams
        regname = "seed_%d" % (stream // 4)
        return (self.read_uint(regname) >> (8 * (stream % 4))) & 0xFF

    def initialize(self, verify=False):
        self.set_seed(verify=verify)


class Input(Block):
    def __init__(self, host, name, nstreams=6, logger=None):
        """
        Instantiate an input contol block.

        Inputs:
            host (casperfpga.CasperFpga): Host FPGA object
            name (string): Name (in simulink) of this block
            nstreams (int): Number of streams this block handles
        """
        super().__init__(host, name, logger=logger)
        self.nstreams = nstreams
        self.ninput_mux_streams = nstreams // 2
        self.USE_NOISE = 0
        self.USE_ADC = 1
        self.USE_ZERO = 2
        self.INT_TIME = 2**20 / 250.0e6
        self._SNAPSHOT_SAMPLES_PER_POL = 2048
        # Cached result of ``"snap_sel" in self.listdev()``. The
        # bitstream's register map is fixed for the run, so dispatch
        # only needs to check once. Lazy-evaluated on first use to
        # keep ``__init__`` from hitting the FPGA.
        self._has_snap_sel = None

    def get_status(self):
        """Return dict of current status."""
        rv = {}
        snapshots = {}
        for stream in range(
            self.ninput_mux_streams // 2
        ):  # bram holds stream and stream+1
            (
                snapshots[2 * stream],
                snapshots[2 * stream + 1],
            ) = self.get_adc_snapshot(stream)
        for stream, snapshot in snapshots.items():
            rv["stream%d_hist" % stream] = np.histogram(
                snapshot, bins=HIST_BINS
            )[0]
            rv["stream%d_mean" % stream] = np.mean(snapshot)
            pwr = np.mean(np.abs(snapshot) ** 2)
            rv["stream%d_power" % stream] = pwr
            rv["stream%d_rms" % stream] = np.sqrt(pwr)
        return rv

    def get_adc_snapshot(self, antenna):
        """
        Get a block of samples from both pols of `antenna`
        returns samples_x, samples_y
        """
        if self._has_snap_sel is None:
            self._has_snap_sel = "snap_sel" in self.listdev()
        if self._has_snap_sel:
            return self._get_adc_snapshot_single_ant(antenna)
        else:
            return self._get_adc_snapshot_all_ants(antenna)

    def _get_adc_snapshot_single_ant(self, antenna):
        """
        Get a block of samples from both pols of `antenna`
        returns samples_x, samples_y
        """
        self.write_int("snap_sel", antenna)
        self.write_int("snapshot_ctrl", 0)
        self.write_int("snapshot_ctrl", 1)
        self.write_int("snapshot_ctrl", 3)
        d = struct.unpack(
            ">%db" % (2 * self._SNAPSHOT_SAMPLES_PER_POL),
            self.read("snapshot_bram", 2 * self._SNAPSHOT_SAMPLES_PER_POL),
        )
        x = []
        y = []
        for i in range(self._SNAPSHOT_SAMPLES_PER_POL // 2):
            x += [d[4 * i]]
            x += [d[4 * i + 1]]
            y += [d[4 * i + 2]]
            y += [d[4 * i + 3]]
        return np.array(x), np.array(y)

    def _get_adc_snapshot_all_ants(self, antenna):
        """
        Get a block of samples from both pols of `antenna`
        returns samples_x, samples_y
        """
        self.write_int("snapshot_ctrl", 0)
        self.write_int("snapshot_ctrl", 1)
        self.write_int("snapshot_ctrl", 3)
        d = struct.unpack(
            ">%db" % (16 * self._SNAPSHOT_SAMPLES_PER_POL // 2),
            self.read(
                "snapshot_bram", 16 * self._SNAPSHOT_SAMPLES_PER_POL // 2
            ),
        )
        x = []
        y = []
        for i in range(self._SNAPSHOT_SAMPLES_PER_POL // 2):
            # Add 1 to antenna since there is a dummy ant 0 which is all zeros
            x += [d[16 * i + 4 * (antenna + 1)]]
            x += [d[16 * i + 4 * (antenna + 1) + 1]]
            y += [d[16 * i + 4 * (antenna + 1) + 2]]
            y += [d[16 * i + 4 * (antenna + 1) + 3]]
        return np.array(x), np.array(y)

    def get_power_spectra(self, antenna, acc_len=1):
        """
        Perform a software FFT of samples from `antenna`.
        Accumulate power from `acc_len` snapshots.
        returns power_spectra_X, power_spectra_Y
        """
        X = np.zeros(self._SNAPSHOT_SAMPLES_PER_POL // 2 + 1)
        Y = np.zeros(self._SNAPSHOT_SAMPLES_PER_POL // 2 + 1)
        for i in range(acc_len):
            x, y = self.get_adc_snapshot(antenna)
            X += np.abs(np.fft.rfft(x)) ** 2
            Y += np.abs(np.fft.rfft(y)) ** 2
        return X, Y

    def _select_input(self, input_code, stream=None, verify=False):
        if stream is None:
            streams = list(range(self.ninput_mux_streams))
        else:
            streams = [stream]
        for stream in streams:
            self.set_reg_bits(
                "source_sel",
                input_code,
                2 * (self.ninput_mux_streams - 1 - stream),
                2,
            )
            if verify:
                assert (
                    self.get_reg_bits(
                        "source_sel",
                        2 * (self.ninput_mux_streams - 1 - stream),
                        2,
                    )
                    == input_code
                )

    def use_noise(self, stream=None, verify=False):
        """
        Switch input to internal noise source.
        Inputs:
            stream (int): Which stream to switch. If None, switch all.
        """
        self._select_input(self.USE_NOISE, stream=stream, verify=verify)

    def use_adc(self, stream=None, verify=False):
        """
        Switch input to ADC.
        Inputs:
            stream (int): Which stream to switch. If None, switch all.
        """
        self._select_input(self.USE_ADC, stream=stream, verify=verify)

    def use_zero(self, stream=None, verify=False):
        """
        Switch input to zeros.
        Inputs:
            stream (int): Which stream to switch. If None, switch all.
        """
        self._select_input(self.USE_ZERO, stream=stream, verify=verify)

    def initialize(self, verify=False):
        """
        Switch to ADCs.
        """
        self.use_adc(verify=verify)


class Pfb(Block):
    def __init__(self, host, name, logger=None):
        super().__init__(host, name, logger=logger)
        self.SHIFT_OFFSET = 0
        self.SHIFT_WIDTH = 16
        self.PRESHIFT_OFFSET = 16
        self.PRESHIFT_WIDTH = 2
        self.STAT_RST_BIT = 18

    def get_fft_shift(self):
        """
        Return the current FFT shift schedule (LSB = stage 1, MSB = stage N).
        """
        return self.get_reg_bits("ctrl", self.SHIFT_OFFSET, self.SHIFT_WIDTH)

    def set_fft_shift(self, shift, verify=False):
        """
        Set the FFT shift schedule to the specified unsigned integer
        (LSB = stage 1, MSB = stage N).
        """
        self.set_reg_bits("ctrl", shift, self.SHIFT_OFFSET, self.SHIFT_WIDTH)
        if verify:
            assert shift == self.get_fft_shift()

    def get_fft_preshift(self):
        """
        Return the current FFT preshift value.
        """
        return self.get_reg_bits(
            "ctrl", self.PRESHIFT_OFFSET, self.PRESHIFT_WIDTH
        )

    def set_fft_preshift(self, shift, verify=False):
        self.set_reg_bits(
            "ctrl", shift, self.PRESHIFT_OFFSET, self.PRESHIFT_WIDTH
        )
        if verify:
            assert shift == self.get_fft_preshift()

    def rst_stats(self):
        for i in [1, 0]:
            self.set_reg_bits("ctrl", i, self.STAT_RST_BIT)

    def is_overflowing(self):
        return self.read_uint("status") != 0

    def initialize(self, fft_shift=0b110101010101, verify=False):
        self.write_int("ctrl", 0)
        self.set_fft_shift(fft_shift, verify=verify)
        self.rst_stats()


class Pam(Block):
    ADDR_VOLT = 0x36
    ADDR_ROM = 0x52
    ADDR_SN = 0x50
    ADDR_INA = 0x44
    ADDR_GPIO = 0x21

    CLK_I2C_BUS = 10  # 10 kHz
    CLK_I2C_REF = 100  # reference clk at 100 MHz

    I2C_RETRY = 3

    SHUNT_RESISTOR = 0.1

    RMS2DC_SLOPE = 27.31294863
    RMS2DC_INTERCEPT = -55.15991678

    def __init__(self, host, name, logger=None):
        """Post Amplifier Module (PAM) digital control class
        Features
        attenuation Attenuation for East and North Pol
        shunt       Voltage and current of the power supply
        rom         Memo
        id          Device ID
        power       Power level of East and North Pol

        host: CasperFpga instance
        name: Select one of the three PAMs(/Antennas) under the control of
                a SNAP board: 'i2c_ant1', 'i2c_ant2' or 'i2c_ant3'.
        """
        super().__init__(host, name, logger=logger)

        self.i2c = i2c.I2C(host, name, retry=self.I2C_RETRY)
        self._cached_atten = None  # for checking I2C stability

    def _warning(self, msg, *args, **kwargs):
        self.logger.log(I2CWARNING, self._prefix_log(msg), *args, **kwargs)

    def initialize(self, verify=False):
        self.i2c.enable_core()
        # set i2c bus to 10 kHz
        self.i2c.setClock(self.CLK_I2C_BUS, self.CLK_I2C_REF)

    def get_status(self):
        """Return a dict of config status."""
        rv = {}
        try:
            rv["atten_e"], rv["atten_n"] = self.get_attenuation()
            rv["power_e"] = self.power("east")
            rv["power_n"] = self.power("north")
            rv["voltage"] = self.shunt("u")
            rv["current"] = self.shunt("i")
            rv["id"] = self.id()
        except (RuntimeError, IOError):
            rv["atten_e"] = ERROR_VALUE
            rv["atten_n"] = ERROR_VALUE
            rv["power_e"] = ERROR_VALUE
            rv["power_n"] = ERROR_VALUE
            rv["voltage"] = ERROR_VALUE
            rv["current"] = ERROR_VALUE
            rv["id"] = ERROR_STRING
        return rv

    def get_attenuation(self):
        """Get East and North attenuation
        returns: (east attenuation (dB int), north (dB int)
        """
        if not hasattr(self, "_atten"):
            self._atten = i2c_gpio.PCF8574(self.i2c, self.ADDR_GPIO)
        val = self._atten.read()
        east, north = self._gpio2db(val)
        if self._cached_atten is not None and self._cached_atten != (
            east,
            north,
        ):
            self._warning(
                "Read value %s != written value %s"
                % ((east, north), self._cached_atten)
            )
        return east, north

    def set_attenuation(self, east, north, verify=False):
        """Set East and North attenuation in dB
        attenuation values must be integers in range(16)

        Example:
        attenuation(east=0,north=15)
        """
        if not hasattr(self, "_atten"):
            self._atten = i2c_gpio.PCF8574(self.i2c, self.ADDR_GPIO)

        self._atten.write(self._db2gpio(east, north))
        self._cached_atten = (east, north)  # cache for stability check
        if verify:
            assert (east, north) == self.get_attenuation()

    def shunt(self, name="i"):
        """Get current/voltage of the power supply

        Example:
        shunt(name='i')     # returns current in Amps
        shunt(name='u')     # returns voltage in Volt
        """
        if not hasattr(self, "_cur"):
            try:
                # Current sensor
                self._cur = i2c_volt.INA219(self.i2c, self.ADDR_INA)
                self._cur.init()
            except AttributeError:
                raise AttributeError
            except Exception as e:
                self._warning(
                    "Failed to initialize I2C current sensor: " + str(e)
                )
                return None

        try:
            if name == "i":
                vshunt = self._cur.readVolt("shunt")
                ishunt = vshunt * 1.0 / self.SHUNT_RESISTOR
                return ishunt if ishunt < 6 else None
            elif name == "u":
                vbus = self._cur.readVolt("bus")
                return vbus if vbus < 32 else None
            else:
                raise ValueError("Invalid parameter.")
        except Exception:
            self._warning("Failed to read I2C PAM current sensor")
            del self._cur
            return None

    def id(self):
        """Get the unique eight-byte serial number of the module"""
        if not hasattr(self, "_id"):
            try:
                # ID chip
                self._sn = i2c_sn.DS28CM00(self.i2c, self.ADDR_SN)
            except AttributeError:
                raise AttributeError
            except Exception as e:
                self._warning("Failed to initialize I2C ID chip:  " + str(e))
                return None

        try:
            return self._sn.readSN()
        except Exception:
            self._warning("Failed to read I2C ID chip")
            return None

    def power(self, name="east"):
        """Get power level of the East or North RF path

        Example:
        power(name='east')  # returns power level of east in dBm
        power(name='north') # returns power level of north in dBm
        """
        if not hasattr(self, "_pow"):
            try:
                # Power detector
                self._pow = i2c_volt.MAX11644(self.i2c, self.ADDR_VOLT)
                self._pow.init()
            except AttributeError:
                raise AttributeError
            except Exception as e:
                self._warning(
                    "Failed to initialize I2C power sensor: " + str(e)
                )
                return None
        LOSS = 9.8
        if name not in ["east", "north"]:
            raise ValueError("Invalid parameter.")

        try:
            if name == "east":
                vp = self._pow.readVolt("AIN0")
            elif name == "north":
                vp = self._pow.readVolt("AIN1")

            assert vp >= 0 and vp <= 3.3

            return (
                self._dc2dbm(vp, self.RMS2DC_SLOPE, self.RMS2DC_INTERCEPT)
                + LOSS
            )
        except Exception:
            self._warning("Failed to read I2C power sensor")
            del self._pow
            return None

    def rom(self, string=None):
        """Read string from ROM or write String to ROM

        Example:
        rom()               # returns a string ended with a '\0'
        rom('hello')        # write 'hello\0' into ROM
        """
        if not hasattr(self, "_rom"):
            try:
                # ROM
                self._rom = i2c_eeprom.EEP24XX64(self.i2c, self.ADDR_ROM)
            except AttributeError:
                raise AttributeError
            except Exception as e:
                self._warning("Failed to initialize I2C ROM: " + str(e))
                return None

        try:
            if string is None:
                return self._rom.readString()
            else:
                self._rom.writeString(string)
        except Exception:
            self._warning("Failed to operate I2C ROM")
            return None

    def _db2gpio(self, ae, an):
        assert ae in range(0, 16)
        assert an in range(0, 16)
        ae = 15 - ae
        an = 15 - an
        val_str = "{0:08b}".format((ae << 4) + an)
        val = int(val_str, 2)
        return val

    def _gpio2db(self, val):
        assert val in range(0, 256)
        val_str = "{0:08b}".format(val)
        ae = int(val_str[0:4], 2)
        an = int(val_str[4:8], 2)
        return 15 - ae, 15 - an

    def _dc2dbm(self, val, slope, intercept):
        assert val >= 0 and val <= 3.3, (
            "Input value {} out range of 0-3.3V".format(val)
        )
        res = val * slope + intercept
        return res
