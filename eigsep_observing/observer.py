from itertools import cycle
import logging
import queue
import time
from threading import Event, Thread

from . import EigsepRedis, io
from .config import default_obs_config


def make_schedule(switch_schedule):
    """
    Create a schedule for switching between VNA and SNAP observing. This
    creates a cycle object that can iterate indefinitely over the switch
    schedule.

    Parameters
    ----------
    switch_schedule : dict
        The switch schedule used for observing. A dictionary with keys
        `vna'', ``snap_repeat'', ``sky'', ``load'', and ``noise''. The
        first two keys specify the number of measurements with the VNA and
        the SNAP respectivtly. When measuring PSDs with the SNAP, the
        ``sky'', ``load'', and ``noise'' keys specify the number of
        measurements to take for each state.

    Returns
    -------
    schedule : cycle
        A cycle object that iterates over the switch schedule. The schedule
        is a list of tuples, where each tuple contains the state and the
        number of measurements to take.

    Notes
    -----
    Defaults are 0 for all states, except for ``snap_repeat'', which is 1.

    """
    n_vna = switch_schedule.get("vna", 0)
    if n_vna > 0:
        schedule = [("vna", n_vna)]
    else:
        schedule = []
    block = [(k, switch_schedule.get(k, 0)) for k in ("sky", "load", "noise")]
    block = [x for x in block if x[1] > 0]
    n_repeat = switch_schedule.get("snap_repeat", 1)
    if n_repeat > 0:
        schedule += n_repeat * block
    return cycle(schedule)


class EigObserver:

    def __init__(self, fpga, cfg=default_obs_config, logger=None):
        """
        Main controll class for Eigsep observing. This code is meant to run
        on the Raspberry Pi. It uses EigsepFpga to initialize observing
        with the SNAP correlator and communicates with the LattePanda in the
        EIGSEP box via Redis. It pulls sensor readings from the LattePanda
        and puts them in the file headers, synchronized with the data stream.

        Parameters
        ----------
        fpga : EigsepFpga
            The EigsepFpga object to use for observing.
        cfg : eigsep_observing.config.ObsConfig
            The configuration object to use for observing. This is a
            data class specifying the sensors and switch schedule to use.

        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)
        self.logger = logger
        self.fpga = fpga
        self.cfg = cfg
        self.redis = EigsepRedis()

        picos = {}
        for name, pico in self.cfg.sensors.items():
            picos[name] = pico
        picos["switch"] = self.cfg.switch_pico
        self.redis.send_ctrl("init:picos", **picos)

    def set_mode(self, mode):
        """
        Switch observing mode with RF switches and start VNA observing if
        needed.

        Parameters
        ----------
        mode : str
            Observing mode. Either ``sky'', ``load'', ``noise'' for
            correlations, or ``ant''  or ``rec'' for S11 measurements with
            the VNA.

        Raises
        ------
        ValueError
            If the mode is not one of the valid modes.

        """
        if mode in ("ant", "rec"):
            self.logger.info(f"Switching to VNA mode, measuring {mode}")
            try:
                self.observe_vna(mode)
            except ValueError as e:
                self.logger.error(f"VNA error: {e}")
                raise
            except TimeoutError:
                self.logger.error("VNA timeout. Check connection.")
            except RuntimeError as e:
                self.logger.error(f"Unexpected status: {e}")
        elif mode in ("sky", "load", "noise"):
            self.logger.info(f"Switching to {mode} measurements")
            if mode == "sky":
                redis_cmd = "switch:RFANT"
            elif mode == "load":
                redis_cmd = "switch:RFLOAD"
            elif mode == "noise":
                redis_cmd = "switch:RFN"
            self.redis.send_ctrl(redis_cmd)
        else:
            raise ValueError(
                f"Invalid mode: {mode}. Must be one of "
                "'sky', 'load', 'noise', 'ant', or 'rec'."
            )

    def observe_vna(self, mode, timeout=300):
        """
        VNA observations. Performs OSL calibration measurements and
        measurment of the device(s) under test.

        Parameters
        ----------
        mode : str
            The mode to set. Either ``ant'' or ``rec''. The former
            case measures S11 of antenna and noise source. The latter
            uses less power and measures S11 of the receiver.
        timeout : int
            The time in seconds to wait for the VNA to complete.

        Raises
        ------
        ValueError
            If the mode is not one of the valid VNA commands.
        TimeoutError
            If the VNA does not complete within the timeout period.
        RuntimeError
            If the VNA returns an error status.

        """
        cmd = f"vna:{mode}"
        if cmd not in self.redis.vna_commands:
            raise ValueError(f"Invalid VNA command: {cmd}.")

        kwargs = {
            "ip": self.cfg.vna_ip,
            "port": self.cfg.vna_port,
            "timeout": self.cfg.vna_timeout,
            "save_dir": self.cfg.vna_save_dir,
            "fstart": self.cfg.vna_fstart,
            "fstop": self.cfg.vna_fstop,
            "npoints": self.cfg.vna_npoints,
            "ifbw": self.cfg.vna_ifbw,
            "power_dBm": self.cfg.vna_power[mode],
        }

        self.redis.send_ctrl(cmd, **kwargs)
        tstart = time.time()
        while True:
            entry_id, status = self.redis.read_status()
            if status == "VNA_TIMEOUT" or time.time() - tstart > timeout:
                raise TimeoutError
            if status is None:
                if entry_id is None:
                    self.logger.debug("No message yet. Waiting.")
                else:
                    self.logger.warning("Invalid status. Waiting.")
                time.sleep(1)
                continue
            if status != "VNA_COMPLETE":
                raise RuntimeError(f"VNA error, status: {status}")
            self.logger.info("VNA observation complete.")
            return

    # XXX
    def rotate_motors(self, motors):
        raise NotImplementedError

    # XXX how to handle motors?
    def observe(
        self,
        pairs=None,
        timeout=10,
        update_redis=True,
        write_files=True,
    ):
        """
        Start observing, reading data from the correlator and optionally
        the VNA. This method implements automatic switching between
        observing modes according to the switch schedule. Metadata is
        pushed and collected from Redis, and data is written to files.

        Parameters
        ----------
        pairs : list
            The list of pairs to observe. If None, all pairs will be
            observed.
        timeout : int
            The time in seconds to wait for data from the correlator.
        update_redis : bool
            Push data to Redis.
        write_files : bool
            Write data to files.


        """
        if pairs is None:
            pairs = self.fpga.autos + self.fpga.crosses

        self.fpga.queue = queue.Queue(maxsize=0)
        self.fpga.pause_event = Event()
        self.fpga.stop_event = Event()

        self.pause_event.set()

        thd = Thread(
            target=self.fpga._read_integrations,
            args=(pairs),
            kwargs={"timeout": timeout},
        )
        thd.start()

        if write_files:
            self.file = io.File(
                self.fpga.cfg.save_dir,
                pairs,
                self.fpga.cfg.ntimes,
                self.fpga.header,
                redis=self.redis,
            )

        self.schedule_cycle = make_schedule(self.cfg.switch_schedule)
        remaining = -1  # initialize remaining to trigger first switch
        while not self.fpga.stop_event.is_set():
            if remaining <= 0:
                self.fpga.pause_event.set()
                # drain queue here since we've read what we wanted to
                while True:
                    try:
                        _ = self.fpga.queue.get_nowait()
                    except queue.Empty:
                        break
                mode, remaining = next(self.schedule_cycle)
                self.set_mode(mode)
                self.fpga.pause_event.clear()
            try:
                d = self.fpga.queue.get(block=True, timeout=timeout)
            except queue.Empty:
                self.logger.warning(
                    f"Queue empty after {timeout} seconds. "
                    "Continuing to wait for data."
                )
                continue
            if d is None:
                if self.fpga.stop_event.is_set():
                    self.logger.info("Stopping observing.")
                    break
                continue
            data = d["data"]  # data is a dict with bytes
            cnt = d["cnt"]
            if update_redis:
                self.fpga.update_redis(data, cnt)  # push bytes to Redis
            if write_files:
                # unpack data from bytes for writing to file
                unpacked_data = self.fpga.unpack_data(data)
                filename = self.file.add_data(unpacked_data)
                if filename is not None:
                    self.logger.info(f"Writing file {filename}")
            remaining -= 1
        if self.file is not None:
            if len(self.file) > 0:
                self.logger.info("Writing short final file.")
                self.file.corr_write()

        thd.join()
        self.logger.info("Observing complete.")

    def end_observing(self):
        self.fpga.end_observing()
