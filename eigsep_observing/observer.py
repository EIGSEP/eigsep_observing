from itertools import cycle
import logging
import queue
import time
from threading import Event, Thread

from switch_network import SwitchNetwork

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

    def __init__(
        self,
        cfg=default_obs_config,
        fpga=None,
        logger=None,
    ):
        """
        Main controll class for Eigsep observing. This code is meant to run
        on the Raspberry Pi. It uses EigsepFpga to initialize observing
        with the SNAP correlator and communicates with the LattePanda in the
        EIGSEP box via Redis. It pulls sensor readings from the LattePanda
        and puts them in the file headers, synchronized with the data stream.

        Parameters
        ----------
        cfg : eigsep_observing.config.ObsConfig
            The configuration object to use for observing. This is a
            data class specifying the sensors and switch schedule to use.
        fpga : EigsepFpga
            The EigsepFpga object to use for observing.

        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)
        self.logger = logger
        self._use_vna = cfg.switch_schedule.get("vna", 0) > 0
        self.fpga = fpga
        if self.fpga is None:
            self.logger.warning(
                "No Fpga instance. Running isolated VNA measurements. "
            )
            cfg.switch_schedule["snap_repeat"] = 0  # no SNAP observing

            if self._use_vna:
                raise ValueError(
                    "In isolated VNA mode, but no VNA measurements "
                )

        self.cfg = cfg
        self.redis = EigsepRedis()
        self.sw_network = SwitchNetwork(serport=cfg.pico_id["switch"])

        if self._use_vna:
            self.init_vna()  # XXX does this do anything?

        if self.cfg.sensors is not None:
            self.init_sensors()

    def init_sensors(self):
        """
        Use Redis to initialize observing with senors. Need to send START
        command to get pico scripts running and make the observe function
        ready to pick up the data stream and place into the header.
        """
        pass

    def set_mode(self, mode, update_redis=True):
        self.logger.info(f"Switching to {mode} mode")
        # XXX need to put some list into redis of all the modes in the file
        if update_redis:
            self.redis.add_metadata(
        self.sw_network.switch(mode, verbose=False)
        self.mode = mode
        if "VNA" in mode:  # XXX or whatever we call the modes
            self.observe_vna()
        pass

    def observe_vna(self, timeout=300):
        """
        VNA observations.

        Send START to VNA, wait for COMPLETE message.

        Parameters
        ----------
        timeout : int
            The time in seconds to wait for the VNA to complete.

        Returns
        -------
        int
            Exit code. 0 if sucessful, 1 if timeout.

        """
        # send start
        # redis set VNA_START
        tstart = time.time()
        while True:
            # redis get VNA_COMPLETE
            # if VNA_COMPLETE:
            #    return 0
            if time.time() - tstart > timeout:
                self.logger.warning("VNA timed out.")
                return 1
            time.sleep(1)

    def observe(
        self,
        pairs=None,
        timeout=10,
        update_redis=True,
        write_files=True,
    ):
        """
        Start observing. Different schedules are allowed:

        1. Isolated SNAP correlator observing, like before.
        2. Isolated VNA observing.

        The first two cases may both include sensor readings and motor
        rotations. SNAP observing could also include switching.

        3. An all of the above case, where we regularly switch between
        VNA measurements and SNAP measurements.

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
                self.fpga.metadata,
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
                self.logging.info(f"Switching to {mode} mode")
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
            data = d["data"]
            cnt = d["cnt"]
            if update_redis:
                self.fpga.update_redis(data, cnt)
            if write_files:
                filename = self.file.add_data(data)
                if filename is not None:
                    self.logger.info(f"Writing file {filename}")
            remaining -= 1
        if self.file is not None:
            if len(self.file) > 0:
                self.logger.info("Writing short final file.")
                self.file.corr_write()

        thd.join()
        self.logger.info("Observing complete.")
