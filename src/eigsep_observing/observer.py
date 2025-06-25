import logging
import threading

from . import io
from .utils import require_panda, require_snap

logger = logging.getLogger(__name__)


class EigObserver:

    def __init__(self, redis_snap=None, redis_panda=None):
        """
        Main controll class and filewriter for Eigsep observing.
        Provides methods to:
         - remotely control hardware in the EIGSEP box, including
           motors, VNA, and RF switches,
         - read correlator data from the SNAP,
         - read S11 measurements from the VNA,
         - read metadata from sensors connected to the LattePanda,
         - write data to files.

        Parameters
        ----------
        redis_snap : EigsepRedis
            The Redis connection to the Rasperry Pi controlling the
            SNAP correlator.
        redis_panda : EigsepRedis
            The Redis connection to the LattePanda server.

        Notes
        -----
        At least one of the Redis connections must be provided. Connect
        to the SNAP Redis server for reading correlator data, and to
        the LattePanda Redis server for reading metadata and controlling
        the VNA and RF switches.

        """
        self.logger = logger

        # redis connections
        self.redis_snap = redis_snap
        self.redis_panda = redis_panda

        if self.redis_snap is not None:
            self.cfg = self.redis_snap.get_config()
        elif self.redis_panda is not None:
            self.cfg = self.redis_panda.get_config()
        else:
            raise ValueError("At least one Redis connection must be provided.")

        self.stop_events = {
            "switches": threading.Event(),
            "vna": threading.Event(),
            "motors": threading.Event(),
            "snap": threading.Event(),
        }
        self.switch_lock = threading.Lock()  # lock for RF switches

        # start a status thread
        if self.redis_panda is not None:
            status_thread = threading.Thread(
                target=self.status_logger,
                daemon=True,
            )
            status_thread.start()

    @property
    def snap_connected(self):
        """
        Check if the SNAP Redis connection is established.
        """
        return self.redis_snap is not None

    @property
    def panda_connected(self):
        """
        Check if the LattePanda Redis connection is established.
        """
        if self.redis_panda is None:
            return False
        return self.redis_panda.client_heartbeat_check()

    @require_panda
    def status_logger(self):
        """
        Log status messages from the LattePanda Redis server.
        """
        while True:
            level, status = self.redis_panda.read_status()
            if status is None:
                continue
            self.logger.log(level, status)

    @require_panda
    def set_mode(self, mode):
        """
        Switch observing mode with RF switches.

        Parameters
        ----------
        mode : str
            Observing mode. Either ``sky``, ``load``, ``noise``.

        Raises
        ------
        AttributeError
            If the `redis_panda` attribute is not set.
        ValueError
            If the mode is not one of the valid modes.

        """
        cmd_mode_map = {
            "sky": "switch:RFANT",
            "load": "switch:RFLOAD",
            "noise": "switch:RFN",
        }
        if mode not in cmd_mode_map:
            raise ValueError(
                f"Invalid mode: {mode}. Must be one of "
                f"{list(cmd_mode_map.keys())}."
            )
        self.logger.info(f"Switching to {mode} measurements")
        self.redis_panda.send_ctrl(cmd_mode_map[mode])

    @require_panda
    def do_switching(self):
        """
        Use the RF switches to switch between sky, load, and noise
        source measurements according to the switch schedule.

        Notes
        -----
        The majority of the observing time is spent on sky
        measurements. Therefore, S11 measurements are only allowed
        to interrupt the sky measurements, and not the load or
        noise source measurements.

        """
        switch_schedule = self.cfg["switch_schedule"]
        while not self.stop_events["switches"].is_set():
            with self.switch_lock:
                for mode in ["load", "noise"]:
                    self.logger.info(f"Switching to {mode} measurements")
                    self.set_mode(mode)
                    if self.stop_events["switches"].wait(
                        switch_schedule[mode]
                    ):
                        self.logger.info("Switching stopped by event")
                        return
            self.logger.info("Switching to sky measurements")
            self.stop_events["switches"].wait(switch_schedule["sky"])

    @require_panda
    def measure_s11(self, mode, timeout=300, write_files=True):
        """
        VNA observations. Performs OSL calibration measurements and
        measurment of the device(s) under test.

        Parameters
        ----------
        mode : str
            The mode to set. Either `ant` or `rec`. The former
            case measures S11 of antenna and noise source. The latter
            uses less power and measures S11 of the receiver.
        timeout : int
            The time in seconds to wait for the VNA to complete.
        write_files : bool
            If True, write the VNA data to files. If False, only
            return the data without writing to files.

        Returns
        -------
        data : dict
            The S11 measurement data from the VNA. Only returned if
            `write_files` is False.
        cal_data : dict
            S11 measurement data from the OSL calibration. Only
            returned if `write_files` is False.

        Raises
        ------
        AttributeError
            If the `redis_panda` attribute is not set.
        ValueError
            If ``mode`` is not `ant` or `rec`.

        """
        if mode not in ("ant", "rec"):
            raise ValueError(
                f"Invalid mode: {mode}. Must be one of 'ant' or 'rec'."
            )
        cmd = f"vna:{mode}"
        kwargs = self.cfg["vna_settings"].copy()
        kwargs["power_dBm"] = kwargs["power_dBm"][mode]
        self.redis_panda.send_ctrl(cmd, **kwargs)
        try:
            out = self.redis_panda.read_vna_data(timeout=120)
        except TimeoutError:
            self.logger.error(
                "Timeout while waiting for VNA data. "
                "Check the VNA connection and settings."
            )
            return None, None
        eid, data, cal_data, header, metadata = out
        if write_files:
            io.write_s11_file(
                data,
                header,
                metadata=metadata,
                cal_data=cal_data,
                save_dir=self.cfg["vna_save_dir"],
            )
        else:
            return data, cal_data

    @require_panda
    def observe_vna(self):
        """
        Observe with VNA and write data to files.
        """
        while not self.stop_events["vna"].is_set():
            with self.switch_lock:
                for mode in ["ant", "rec"]:
                    self.logger.info(f"Measuring S11 of {mode} with VNA")
                    self.measure_s11(mode, write_files=True)
            # wait for the next iteration
            self.stop_events["vna"].wait(self.cfg["vna_interval"])

    # XXX
    @require_panda
    def rotate_motors(self, motors):
        """
        Raises
        -------
        AttributeError
            If the `redis_panda` attribute is not set.
        """
        # runs if not stop_events[motors].is_set()
        raise NotImplementedError

    @require_snap
    def record_corr_data(self, pairs=None, timeout=10):
        """
        Read data from the SNAP correlator via Redis and write it to
        file.

        Parameters
        ----------
        pairs : list
            The list of pairs to observe. If None, all pairs will be
            observed.
        timeout : int
            The time in seconds to wait for data from the correlator.

        """
        file = io.File(
            self.cfg["save_dir"],
            pairs,
            self.cfg["ntimes"],
            self.redis_snap,  # XXX read from fpga.header
            redis=self.redis_panda,
        )

        while not self.stop_events["snap"].is_set():
            # blocking read from Redis
            data = self.redis_snap.read_corr_data(
                pairs=pairs, timeout=timeout, unpack=True
            )
            filename = file.add_data(data)
            if filename is not None:  # file buffer is full, file written
                self.logger.info(f"Writing file {filename}")

        # write short final file if there is more data
        if len(file) > 0:
            self.logger.info("Writing short final file.")
            file.corr_write()
