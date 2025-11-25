import logging
import threading

from . import io
from .utils import require_panda

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
            self.corr_cfg = self.redis_snap.get_corr_config()
        if self.redis_panda is not None:
            self.cfg = self.redis_panda.get_config()

        self.stop_event = threading.Event()  # main stop event

        # start a status thread
        self.logger.info("Starting status thread.")
        self.status_thread = threading.Thread(target=self.status_logger)
        self.status_thread.start()

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
    def reprogram_panda(self, force=False, timeout=5):
        """
        Reprogram the LattePanda Redis server with the current
        configuration.

        Parameters
        ----------
        force : bool
            Reprogram if config appears to be the same as before.

        Raises
        ------
        AttributeError
            If the `redis_panda` attribute is not set.

        """
        self.logger.info("Reprogramming LattePanda with current configuration")
        self.redis_panda.send_ctrl("ctrl:reprogram", force=force)
        self.cfg = self.redis_panda.get_config()  # update cfg

    def status_logger(self):
        """
        Log status messages from the LattePanda Redis server.
        """
        while not self.panda_connected:
            self.logger.debug("Status thread waiting for Panda connection.")
            self.stop_event.wait(1)
        self.logger.info("Status thread started. Logging Panda status.")

        while not self.stop_event.is_set():
            while not self.panda_connected:
                self.logger.warning("Panda disconnected")
                self.stop_event.wait(1)
            self.logger.debug("Panda connected.")
            level, status = self.redis_panda.read_status(timeout=10)
            if status is None:
                # Check stop event with timeout
                if self.stop_event.wait(0.1):
                    break
                continue
            self.logger.log(level, status)

    @require_panda
    def set_mode(self, mode):
        """
        Switch observing mode with RF switches.

        Parameters
        ----------
        mode : str
            Observing mode. Either `RFANT` (sky), `RFNOFF` (load), or
            `RFNON` (noise).

        Raises
        ------
        AttributeError
            If the `redis_panda` attribute is not set.
        ValueError
            If the mode is not one of the valid modes.

        """
        modes = ("RFANT", "RFNOFF", "RFNON")
        if mode not in modes:
            raise ValueError(f"Invalid mode: {mode}. Must be one of {modes}.")
        self.logger.info(f"Switching to {mode} measurements")
        self.redis_panda.send_ctrl(f"switch:{mode}")

    @require_panda
    def measure_s11(self, mode, timeout=300, write_files=True):
        """
        VNA observations. Performs OSL calibration measurements and
        measurement of the device(s) under test.

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
            out = self.redis_panda.read_vna_data(timeout=timeout)
        except TimeoutError:
            self.logger.error(
                "Timeout while waiting for VNA data. "
                "Check the VNA connection and settings."
            )
            return None
        data, header, metadata = out
        if write_files:
            io.write_s11_file(
                data,
                header,
                metadata=metadata,
                save_dir=self.cfg["vna_save_dir"],
            )
        else:
            return data

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

    def record_corr_data(self, save_dir, ntimes=240, timeout=20):
        """
        Read data from the SNAP correlator via Redis and write it to
        file.

        Parameters
        ----------
        save_dir : str or Path
            Directory to save the correlator data files.
        ntimes : int
            Number of spectra per file.
        timeout : int
            The time in seconds to wait for data from the correlator.

        """
        pairs = self.corr_cfg["pairs"]
        t_int = self.corr_cfg["integration_time"]
        file_time = ntimes * t_int
        self.logger.info(
            "Reading correlator data from SNAP"
            f"Integration time: {t_int} s, "
            f"File time: {file_time} s"
        )
        file = io.File(
            save_dir,
            pairs,
            ntimes,
            self.corr_cfg,
        )

        while not self.snap_connected:
            self.logger.warning(
                "Waiting for SNAP Redis connection to be established."
            )
            self.stop_event.wait(1)

        while not self.stop_event.is_set():
            if file.counter == 0:  # look up header in Redis once per file
                try:
                    header = self.redis_snap.get_corr_header()
                except ValueError as e:
                    self.logger.error(f"Error reading header from SNAP: {e}")
                    header = None
                file.set_header(header=header)
            # blocking read from Redis
            acc_cnt, sync_time, data = self.redis_snap.read_corr_data(
                pairs=pairs, timeout=timeout, unpack=True
            )
            self.logger.info(f"{acc_cnt=}")
            if self.panda_connected:
                # metadata = self.redis_panda.get_metadata()
                metadata = self.redis_panda.get_live_metadata()
            else:
                metadata = None
            file.add_data(acc_cnt, sync_time, data, metadata=metadata)

        # write short final file if there is more data
        if len(file) > 0:
            self.logger.info("Writing short final file.")
            file.corr_write()

    def record_vna_data(self, save_dir):
        """
        Read VNA data from the LattePanda Redis server and write it to
        file.

        Parameters
        ----------
        save_dir : str or Path
            Directory to save the VNA data files.

        """
        while not self.panda_connected:
            self.logger.warning(
                "Waiting for LattePanda Redis connection to be established."
            )
            self.stop_event.wait(1)
        while not self.stop_event.is_set():
            data, header, metadata = self.redis_panda.read_vna_data(timeout=0)
            if data is None:
                self.logger.warning("No VNA data available. Waiting.")
                self.stop_event.wait(1)
                continue
            io.write_s11_file(
                data,
                header,
                metadata=metadata,
                save_dir=save_dir,
            )
            self.logger.info(f"Wrote VNA data to {save_dir}.")
