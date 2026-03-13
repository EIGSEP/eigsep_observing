import logging
import threading
import time

from . import io

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
        self.status_thread = threading.Thread(
            target=self.status_logger, daemon=True
        )
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

    def status_logger(self):
        """
        Log status messages from the LattePanda Redis server.
        """
        while not self.panda_connected:
            self.logger.debug("Status thread waiting for Panda connection.")
            if self.stop_event.wait(1):
                return
        self.logger.info("Status thread started. Logging Panda status.")

        while not self.stop_event.is_set():
            t0_status = time.time()
            while not self.panda_connected:
                # print every 10 seconds
                if time.time() - t0_status > 10:
                    self.logger.warning("Panda disconnected")
                    t0_status = time.time()
                if self.stop_event.wait(1):  # wait 1s before checking again
                    return
            self.logger.debug("Panda connected.")
            level, status = self.redis_panda.read_status(timeout=0.1)
            if status is not None:
                self.logger.log(level, status)

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
