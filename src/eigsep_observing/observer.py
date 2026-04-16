import logging
import threading
import time

from . import io

logger = logging.getLogger(__name__)


def _tick_liveness_deadline(deadline, liveness_timeout, reason):
    """Advance the SNAP-liveness deadline; raise if expired.

    Parameters
    ----------
    deadline : float or None
        Current deadline (``time.monotonic()`` seconds), or ``None``
        if no failure has been seen since the last successful write.
    liveness_timeout : float
        Tolerated duration without a complete corr row, in seconds.
    reason : str
        What triggered this tick; surfaced in the ``RuntimeError``.

    Returns
    -------
    float
        Updated deadline. Set to ``monotonic() + liveness_timeout`` on
        the first failure since the last clear; unchanged thereafter.

    Raises
    ------
    RuntimeError
        If ``deadline`` is in the past.
    """
    now = time.monotonic()
    if deadline is None:
        return now + liveness_timeout
    if now > deadline:
        raise RuntimeError(
            f"SNAP has not produced a complete corr row for "
            f"{liveness_timeout}s: {reason}"
        )
    return deadline


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
        redis_snap : EigsepObsRedis
            The Redis connection to the Rasperry Pi controlling the
            SNAP correlator.
        redis_panda : EigsepObsRedis
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
            self.corr_cfg = self.redis_snap.corr_config.get()
        if self.redis_panda is not None:
            self.cfg = self.redis_panda.config.get()

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
        return self.redis_panda.heartbeat_reader.check()

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
            level, status = self.redis_panda.status_reader.read(timeout=0.1)
            if status is not None:
                self.logger.log(level, status)

    def record_corr_data(
        self, save_dir, ntimes=240, timeout=20, liveness_timeout=300
    ):
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
        liveness_timeout : float
            Bounded wait, in seconds, for the SNAP to produce a
            complete corr row (valid header with non-zero
            ``sync_time`` *and* an entry on the corr stream).
            Exceeding this deadline raises ``RuntimeError`` so the
            process crashes visibly rather than silently accumulating
            unusable data.

        Notes
        -----
        Every failure mode that means "SNAP is not producing a
        complete corr row right now" shares a single watchdog:

        - ``ValueError`` from ``get_header`` with no cached header
        - ``sync_time == 0`` on a fetched header
        - ``TimeoutError`` from ``corr_reader.read``
        - ``(None, {})`` from ``corr_reader.read`` (stream absent)

        The deadline starts on the first such failure and is cleared
        when ``file.add_data`` completes. Crossing it raises
        ``RuntimeError``. The consumer only cares *whether* a row
        arrived, not *why* it did not — duck-typing on "did I get a
        complete row?" is more trustworthy than classifying header vs.
        stream failures, because ``get_header`` reads a persistent
        Redis hash and a stale header can survive a dead SNAP.

        A ``ValueError`` from ``get_header`` *with* a cached header
        is treated as a transient metadata-path blip: the cached
        header is used, the loop proceeds to read/write the next row,
        and the deadline is cleared on the next successful write.
        Corr data on the stream has valid timestamps; the blip must
        not block corr writes ("corr data is sacred").

        A valid ``sync_time`` that differs from the cached one is
        treated as a mid-run SNAP re-sync: the current file is closed
        and a new one is opened with the new anchor. This is a
        legitimate state change, not a failure, so the deadline is
        untouched.
        """
        pairs = self.corr_cfg["pairs"]
        t_int = self.corr_cfg["integration_time"]
        file_time = ntimes * t_int
        self.logger.info(
            "Reading correlator data from SNAP. "
            f"Integration time: {t_int} s, "
            f"File time: {file_time} s"
        )

        file = io.File(save_dir, pairs, ntimes, self.corr_cfg)
        cached_header = None
        cached_sync_time = None
        last_write_deadline = None
        try:
            while not self.stop_event.is_set():
                if file.counter == 0:
                    try:
                        header = self.redis_snap.corr_config.get_header()
                    except ValueError as e:
                        if cached_header is not None:
                            self.logger.warning(
                                f"Error reading header from SNAP: {e}. "
                                "Using cached corr header."
                            )
                            file.set_header(header=cached_header)
                        else:
                            last_write_deadline = _tick_liveness_deadline(
                                last_write_deadline,
                                liveness_timeout,
                                f"header fetch failed: {e}",
                            )
                            self.logger.error(
                                f"Error reading header from SNAP: {e}. "
                                "Waiting for a valid header."
                            )
                            if self.stop_event.wait(1):
                                return
                            continue
                    else:
                        new_sync_time = header.get("sync_time")
                        if not new_sync_time:
                            last_write_deadline = _tick_liveness_deadline(
                                last_write_deadline,
                                liveness_timeout,
                                "sync_time=0 (SNAP not synchronized)",
                            )
                            self.logger.error(
                                "No sync_time in corr header. Cannot "
                                "derive accurate timestamps. Waiting "
                                "for SNAP to synchronize."
                            )
                            if self.stop_event.wait(1):
                                return
                            continue
                        if (
                            cached_sync_time is not None
                            and new_sync_time != cached_sync_time
                        ):
                            self.logger.warning(
                                f"SNAP re-synchronized from "
                                f"{cached_sync_time} to {new_sync_time}; "
                                "rolling to new file."
                            )
                            file.close()
                            file = io.File(
                                save_dir, pairs, ntimes, self.corr_cfg
                            )
                        cached_header = header
                        cached_sync_time = new_sync_time
                        file.set_header(header=header)
                try:
                    acc_cnt, data = self.redis_snap.corr_reader.read(
                        pairs=pairs, timeout=timeout, unpack=True
                    )
                except TimeoutError:
                    last_write_deadline = _tick_liveness_deadline(
                        last_write_deadline,
                        liveness_timeout,
                        f"no corr entry within {timeout}s",
                    )
                    self.logger.error(
                        f"No correlation data received within {timeout}s. "
                        "Waiting for SNAP to produce data."
                    )
                    continue
                if acc_cnt is None:
                    last_write_deadline = _tick_liveness_deadline(
                        last_write_deadline,
                        liveness_timeout,
                        "corr stream does not exist yet",
                    )
                    if self.stop_event.wait(1):
                        return
                    continue
                self.logger.info(f"{acc_cnt=}")
                if self.panda_connected:
                    metadata = self.redis_panda.metadata_stream.drain()
                else:
                    metadata = None
                file.add_data(
                    acc_cnt, cached_sync_time, data, metadata=metadata
                )
                last_write_deadline = None
        finally:
            file.close()

    def record_vna_data(self, save_dir, timeout=60):
        """
        Read VNA data from the LattePanda Redis server and write it to
        file.

        Parameters
        ----------
        save_dir : str or Path
            Directory to save the VNA data files.
        timeout : int
            Timeout in seconds for each blocking read. The loop retries
            after each timeout, allowing it to check for stop events
            and panda reconnection.

        """
        while not self.panda_connected:
            self.logger.warning(
                "Waiting for LattePanda Redis connection to be established."
            )
            # wait(1) returns True when stop is requested
            if self.stop_event.wait(1):
                return
        while not self.stop_event.is_set():
            # Panda can disconnect mid-operation after the initial
            # wait above; check here to avoid a full timeout cycle.
            if not self.panda_connected:
                self.logger.warning("Panda disconnected, waiting.")
                if self.stop_event.wait(1):
                    return
                continue
            try:
                data, header, metadata = self.redis_panda.vna_reader.read(
                    timeout=timeout
                )
            except TimeoutError:
                continue
            if data is None:
                self.logger.warning("No VNA data available. Waiting.")
                if self.stop_event.wait(1):
                    return
                continue
            io.write_s11_file(
                data,
                header,
                metadata=metadata,
                save_dir=save_dir,
            )
            self.logger.info(f"Wrote VNA data to {save_dir}.")
