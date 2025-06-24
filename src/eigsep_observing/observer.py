from itertools import cycle
from threading import Event

from . import io
from .utils import eig_logger, require_panda, require_snap


def make_schedule(switch_schedule):
    """
    Create a schedule for switching between VNA and SNAP observing. This
    creates a cycle object that can iterate indefinitely over the switch
    schedule.

    Parameters
    ----------
    switch_schedule : dict
        The switch schedule used for observing. A dictionary with keys
        `vna``, ``snap_repeat``, ``sky``, ``load``, and ``noise``. The
        first two keys specify the number of measurements with the VNA
        and the SNAP respectively. When measuring PSDs with the SNAP,
        the ``sky``, ``load``, and ``noise`` keys specify the number of
        measurements to take for each state.

    Returns
    -------
    schedule : cycle
        A cycle object that iterates over the switch schedule. The
        schedule is a list of tuples, where each tuple contains the
        state and the number of measurements to take.

    Raises
    ------
    KeyError
        If the switch schedule contains an invalid key.

    ValueError
        If the switch schedule is empty, i.e. no states are specified.

    Notes
    -----
    Defaults are 0 for all states, except for ``snap_repeat``, which
    defaults to 1.

    """
    keys = ("sky", "load", "noise", "snap_repeat", "vna")
    for k in switch_schedule:
        if k not in keys:
            raise KeyError(f"Invalid key in switch schedule: {k}.")
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
    if not schedule:
        raise ValueError(
            "Switch schedule is empty. Specify at least one state to observe."
        )
    return cycle(schedule)


class EigObserver:

    def __init__(self, redis_snap=None, redis_panda=None, logger=None):
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
        logger : logging.Logger
            The logger to use for logging messages. If None, a default
            logger is created, using the `eig_logger` utility.

        Notes
        -----
        At least one of the Redis connections must be provided. Connect
        to the SNAP Redis server for reading correlator data, and to
        the LattePanda Redis server for reading metadata and controlling
        the VNA and RF switches.

        """
        if logger is None:
            logger = eig_logger(__name__)
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

        self.stop_event = Event()

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
        self.redis.send_ctrl(cmd_mode_map[mode])

    @require_panda
    def observe_vna(self, mode, timeout=300, write_files=True):
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
        kwargs = {
            "fstart": self.cfg.vna_fstart,
            "fstop": self.cfg.vna_fstop,
            "npoints": self.cfg.vna_npoints,
            "ifbw": self.cfg.vna_ifbw,
            "power_dBm": self.cfg.vna_power[mode],
        }

        self.redis.send_ctrl(cmd, **kwargs)
        try:
            out = self.redis.read_vna_data(timeout=120)
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
                save_dir=self.cfg.vna_save_dir,
            )
        else:
            return data, cal_data

    # XXX
    @require_panda
    def rotate_motors(self, motors):
        """
        Raises
        -------
        AttributeError
            If the `redis_panda` attribute is not set.
        """
        raise NotImplementedError

    @require_snap
    def observe_snap(self, pairs=None, timeout=10):
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
            self.cfg.save_dir,
            pairs,
            self.cfg.ntimes,
            self.cfg.snap_header,  # XXX some verification of fpga.header
            redis=self.redis,
        )

        while not self.stop_event.is_set():  # XXX implement stop_event
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

    def end_observing(self):
        self.stop_event.set()
        self.logger.info("Observing ended.")
