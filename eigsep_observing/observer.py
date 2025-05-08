import logging
from . import EigsepRedis


class EigObserver:

    def __init__(
        self,
        fpga=None,
        sensors=None,
        switch_schedule=None,  # XXX default switch schedule
        logger=None,
    ):
        """`
        Main controll class for Eigsep observing. This code is meant to run
        on the Raspberry Pi. It uses EigsepFpga to initialize observing
        with the SNAP correlator and communicates with the LattePanda in the
        EIGSEP box via Redis. It pulls sensor readings from the LattePanda
        and puts them in the file headers, synchronized with the data stream.

        Parameters
        ----------
        fpga : EigsepFpga
            The EigsepFpga object to use for observing.
        sensors : list of Sensor
            The list of Sensor objects to use for observing.
        switch_schedule : dict
            The switch schedule to use for observing. This is a dictionary
            with keys ``vna'', ``load'', ``noise'', and ``sky''. The values
            represents the number of measurements to take for each state.

        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)
        self.logger = logger

        self.switch_schedule = switch_schedule
        if self.switch_schedule["vna"] > 0:
            use_vna = True
            self._mode = "vna"
        if (
            self.switch_schedule["load"] > 0
            or self.switch_schedule["noise"] > 0
            or self.switch_schedule["sky"] > 0
        ):
            use_snap = True
            self._mode = "snap"
        if use_vna and use_snap:
            self._mode = "both"

        self.fpga = fpga
        if self.fpga is None:
            self.logger.warning(
                "No Fpga instance. Running isolated VNA measurements. "
            )

            self._mode = "vna"
            if not use_vna:
                raise ValueError(
                    "In isolated VNA mode, but `use_vna`` is False. "
                )

        self.redis = EigsepRedis()
        self.sensors = sensors
        self.use_vna = use_vna

        if sensors is not None:
            self.init_sensors()

        # if switching:
        #    self.init_switching()

        # if use_vna:
        #    self.init_vna()

    def init_sensors(self):
        """
        Use Redis to initialize observing with senors. Need to send START
        command to get pico scripts running and make the observe function
        ready to pick up the data stream and place into the header.
        """
        pass

    def init_switching(self):
        """
        Use Redis to initialize observing with swtiching. Got to have a switch
        schedule going. The state absolutely needs to be in the header.
        """
        # XXX here we need to set self.SwitchingSchedule or some config
        pass

    def set_mode(self, mode):
        # XXX need to update redis!
        # self.mode = mode
        pass

    def _observe_vna(self):
        """
        VNA observations.

        """
        pass

    def _observe_snap(self):
        pass

    def observe(
        self,
        schedule,  # XXX set default schedule to sky obs, no switch!
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

        """
        if mode == "vna":
            return self._observe_vna()

        if pairs is None:
            pairs = self.fpga.autos + self.fpga.crosses

        # switch to load and noise source based on switch schedule
        # schedule sets 1, 1, N
        # switch load
        # observe one spectrum (load)
        # switch noise source
        # observe one spectrum (noise source)
        # switch back to sky
        # observe N spectra (sky)

        # pause observing, write file etc
        # switch to VNA mode

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
            )

        # XXX
        # self.schedule = cycle
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
                mode, remaining = next(self.schedule)
                self.logging.info(f"Switching to {mode} mode")
                self.set_mode(mode)
                if mode == "vna":
                    self.observe_vna()
                    remaining -= 1
                    continue
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
            # XXX update redis and write to file
            data = d["data"]
            cnt = d["cnt"]
            if update_redis:
                self.fpga.update_redis(data, cnt)
            if write_files:
                filename = self.file.add_data(data, cnt, mode=self.mode)
            remaining -= 1

