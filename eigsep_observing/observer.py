import logging

class EigObserver:

    def __init__(
        self,
        fpga=None,
        sensors=None,
        switching=False,
        use_vna=False,
        redis=None,
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
        switching : bool
            Use Dicke switching for observing.
        use_vna : bool
            Switch between sky measurements and VNA measurements.
        redis : Redis
            Instance of Redis to use for communication with the LattePanda.
            Must be provided if any of the other parameters are True.

        """
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)
        self.logger = logger

        self.fpga = fpga
        if self.fpga is None:
            logger.warning(
                "No Fpga instance. Running isolated VNA measurements. "
            )
            self.mode = "vna"
            switching = False
            if redis is None or not use_vna:
                raise ValueError(
                    "In isolated VNA mode, but no Redis instance provided. "
                    "or ``use_vna`` is False. "
                )
        else:
            self.init_fpga()
        
        if redis is None:
            logger.warning(
                "No Redis instance provided. Running without communcation "
                "with LattePanda. Falling back to isolated SNAP correlator "
                "observing."
            )
            self.mode = "snap"
            self.redis = None
            self.sensors = None
            self.switching = False
            self.use_vna = False
            return
        
        self.redis = redis
        self.sensors = sensors
        self.switching = switching
        self.use_vna = use_vna

        if sensors is not None:
            self.init_sensors()
        
        #if switching:
        #    self.init_switching()

        #if use_vna:
        #    self.init_vna()

    def init_fpga(self):
        """
        Call init methods in EigsepFpga to initialize the SNAP correlator.
        """
        pass

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


   def observe(self):
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
           return self.observe_vna()

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


