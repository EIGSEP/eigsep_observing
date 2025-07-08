from datetime import datetime
import time

from eigsep_corr.fpga import EigsepFpga as CorrEigsepFpga

from .eig_redis import EigsepRedis


class EigsepFpga(CorrEigsepFpga):

    @staticmethod
    def _create_redis(host: str, port: int) -> EigsepRedis:
        """
        Create an EigsepRedis instance.

        Parameters
        ----------
        host : str
            The hostname for the Redis server.
        port : int
            The port number for the Redis server.

        Returns
        -------
        EigsepRedis
            An instance of EigsepRedis connected to the specified Redis
            server.

        """
        return EigsepRedis(host=host, port=port)

    def upload_config(self, validate: bool = True) -> None:
        """
        Upload the configuration to Redis.

        Parameters
        ----------
        validate : bool, optional
            Whether to validate the configuration with hardware
            before uploading.

        Raises
        -------
        RuntimeError
            If 'validate' is True and the configuration does not match
            the hardware configuration.

        """
        if validate:
            try:
                self.validate_config()
            except RuntimeError as e:
                self.logger.error(f"Configuration validation failed: {e}")
                raise RuntimeError("Configuration validation failed") from e
        self.logger.debug("Uploading configuration to Redis.")
        self.redis.upload_corr_config(self.cfg, from_file=False)

    def synchronize(self, delay=0):
        """
        Synchronize the correlator clock.

        Parameters
        ----------
        delay : int
            Delay in FPGA clock ticks beween arrival of an external
            sync pulse and the issuing of an internal trigger.

        """
        super().synchronize(delay=delay, update_redis=False)
        sync_time = {
            "sync_time_unix": self.sync_time,
            "sync_date": datetime.fromtimestamp(self.sync_time).isoformat(),
        }
        self.redis.add_metadata("corr_sync_time", sync_time)

    def initialize(
        self,
        initialize_adc=True,
        initialize_fpga=True,
        sync=True,
    ):
        """

        Parameters
        ----------
        initialize_adc : bool
            Initialize the ADCs.
        initialize_fpga : bool
            Initialize the FPGA.
        sync : bool
            Synchronize the correlator clock.


        Notes
        -----
        This is a convenience method that calls the methods
            - `initialize_adc`
            - `initialize_fpga`
            - `set_input`
            - `synchronize`
        in the specified order with their default parameters.

        This method overrides the `initialize` method to no longer
        accept the `update_redis` parameter. It is now required.
        Related to this, the signature of the `synchronize` method has
        been modified.

        """
        super().initialize(
            initialize_adc=initialize_adc,
            initialize_fpga=initialize_fpga,
            sync=False,
            update_redis=False,
        )
        if sync:
            self.logger.debug("Synchronizing correlator clock.")
            self.synchronize()

    def update_redis(self, data, cnt):
        """
        Stream data and metadata to Redis.

        Parameters
        ----------
        data : dict
            A dictionary of raw data from the correlator.
        cnt : int
            Accumulation count from the correlator.

        """
        self.redis.add_corr_data(data, cnt, dtype=self.cfg["dtype"])

    def _read_integrations(self, pairs, prev_cnt):
        """
        Read one integration from the correlator.

        Parameters
        ----------
        pairs : list of str
            List of correlation pairs to read.
        prev_cnt : int
            The previous accumulation count.

        Returns
        -------
        data : dict
            A dictionary containing the raw data from the correlator.
        cnt : int
            The current accumulation count.
        """
        cnt = self.fpga.read_int("corr_acc_cnt")
        dcnt = cnt - prev_cnt
        if dcnt == 0:
            return None, cnt
        if dcnt > 1:
            self.logger.warning(f"Missed {dcnt - 1} integration(s).")
        self.logger.info(f"Reading acc_cnt={cnt} from correlator.")
        data = self.read_data(pairs=pairs, unpack=False)
        if cnt != self.fpga.read_int("corr_acc_cnt"):
            self.logger.error(
                f"Read of acc_cnt={cnt} FAILED to complete before next "
                "integration. "
            )
        return data, cnt

    def end_observing(self):
        raise NotImplementedError("Not implemented in eigsep_observing")

    def observe(self, pairs=None, timeout=10):
        """
        Read correlator data and stream it to Redis.

        Parameters
        ----------
        pairs : list of str
            List of correlation pairs to read. If None, all pairs are
            read and streamed.
        timeout : int
            Timeout in seconds for reading data from the correlator.

        Raises
        -------
        TimeoutError
            If the read operation times out.

        """
        self.upload_config(validate=True)
        t_int = self.header["integration_time"]
        self.logger.info(f"Integration time is {t_int} seconds.")
        if pairs is None:
            pairs = self.autos + self.crosses
        self.logger.info(f"Starting observation for pairs: {pairs}.")
        t = time.time()
        while True:
            if time.time() - t > timeout:
                raise TimeoutError("Read operation timed out.")
            data, cnt = self._read_integrations(pairs, self.prev_cnt)
            if data is None:
                time.sleep(0.1)
                continue
            self.update_redis(data, cnt)
            self.prev_cnt = cnt
            t = time.time()
