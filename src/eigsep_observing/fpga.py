from datetime import datetime
import eigsep_corr
from .redis import EigsepRedis


class EigsepFpga(eigsep_corr.EigsepFpga):

    @staticmethod
    def _create_redis(host, port):
        """
        Override the _create_redis method to use EigsepRedis.

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

    def upload_config(self, validate=True):
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
        Override the synchronize method to use new EigsepRedis class.
        """
        super().synchronize(delay=delay, update_redis=False)
        sync_time = {
            "sync_time_unix": self.sync_time,
            "sync_date": datetime.fromtimestamp(self.sync_time).isoformat(),
        }
        self.redis.add_metadata("corr_sync_time", sync_time)

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
        # stream data, cnt
        raise NotImplementedError("not yet, this is important!")

    def _read_integrations(self):
        raise NotImplementedError("Not implemented, use observe directly!")

    def end_observing(self):
        self.logger.warn("End observing not implemented in EigsepFpga!")
        return

    # XXX get rid of pairs maybe
    def observe(self, pairs=None, timeout=10):  # XXX here
        raise NotImplementedError("Not implemented yet, this is important!")
        # XXX remember to grab self.header and sead over redis
        # at beginning since it's static
        self.upload_config(validate=True)
