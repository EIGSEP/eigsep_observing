import json
import logging

import numpy as np

from eigsep_redis import EigsepRedis

from .utils import load_config

logger = logging.getLogger(__name__)


class EigsepObsRedis(EigsepRedis):
    """
    Observing-side Redis client. Adds correlator and VNA data paths
    on top of the generic ``eigsep_redis.EigsepRedis`` bus primitives.

    The split between base and subclass is deliberate: ``picohost``
    depends only on the base bus (connection, streams, metadata,
    heartbeat, status); the methods here hardcode correlator pair
    layouts, VNA trace dtypes, and SNAP-side serialization conventions
    that producers outside observing don't care about. See
    ``CLAUDE.md`` for the full rationale.
    """

    maxlen = {**EigsepRedis.maxlen, "vna_data": 1000}

    def upload_corr_config(self, config, from_file=False):
        """
        Upload the SNAP configuration to Redis. This is the
        configuration parameters used for programming the SNAP.

        Parameters
        ----------
        config : str or dict
            Path to the configuration file if `from_file` is True, or a
            dictionary containing the configuration data if `from_file`
            is False.
        from_file : bool

        """
        if from_file:
            config = load_config(config)
        self._upload_dict(config, "corr_config")

    def get_corr_config(self):
        """
        Get the SNAP configuration file from Redis. These are the
        configuration parameters used for programming the SNAP.

        Returns
        -------
        config : dict
            Dictionary containing the SNAP configuration data.

        Raises
        ------
        ValueError
            If no configuration is found in Redis.

        """
        raw = self.get_raw("corr_config")
        if raw is None:
            raise ValueError("No SNAP configuration found in Redis.")
        return json.loads(raw)

    def upload_corr_header(self, header):
        """
        Upload correlation data header, from `fpga.header` attribute.

        Parameters
        ----------
        header : dict

        """
        self._upload_dict(header, "corr_header")

    def get_corr_header(self):
        """
        Get the correlation data header from Redis.

        Returns
        -------
        header : dict
            Dictionary containing the correlation data header.

        Raises
        ------
        ValueError
            If no correlation header is found in Redis.

        """
        raw = self.get_raw("corr_header")
        if raw is None:
            raise ValueError("No correlation header found in Redis.")
        return json.loads(raw)

    # ---------- correlation data and s11 measurements ----------

    def add_corr_data(self, data, cnt, dtype=">i4"):
        """
        Upload raw correlation data to Redis.

        Parameters
        ----------
        data : dict
            Dictionary holding correlation data. Keys are correlation
            pairs and values are bytes. See the `read_data` method of
            EigsepFpga in eigsep_observing.fpga for the expected format.
        cnt : int
            Accumulation count, read from SNAP.
        dtype : str
            Data type of the correlation data. Default is '>i4'
            (big-endian 32-bit integer). This is used for unpacking the
            data on the consumer side.


        """
        redis_data = {p.encode("utf-8"): d for p, d in data.items()}
        # add pairs to the set of correlation pairs
        self.r.sadd("corr_pairs", *redis_data.keys())
        # add acc_cnt and dtype to the dict
        redis_data["acc_cnt"] = str(cnt).encode("utf-8")
        redis_data["dtype"] = dtype.encode("utf-8")
        # add the data to the stream
        self.r.xadd(
            "stream:corr",
            redis_data,
            maxlen=self.maxlen["data"],
            approximate=True,
        )
        self.r.sadd("data_streams", "stream:corr")

    def read_corr_data(self, pairs=None, timeout=10, unpack=True):
        """
        Read raw correlation data from Redis and optionally unpack
        from bytes. This is a blocking read, so it will wait until
        data is available.

        Parameters
        ----------
        pairs : list of str
            List of correlation pairs to read. If None, read all pairs.
        timeout : int
            Timeout in seconds for blocking read.
        unpack : bool
            If True, unpack the data from bytes to numpy arrays.
            If False, return the raw bytes.

        Returns
        -------
        acc_cnt : int
            Accumulation count, read from the correlation data.
        sync_time : float
            Synchronization time, when `acc_cnt` is 0.
        data : dict
            If `unpack` is True, return a dictionary with keys as
            correlation pairs and values as numpy arrays of complex
            numbers. If `unpack` is False, values are the raw bytes.

        Raises
        ------
        TimeoutError
            If no data is received within the timeout period.

        Notes
        -----
        The length of the data arrays is the product of the 'nchan' and
        'acc_bins' parameters in the SNAP configuration. The 'nchan'
        parameter is the number of frequency channels, while 'acc_bins'
        is the number of spectra read at each integration step.

        """
        if not self.r.sismember("data_streams", "stream:corr"):
            self.logger.warning(
                "No correlation data stream found. "
                "Please ensure the SNAP is running and sending data."
            )
            return None, None, {}
        if pairs is None:
            pairs = self.r.smembers("corr_pairs")
        last_id = self.data_streams["stream:corr"]
        out = self.r.xread(
            {"stream:corr": last_id},
            count=1,
            block=int(timeout * 1000),
        )
        if not out:
            raise TimeoutError("No correlation data received within timeout.")
        eid, fields = out[0][1][0]
        self._set_last_read_id("stream:corr", eid)  # update last read id
        acc_cnt = int(fields.pop(b"acc_cnt").decode())
        dtype = fields.pop(b"dtype").decode()
        data = {}
        for k, v in fields.items():
            if unpack:
                # unpack the bytes to numpy array of complex numbers
                arr = np.frombuffer(v, dtype=dtype)
            else:
                # return raw bytes
                arr = v
            data[k.decode()] = arr
        sync_time = self.get_live_metadata(keys="corr_sync_time")[
            "sync_time_unix"
        ]
        return acc_cnt, sync_time, data

    def add_vna_data(self, data, header=None, metadata=None):
        """
        Send VNA data to Redis using a stream. Used by client.

        Parameters
        ----------
        data : dict
            Dictionary holding VNA data. Keys are measurement modes and
            values are arrays of complex numbers.
        header : dict
            VNA configuration. To be placed in file header.
        metadata : dict
            Live sensor metadata. To be placed in file header.

        Raises
        ------
        ValueError
            If data is empty or does not contain valid arrays.

        """
        # get array metadata
        arr = next(iter(data.values()), None)
        if arr is None:
            raise ValueError("Data cannot be empty")
        arr_meta = {
            "dtype": arr.dtype.str,
            "shape": arr.shape,
            "order": "C" if arr.flags["C_CONTIGUOUS"] else "F",
        }
        payload = {}
        for k, arr in data.items():
            payload[k] = arr.tobytes()
        payload["arr_meta"] = json.dumps(arr_meta)
        if header is not None:
            _hdr = header.copy()
            for k, v in _hdr.items():
                if isinstance(v, np.ndarray):
                    _hdr[k] = v.tolist()
            payload["header"] = json.dumps(_hdr)
        if metadata is not None:
            _md = metadata.copy()
            for k, v in _md.items():
                if isinstance(v, np.ndarray):
                    _md[k] = v.tolist()
            payload["metadata"] = json.dumps(_md)
        self.r.xadd(
            "stream:vna",
            payload,
            maxlen=self.maxlen["vna_data"],
            approximate=True,
        )
        self.r.sadd("data_streams", "stream:vna")

    def read_vna_data(self, timeout=0):
        """
        Blocking read of VNA data stream from Redis. Used by server.

        Parameters
        ----------
        timeout : int
            Timeout in seconds for blocking read. Set to 0 to block
            indefinitely.

        Returns
        -------
        data : dict
            Dictionary holding VNA data. Keys are measurement modes and
            values are arrays of complex numbers.
        header : dict
            VNA configuration. To be placed in file header.
        metadata : dict
            Live sensor metadata. To be placed in file header.

        Raises
        ------
        TimeoutError
            If no data is received within the timeout period.

        Notes
        -----
        This is a blocking read with a timeout of ``timeout`` seconds.

        """
        if not self.r.sismember("data_streams", "stream:vna"):
            self.logger.warning(
                "No VNA data stream found. "
                "Please ensure the VNA is running and sending data."
            )
            return None, None, None
        last_id = self.data_streams["stream:vna"]
        out = self.r.xread(
            {"stream:vna": last_id},
            count=1,
            block=int(timeout * 1000),
        )
        if not out:
            raise TimeoutError("No VNA data received within timeout.")
        eid, fields = out[0][1][0]
        self._set_last_read_id("stream:vna", eid)  # update last read id
        # extract header, metadata, array_meta
        arr_meta = json.loads(fields.pop(b"arr_meta").decode("utf-8"))
        if b"header" in fields:
            header = json.loads(fields.pop(b"header").decode("utf-8"))
        else:
            header = None
        if b"metadata" in fields:
            metadata = json.loads(fields.pop(b"metadata").decode("utf-8"))
        else:
            metadata = None
        # decode the data arrays
        vna_data = {}
        for k, v in fields.items():
            arr = np.frombuffer(v, dtype=np.dtype(arr_meta["dtype"])).reshape(
                arr_meta["shape"], order=arr_meta["order"]
            )
            vna_data[k.decode()] = arr
        return vna_data, header, metadata
