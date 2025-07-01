"""
Spectrum capture functionality for EIGSEP observing system.
"""

import time
import numpy as np


class SpectrumCapture:
    """Capture and save correlation spectra from Redis."""

    def __init__(self, redis_client):
        """
        Initialize spectrum capture.

        Parameters
        ----------
        redis_client : EigsepRedis
            Redis client instance
        """
        self.redis = redis_client

        # Get configuration from Redis
        try:
            self.corr_cfg = self.redis.get_corr_config()
            self.nchan = self.corr_cfg.get("n_chans", 1024)
            self.sample_rate = self.corr_cfg.get("sample_rate", 500)
        except Exception as e:
            print(f"Warning: Could not get config from Redis: {e}")
            self.nchan = 1024
            self.sample_rate = 500

    def get_spectrum_data(self, pairs=None):
        """
        Get current spectrum data from Redis.

        Parameters
        ----------
        pairs : list of str, optional
            Correlation pairs to retrieve

        Returns
        -------
        data : dict
            Dictionary with pairs as keys and spectrum data as values
        metadata : dict
            Metadata including timestamp and configuration
        """
        if pairs is None:
            pairs = [
                "0",
                "1",
                "2",
                "3",
                "4",
                "5",
                "02",
                "04",
                "24",
                "13",
                "15",
                "35",
            ]

        data = {}
        metadata = {
            "timestamp": time.time(),
            "nchan": self.nchan,
            "sample_rate": self.sample_rate,
            "pairs": pairs,
        }

        # Get accumulation count
        try:
            acc_cnt = self.redis.get_raw("ACC_CNT")
            if acc_cnt is not None:
                metadata["acc_cnt"] = (
                    acc_cnt.decode()
                    if isinstance(acc_cnt, bytes)
                    else str(acc_cnt)
                )
        except Exception:
            pass

        # Get spectrum data for each pair
        for p in pairs:
            try:
                data_key = f"data:{p}"
                data_bytes = self.redis.get_raw(data_key)

                if data_bytes is not None:
                    dt = np.dtype(np.int32).newbyteorder(">")
                    spectrum = np.frombuffer(data_bytes, dtype=dt)

                    if len(p) == 1:  # Auto-correlation
                        data[p] = {"magnitude": spectrum}
                    else:  # Cross-correlation
                        if len(spectrum) >= 2:
                            real = spectrum[::2].astype(np.int64)
                            imag = spectrum[1::2].astype(np.int64)
                            mag = np.sqrt(real**2 + imag**2)
                            phase = np.arctan2(imag, real)
                            data[p] = {
                                "real": real,
                                "imag": imag,
                                "magnitude": mag,
                                "phase": phase,
                            }
            except Exception as e:
                print(f"Error getting data for pair {p}: {e}")

        return data, metadata

    def save_last_n_spectra(
        self, n_spectra=10, pairs=None, filename=None, interval=1.0
    ):
        """
        Capture and save the last N spectra.

        Parameters
        ----------
        n_spectra : int
            Number of spectra to capture
        pairs : list of str, optional
            Correlation pairs to save
        filename : str, optional
            Output filename (default: auto-generated)
        interval : float
            Time interval between captures in seconds

        Returns
        -------
        filename : str
            Path to saved file
        """
        import json
        from datetime import datetime

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"corr_spectra_{timestamp}.json"

        print(f"Capturing {n_spectra} spectra at {interval}s intervals...")

        spectra_list = []
        for i in range(n_spectra):
            print(f"Capturing spectrum {i+1}/{n_spectra}")
            data, metadata = self.get_spectrum_data(pairs)

            # Add sequence number
            metadata["sequence"] = i

            spectra_list.append({"data": data, "metadata": metadata})

            if i < n_spectra - 1:  # Don't sleep after last capture
                time.sleep(interval)

        # Save to JSON file
        with open(filename, "w") as f:
            # Convert numpy arrays to lists for JSON serialization
            json_data = []
            for spectrum in spectra_list:
                json_spectrum = {"metadata": spectrum["metadata"], "data": {}}
                for pair, pair_data in spectrum["data"].items():
                    json_spectrum["data"][pair] = {}
                    for key, value in pair_data.items():
                        if isinstance(value, np.ndarray):
                            json_spectrum["data"][pair][key] = value.tolist()
                        else:
                            json_spectrum["data"][pair][key] = value
                json_data.append(json_spectrum)

            json.dump(json_data, f, indent=2)

        print(f"Saved {n_spectra} spectra to {filename}")
        return filename
