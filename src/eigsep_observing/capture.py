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

    def save_n_consecutive_spectra(
        self, n_spectra=10, pairs=None, filename=None, timeout=30.0
    ):
        """
        Capture and save N consecutive spectra based on acc_cnt changes.

        Parameters
        ----------
        n_spectra : int
            Number of consecutive spectra to capture
        pairs : list of str, optional
            Correlation pairs to save
        filename : str, optional
            Output filename (default: auto-generated)
        timeout : float
            Maximum time to wait for all spectra in seconds

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

        print(
            f"Capturing {n_spectra} consecutive spectra "
            f"based on acc_cnt changes..."
        )

        # Get initial acc_cnt
        last_acc_cnt = None
        try:
            acc_cnt = self.redis.get_raw("ACC_CNT")
            if acc_cnt is not None:
                last_acc_cnt = (
                    acc_cnt.decode()
                    if isinstance(acc_cnt, bytes)
                    else str(acc_cnt)
                )
        except Exception:
            pass

        spectra_list = []
        start_time = time.time()
        captured = 0

        while captured < n_spectra and (time.time() - start_time) < timeout:
            try:
                # Check for acc_cnt change
                acc_cnt = self.redis.get_raw("ACC_CNT")
                if acc_cnt is not None:
                    current_acc_cnt = (
                        acc_cnt.decode()
                        if isinstance(acc_cnt, bytes)
                        else str(acc_cnt)
                    )

                    # If acc_cnt changed, capture spectrum
                    if (current_acc_cnt != last_acc_cnt and
                            last_acc_cnt is not None):
                        print(
                            f"Capturing spectrum {captured+1}/{n_spectra} "
                            f"(ACC_CNT: {current_acc_cnt})"
                        )
                        data, metadata = self.get_spectrum_data(pairs)

                        # Add sequence number
                        metadata["sequence"] = captured

                        spectra_list.append(
                            {"data": data, "metadata": metadata}
                        )
                        captured += 1

                    last_acc_cnt = current_acc_cnt

                # Small sleep to avoid overwhelming the system
                time.sleep(0.01)

            except Exception as e:
                print(f"Error during capture: {e}")
                time.sleep(0.1)

        if captured < n_spectra:
            print(
                f"Warning: Only captured {captured}/{n_spectra} spectra "
                f"(timeout reached)"
            )
        else:
            print(f"Successfully captured {captured} consecutive spectra")

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

        print(f"Saved {captured} spectra to {filename}")
        return filename

    def save_last_n_spectra(
        self, n_spectra=10, pairs=None, filename=None, interval=None
    ):
        """
        Legacy method for backward compatibility.
        Captures N consecutive spectra based on acc_cnt changes.

        Note: interval parameter is ignored - capture is based on
        acc_cnt changes.

        Parameters
        ----------
        n_spectra : int
            Number of consecutive spectra to capture
        pairs : list of str, optional
            Correlation pairs to save
        filename : str, optional
            Output filename (default: auto-generated)
        interval : float, optional
            IGNORED - kept for backward compatibility

        Returns
        -------
        filename : str
            Path to saved file
        """
        if interval is not None:
            print(
                "Warning: interval parameter is ignored. "
                "Capture is based on acc_cnt changes."
            )

        return self.save_n_consecutive_spectra(
            n_spectra=n_spectra,
            pairs=pairs,
            filename=filename
        )
