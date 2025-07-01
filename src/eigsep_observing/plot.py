"""
Live plotting functionality for EIGSEP observing system.
"""

import time
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

try:
    from eigsep_corr.utils import calc_freqs_dfreq
except ImportError:
    calc_freqs_dfreq = None


class LivePlotter:
    """Real-time plotter for correlation spectra from Redis streams."""

    def __init__(
        self,
        redis_client,
        pairs=None,
        plot_delay=False,
        log_scale=True,
        poll_interval=50,
    ):
        """
        Initialize the live plotter.

        Parameters
        ----------
        redis_client : EigsepRedis
            Redis client instance
        pairs : list of str
            Correlation pairs to plot (e.g., ['0', '1', '02', '13'])
        plot_delay : bool
            Whether to plot delay spectrum
        log_scale : bool
            Use logarithmic scale for magnitude plot
        poll_interval : int
            Polling interval in milliseconds to check for acc_cnt changes
        """
        self.redis = redis_client
        self.pairs = pairs or [
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
        self.plot_delay = plot_delay
        self.log_scale = log_scale
        self.poll_interval = poll_interval
        self.last_acc_cnt = None

        # Get configuration from Redis
        try:
            self.corr_cfg = self.redis.get_corr_config()
            self.nchan = self.corr_cfg.get("n_chans", 1024)
            self.sample_rate = self.corr_cfg.get("sample_rate", 500)
        except Exception as e:
            print(f"Warning: Could not get config from Redis: {e}")
            print("Using default values: nchan=1024, sample_rate=500")
            self.nchan = 1024
            self.sample_rate = 500

        # Frequency axis
        if calc_freqs_dfreq is not None:
            try:
                freqs, _ = calc_freqs_dfreq(self.nchan, self.sample_rate)
                self.x = freqs
            except Exception:
                self.x = np.linspace(
                    0, self.sample_rate / 2, num=self.nchan, endpoint=False
                )
        else:
            self.x = np.linspace(
                0, self.sample_rate / 2, num=self.nchan, endpoint=False
            )

        # Color mapping
        self.colors = self._setup_colors()

        # Initialize plots
        self.fig, self.axs = self._setup_plots()
        self.lines = self._setup_lines()

        # Animation
        self.ani = None

    def _setup_colors(self):
        """Set up color mapping for correlation pairs."""
        colors = {}
        for i, p in enumerate(self.pairs):
            if i == 0:
                colors[p] = "black"
            elif i == 1:
                colors[p] = "lime"
            else:
                colors[p] = f"C{i-2}"
        return colors

    def _setup_plots(self):
        """Set up matplotlib figure and axes."""
        nrows = 3 if self.plot_delay else 2
        plt.ion()
        fig, axs = plt.subplots(figsize=(12, 8), nrows=nrows)

        # Magnitude plot
        axs[0].grid(True)
        axs[0].set_ylabel("Magnitude")
        axs[0].set_title("Live Correlation Spectra")
        if self.log_scale:
            axs[0].set_yscale("log")
            axs[0].set_ylim(1e-2, 1e9)
        else:
            axs[0].set_ylim(0, 3e6)

        # Phase plot
        axs[1].grid(True)
        axs[1].set_ylabel("Phase (rad)")
        axs[1].set_ylim(-np.pi, np.pi)
        axs[1].set_xlabel("Frequency (MHz)")

        # Delay plot (optional)
        if self.plot_delay:
            axs[2].grid(True)
            axs[2].set_ylabel("Delay Spectrum")
            axs[2].set_xlabel("Delay (ns)")

        # Share x-axis
        if len(axs) > 1:
            axs[0].sharex(axs[1])
            if self.plot_delay:
                axs[1].sharex(axs[2])

        # Legend
        axs[0].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        plt.tight_layout()
        return fig, axs

    def _setup_lines(self):
        """Initialize plot lines for each correlation pair."""
        lines = {
            "mag": {},
            "phase": {},
            "delay": {} if self.plot_delay else None,
        }

        for p in self.pairs:
            line_kwargs = {
                "color": self.colors[p],
                "label": p,
                "linewidth": 1.5,
            }

            # Magnitude line
            if self.log_scale:
                (line,) = self.axs[0].semilogy(
                    self.x, np.ones(self.nchan), **line_kwargs
                )
            else:
                (line,) = self.axs[0].plot(
                    self.x, np.ones(self.nchan), **line_kwargs
                )
            lines["mag"][p] = line

            # Phase line (only for cross-correlations)
            if len(p) == 2:
                (line,) = self.axs[1].plot(
                    self.x, np.zeros(self.nchan), **line_kwargs
                )
                lines["phase"][p] = line

                # Delay line (optional)
                if self.plot_delay:
                    tau = np.fft.rfftfreq(self.nchan, d=self.x[1] - self.x[0])
                    tau *= 1e3  # convert to ns
                    (line,) = self.axs[2].plot(
                        tau, np.ones_like(tau), **line_kwargs
                    )
                    lines["delay"][p] = line

        return lines

    def update_plot(self, frame):
        """Update plot data (called by animation)."""
        try:
            # Check if acc_cnt has changed
            acc_cnt = self.redis.get_raw("ACC_CNT")
            if acc_cnt is not None:
                current_acc_cnt = (
                    acc_cnt.decode()
                    if isinstance(acc_cnt, bytes)
                    else str(acc_cnt)
                )

                # Only update plot if acc_cnt has changed
                if current_acc_cnt == self.last_acc_cnt:
                    return list(self.lines["mag"].values())

                self.last_acc_cnt = current_acc_cnt
            for p in self.pairs:
                # Get data from Redis
                data_key = f"data:{p}"
                data_bytes = self.redis.get_raw(data_key)

                if data_bytes is None:
                    continue

                # Parse correlation data
                dt = np.dtype(np.int32).newbyteorder(">")
                data = np.frombuffer(data_bytes, dtype=dt)

                if len(data) == 0:
                    continue

                # Update magnitude plot
                if len(p) == 1:  # Auto-correlation
                    if len(data) == len(self.x):
                        self.lines["mag"][p].set_ydata(data)
                else:  # Cross-correlation
                    if len(data) < 2:
                        continue
                    real = data[::2].astype(np.int64)
                    imag = data[1::2].astype(np.int64)

                    # Ensure arrays have correct length
                    min_len = min(len(real), len(imag), self.nchan)
                    real = real[:min_len]
                    imag = imag[:min_len]

                    # Calculate magnitude and phase
                    mag = np.sqrt(real**2 + imag**2)
                    phase = np.arctan2(imag, real)

                    # Update plots
                    if min_len == len(self.x):
                        self.lines["mag"][p].set_ydata(mag)
                        if p in self.lines["phase"]:
                            self.lines["phase"][p].set_ydata(phase)

                        # Update delay spectrum if enabled
                        if self.plot_delay and p in self.lines["delay"]:
                            dly = np.abs(np.fft.rfft(np.exp(1j * phase))) ** 2
                            self.lines["delay"][p].set_ydata(dly)

            # Update title with current acc_cnt
            if self.last_acc_cnt is not None:
                self.fig.suptitle(
                    f"Live Correlation Spectra (ACC_CNT: {self.last_acc_cnt})"
                )

        except Exception as e:
            print(f"Error updating plot: {e}")

        return list(self.lines["mag"].values())

    def start(self):
        """Start the live plotting animation."""
        print("Starting live plotter...")
        print(
            f"Configuration: nchan={self.nchan}, "
            f"sample_rate={self.sample_rate}"
        )
        print(f"Plotting pairs: {self.pairs}")
        print("Press Ctrl+C to stop")

        self.ani = FuncAnimation(
            self.fig,
            self.update_plot,
            interval=self.poll_interval,
            blit=False,
            cache_frame_data=False,
        )

        try:
            plt.show()
            # Keep the script running
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("\nStopping live plotter...")
            if self.ani:
                self.ani.event_source.stop()
            plt.close(self.fig)
