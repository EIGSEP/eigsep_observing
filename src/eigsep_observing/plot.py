from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from eigsep_corr.utils import calc_freqs_dfreq

from .io import reshape_data


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
        self.plot_phase = any(len(p) == 2 for p in self.pairs)
        if self.plot_phase:
            self.plot_delay = plot_delay
        else:
            self.plot_delay = False
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
        freqs, _ = calc_freqs_dfreq(self.sample_rate, self.nchan)
        self.x = freqs

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
        nrows = 1
        if self.plot_phase:
            nrows += 1
        if self.plot_delay:
            nrows += 1
        fig, axs = plt.subplots(figsize=(12, 8), nrows=nrows)
        if nrows == 1:
            axs = [axs]
        # Magnitude plot
        axs[0].grid(True)
        axs[0].set_ylabel("Magnitude")
        if self.log_scale:
            axs[0].set_yscale("log")
            axs[0].set_ylim(1e-2, 1e9)
        else:
            axs[0].set_ylim(0, 3e6)

        # Phase plot
        if self.plot_phase:
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
            "phase": {} if self.plot_phase else None,
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
        acc_cnt, sync_time, data = self.redis.read_corr_data(
            pairs=self.pairs, timeout=0
        )
        data = {k: v for k, v in data.items() if k in self.pairs}
        data = reshape_data(data, avg_even_odd=True)
        # Update magnitude plot
        for p, d in data.items():
            if len(p) == 1:  # Auto-correlation
                self.lines["mag"][p].set_ydata(d)
            else:  # Cross-correlation
                mag = np.abs(d)
                phase = np.angle(d)
                self.lines["mag"][p].set_ydata(mag)
                self.lines["phase"][p].set_ydata(phase)

                # Update delay spectrum if enabled
                if self.plot_delay:
                    dly = np.abs(np.fft.rfft(np.exp(1j * phase))) ** 2
                    self.lines["delay"][p].set_ydata(dly)

        sync = datetime.fromtimestamp(sync_time).strftime("%Y-%m-%d %H:%M:%S")
        self.fig.suptitle(f"{acc_cnt=}, sync_time={sync}")

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
        except KeyboardInterrupt:
            print("\nStopping live plotter...")
            if self.ani:
                self.ani.event_source.stop()
            plt.close(self.fig)
