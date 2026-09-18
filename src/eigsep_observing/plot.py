import time
from collections import deque

import matplotlib.pyplot as plt
import numpy as np
from eigsep_redis.metadata import MetadataSnapshotReader
from matplotlib.animation import FuncAnimation

from .corr import CorrConfigStore, CorrReader
from .io import corr_pair_labels, reshape_data
from .utils import calc_freqs_dfreq


class LivePlotter:
    """Real-time plotter for correlation spectra from Redis streams."""

    def __init__(
        self,
        transport,
        pairs=None,
        plot_delay=False,
        log_scale=True,
        poll_interval=50,
        channel=None,
        history_len=200,
        transport_panda=None,
        metadata_key=None,
        metadata_field="T_now",
    ):
        """
        Initialize the live plotter.

        Parameters
        ----------
        transport : eigsep_redis.Transport
            Shared Redis transport (SNAP side). The plotter builds its
            own ``CorrReader`` / ``CorrConfigStore`` surfaces from it.
        pairs : list of str
            Correlation pairs to plot (e.g., ['0', '1', '02', '13'])
        plot_delay : bool
            Whether to plot delay spectrum
        log_scale : bool
            Use logarithmic scale for magnitude plot
        poll_interval : int
            Polling interval in milliseconds to check for new data.
        channel : int or None
            If given, adds a subplot tracking the magnitude at this
            single channel index across successive integrations (a
            rolling strip chart), retaining up to ``history_len``
            points per pair. ``None`` (default) disables this subplot
            — corr-only callers pay no extra cost.
        history_len : int
            Number of points retained in the channel-history and
            metadata strip charts (oldest points drop off as new ones
            arrive).
        transport_panda : eigsep_redis.Transport or None
            Redis transport for the panda side (``panda_ip``), required
            only when ``metadata_key`` is given — pico sensor metadata
            (e.g. ``tempctrl_load``) is published on the panda's Redis,
            a separate instance from the SNAP-side ``transport``. A
            connection failure here (panda down) only disables the
            metadata strip chart; it never blocks the corr spectra —
            corr data is sacred.
        metadata_key : str or None
            Sensor stream name to plot alongside the corr spectra
            (e.g. ``"tempctrl_load"``). Requires ``transport_panda``.
            ``None`` (default) disables this subplot.
        metadata_field : str
            Field within ``metadata_key``'s snapshot dict to plot
            (e.g. ``"T_now"`` for the tempctrl LOAD channel's live
            temperature).
        """
        self.transport = transport
        self.corr_reader = CorrReader(transport)
        self.corr_config = CorrConfigStore(transport)
        self.pairs = pairs or [
            "0",
            "1",
            "2",
            "3",
            "4",
            #            "5",
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

        # -- channel-history strip chart (opt-in) --------------------
        self.channel = channel
        self.history_len = history_len
        self._last_acc_cnt = None
        self._integration_i = 0
        self.channel_history = (
            {p: deque(maxlen=history_len) for p in self.pairs}
            if channel is not None
            else None
        )
        self.channel_history_x = (
            deque(maxlen=history_len) if channel is not None else None
        )

        # -- metadata strip chart (opt-in) ----------------------------
        if metadata_key is not None and transport_panda is None:
            raise ValueError(
                "metadata_key requires transport_panda: pico sensor "
                "metadata is published on the panda-side Redis, not "
                "the SNAP-side transport."
            )
        self.metadata_key = metadata_key
        self.metadata_field = metadata_field
        self.metadata_snapshot = (
            MetadataSnapshotReader(transport_panda)
            if transport_panda is not None
            else None
        )
        self.metadata_history = (
            deque(maxlen=history_len) if metadata_key is not None else None
        )
        self.metadata_history_x = (
            deque(maxlen=history_len) if metadata_key is not None else None
        )
        self._t0 = time.time()

        # Get configuration from Redis
        self.corr_cfg = self.corr_config.get()
        # Header carries the effective input->antenna map (mux-aware).
        # Absent on a cold Redis -> fall back to raw digital labels.
        try:
            self.corr_header = self.corr_config.get_header()
        except ValueError:
            self.corr_header = {}
        self.labels = corr_pair_labels(self.corr_header, self.pairs)
        self.nchan = self.corr_cfg.get("n_chans", 1024)
        self.sample_rate = self.corr_cfg.get("sample_rate", 500)
        if self.channel is not None and not (0 <= self.channel < self.nchan):
            raise ValueError(
                f"channel={self.channel} out of range for nchan="
                f"{self.nchan} (from corr_config)."
            )

        # Frequency axis
        freqs, _ = calc_freqs_dfreq(self.sample_rate, self.nchan)
        self.x = freqs

        # Color mapping
        self.colors = self._setup_colors()

        # Initialize plots
        fig, self.axs = self._setup_plots()
        self.lines = self._setup_lines()
        self.axs[0].legend(bbox_to_anchor=(1.01, 1), loc="upper left")
        if self.ax_channel_hist is not None:
            self.ax_channel_hist.legend(
                bbox_to_anchor=(1.01, 1), loc="upper left"
            )
        self.fig = fig

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
                colors[p] = f"C{i - 2}"
        return colors

    def _setup_plots(self):
        """Set up matplotlib figure and axes."""
        nrows = 1
        if self.plot_phase:
            nrows += 1
        if self.plot_delay:
            nrows += 1
        if self.channel is not None:
            nrows += 1
        if self.metadata_key is not None:
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

        # Share x-axis across the frequency/delay-based rows only —
        # the channel-history and metadata strip charts below have
        # their own "integration index" / "time" x-axis and must not
        # be linked to the frequency axis.
        freq_rows = 1 + int(self.plot_phase) + int(self.plot_delay)
        if freq_rows > 1:
            axs[0].sharex(axs[1])
            if self.plot_delay:
                axs[1].sharex(axs[2])

        # Channel-history strip chart (optional): magnitude at a single
        # channel index vs. integration number, retaining history —
        # unlike the spectrum rows above, which redraw in place every
        # frame and show only the latest integration.
        next_row = freq_rows
        self.ax_channel_hist = None
        if self.channel is not None:
            ax = axs[next_row]
            ax.grid(True)
            ax.set_ylabel(f"Chan {self.channel} mag")
            ax.set_xlabel("Integration #")
            if self.log_scale:
                ax.set_yscale("log")
            self.ax_channel_hist = ax
            next_row += 1

        # Metadata strip chart (optional): a single sensor field vs.
        # wallclock time (seconds since plotter start).
        self.ax_metadata = None
        if self.metadata_key is not None:
            ax = axs[next_row]
            ax.grid(True)
            ax.set_ylabel(f"{self.metadata_key}.{self.metadata_field}")
            ax.set_xlabel("Time (s)")
            self.ax_metadata = ax
            next_row += 1

        plt.tight_layout()
        plt.subplots_adjust(right=0.82)
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
                "label": self.labels.get(p) or p,
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

        # Channel-history strip chart: one line per pair, empty until
        # the first integration arrives (set_data populates it).
        if self.channel is not None:
            lines["channel_hist"] = {}
            for p in self.pairs:
                (line,) = self.ax_channel_hist.plot(
                    [],
                    [],
                    color=self.colors[p],
                    label=self.labels.get(p) or p,
                    linewidth=1.5,
                )
                lines["channel_hist"][p] = line

        # Metadata strip chart: single line, empty until the first
        # reading arrives.
        if self.metadata_key is not None:
            (line,) = self.ax_metadata.plot(
                [], [], color="C0", linewidth=1.5
            )
            lines["metadata"] = line

        return lines

    def update_plot(self, frame):
        """Update plot data (called by animation)."""
        acc_cnt, data = self.corr_reader.read(pairs=self.pairs, timeout=0)
        data = {k: v for k, v in data.items() if k in self.pairs}
        data = reshape_data(
            data,
            acc_bins=self.corr_cfg.get("acc_bins", 2),
            avg_even_odd=self.corr_cfg.get("avg_even_odd", True),
        )
        # Update magnitude plot
        mags = {}
        for p, d in data.items():
            if len(p) == 1:  # Auto-correlation
                mag = d
                self.lines["mag"][p].set_ydata(mag)
            else:  # Cross-correlation
                # reshape_data returns (nchan, 2) int32; reconstruct
                # complex for magnitude/phase extraction.
                d = d[..., 0] + 1j * d[..., 1]
                mag = np.abs(d)
                phase = np.angle(d)
                self.lines["mag"][p].set_ydata(mag)
                self.lines["phase"][p].set_ydata(phase)

                # Update delay spectrum if enabled
                if self.plot_delay:
                    dly = np.abs(np.fft.rfft(np.exp(1j * phase))) ** 2
                    self.lines["delay"][p].set_ydata(dly)
            mags[p] = mag

        # Channel-history strip chart: only advance on a genuinely new
        # integration (acc_cnt changed) — timeout=0 can otherwise
        # return the same integration on back-to-back animation
        # frames, which would pad the history with duplicate points.
        if (
            self.channel is not None
            and acc_cnt is not None
            and acc_cnt != self._last_acc_cnt
        ):
            self._last_acc_cnt = acc_cnt
            self._integration_i += 1
            self.channel_history_x.append(self._integration_i)
            for p in self.pairs:
                mag = mags.get(p)
                # mag carries a leading singleton ntimes axis (one
                # integration per read); flatten before indexing by
                # channel so this doesn't care whether reshape_data
                # handed back (nchan,) or (1, nchan).
                value = (
                    float(np.asarray(mag).reshape(-1)[self.channel])
                    if mag is not None
                    else None
                )
                self.channel_history[p].append(value)
                self.lines["channel_hist"][p].set_data(
                    list(self.channel_history_x), list(self.channel_history[p])
                )
            self.ax_channel_hist.relim()
            self.ax_channel_hist.autoscale_view()

        # Metadata strip chart: independent of the corr cadence, reads
        # whatever the panda's snapshot hash currently holds. A dead
        # panda (ConnectionError) only disables this subplot for the
        # tick — corr data is sacred and must never block on it.
        if self.metadata_key is not None:
            try:
                entry = self.metadata_snapshot.get(self.metadata_key)
            except KeyError:
                # Stream not published yet (panda/pico not up yet) —
                # benign startup state, not an error.
                entry = None
            except ConnectionError as exc:
                entry = None
                print(
                    f"live_plotter: metadata read failed for "
                    f"{self.metadata_key!r}: {exc}"
                )
            value = (
                entry.get(self.metadata_field)
                if isinstance(entry, dict)
                else None
            )
            if value is not None:
                self.metadata_history_x.append(time.time() - self._t0)
                self.metadata_history.append(float(value))
                self.lines["metadata"].set_data(
                    list(self.metadata_history_x), list(self.metadata_history)
                )
                self.ax_metadata.relim()
                self.ax_metadata.autoscale_view()

        artists = list(self.lines["mag"].values())
        if self.channel is not None:
            artists += list(self.lines["channel_hist"].values())
        if self.metadata_key is not None:
            artists.append(self.lines["metadata"])
        return artists

    def start(self):
        """Start the live plotting animation."""

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
            print("\nStopping live plotter.")
        finally:
            if self.ani:
                self.ani.event_source.stop()
            plt.close(self.fig)
