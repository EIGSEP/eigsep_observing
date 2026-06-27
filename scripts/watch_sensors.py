"""Passive multi-sensor terminal monitor.

Reads the panda metadata snapshot at ``--interval`` seconds and displays a
refreshing terminal table for all registered panda sensor streams, or a
user-selected subset via ``--streams``.  With ``--plot`` a matplotlib window
opens with rolling time-series traces for the primary float fields of each
selected stream.

Purely read-only — uses :class:`~eigsep_redis.MetadataSnapshotReader` (no
position pointer, no competing with the corr loop or stream recorder).  Claims
no ``run_tag``; safe to run alongside any active driver (``panda_observe``,
``motor_scan``, ``vna_manual``, etc.).
"""

from argparse import ArgumentParser
import logging
import sys
import time

from eigsep_redis import MetadataSnapshotReader

from eigsep_observing._scripts_util import add_redis_args, build_transport
from eigsep_observing.io import SENSOR_SCHEMAS
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)

PLOT_WINDOW_S = 60.0

# adc_stats is published on the SNAP transport; the panda transport this
# script connects to never carries adc_stats readings.  Exclude it from the
# default stream list so --help stays clean.
_PANDA_STREAMS = [s for s in SENSOR_SCHEMAS if s != "adc_stats"]

# Curated float fields to trace per stream in the rolling plot.  Keeps
# tempctrl panels readable (3 key fields instead of 11) and potmon focused on
# derived angles rather than calibration constants.  Streams absent from this
# dict fall back to all float fields in their SENSOR_SCHEMAS entry.
_PLOT_FIELDS = {
    "imu_el": ("yaw", "pitch", "roll"),
    "imu_az": ("yaw", "pitch", "roll"),
    "tempctrl_lna": ("T_now", "T_target", "drive_level"),
    "tempctrl_load": ("T_now", "T_target", "drive_level"),
    "potmon": ("pot_az_angle",),
    "motor": ("az_pos", "el_pos"),
    "lidar": ("distance_m",),
    "system_current": ("current_a",),
}

# Shown in the stream header line instead; redundant in the field list.
_SKIP_TEXT = frozenset({"sensor_name", "app_id", "status"})


def _plot_fields_for(stream):
    """Float fields to trace for ``stream`` in the rolling plot."""
    if stream in _PLOT_FIELDS:
        return list(_PLOT_FIELDS[stream])
    schema = SENSOR_SCHEMAS.get(stream, {})
    return [k for k, t in schema.items() if t is float]


# ── text display ──────────────────────────────────────────────────────────────


def _fmt(v):
    if v is None:
        return "--"
    if isinstance(v, float):
        return f"{v:.4g}"
    return str(v)


def _render(snap, streams):
    sys.stdout.write("\x1b[2J\x1b[H")
    print(f"watch_sensors @ {time.strftime('%H:%M:%S')}   Ctrl-C to exit")
    for name in streams:
        reading = snap.get(name)
        print()
        if reading is None:
            print(f"=== {name} ===  (no data — is the pico up?)")
            continue
        status = reading.get("status", "?")
        print(f"=== {name} === [{status}]")
        pairs = [(k, v) for k, v in reading.items() if k not in _SKIP_TEXT]
        for i in range(0, len(pairs), 3):
            chunk = pairs[i : i + 3]
            print("  " + "   ".join(f"{k}: {_fmt(v)}" for k, v in chunk))
    print()
    sys.stdout.flush()


# ── rolling plot ──────────────────────────────────────────────────────────────


class _PlotHistory:
    """Rolling buffer of float readings for one stream."""

    def __init__(self, fields, *, window_s):
        self.fields = list(fields)
        self.window_s = window_s
        self._t0 = None
        self.t = []
        self.values = {f: [] for f in self.fields}

    def record(self, reading, *, now):
        if self._t0 is None:
            self._t0 = now
        self.t.append(now - self._t0)
        for f in self.fields:
            v = (reading or {}).get(f)
            ok = isinstance(v, (int, float)) and not isinstance(v, bool)
            self.values[f].append(float(v) if ok else float("nan"))
        cutoff = self.t[-1] - self.window_s
        while self.t and self.t[0] < cutoff:
            self.t.pop(0)
            for vals in self.values.values():
                vals.pop(0)


class _LivePlot:
    """Rolling float traces, one matplotlib panel per stream."""

    def __init__(self, streams, *, window_s):
        import matplotlib.pyplot as plt

        self._plt = plt
        # rfswitch and any other stream with no float fields are excluded from
        # the plot (still shown in the text display).
        self._streams = [s for s in streams if _plot_fields_for(s)]
        if not self._streams:
            raise SystemExit(
                "ERROR: none of the selected streams have float fields to plot."
            )
        self.histories = {
            s: _PlotHistory(_plot_fields_for(s), window_s=window_s)
            for s in self._streams
        }
        n = len(self._streams)
        self.fig, axes = plt.subplots(
            n, 1, sharex=True, squeeze=False, figsize=(10, max(3, 3 * n))
        )
        self._axes = dict(zip(self._streams, axes.flat))
        self._lines = {}
        for name in self._streams:
            ax = self._axes[name]
            self._lines[name] = {}
            for i, field in enumerate(_plot_fields_for(name)):
                (line,) = ax.plot([], [], color=f"C{i}", label=field)
                self._lines[name][field] = line
            ax.set_title(name)
            ax.grid(True, alpha=0.3)
            ax.legend(loc="upper left")
        list(self._axes.values())[-1].set_xlabel("elapsed time (s)")
        self.fig.tight_layout()

    def update(self, snap):
        now = time.monotonic()
        for name in self._streams:
            hist = self.histories[name]
            hist.record(snap.get(name), now=now)
            ax = self._axes[name]
            for field, line in self._lines[name].items():
                line.set_data(hist.t, hist.values[field])
            ax.relim()
            ax.autoscale_view()

    def pause(self, interval_s):
        self._plt.pause(interval_s)


def _require_interactive_backend():
    import matplotlib

    backend = matplotlib.get_backend()
    if backend.lower() in ("agg", "pdf", "ps", "svg", "template", "cairo"):
        raise SystemExit(
            f"ERROR: --plot needs a GUI display but matplotlib selected "
            f"the non-interactive {backend!r} backend. Run from a desktop "
            f"session or with X forwarding (ssh -X)."
        )


# ── main loop ─────────────────────────────────────────────────────────────────


def _render_loop(snapshot, streams, interval_s, plot):
    while True:
        snap = snapshot.get()
        _render(snap, streams)
        if plot is None:
            time.sleep(interval_s)
        else:
            plot.update(snap)
            plot.pause(interval_s)


def _parse_args():
    p = ArgumentParser(
        description=(
            "Passive multi-sensor terminal monitor. "
            "Reads the panda metadata snapshot and refreshes in-place. "
            "Add --plot for a matplotlib rolling-trace window."
        ),
    )
    p.add_argument(
        "--streams",
        nargs="+",
        default=list(_PANDA_STREAMS),
        metavar="STREAM",
        help=(
            f"Streams to display. "
            f"Available: {', '.join(_PANDA_STREAMS)}. "
            "Default: all panda streams."
        ),
    )
    p.add_argument(
        "--plot",
        action="store_true",
        help=(
            "Open a matplotlib window with rolling float traces "
            "(one panel per stream). Requires a GUI display or ssh -X."
        ),
    )
    p.add_argument(
        "--window",
        type=float,
        default=PLOT_WINDOW_S,
        dest="window_s",
        metavar="SECS",
        help=f"Rolling plot window in seconds (default: {PLOT_WINDOW_S}).",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=0.2,
        metavar="SECS",
        help="Refresh interval in seconds (default: 0.2).",
    )
    p.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient.",
    )
    add_redis_args(p, default_host="10.10.10.11")
    return p.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    # Passive readout: no run_tag.session, by design. Snapshot-only
    # (MetadataSnapshotReader), no commands or files, must coexist with
    # any active driver. See scripts/CLAUDE.md for the passive/active rule.
    snapshot = MetadataSnapshotReader(transport)
    plot = None
    if args.plot:
        _require_interactive_backend()
        plot = _LivePlot(args.streams, window_s=args.window_s)
    try:
        _render_loop(snapshot, args.streams, args.interval, plot)
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
