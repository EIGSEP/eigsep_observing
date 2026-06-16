"""Live IMU readout for bring-up (imu_el and/or imu_az).

The two IMU picos share firmware (BNO085 in UART RVC mode under
picohost 1.0.0) and publish only ``yaw``/``pitch``/``roll`` (deg) and
``accel_x``/``accel_y``/``accel_z`` (m/s²). Tilt the rig by hand and
watch the numbers move; pitch and roll get an ASCII level bar centered
at zero so a visual sweep is immediately obvious.

``--plot`` additionally opens a matplotlib window with rolling
yaw/pitch/roll traces (last ``PLOT_WINDOW_S`` seconds, one panel per
IMU) fed from the same snapshot reads as the text readout, so a slow
drift or a sign flip is visible at a glance. Angles only — accel stays
on the text readout. Requires a GUI display (desktop session or
``ssh -X``); the script exits with an actionable error if matplotlib
fell back to a non-interactive backend.

Read-only — IMUs accept no commands. The script asserts at import that
the schema hasn't drifted (e.g. picohost re-adding a quaternion field
would change reduction semantics in ``io.py`` and warrant updating
this display).
"""

from argparse import ArgumentParser
import logging
import sys
import time

from eigsep_redis import MetadataSnapshotReader
import matplotlib
import matplotlib.pyplot as plt

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import build_transport
from eigsep_observing.io import _IMU_SCHEMA
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)

# Field names this script renders. Asserting against the schema keeps
# the display honest: if the IMU schema grows or loses a field, this
# import-time check fires so the script can't silently mis-render.
_EXPECTED = {
    "yaw",
    "pitch",
    "roll",
    "accel_x",
    "accel_y",
    "accel_z",
}
_missing = _EXPECTED - set(_IMU_SCHEMA)
assert not _missing, (
    f"imu_manual expects fields {_EXPECTED} from _IMU_SCHEMA; missing "
    f"{_missing}. Has the IMU schema changed in io.py?"
)

BAR_WIDTH = 41  # odd so the zero-tick lines up

# Rolling live-plot window (`--plot`): at the default 0.2 s refresh this
# is ~300 points per trace, cheap to redraw in full every tick.
PLOT_WINDOW_S = 60.0
# Angle fields traced by `--plot`. Accel stays on the text readout only:
# the bring-up signal is orientation, and three more autoscaled panels
# would shrink the traces the operator actually watches.
PLOT_FIELDS = ("yaw", "pitch", "roll")


def _read_imus(snapshot, names):
    """One snapshot read shared by the text readout and the live plot.

    Maps each name to its latest reading dict, or None if that IMU has
    never published.
    """
    snap = snapshot.get()
    return {name: snap.get(name) or None for name in names}


class _PlotHistory:
    """Rolling buffer of angle readings for the live plot.

    One sample per loop tick: elapsed seconds since the first sample,
    plus each IMU's :data:`PLOT_FIELDS` values. A field that is missing
    or non-numeric (sensor error nulls fields per ``_IMU_SCHEMA``) is
    stored as ``float("nan")`` so a dropout becomes a gap in the trace
    rather than a spurious zero or a crash. Samples older than
    ``window_s`` are dropped from the front so the plot stays a
    fixed-width rolling window (and memory stays bounded).
    """

    def __init__(self, names, *, window_s=PLOT_WINDOW_S):
        self.names = list(names)
        self.window_s = window_s
        self._t0 = None
        self.t = []
        # {name: {field: [values]}}
        self.values = {
            name: {field: [] for field in PLOT_FIELDS} for name in self.names
        }

    def record(self, readings, *, now):
        """Append one sample from ``readings`` at monotonic ``now``.

        ``now`` is passed in (rather than read here) so the loop's
        single ``time.monotonic()`` call is reused and the elapsed-time
        axis is consistent with the render cadence.
        """
        if self._t0 is None:
            self._t0 = now
        self.t.append(now - self._t0)
        for name in self.names:
            r = readings.get(name) or {}
            for field in PLOT_FIELDS:
                v = r.get(field)
                ok = isinstance(v, (int, float)) and not isinstance(v, bool)
                self.values[name][field].append(
                    float(v) if ok else float("nan")
                )
        cutoff = self.t[-1] - self.window_s
        while self.t and self.t[0] < cutoff:
            self.t.pop(0)
            for fields in self.values.values():
                for vals in fields.values():
                    vals.pop(0)

    def __len__(self):
        return len(self.t)


class _LivePlot:
    """Matplotlib window with one rolling yaw/pitch/roll panel per IMU.

    ``update`` records into the owned :class:`_PlotHistory` and pushes
    the buffers onto the lines; the render loop's ``plt.pause`` does
    the actual drawing and keeps the GUI responsive between ticks.
    """

    def __init__(self, names, *, window_s=PLOT_WINDOW_S):
        self.history = _PlotHistory(names, window_s=window_s)
        self.fig, axes = plt.subplots(
            len(names),
            1,
            sharex=True,
            squeeze=False,
            figsize=(10, 3 * len(names)),
        )
        self.axes = list(axes.flat)
        self.lines = {}
        for ax, name in zip(self.axes, names):
            self.lines[name] = {}
            for i, field in enumerate(PLOT_FIELDS):
                (line,) = ax.plot([], [], color=f"C{i}", label=field)
                self.lines[name][field] = line
            ax.set_title(name)
            ax.set_ylabel("angle (deg)")
            ax.grid(True, alpha=0.3)
            ax.legend(loc="upper left")
        self.axes[-1].set_xlabel("elapsed time (s)")
        self.fig.tight_layout()

    def update(self, readings, *, now):
        self.history.record(readings, now=now)
        t = self.history.t
        for name, fields in self.lines.items():
            for field, line in fields.items():
                line.set_data(t, self.history.values[name][field])
        for ax in self.axes:
            ax.relim()
            ax.autoscale_view()


def _require_interactive_backend():
    """Exit with an actionable error if matplotlib has no GUI backend.

    Headless (no display), matplotlib silently falls back to the
    non-interactive Agg backend; a "live" window would then never
    appear and ``plt.pause`` would warn on every tick. Fail once,
    loudly, before the loop starts.
    """
    backend = matplotlib.get_backend()
    if backend.lower() in ("agg", "pdf", "ps", "svg", "template", "cairo"):
        raise SystemExit(
            f"ERROR: --plot needs a GUI display but matplotlib selected "
            f"the non-interactive {backend!r} backend. Run from a desktop "
            f"session or with X forwarding (ssh -X)."
        )


def _angle_bar(value, *, full_scale=90.0):
    """Center-zero bar for [-full_scale, +full_scale] degrees."""
    if value is None:
        return " " * BAR_WIDTH
    clamped = max(-full_scale, min(full_scale, float(value)))
    frac = (clamped + full_scale) / (2 * full_scale)
    pos = int(round(frac * (BAR_WIDTH - 1)))
    cells = [" "] * BAR_WIDTH
    cells[BAR_WIDTH // 2] = "|"  # zero tick
    cells[pos] = "#"
    return "".join(cells)


def _fmt_field(reading, key, fmt):
    if reading is None:
        return "   --"
    v = reading.get(key)
    if not isinstance(v, (int, float)):
        return "   --"
    return format(v, fmt)


def _render(readings, names):
    sys.stdout.write("\x1b[2J\x1b[H")
    print(f"imu_manual @ {time.strftime('%H:%M:%S')}")
    for name in names:
        r = readings[name]
        print()
        print(f"=== {name} ===")
        if r is None:
            print("  (no reading; is the pico flashed and the manager up?)")
            continue
        yaw = _fmt_field(r, "yaw", "7.2f")
        pitch = _fmt_field(r, "pitch", "7.2f")
        roll = _fmt_field(r, "roll", "7.2f")
        ax = _fmt_field(r, "accel_x", "6.2f")
        ay = _fmt_field(r, "accel_y", "6.2f")
        az = _fmt_field(r, "accel_z", "6.2f")
        print(f"  yaw   {yaw} deg")
        print(f"  pitch {pitch} deg  [{_angle_bar(r.get('pitch'))}]")
        print(f"  roll  {roll} deg  [{_angle_bar(r.get('roll'))}]")
        print(f"  accel ({ax}, {ay}, {az}) m/s²  status={r.get('status')!r}")
    print()
    print("Ctrl-C to exit.")
    sys.stdout.flush()


def _render_loop(snapshot, names, interval_s, plot=None):
    while True:
        readings = _read_imus(snapshot, names)
        _render(readings, names)
        if plot is None:
            time.sleep(interval_s)
        else:
            plot.update(readings, now=time.monotonic())
            # pause instead of sleep: draws the figure and runs the GUI
            # event loop so the window stays responsive between ticks.
            plt.pause(interval_s)


def _parse_args():
    parser = ArgumentParser(description="Live IMU readout for imu_el/imu_az.")
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient",
    )
    parser.add_argument(
        "--redis-host",
        default="localhost",
        help="Redis host (default: localhost). Set to the panda's IP to "
        "run this readout from another computer on the rig network.",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port (default: 6379). Ignored in --dummy mode, which "
        "always targets the local fakeredis on 6380.",
    )
    parser.add_argument(
        "--which",
        choices=("el", "az", "both"),
        default="both",
        help="Which IMU(s) to display (default: both).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.2,
        help="Refresh interval in seconds (default: 0.2).",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Open a live matplotlib window with rolling yaw/pitch/roll "
        "traces (one panel per IMU). Requires a GUI display, e.g. a "
        "desktop session or ssh -X.",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    with run_tag.session(transport, "imu_manual"):
        snapshot = MetadataSnapshotReader(transport)
        if args.which == "both":
            names = ["imu_el", "imu_az"]
        else:
            names = [f"imu_{args.which}"]
        plot = None
        if args.plot:
            _require_interactive_backend()
            plot = _LivePlot(names)
        try:
            _render_loop(snapshot, names, args.interval, plot=plot)
        except KeyboardInterrupt:
            print()


if __name__ == "__main__":
    main()
