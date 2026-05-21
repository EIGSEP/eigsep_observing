"""Live lidar readout for bring-up.

Prints ``distance_m`` from the lidar pico's metadata snapshot at ~5 Hz
with a horizontal bar so an operator can wave a hand in front of the
sensor and visually confirm the value tracks. Read-only; the lidar
firmware has no commands.

The bar saturates at ``--max-range`` so the visualization stays useful
for short-range bench testing without rebuilding firmware.
"""

from argparse import ArgumentParser
import logging
import sys
import time

from eigsep_redis import MetadataSnapshotReader

from eigsep_observing._scripts_util import build_transport
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)

BAR_WIDTH = 60


def _read_lidar(snapshot):
    snap = snapshot.get().get("lidar")
    if not snap:
        return None, None
    return snap.get("distance_m"), snap.get("status")


def _format_bar(distance, max_range):
    if distance is None:
        return "[" + " " * BAR_WIDTH + "]"
    frac = max(0.0, min(1.0, distance / max_range))
    filled = int(round(frac * BAR_WIDTH))
    return "[" + "#" * filled + " " * (BAR_WIDTH - filled) + "]"


def _format_age(ts, now):
    if ts is None:
        return " --"
    age = max(0.0, now - ts)
    if age < 60:
        return f"{age:5.1f}s"
    return f"{age / 60:5.1f}m"


def _render_loop(snapshot, interval_s, max_range):
    while True:
        distance, status = _read_lidar(snapshot)
        # Read the panda-stamped _ts so we can flag a stale sensor
        # (e.g. lidar pico crashed and no longer publishes).
        meta = snapshot.get()
        ts = meta.get("lidar_ts") if isinstance(meta, dict) else None
        age = _format_age(ts, time.time())
        bar = _format_bar(distance, max_range)
        if distance is None:
            value = "  --"
        else:
            value = f"{distance:5.2f} m"
        # ANSI clear + home so the operator sees one stable line that
        # updates in place, like ``pico_preflight --watch``.
        sys.stdout.write("\x1b[2J\x1b[H")
        print(f"lidar manual (max range {max_range:.1f} m)")
        print()
        print(f"  distance: {value}   status: {status!r}   age: {age}")
        print(f"  {bar}")
        print()
        print("Ctrl-C to exit.")
        sys.stdout.flush()
        time.sleep(interval_s)


def _parse_args():
    parser = ArgumentParser(description="Live lidar distance readout.")
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.2,
        help="Refresh interval in seconds (default: 0.2).",
    )
    parser.add_argument(
        "--max-range",
        type=float,
        default=5.0,
        help="Bar saturates at this distance in meters (default: 5.0).",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(args.dummy)
    snapshot = MetadataSnapshotReader(transport)
    try:
        _render_loop(snapshot, args.interval, args.max_range)
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
