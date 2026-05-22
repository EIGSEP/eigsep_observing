"""Live IMU readout for bring-up (imu_el and/or imu_az).

The two IMU picos share firmware (BNO085 in UART RVC mode under
picohost 1.0.0) and publish only ``yaw``/``pitch``/``roll`` (deg) and
``accel_x``/``accel_y``/``accel_z`` (m/s²). Tilt the rig by hand and
watch the numbers move; pitch and roll get an ASCII level bar centered
at zero so a visual sweep is immediately obvious.

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


def _read_imu(snapshot, name):
    snap = snapshot.get().get(name)
    return snap or None


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


def _render(snapshot, names):
    readings = {name: _read_imu(snapshot, name) for name in names}
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


def _render_loop(snapshot, names, interval_s):
    while True:
        _render(snapshot, names)
        time.sleep(interval_s)


def _parse_args():
    parser = ArgumentParser(description="Live IMU readout for imu_el/imu_az.")
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient",
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
    return parser.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(args.dummy)
    with run_tag.session(transport, "imu_manual"):
        snapshot = MetadataSnapshotReader(transport)
        if args.which == "both":
            names = ["imu_el", "imu_az"]
        else:
            names = [f"imu_{args.which}"]
        try:
            _render_loop(snapshot, names, args.interval)
        except KeyboardInterrupt:
            print()


if __name__ == "__main__":
    main()
