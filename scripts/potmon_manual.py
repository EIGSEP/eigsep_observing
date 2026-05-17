"""Live potentiometer monitor readout for bring-up.

Displays raw voltages and (calibration-derived) angles for both axes
from the ``potmon`` metadata snapshot. Rotate the elevation / azimuth
pots by hand and verify the values move smoothly through their range.

Read-only — calibration is a separate concern. Run the picohost
``calibrate_pot.py`` script first if the angle columns show ``--``
(uncalibrated streams publish ``None`` for the cal/angle fields, which
this script renders as ``--`` per the SENSOR_SCHEMAS contract).
"""

from argparse import ArgumentParser
import logging
import sys
import time

from eigsep_redis import MetadataSnapshotReader

from eigsep_observing._scripts_util import build_transport
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


def _read_potmon(snapshot):
    try:
        snap = snapshot.get("potmon")
    except KeyError:
        return None
    return snap or None


def _fmt(value, fmt):
    if not isinstance(value, (int, float)):
        return "    --"
    return format(value, fmt)


def _render(snapshot):
    snap = _read_potmon(snapshot)
    sys.stdout.write("\x1b[2J\x1b[H")
    print(f"potmon_manual @ {time.strftime('%H:%M:%S')}")
    print()
    if snap is None:
        print("  (no potmon reading; is the pico flashed and the manager up?)")
        print()
        print("Ctrl-C to exit.")
        sys.stdout.flush()
        return
    el_v = _fmt(snap.get("pot_el_voltage"), "6.3f")
    el_a = _fmt(snap.get("pot_el_angle"), "7.2f")
    az_v = _fmt(snap.get("pot_az_voltage"), "6.3f")
    az_a = _fmt(snap.get("pot_az_angle"), "7.2f")
    print("  axis     voltage      angle")
    print("  ----    --------    --------")
    print(f"  el      {el_v} V   {el_a} deg")
    print(f"  az      {az_v} V   {az_a} deg")
    print()
    print(f"  status: {snap.get('status')!r}")
    cal_el = snap.get("pot_el_cal_slope")
    cal_az = snap.get("pot_az_cal_slope")
    if cal_el is None or cal_az is None:
        # Uncalibrated streams emit None for all cal/angle fields. Tell
        # the operator how to fix it instead of leaving them to wonder
        # why the angle column is blank.
        print()
        print(
            "  one or both axes uncalibrated — run picohost's "
            "calibrate_pot.py first."
        )
    print()
    print("Ctrl-C to exit.")
    sys.stdout.flush()


def _render_loop(snapshot, interval_s):
    while True:
        _render(snapshot)
        time.sleep(interval_s)


def _parse_args():
    parser = ArgumentParser(
        description="Live potentiometer voltage and angle readout."
    )
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
    return parser.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(args.dummy)
    snapshot = MetadataSnapshotReader(transport)
    try:
        _render_loop(snapshot, args.interval)
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
