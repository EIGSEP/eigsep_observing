"""Pre-flight summary of every pico the manager knows about.

Reads three places in Redis on the pico-manager host:

- ``pico_config`` (``PicoConfigStore``) — the device list ``flash-picos``
  uploaded after the last flash pass. Tells us what was *flashed*.
- ``heartbeat:pico:{name}`` (``HeartbeatReader``) — TTL-backed
  liveness, asserted by the manager on every health check
  (``HEARTBEAT_TTL = 4 * HEALTH_CHECK_INTERVAL = 20 s`` today).
- ``metadata`` hash + ``{name}_ts`` (``MetadataSnapshotReader``) —
  latest 200 ms sample plus the panda-side ``_ts`` so we can show
  freshness even when the heartbeat alone says "alive".

Prints one line per logical device in ``picohost.manager.APP_NAMES``,
including ones that aren't flashed (shown as ``--``). Use this before
starting ``eigsep-panda`` to confirm every expected pico is reporting.

When the manager is NOT running (e.g. picos are plugged into a bench
Pi without ``pico-manager.service``), heartbeats and metadata will
both be empty — go to the raw serial path instead:
``python -m picohost.scripts.monitor_picos /dev/ttyACMx``.
"""

import argparse
import sys
import time

from eigsep_redis import HeartbeatReader, MetadataSnapshotReader, Transport
from picohost.buses import PicoConfigStore
from picohost.keys import pico_heartbeat_name
from picohost.manager import APP_NAMES

# Compact one-line summary per device type. Keep keys short — this
# script is a glance, not a dump. Use scripts/monitor_meta.py for the
# full reading.
SUMMARY_FIELDS = {
    "motor": ("az_pos", "el_pos"),
    "tempctrl": ("LNA_T_now", "LOAD_T_now", "LNA_drive_level"),
    "potmon": ("pot_el_angle", "pot_az_angle"),
    "imu_el": ("yaw", "pitch", "roll"),
    "imu_az": ("yaw", "pitch", "roll"),
    "lidar": ("distance_m",),
    "rfswitch": ("sw_state_name",),
}


def _fmt_age(ts, now):
    if not isinstance(ts, (int, float)):
        return "  --"
    age = now - ts
    if age < 0:
        return "  0s"
    if age < 60:
        return f"{age:4.1f}s"
    if age < 3600:
        return f"{age / 60:4.1f}m"
    return f"{age / 3600:4.1f}h"


def _fmt_summary(name, reading):
    if not isinstance(reading, dict):
        return ""
    fields = SUMMARY_FIELDS.get(name, ())
    parts = []
    for k in fields:
        if k not in reading:
            continue
        v = reading[k]
        if isinstance(v, float):
            parts.append(f"{k}={v:.2f}")
        else:
            parts.append(f"{k}={v}")
    return " ".join(parts)


def _flashed_lookup(devices):
    """Return ``{name: port}`` for flashed devices, ``{}`` if no config."""
    if not devices:
        return {}
    out = {}
    for d in devices:
        app_id = d.get("app_id")
        port = d.get("port", "?")
        name = APP_NAMES.get(app_id)
        if name is None:
            continue
        out[name] = port
    return out


def render(transport):
    config_store = PicoConfigStore(transport)
    snapshot = MetadataSnapshotReader(transport)
    # The script renders its own staleness column; the reader's
    # WARNING log on stale keys would just be duplicate noise here.
    snapshot.max_age_s = float("inf")

    devices = config_store.get()
    flashed = _flashed_lookup(devices)
    meta = snapshot.get()
    now = time.time()

    print(f"{'device':10} {'port':14} {'alive':5} {'age':>5}  reading")
    print("-" * 72)
    for name in APP_NAMES.values():
        port = flashed.get(name, "--")
        hb = HeartbeatReader(transport, name=pico_heartbeat_name(name))
        alive = "yes" if hb.check() else "no"
        ts = meta.get(f"{name}_ts")
        age = _fmt_age(ts, now)
        summary = _fmt_summary(name, meta.get(name))
        print(f"{name:10} {port:14} {alive:5} {age:>5}  {summary}")

    if devices is None:
        print()
        print(
            "note: pico_config is empty — flash-picos has not run "
            "against this Redis, or the manager cleared it."
        )


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--host",
        default="10.10.10.11",
        help="Redis host (default: panda IP 10.10.10.11)",
    )
    p.add_argument(
        "--watch",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Refresh every N seconds (Ctrl-C to stop)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    transport = Transport(args.host)
    if args.watch is None:
        render(transport)
        return 0
    try:
        while True:
            # ANSI clear + home so the table redraws in place.
            sys.stdout.write("\x1b[2J\x1b[H")
            print(
                f"pico_preflight @ {args.host}  ({time.strftime('%H:%M:%S')})"
            )
            print()
            render(transport)
            sys.stdout.flush()
            time.sleep(args.watch)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
