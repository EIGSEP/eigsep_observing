"""Pre-flight summary of every pico the manager knows about.

Reads three places in Redis on the pico-manager host:

- ``pico_config`` (``PicoConfigStore``) — the device list ``flash-picos``
  uploaded after the last flash pass. Tells us what was *flashed*.
- ``heartbeat:pico:{name}`` (``HeartbeatReader``) — TTL-backed
  liveness, asserted by the manager on every health check
  (``HEARTBEAT_TTL = 4 * HEALTH_CHECK_INTERVAL = 20 s`` today).
- ``metadata`` hash + ``{stream}_ts`` (``MetadataSnapshotReader``) —
  latest 200 ms sample plus the panda-side ``_ts`` so we can show
  freshness even when the heartbeat alone says "alive".

Prints one line per published metadata stream. That is usually one
row per logical device in ``picohost.manager.APP_NAMES`` (including
ones that aren't flashed, shown as ``--``); the one exception is
``tempctrl``, whose single Pico drives two Peltier channels and so
fans out into ``tempctrl_lna`` and ``tempctrl_load`` (see
``picohost.base.PicoPeltier._peltier_redis_handler``). Both channel
rows share the same port and heartbeat. Use this before starting
``eigsep-panda`` to confirm every expected pico is reporting.

When the manager is NOT running (e.g. picos are plugged into a bench
Pi without ``pico-manager.service``), heartbeats and metadata will
both be empty — go to the raw serial path instead:
``python -m picohost/scripts/monitor_picos /dev/ttyACMx``.
"""

import argparse
import sys
import time

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import add_redis_args
from eigsep_observing.io import SENSOR_SCHEMAS
from eigsep_redis import HeartbeatReader, MetadataSnapshotReader, Transport
from picohost.buses import PicoConfigStore
from picohost.keys import pico_heartbeat_name
from picohost.manager import APP_NAMES

# Per-channel Peltier fields are unprefixed in the split streams
# (``T_now`` / ``drive_level``, not ``LNA_T_now`` / ``LNA_drive_level``)
# because ``_peltier_redis_handler`` strips the ``LNA_`` / ``LOAD_``
# prefix when fanning the firmware tick into ``tempctrl_lna`` /
# ``tempctrl_load``. The stream label in the leftmost column already
# tells us which channel each row is.
_SUMMARY_FIELD_PRIORITY = (
    "az_pos",
    "el_pos",
    "T_now",
    "drive_level",
    "pot_el_angle",
    "pot_az_angle",
    "yaw",
    "pitch",
    "roll",
    "distance_m",
    "sw_state_name",
)
_SUMMARY_FIELD_EXCLUDE = {
    "sensor_name",
    "status",
    "app_id",
    "watchdog_tripped",
    "watchdog_timeout_ms",
}


def _summary_fields():
    fields = {}
    for name, schema in SENSOR_SCHEMAS.items():
        preferred = [k for k in _SUMMARY_FIELD_PRIORITY if k in schema]
        if preferred:
            fields[name] = tuple(preferred)
            continue
        fallback = [k for k in schema if k not in _SUMMARY_FIELD_EXCLUDE][:3]
        fields[name] = tuple(fallback)
    return fields


SUMMARY_FIELDS = _summary_fields()

# Map device name (from ``APP_NAMES``) to the ordered list of metadata
# stream names it publishes. Most devices publish exactly one stream
# named after the device; ``tempctrl`` is the exception. Keys absent
# from this map default to ``(device,)`` via ``_streams_for``.
DEVICE_STREAMS = {
    "tempctrl": ("tempctrl_lna", "tempctrl_load"),
}


def _streams_for(device):
    return DEVICE_STREAMS.get(device, (device,))


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
    # Surface non-healthy status (firmware convention: "update" = ok,
    # "error" = sensor read failure / fault) so a stuck-at-init reading
    # like tempctrl_load T_now=0.00 is recognizable as a fault rather
    # than a real value. Silent on healthy channels.
    status = reading.get("status")
    if status is not None and status != "update":
        parts.append(f"status={status}")
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

    print(f"{'stream':14} {'port':14} {'alive':5} {'age':>5}  reading")
    print("-" * 72)
    for device in APP_NAMES.values():
        port = flashed.get(device, "--")
        hb = HeartbeatReader(transport, name=pico_heartbeat_name(device))
        alive = "yes" if hb.check() else "no"
        for stream in _streams_for(device):
            ts = meta.get(f"{stream}_ts")
            age = _fmt_age(ts, now)
            summary = _fmt_summary(stream, meta.get(stream))
            print(f"{stream:14} {port:14} {alive:5} {age:>5}  {summary}")

    if devices is None:
        print()
        print(
            "note: pico_config is empty — flash-picos has not run "
            "against this Redis, or the manager cleared it."
        )


def _positive_float(value):
    """Validate that --watch is a positive number."""
    try:
        fvalue = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not a valid number")
    if fvalue <= 0:
        raise argparse.ArgumentTypeError(
            f"--watch must be positive, got {fvalue}"
        )
    return fvalue


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_redis_args(p, default_host="10.10.10.11")
    p.add_argument(
        "--watch",
        type=_positive_float,
        default=None,
        metavar="SECONDS",
        help="Refresh every N seconds (Ctrl-C to stop)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    transport = Transport(host=args.redis_host, port=args.redis_port)
    with run_tag.session(transport, "pico_preflight"):
        if args.watch is None:
            render(transport)
            return 0
        try:
            while True:
                # ANSI clear + home so the table redraws in place.
                sys.stdout.write("\x1b[2J\x1b[H")
                print(
                    f"pico_preflight @ {args.redis_host}  "
                    f"({time.strftime('%H:%M:%S')})"
                )
                print()
                render(transport)
                sys.stdout.flush()
                time.sleep(args.watch)
        except KeyboardInterrupt:
            return 0


if __name__ == "__main__":
    sys.exit(main())
