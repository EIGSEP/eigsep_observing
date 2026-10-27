"""Record the simple_therm pico's thermistor temperatures to an .npz file.

Passive readout (see ``scripts/CLAUDE.md``): drains the pico-manager's
``stream:simple_therm`` metadata stream from Redis — no commands, no
``run_tag`` claim — so it coexists with anything else running on the
host. The simple_therm firmware (pico-firmware ``APP_SIMPLE_THERM``,
DIP code 7) reports three thermistor channels (GPIO 26-28) plus the
pico's on-die temperature at ~5 Hz.

Every ``--save-interval`` seconds the whole run so far is rewritten to
one ``simple_therm_<timestamp>.npz`` (atomically, via a temp file and
rename), so a crash or power loss loses at most one interval. Ctrl-C
saves once more and exits. Arrays in the file:

- ``t_unix``      (N,)   float unix seconds, from the Redis entry ID
- ``temp_c``      (N, 3) thermistor temperatures, deg C (NaN = invalid)
- ``voltage``     (N, 3) divider voltages, V
- ``resistance``  (N, 3) thermistor resistances, ohms (NaN = invalid)
- ``die_temp_c``  (N,)   RP2350 on-die temperature, deg C
- ``channel_gpio`` (3,)  GPIO pin of each thermistor column

Load with ``np.load(path)``.
"""

import argparse
import datetime as dt
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

import numpy as np
import redis.exceptions

from eigsep_redis import MetadataStreamReader, entry_id_to_unix

from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport_bare,
)
from eigsep_observing.utils import configure_eig_logger

logger = logging.getLogger(__name__)

SENSOR_NAME = "simple_therm"
STREAM = f"stream:{SENSOR_NAME}"
NUM_CHANNELS = 3
CHANNEL_GPIO = np.array([26, 27, 28])


def _float(value):
    """Firmware nulls (invalid readings) become NaN."""
    return np.nan if value is None else float(value)


class ThermLog:
    """In-memory accumulator for simple_therm status packets."""

    def __init__(self):
        self.t_unix = []
        self.temp_c = []
        self.voltage = []
        self.resistance = []
        self.die_temp_c = []

    def __len__(self):
        return len(self.t_unix)

    def append(self, t_unix, payload):
        self.t_unix.append(t_unix)
        self.temp_c.append(
            [_float(payload.get(f"therm{i}_T")) for i in range(NUM_CHANNELS)]
        )
        self.voltage.append(
            [
                _float(payload.get(f"therm{i}_voltage"))
                for i in range(NUM_CHANNELS)
            ]
        )
        self.resistance.append(
            [
                _float(payload.get(f"therm{i}_resistance"))
                for i in range(NUM_CHANNELS)
            ]
        )
        self.die_temp_c.append(_float(payload.get("die_T")))

    def arrays(self):
        shape = (len(self), NUM_CHANNELS)
        return {
            "t_unix": np.asarray(self.t_unix, dtype=float),
            "temp_c": np.asarray(self.temp_c, dtype=float).reshape(shape),
            "voltage": np.asarray(self.voltage, dtype=float).reshape(shape),
            "resistance": np.asarray(self.resistance, dtype=float).reshape(
                shape
            ),
            "die_temp_c": np.asarray(self.die_temp_c, dtype=float),
            "channel_gpio": CHANNEL_GPIO,
        }

    def save(self, path):
        """Atomically (re)write the whole log to ``path``."""
        path = Path(path)
        tmp = path.with_name(path.name + ".tmp")
        with open(tmp, "wb") as f:
            np.savez(f, **self.arrays())
        os.replace(tmp, path)


def drain_into(reader, log):
    """Append every new simple_therm entry to ``log``; return the count."""
    drained = reader.drain(STREAM, with_ids=True)
    n = 0
    for entry_id, payload in drained.get(STREAM, []):
        if not isinstance(payload, dict):
            logger.error(
                "Skipping non-dict payload on %s: %r", STREAM, payload
            )
            continue
        log.append(entry_id_to_unix(entry_id), payload)
        n += 1
    return n


def run(transport, out_path, poll_interval, save_interval, stop_event):
    """Drain until ``stop_event`` is set, saving every ``save_interval``."""
    reader = MetadataStreamReader(transport)
    # Start from "now": don't replay whatever backlog is in the stream.
    reader.skip_to_latest(STREAM)
    log = ThermLog()
    last_save = time.monotonic()
    last_count = 0
    try:
        while not stop_event.is_set():
            try:
                drain_into(reader, log)
            except redis.exceptions.ConnectionError as exc:
                logger.warning("drain failed: %s", exc)
            stop_event.wait(poll_interval)
            now = time.monotonic()
            if out_path is not None and now - last_save >= save_interval:
                last_save = now
                _checkpoint(log, out_path, last_count)
                last_count = len(log)
    finally:
        try:
            drain_into(reader, log)
        except redis.exceptions.ConnectionError:
            pass
        if out_path is not None:
            _checkpoint(log, out_path, last_count)
    return log


def _checkpoint(log, out_path, last_count):
    if len(log) == last_count:
        logger.warning(
            "No new %s samples since last save. Is the pico (DIP code 7) "
            "plugged in and pico-manager running?",
            SENSOR_NAME,
        )
    if len(log) == 0:
        return
    log.save(out_path)
    latest = ", ".join(f"{t:.2f}" for t in log.temp_c[-1])
    logger.info(
        "Saved %d samples to %s (latest T = [%s] C, die %.1f C)",
        len(log),
        out_path,
        latest,
        log.die_temp_c[-1],
    )


def _start_dummy_pico(transport):
    """Dummy simple_therm pico publishing to ``transport`` (needs picohost
    with simple_therm support)."""
    from eigsep_redis import MetadataWriter
    from picohost.testing import DummyPicoSimpleTherm

    return DummyPicoSimpleTherm(
        "/dev/dummy",
        metadata_writer=MetadataWriter(transport),
        name=SENSOR_NAME,
    )


def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=(
            "Record simple_therm pico temperatures from Redis to an .npz "
            "file, rewritten periodically."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_redis_args(p)
    p.add_argument(
        "--save-dir",
        type=Path,
        default=Path("."),
        help="Directory for the output .npz file.",
    )
    p.add_argument(
        "--save-interval",
        type=float,
        default=60.0,
        help="Seconds between periodic saves of the .npz file.",
    )
    p.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between Redis drains.",
    )
    p.add_argument(
        "--dummy",
        action="store_true",
        help="Use fakeredis on localhost:6380 with an emulated pico.",
    )
    return p.parse_args(argv)


def main(argv=None):
    configure_eig_logger(level=logging.INFO)
    args = _parse_args(argv)

    args.save_dir.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = args.save_dir / f"{SENSOR_NAME}_{timestamp}.npz"

    try:
        transport = build_transport_bare(
            args.dummy, host=args.redis_host, real_port=args.redis_port
        )
        transport.r.ping()
    except redis.exceptions.ConnectionError as exc:
        logger.error(
            "Cannot connect to Redis at %s:%s: %s",
            args.redis_host,
            args.redis_port,
            exc,
        )
        return 1

    dummy_pico = _start_dummy_pico(transport) if args.dummy else None

    stop_event = threading.Event()

    def _handle(signum, _frame):
        logger.info("Signal %s received, stopping.", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    logger.info(
        "Recording %s to %s (saving every %.0fs). Ctrl-C to stop.",
        SENSOR_NAME,
        out_path,
        args.save_interval,
    )
    try:
        log = run(
            transport,
            out_path,
            args.poll_interval,
            args.save_interval,
            stop_event,
        )
    finally:
        if dummy_pico is not None:
            dummy_pico.disconnect()
            transport.reset()
    logger.info("Done: %d samples in %s", len(log), out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
