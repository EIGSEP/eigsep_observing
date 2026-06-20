"""Standalone metadata recorder.

Drains all registered panda metadata streams and writes the raw entries
(no averaging) to an HDF5 file, using the *same* on-disk format as a corr
file's metadata sidecar (:func:`eigsep_observing.io.write_metadata_hdf5`):
one JSON list-of-dicts per stream under a ``metadata`` group. Designed for
hardware tests where the SNAP correlator is not in the loop, so the
corr-side metadata-to-disk pipeline (``EigObserver.record_corr_data``) is
unavailable.

Each drained Redis entry becomes one dict in its stream's list — the raw
payload plus a folded-in ``_ts_unix`` (float unix seconds, from the Redis
stream entry ID's millisecond prefix, the most accurate per-sample
timestamp available since the picos do not embed wall-clock timestamps in
their payloads). Because the format matches io.py's JSON serialization, a
``None`` field (a nulled sensor reading) round-trips as ``None`` rather
than a zero/empty sentinel. Read it back with
:func:`eigsep_observing.io.read_metadata_hdf5`.

Samples accumulate in memory and the file is written once on exit; the
SIGINT/SIGTERM handlers stop the loop so a normal Ctrl-C still saves. A
hard crash (SIGKILL / power loss) loses the run — acceptable for the
attended bring-up runs this tool is for, and it keeps the format identical
to the corr sidecar so the two can be changed together later.

Coexists safely with ``eigsep-observe``: each consumer has its own
``Transport`` with an independent local read pointer (same pattern
``live_status/aggregator.py`` already uses).
"""

import argparse
import datetime as dt
import logging
import signal
import sys
import threading
from pathlib import Path

import redis.exceptions

from eigsep_redis import MetadataStreamReader, entry_id_to_unix

from eigsep_observing._scripts_util import add_redis_args, build_transport
from eigsep_observing.io import write_metadata_hdf5
from eigsep_observing.utils import configure_eig_logger

logger = logging.getLogger(__name__)


def _group_name(stream):
    """``stream:imu_el`` → ``imu_el``."""
    return (
        stream.split("stream:", 1)[1]
        if stream.startswith("stream:")
        else stream
    )


def _drain_into(reader, collected):
    """Drain all registered streams once, accumulating into ``collected``.

    Each Redis entry becomes one dict appended to ``collected[stream]``:
    the raw payload plus a folded-in ``_ts_unix`` (from the entry ID).
    The ``stream:`` prefix is stripped. Non-dict payloads are logged at
    ERROR and skipped — the producer is at fault, not the recorder.
    """
    drained = reader.drain(with_ids=True)
    for stream, entries in drained.items():
        bucket = collected.setdefault(_group_name(stream), [])
        for entry_id, payload in entries:
            if not isinstance(payload, dict):
                logger.error(
                    "Skipping non-dict payload on %s: %r", stream, payload
                )
                continue
            bucket.append({"_ts_unix": entry_id_to_unix(entry_id), **payload})


def _collect(transport, collected, interval, stop_event):
    """Drain into ``collected`` until ``stop_event`` is set.

    ``drain`` is non-blocking, so the loop is paced by ``stop_event.wait``
    (which also keeps SIGINT responsive). Picos publish at ~5 Hz; a 1 s
    default batches ~5 entries per drain.
    """
    reader = MetadataStreamReader(transport)
    while not stop_event.is_set():
        try:
            _drain_into(reader, collected)
        except redis.exceptions.ConnectionError as exc:
            logger.warning("drain failed: %s", exc)
        stop_event.wait(interval)


def _build_transport(host, port, dummy):
    # build_transport attaches a DummyPandaClient in dummy mode (fakeredis
    # on 6380) so the dummy picos publish to the same transport; real mode
    # is a plain Transport at host:port.
    return build_transport(dummy, host=host, real_port=port)


def _parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Record panda metadata streams to HDF5. No averaging — "
            "every Redis stream entry becomes one row, in the same "
            "format as a corr file's metadata sidecar."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_redis_args(p, default_host="10.10.10.11")
    p.add_argument(
        "--save-dir",
        type=Path,
        default=Path("."),
        help="Directory for the output HDF5 file.",
    )
    p.add_argument(
        "--no-save",
        action="store_true",
        help=(
            "Drain streams without writing a file — useful for monitoring "
            "stream activity in the lab without accumulating output."
        ),
    )
    p.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help=(
            "Drain pacing, in seconds — the maximum stall between SIGINT "
            "and exit. Samples accumulate between drains and are written "
            "once on exit."
        ),
    )
    p.add_argument(
        "--dummy",
        action="store_true",
        help="Connect to fakeredis on localhost:6380 with dummy picos.",
    )
    return p.parse_args()


def main():
    configure_eig_logger(level=logging.INFO)
    args = _parse_args()

    if not args.no_save:
        save_dir = args.save_dir
        save_dir.mkdir(parents=True, exist_ok=True)

    try:
        transport = _build_transport(
            args.redis_host, args.redis_port, args.dummy
        )
    except redis.exceptions.ConnectionError as exc:
        logger.error(
            "Cannot connect to panda Redis at %s: %s. "
            "Is the panda up and reachable?",
            args.redis_host,
            exc,
        )
        return 1

    if args.no_save:
        out_path = None
        logger.info(
            "Monitoring metadata streams (no file output, interval %.1fs)",
            args.interval,
        )
    else:
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = args.save_dir / f"metadata_{timestamp}.h5"
        logger.info(
            "Recording metadata to %s (interval %.1fs)",
            out_path,
            args.interval,
        )

    stop_event = threading.Event()

    def _handle(signum, _frame):
        logger.info("Signal %s received, stopping.", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    collected = {}
    try:
        _collect(transport, collected, args.interval, stop_event)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, stopping.")
    finally:
        if out_path is not None:
            # Write whatever we captured, even on an uncaught exit, so a
            # run is never lost to a missed signal.
            write_metadata_hdf5(out_path, collected)
        if args.dummy:
            dummy_client = getattr(transport, "_dummy_client", None)
            if dummy_client is not None:
                dummy_client.stop()
            transport.reset()

    if out_path is not None:
        logger.info(
            "Closed %s with %d stream(s).",
            out_path,
            len(collected),
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
