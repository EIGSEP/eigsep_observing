"""Standalone metadata recorder.

Drains all registered panda metadata streams and writes raw entries
(no averaging) to an HDF5 file. Designed for hardware tests where the
SNAP correlator is not in the loop, so the corr-side metadata-to-disk
pipeline (``EigObserver.record_corr_data``) is unavailable.

Each Redis stream becomes one HDF5 group named after the metadata key
(e.g. ``imu_el``, ``tempctrl_lna``). Inside the group:

- one 1-D resizable dataset per payload field, typed via
  :data:`eigsep_observing.io.SENSOR_SCHEMAS` where the key is known
  (with lazy per-sample inference for fields outside the schema), and
- a ``_ts_unix`` dataset (float64) populated from the Redis stream
  entry ID's millisecond prefix — the most accurate per-sample
  timestamp available, since the picos do not embed wall-clock
  timestamps in their payloads.

Coexists safely with ``eigsep-observe``: each consumer has its own
``Transport`` with an independent local read pointer (same pattern
``live_status/aggregator.py`` already uses).
"""

import argparse
import datetime as dt
import json
import logging
import signal
import sys
import threading
from pathlib import Path

import h5py
import numpy as np
import redis.exceptions

from eigsep_redis import Transport
from eigsep_redis.keys import METADATA_STREAMS_SET

from eigsep_observing.io import SENSOR_SCHEMAS
from eigsep_observing.utils import configure_eig_logger

logger = logging.getLogger(__name__)


_SCHEMA_DTYPES = {
    str: h5py.string_dtype(encoding="utf-8"),
    bool: np.dtype(np.uint8),
    int: np.dtype(np.int64),
    float: np.dtype(np.float64),
}


def _dtype_for_value(value):
    """Lazy dtype pick for a single sample value (used for fields not
    declared in :data:`SENSOR_SCHEMAS`). Booleans must be checked
    before ints — Python ``bool`` is a subclass of ``int``."""
    if isinstance(value, bool):
        return _SCHEMA_DTYPES[bool]
    if isinstance(value, int):
        return _SCHEMA_DTYPES[int]
    if isinstance(value, float):
        return _SCHEMA_DTYPES[float]
    if isinstance(value, str):
        return _SCHEMA_DTYPES[str]
    return _SCHEMA_DTYPES[str]


class _StreamWriter:
    """Per-stream HDF5 group with per-field resizable datasets.

    Pre-creates datasets for every field in ``schema`` so a stream
    with declared fields produces a uniform file even if some samples
    omit a field. Unknown fields encountered later are added lazily
    with a per-sample dtype inference and back-filled with default
    sentinels for prior rows.
    """

    _CHUNK = 256

    def __init__(self, h5file, group_name, schema=None):
        self.group = h5file.create_group(group_name)
        self.count = 0
        self._dsets = {}
        self._make_dset("_ts_unix", np.dtype(np.float64))
        if schema:
            for field, py_type in schema.items():
                dtype = _SCHEMA_DTYPES.get(py_type, _SCHEMA_DTYPES[str])
                self._make_dset(field, dtype)

    def _make_dset(self, name, dtype):
        dset = self.group.create_dataset(
            name,
            shape=(self.count,),
            maxshape=(None,),
            dtype=dtype,
            chunks=(self._CHUNK,),
        )
        self._dsets[name] = dset

    def _ensure_dset(self, name, value):
        if name in self._dsets:
            return self._dsets[name]
        self._make_dset(name, _dtype_for_value(value))
        return self._dsets[name]

    def append(self, ts_unix, payload):
        new_count = self.count + 1
        for dset in self._dsets.values():
            dset.resize((new_count,))
        self._dsets["_ts_unix"][self.count] = ts_unix
        for field, value in payload.items():
            dset = self._ensure_dset(field, value)
            # Re-resize in case _ensure_dset just created a dataset
            # that was sized at the pre-append count.
            if dset.shape[0] < new_count:
                dset.resize((new_count,))
            if value is None:
                continue  # leave default sentinel (0 / "")
            try:
                if h5py.check_string_dtype(dset.dtype):
                    dset[self.count] = (
                        value if isinstance(value, str) else json.dumps(value)
                    )
                elif dset.dtype == np.uint8:
                    dset[self.count] = np.uint8(bool(value))
                else:
                    dset[self.count] = value
            except (TypeError, ValueError) as exc:
                logger.warning(
                    "Cannot write %s=%r (dtype=%s) to %s: %s",
                    field,
                    value,
                    dset.dtype,
                    self.group.name,
                    exc,
                )
        self.count = new_count


def _entry_id_to_unix(eid_bytes):
    """Redis stream entry IDs are ``{millis}-{seq}``. Return seconds float."""
    millis = int(eid_bytes.decode().split("-", 1)[0])
    return millis / 1000.0


def _group_name(stream):
    """``stream:imu_el`` → ``imu_el``."""
    return (
        stream.split("stream:", 1)[1]
        if stream.startswith("stream:")
        else stream
    )


def _schema_for(stream):
    """Return the SENSOR_SCHEMAS entry matching this stream, or None."""
    return SENSOR_SCHEMAS.get(_group_name(stream))


def _build_transport(panda_ip, dummy):
    if dummy:
        # Match the convention used by other manual scripts: fakeredis
        # on port 6380 with a DummyPandaClient attached so the dummy
        # picos publish to the same transport.
        from eigsep_observing._scripts_util import build_transport

        return build_transport(dummy=True)
    return Transport(host=panda_ip, port=6379)


def _parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Record panda metadata streams to HDF5. No averaging — "
            "every Redis stream entry becomes one HDF5 row."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--panda-ip", default="10.10.10.11")
    p.add_argument(
        "--save-dir",
        type=Path,
        default=Path("."),
        help="Directory for the output HDF5 file.",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help=(
            "Drain block timeout, in seconds. The recorder will return "
            "from xread as soon as any entry arrives, so this is the "
            "maximum stall between SIGINT and exit."
        ),
    )
    p.add_argument(
        "--dummy",
        action="store_true",
        help="Connect to fakeredis on localhost:6380 with dummy picos.",
    )
    return p.parse_args()


def _run(transport, out_path, interval, stop_event):
    writers = {}  # stream_name -> _StreamWriter
    last_ids = {}  # stream_name -> last-read-id bytes

    with h5py.File(out_path, "w") as h5file:
        while not stop_event.is_set():
            try:
                members = transport.r.smembers(METADATA_STREAMS_SET)
            except redis.exceptions.ConnectionError as exc:
                logger.warning("Stream discovery failed: %s", exc)
                stop_event.wait(interval)
                continue

            for s in members:
                key = s.decode()
                if key in last_ids:
                    continue
                # New stream: start from current tail so we don't
                # replay backlog at startup.
                try:
                    info = transport.r.xinfo_stream(key)
                    last_ids[key] = info["last-generated-id"]
                except redis.exceptions.ResponseError:
                    last_ids[key] = b"0-0"

            if not last_ids:
                logger.debug("No metadata streams registered yet.")
                stop_event.wait(interval)
                continue

            try:
                resp = (
                    transport.r.xread(last_ids, block=int(interval * 1000))
                    or []
                )
            except redis.exceptions.ConnectionError as exc:
                logger.warning("xread failed: %s", exc)
                stop_event.wait(interval)
                continue

            for stream_bytes, entries in resp:
                stream = stream_bytes.decode()
                writer = writers.get(stream)
                if writer is None:
                    writer = _StreamWriter(
                        h5file,
                        _group_name(stream),
                        schema=_schema_for(stream),
                    )
                    writers[stream] = writer

                for entry_id, fields in entries:
                    last_ids[stream] = entry_id
                    raw = fields.get(b"value")
                    if raw is None:
                        continue
                    try:
                        payload = json.loads(raw)
                    except (ValueError, TypeError) as exc:
                        logger.error(
                            "Failed to decode entry on %s: %s", stream, exc
                        )
                        continue
                    if not isinstance(payload, dict):
                        logger.error(
                            "Skipping non-dict payload on %s: %r",
                            stream,
                            payload,
                        )
                        continue
                    writer.append(_entry_id_to_unix(entry_id), payload)

            h5file.flush()

    return writers


def main():
    configure_eig_logger(level=logging.INFO)
    args = _parse_args()

    save_dir = args.save_dir
    save_dir.mkdir(parents=True, exist_ok=True)

    try:
        transport = _build_transport(args.panda_ip, args.dummy)
    except redis.exceptions.ConnectionError as exc:
        logger.error(
            "Cannot connect to panda Redis at %s: %s. "
            "Is the panda up and reachable?",
            args.panda_ip,
            exc,
        )
        return 1

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = save_dir / f"metadata_{timestamp}.h5"
    logger.info(
        "Recording metadata to %s (interval %.1fs)", out_path, args.interval
    )

    stop_event = threading.Event()

    def _handle(signum, _frame):
        logger.info("Signal %s received, stopping.", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    try:
        writers = _run(transport, out_path, args.interval, stop_event)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, stopping.")
        writers = {}
    finally:
        if args.dummy:
            dummy_client = getattr(transport, "_dummy_client", None)
            if dummy_client is not None:
                dummy_client.stop()
            transport.reset()

    logger.info(
        "Closed %s with %d stream(s).",
        out_path,
        len(writers),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
