"""Capture N spectra from the SNAP correlator into a single HDF5 file.

Used for testing / on-off measurements where we deliberately do *not*
want full timestream observing files. Output is the standard
``io.write_hdf5`` shape, so it's read back with::

    from eigsep_observing.io import read_hdf5
    data, header, metadata = read_hdf5("spectrum.h5")

- ``data``: ``dict[str, np.ndarray]`` keyed by pair name. Autos
  (``"0"``..``"5"``) are int32 shape ``(num_spec, nchan)``; cross
  pairs (``"02"``, ``"04"``, ...) are reconstructed to complex128
  shape ``(num_spec, nchan)``.
- ``header``: every ``CORR_HEADER_SCHEMA`` field
  (``nchan``, ``sample_rate``, ``integration_time``, ``acc_bins``,
  ``dtype``, ``avg_even_odd``, ``wiring``) plus ``sync_time``,
  ``header_upload_unix``, ``acc_cnt`` (one per spectrum), and
  ``times``/``freqs``/``dfreq`` computed by ``append_corr_header``.
- ``metadata``: ``{}`` when ``--panda-host`` is not set. Otherwise a
  per-key list of length ``num_spec`` with one
  ``MetadataSnapshotReader.get()`` value per spectrum (point-in-time,
  mirroring the VNA path). ``_ts`` freshness keys are filtered out.
"""

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging

import numpy as np

from eigsep_redis import MetadataSnapshotReader, Transport

from eigsep_observing.corr import CorrConfigStore, CorrReader
from eigsep_observing.io import append_corr_header, reshape_data, write_hdf5

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

parser = ArgumentParser(
    description="Capture a spectrum from the SNAP correlator.",
    formatter_class=ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "num_spec",
    type=int,
    default=1,
    help="Number of spectra to capture.",
)
parser.add_argument(
    "out_filename",
    type=str,
    help="Output filename to save the spectrum data.",
    default="spectrum.h5",
)
parser.add_argument(
    "--pairs",
    type=int,
    nargs="+",
    default=None,
    help="List of antenna pair indices to capture data from.",
)
parser.add_argument(
    "--redis-host",
    type=str,
    default="10.10.10.10",
    help="Hostname or IP address of the SNAP Redis server.",
)
parser.add_argument(
    "--redis-port",
    type=int,
    default=6379,
    help="Port number of the SNAP Redis server.",
)
parser.add_argument(
    "--panda-host",
    type=str,
    default=None,
    help=(
        "Hostname or IP of the LattePanda Redis. If set, a metadata "
        "snapshot is captured per spectrum; if unset, no metadata is "
        "saved."
    ),
)
parser.add_argument(
    "--panda-port",
    type=int,
    default=6379,
    help="Port number of the LattePanda Redis server.",
)
args = parser.parse_args()

transport_snap = Transport(host=args.redis_host, port=args.redis_port)
corr_reader = CorrReader(transport_snap)
corr_store = CorrConfigStore(transport_snap)
logger.info(f"Connected to SNAP Redis at {args.redis_host}:{args.redis_port}")

header = corr_store.get_header()
avg_even_odd = header["avg_even_odd"]

snapshot_reader = None
if args.panda_host is not None:
    transport_panda = Transport(host=args.panda_host, port=args.panda_port)
    snapshot_reader = MetadataSnapshotReader(transport_panda)
    logger.info(
        f"Connected to LattePanda Redis at {args.panda_host}:{args.panda_port}"
    )

all_autos = [str(i) for i in range(6)]
all_cross = ["02", "04", "13", "15", "24", "35"]
pairs = args.pairs or all_autos + all_cross

all_data = {}
acc_cnts = []
metadata_lists = {} if snapshot_reader is not None else None

logger.info(f"Capturing {args.num_spec} spectra for pairs: {pairs}")
for i in range(args.num_spec):
    acc_cnt, data = corr_reader.read(pairs=pairs, timeout=10)
    data = reshape_data(data, avg_even_odd=avg_even_odd)
    acc_cnts.append(acc_cnt)
    for k, v in data.items():
        all_data.setdefault(k, []).append(v)
    if snapshot_reader is not None:
        snap = snapshot_reader.get()
        for k, v in snap.items():
            if k.endswith("_ts"):
                continue
            metadata_lists.setdefault(k, []).append(v)

all_data = {k: np.array(v) for k, v in all_data.items()}
header = append_corr_header(
    header, np.array(acc_cnts), header["sync_time"]
)

write_hdf5(args.out_filename, all_data, header, metadata=metadata_lists)
logger.info(f"Saved captured spectra to {args.out_filename}")
