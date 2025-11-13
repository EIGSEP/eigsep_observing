from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from eigsep_observing import EigsepRedis
from eigsep_observing.io import reshape_data

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
    default="spectrum.npz",
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
    help="Hostname or IP address of the Redis server.",
)
parser.add_argument(
    "--redis-port",
    type=int,
    default=6379,
    help="Port number of the Redis server.",
)
args = parser.parse_args()

redis = EigsepRedis(host=args.redis_host, port=args.redis_port)
logger.info(
    f"Connected to Redis server at {args.redis_host}:{args.redis_port}"
)

all_autos = [str(i) for i in range(6)]
all_cross = ["02", "04", "13", "15", "24", "35"]
pairs = args.pairs or all_autos + all_cross
all_data = {}
logger.info(f"Capturing {args.num_spec} spectra for pairs: {pairs}")
for i in range(args.num_spec):
    data = redis.read_corr_data(pairs=pairs, timeout=10)[-1]
    data = reshape_data(data, avg_even_odd=True)
    for k, v in data.items():
        if k not in all_data:
            all_data[k] = [v]
        all_data[k].append(v)

all_data = {k: np.array(v) for k, v in all_data.items()}
np.savez(args.out_filename, **all_data)
logger.info(f"Saved captured spectra to {args.out_filename}")
