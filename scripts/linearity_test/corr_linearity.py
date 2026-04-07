"""Correlator linearity test. Grabs auto-correlation spectra from Redis
at each attenuation step (set manually) and saves total power per input.

Usage:
    python corr_linearity.py --redis-host 10.10.10.10 --outfile linearity_corr
"""

import argparse
import numpy as np
from eigsep_observing import EigsepRedis
from eigsep_observing.io import reshape_data

parser = argparse.ArgumentParser(
    description="Correlator linearity test",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--redis-host", default="10.10.10.10", help="Redis host"
)
parser.add_argument(
    "--redis-port", type=int, default=6379, help="Redis port"
)
parser.add_argument(
    "--nsamples",
    type=int,
    default=10,
    help="Number of integrations to average per step",
)
parser.add_argument(
    "--outfile",
    type=str,
    default="linearity_corr",
    help="Output npz filename (without extension)",
)
args = parser.parse_args()

redis = EigsepRedis(host=args.redis_host, port=args.redis_port)
print(f"Connected to Redis at {args.redis_host}:{args.redis_port}")

pairs = ["0", "1"]
attens = []
power = {p: [] for p in pairs}

print("At each attenuation step, press Enter to capture.")
print("Type 'done' to finish and save.\n")

while True:
    resp = input("Attenuation (dB): ").strip()
    if resp.lower() == "done":
        break
    try:
        atten = float(resp)
    except ValueError:
        print("Enter a number or 'done'.")
        continue

    # Skip to latest entry in the stream
    last_id = redis.r.xrevrange("stream:corr", count=1)
    if last_id:
        redis._set_last_read_id("stream:corr", last_id[0][0])

    print(f"  Capturing {args.nsamples} integrations...")
    pwr = {p: [] for p in pairs}
    last_cnt = None
    collected = 0
    while collected < args.nsamples:
        acc_cnt, _, data = redis.read_corr_data(
            pairs=pairs, timeout=10
        )
        if acc_cnt == last_cnt:
            continue
        last_cnt = acc_cnt
        data = {k: v for k, v in data.items() if k in pairs}
        data = reshape_data(data, avg_even_odd=True)
        for p in pairs:
            pwr[p].append(np.sum(data[p]))
        collected += 1

    attens.append(atten)
    for p in pairs:
        mean_pwr = np.mean(pwr[p])
        power[p].append(mean_pwr)
        print(f"  Input {p}: total power = {mean_pwr:.2e}")

print(f"\nSaving {len(attens)} points to {args.outfile}.npz")
np.savez(
    args.outfile,
    attenuation_dB=np.array(attens),
    **{f"power_{p}": np.array(power[p]) for p in pairs},
)
print("Done.")
