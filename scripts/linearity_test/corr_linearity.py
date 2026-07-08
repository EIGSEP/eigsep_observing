"""Correlator linearity test. Grabs auto-correlation spectra from Redis
at each attenuation step (set manually on the noise-source step
attenuator) and saves the full per-channel spectra per input, plus the
corr header as operating-point provenance for ``fit_linearity.py`` —
the fitted linear-range product is only valid at the operating point
(adc_gain, fft_shift, corr_scalar, ...) it was measured at.

Usage:
    python corr_linearity.py --redis-host 10.10.10.10 --outfile linearity_corr
"""

import argparse
import json

import numpy as np

from eigsep_redis import Transport

from eigsep_observing.corr import CorrConfigStore, CorrReader
from eigsep_observing.io import reshape_data
from eigsep_observing.utils import calc_freqs_dfreq

parser = argparse.ArgumentParser(
    description="Correlator linearity test",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("--redis-host", default="10.10.10.10", help="Redis host")
parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
parser.add_argument(
    "--nsamples",
    type=int,
    default=10,
    help="Number of integrations to record per step",
)
parser.add_argument(
    "--outfile",
    type=str,
    default="linearity_corr",
    help="Output npz filename (without extension)",
)
parser.add_argument(
    "--noise-density-dbm-hz",
    type=float,
    default=-75.0,
    help="Noise-source power spectral density at 0 dB attenuation",
)
parser.add_argument(
    "--bandwidth-hz",
    type=float,
    default=225e6,
    help="Lowpass filter cutoff limiting the injected noise band",
)
args = parser.parse_args()

transport = Transport(host=args.redis_host, port=args.redis_port)
corr_reader = CorrReader(transport)
print(f"Connected to Redis at {args.redis_host}:{args.redis_port}")

# The full producer header is the operating-point provenance the
# fitted product gets validated against downstream; without it the
# sweep cannot become a product, so require it up front.
header = CorrConfigStore(transport).get_header()  # raises if unpublished

# Data layout follows the firmware version the producer stamped on
# the header (acc_bins 1 for v2.4 single-spectrum, 2 for legacy
# even/odd). Read it once; the firmware does not change mid-run.
acc_bins = header.get("acc_bins", 2)
avg_even_odd = header.get("avg_even_odd", True)
freqs, _ = calc_freqs_dfreq(
    float(header["sample_rate"]) * 1e6, int(header["nchan"])
)

pairs = ["0", "1"]
attens = []
spectra = {p: [] for p in pairs}
acc_cnts = []

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
    last_id = transport.r.xrevrange("stream:corr", count=1)
    if last_id:
        corr_reader.seek(last_id[0][0])

    print(f"  Capturing {args.nsamples} integrations...")
    step_spectra = {p: [] for p in pairs}
    step_cnts = []
    last_cnt = None
    collected = 0
    while collected < args.nsamples:
        acc_cnt, data = corr_reader.read(pairs=pairs, timeout=10)
        if acc_cnt == last_cnt:
            continue
        last_cnt = acc_cnt
        data = {k: v for k, v in data.items() if k in pairs}
        data = reshape_data(data, acc_bins=acc_bins, avg_even_odd=avg_even_odd)
        for p in pairs:
            # reshape_data returns (ntimes=1, nchan) per auto
            step_spectra[p].append(data[p][0])
        step_cnts.append(acc_cnt)
        collected += 1

    attens.append(atten)
    acc_cnts.append(step_cnts)
    for p in pairs:
        step_arr = np.array(step_spectra[p])
        spectra[p].append(step_arr)
        total = step_arr.mean(axis=0).sum()
        print(f"  Input {p}: total power = {total:.2e}")

print(f"\nSaving {len(attens)} points to {args.outfile}.npz")
np.savez(
    args.outfile,
    attenuation_dB=np.array(attens),
    freqs=freqs,
    header_json=json.dumps(header),
    noise_density_dbm_hz=args.noise_density_dbm_hz,
    bandwidth_hz=args.bandwidth_hz,
    acc_cnts=np.array(acc_cnts),
    **{f"spectra_{p}": np.array(spectra[p]) for p in pairs},
)
print("Done.")
