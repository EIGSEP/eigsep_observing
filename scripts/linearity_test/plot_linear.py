"""Plot linearity test results from ADC and/or correlator npz files.

Usage:
    python plot_linearity.py --adc test_040626/linearity.npz
    python plot_linearity.py --corr linearity_corr.npz
    python plot_linearity.py --adc test_040626/linearity.npz --corr linearity_corr.npz
"""

import argparse

import numpy as np
import matplotlib.pyplot as plt

NOISE_DENSITY_DBM_HZ = -75  # noise source power spectral density
BANDWIDTH_HZ = 225e6  # lowpass filter cutoff
TOTAL_POWER_DBM = NOISE_DENSITY_DBM_HZ + 10 * np.log10(BANDWIDTH_HZ)

parser = argparse.ArgumentParser(
    description="Plot linearity test results",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("--adc", type=str, help="ADC linearity npz file")
parser.add_argument("--corr", type=str, help="Correlator linearity npz file")
args = parser.parse_args()

if not args.adc and not args.corr:
    parser.error("Provide at least one of --adc or --corr")

nplots = bool(args.adc) + bool(args.corr)
fig, axes = plt.subplots(1, nplots, figsize=(7 * nplots, 5))
if nplots == 1:
    axes = [axes]

idx = 0
if args.adc:
    ax = axes[idx]
    d = np.load(args.adc)
    input_power = TOTAL_POWER_DBM - d["attenuation_dB"]
    ax.plot(input_power, d["rms_x"], "o-", label="Input N0")
    ax.plot(input_power, d["rms_y"], "s-", label="Input E2")
    ax.set_yscale("log")
    ax.set_xlabel("Input power (dBm)")
    ax.set_ylabel("RMS (ADC counts)")
    ax.set_title("ADC Linearity")
    ax.legend()
    ax.grid(True)
    idx += 1

if args.corr:
    ax = axes[idx]
    d = np.load(args.corr)
    # Old sweeps (April 2026) stored total power directly as
    # power_{p}; per-channel sweeps store spectra_{p} with shape
    # (nsteps, nsamples, nchan) — reduce to total power on the fly.
    if "power_0" in d.files:
        total_power_dbm = TOTAL_POWER_DBM
        power = {
            k[len("power_") :]: d[k] for k in d.files if k.startswith("power_")
        }
    else:
        total_power_dbm = float(d["noise_density_dbm_hz"]) + 10 * np.log10(
            float(d["bandwidth_hz"])
        )
        power = {
            k[len("spectra_") :]: d[k].mean(axis=1).sum(axis=-1)
            for k in d.files
            if k.startswith("spectra_")
        }
    input_power = total_power_dbm - d["attenuation_dB"]
    markers = "os^vD"
    for i, p in enumerate(sorted(power)):
        ax.plot(
            input_power,
            power[p],
            markers[i % len(markers)] + "-",
            label=f"Input {p}",
        )
    ax.set_yscale("log")
    ax.set_xlabel("Input power (dBm)")
    ax.set_ylabel("Total power (correlator units)")
    ax.set_title("Correlator Linearity")
    ax.legend()
    ax.grid(True)

plt.tight_layout()
plt.show()
