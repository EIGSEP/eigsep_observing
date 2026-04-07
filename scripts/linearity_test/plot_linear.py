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
    input_power = TOTAL_POWER_DBM - d["attenuation_dB"]
    ax.plot(input_power, d["power_0"], "o-", label="Input N0")
    ax.plot(input_power, d["power_1"], "s-", label="Input E2")
    ax.set_yscale("log")
    ax.set_xlabel("Input power (dBm)")
    ax.set_ylabel("Total power (correlator units)")
    ax.set_title("Correlator Linearity")
    ax.legend()
    ax.grid(True)

plt.tight_layout()
plt.show()
