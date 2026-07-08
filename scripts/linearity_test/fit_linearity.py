"""Fit per-channel linear-range bounds from a corr_linearity sweep.

Reads the per-channel sweep npz written by ``corr_linearity.py`` and,
per input and per frequency channel, fits a line to
``log10(counts)`` vs input power (dBm) over a robust middle region
(sigma-clipped). The linear range is the set of attenuation steps
whose measured counts stay within ``--threshold-db`` of the fit; the
per-channel min/max bounds are the measured counts at the lowest and
highest linear step. Channels with too little dynamic range or too
few usable steps (e.g. above the anti-aliasing LPF cutoff, where the
noise source injects no power) get NaN bounds.

The per-input bounds are median-smoothed across frequency and
combined into a conservative envelope (min bound = max over inputs,
max bound = min over inputs), then saved as a linear-range product
npz via :func:`eigsep_observing.linear_range.save_linear_range`. To
deploy the product, commit it to ``src/eigsep_observing/data/`` and
set ``linear_range_file`` in ``corr_config.yaml``.

Usage:
    python fit_linearity.py linearity_corr.npz --plot
"""

import argparse
import json
import time
import warnings
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

from eigsep_observing.linear_range import save_linear_range

MAX_CLIP_ITERATIONS = 5
# Steps within this margin of a channel's weakest measurement are
# averaged into its noise-floor estimate.
FLOOR_BAND_DB = 3.0

parser = argparse.ArgumentParser(
    description="Fit per-channel linear-range bounds from a sweep npz",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("sweep", type=str, help="Sweep npz from corr_linearity")
parser.add_argument(
    "--outfile",
    type=str,
    default=None,
    help=(
        "Output product npz; default "
        "corr_linear_range_v<maj>_<min>_<date>.npz from the sweep header"
    ),
)
parser.add_argument(
    "--threshold-db",
    type=float,
    default=1.0,
    help="Max |deviation| from the fit line for a step to count linear",
)
parser.add_argument(
    "--floor-margin-db",
    type=float,
    default=6.0,
    help="Steps must exceed the channel noise floor by this to enter the fit",
)
parser.add_argument(
    "--min-steps",
    type=int,
    default=4,
    help="Minimum fit steps per channel; fewer masks the channel (NaN)",
)
parser.add_argument(
    "--min-dynamic-range-db",
    type=float,
    default=10.0,
    help="Minimum floor-to-peak range per channel; less masks the "
    "channel (NaN)",
)
parser.add_argument(
    "--smooth-window",
    type=int,
    default=17,
    help="Median-filter window (channels) for the bound curves; odd",
)
parser.add_argument(
    "--noise-density-dbm-hz",
    type=float,
    default=None,
    help="Override the sweep's recorded noise-source density",
)
parser.add_argument(
    "--bandwidth-hz",
    type=float,
    default=None,
    help="Override the sweep's recorded noise bandwidth",
)
parser.add_argument(
    "--plot",
    action="store_true",
    help="Save and show diagnostic plots",
)


def fit_channel(
    p_in_dbm, counts, threshold_db, floor_margin_db, min_steps, min_dr_db
):
    """Fit one channel's log10(counts) vs input power.

    Parameters
    ----------
    p_in_dbm : np.ndarray
        Input power per step (dBm), sorted ascending.
    counts : np.ndarray
        Mean measured counts per step (same order).
    threshold_db, floor_margin_db, min_steps, min_dr_db
        See the CLI flags of the same names.

    Returns
    -------
    slope, intercept, min_counts, max_counts : float
        All NaN when the channel is degenerate (insufficient dynamic
        range or too few fit steps).

    """
    nan4 = (np.nan,) * 4
    positive = counts > 0
    if not positive.any():
        return nan4
    floor_ceiling = counts[positive].min() * 10 ** (FLOOR_BAND_DB / 10)
    floor_mask = positive & (counts <= floor_ceiling)
    floor = counts[floor_mask].mean()
    peak = counts[positive].max()
    if 10 * np.log10(peak / floor) < min_dr_db:
        return nan4
    candidates = positive & (counts > floor * 10 ** (floor_margin_db / 10))
    if candidates.sum() < min_steps:
        return nan4

    with np.errstate(divide="ignore"):
        log_counts = np.log10(np.where(positive, counts, np.nan))
    mask = candidates
    slope = intercept = None
    for _ in range(MAX_CLIP_ITERATIONS):
        slope, intercept = np.polyfit(p_in_dbm[mask], log_counts[mask], 1)
        resid_db = 10 * (log_counts - (slope * p_in_dbm + intercept))
        new_mask = candidates & (np.abs(resid_db) <= threshold_db)
        if new_mask.sum() < min_steps:
            break  # keep the previous (last valid) fit
        if (new_mask == mask).all():
            break
        mask = new_mask

    resid_db = 10 * (log_counts - (slope * p_in_dbm + intercept))
    linear = positive & (np.abs(resid_db) <= threshold_db)
    if not linear.any():
        return nan4
    idx = np.nonzero(linear)[0]
    return slope, intercept, counts[idx[0]], counts[idx[-1]]


def nan_median_smooth(arr, window):
    """NaN-aware sliding median that preserves the input's NaN mask.

    Smoothing must not invent bounds for masked (degenerate-fit)
    channels, so the original NaN mask is re-applied after filtering.
    """
    if window <= 1:
        return arr.copy()
    pad = window // 2
    padded = np.pad(arr, pad, constant_values=np.nan)
    windows = np.lib.stride_tricks.sliding_window_view(padded, window)
    out = np.full(arr.shape, np.nan)
    has_data = (~np.isnan(windows)).any(axis=1)
    out[has_data] = np.nanmedian(windows[has_data], axis=1)
    out[np.isnan(arr)] = np.nan
    return out


def main():
    args = parser.parse_args()
    if args.smooth_window % 2 == 0:
        parser.error("--smooth-window must be odd")

    sweep = np.load(args.sweep, allow_pickle=False)
    header = json.loads(str(sweep["header_json"]))
    freqs = sweep["freqs"]
    inputs = sorted(
        k[len("spectra_") :] for k in sweep.files if k.startswith("spectra_")
    )

    noise_density = (
        args.noise_density_dbm_hz
        if args.noise_density_dbm_hz is not None
        else float(sweep["noise_density_dbm_hz"])
    )
    bandwidth = (
        args.bandwidth_hz
        if args.bandwidth_hz is not None
        else float(sweep["bandwidth_hz"])
    )
    total_power_dbm = noise_density + 10 * np.log10(bandwidth)
    p_in_dbm = total_power_dbm - sweep["attenuation_dB"]

    # Sort steps by ascending input power so "lowest/highest linear
    # step" is well defined regardless of sweep direction.
    order = np.argsort(p_in_dbm)
    p_in_dbm = p_in_dbm[order]

    nchan = freqs.size
    per_input = {}
    counts_by_input = {}
    for p in inputs:
        counts = sweep[f"spectra_{p}"].astype(np.float64).mean(axis=1)
        counts = counts[order]
        counts_by_input[p] = counts
        fields = {
            name: np.full(nchan, np.nan)
            for name in ("slope", "intercept", "linear_min", "linear_max")
        }
        for c in range(nchan):
            slope, intercept, lo, hi = fit_channel(
                p_in_dbm,
                counts[:, c],
                args.threshold_db,
                args.floor_margin_db,
                args.min_steps,
                args.min_dynamic_range_db,
            )
            fields["slope"][c] = slope
            fields["intercept"][c] = intercept
            fields["linear_min"][c] = lo
            fields["linear_max"][c] = hi
        raw_min, raw_max = fields["linear_min"], fields["linear_max"]
        fields["linear_min"] = nan_median_smooth(raw_min, args.smooth_window)
        fields["linear_max"] = nan_median_smooth(raw_max, args.smooth_window)
        per_input[p] = fields
        nfit = np.isfinite(fields["linear_min"]).sum()
        print(f"Input {p}: {nfit}/{nchan} channels fit")

    with warnings.catch_warnings():
        # all-NaN channels are expected (masked band); nanmax/nanmin
        # warn on them and correctly yield NaN.
        warnings.simplefilter("ignore", category=RuntimeWarning)
        linear_min = np.nanmax(
            np.vstack([per_input[p]["linear_min"] for p in inputs]), axis=0
        )
        linear_max = np.nanmin(
            np.vstack([per_input[p]["linear_max"] for p in inputs]), axis=0
        )
    inverted = np.isfinite(linear_min) & (linear_min >= linear_max)
    if inverted.any():
        print(
            f"WARNING: {inverted.sum()} channels have inverted envelope "
            "(inputs disagree); masking them"
        )
        linear_min[inverted] = np.nan
        linear_max[inverted] = np.nan
    nfit = np.isfinite(linear_min).sum()
    print(f"Combined envelope: {nfit}/{nchan} channels bounded")

    outfile = args.outfile
    if outfile is None:
        maj, minor = header.get("fpg_version", ["x", "x"])
        date = time.strftime("%Y-%m-%d")
        outfile = f"corr_linear_range_v{maj}_{minor}_{date}.npz"
    save_linear_range(
        outfile,
        freqs=freqs,
        linear_min=linear_min,
        linear_max=linear_max,
        header=header,
        threshold_db=args.threshold_db,
        smooth_window=args.smooth_window,
        created_unix=time.time(),
        source_file=Path(args.sweep).name,
        per_input=per_input,
    )
    print(f"Saved product to {outfile}")

    if args.plot:
        plot_diagnostics(
            outfile,
            freqs,
            p_in_dbm,
            counts_by_input,
            per_input,
            linear_min,
            linear_max,
        )


def plot_diagnostics(
    outfile,
    freqs,
    p_in_dbm,
    counts_by_input,
    per_input,
    linear_min,
    linear_max,
):
    freq_mhz = freqs * 1e-6
    inputs = sorted(counts_by_input)
    nchan = freqs.size
    sample_chans = [nchan // 4, nchan // 2, 3 * nchan // 4]
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    ax = axes[0]
    for p in inputs:
        counts = counts_by_input[p]
        fields = per_input[p]
        for c in sample_chans:
            (line,) = ax.plot(
                p_in_dbm,
                counts[:, c],
                "o",
                ms=4,
                label=f"in{p} ch{c} ({freq_mhz[c]:.0f} MHz)",
            )
            slope, intercept = fields["slope"][c], fields["intercept"][c]
            if np.isfinite(slope):
                ax.plot(
                    p_in_dbm,
                    10 ** (slope * p_in_dbm + intercept),
                    "--",
                    color=line.get_color(),
                    lw=1,
                )
    ax.set_yscale("log")
    ax.set_xlabel("Input power (dBm)")
    ax.set_ylabel("Counts")
    ax.set_title("Per-channel fits (sample channels)")
    ax.legend(fontsize=7)
    ax.grid(True)

    ax = axes[1]
    for p in inputs:
        fields = per_input[p]
        ax.plot(freq_mhz, fields["linear_min"], lw=1, label=f"in{p} min")
        ax.plot(freq_mhz, fields["linear_max"], lw=1, label=f"in{p} max")
    ax.plot(freq_mhz, linear_min, "k--", lw=1.5, label="envelope min")
    ax.plot(freq_mhz, linear_max, "k--", lw=1.5, label="envelope max")
    ax.set_yscale("log")
    ax.set_xlabel("Frequency (MHz)")
    ax.set_ylabel("Counts")
    ax.set_title("Linear-range bounds")
    ax.legend(fontsize=7)
    ax.grid(True)

    ax = axes[2]
    p = inputs[0]
    counts = counts_by_input[p]
    fields = per_input[p]
    with np.errstate(divide="ignore", invalid="ignore"):
        fit = 10 ** (
            fields["slope"][None, :] * p_in_dbm[:, None]
            + fields["intercept"][None, :]
        )
        resid_db = 10 * np.log10(counts / fit)
    im = ax.pcolormesh(
        freq_mhz, p_in_dbm, resid_db, vmin=-3, vmax=3, cmap="RdBu_r"
    )
    fig.colorbar(im, ax=ax, label="Residual (dB)")
    ax.set_xlabel("Frequency (MHz)")
    ax.set_ylabel("Input power (dBm)")
    ax.set_title(f"Fit residuals, input {p}")

    fig.tight_layout()
    png = str(Path(outfile).with_suffix("")) + "_diagnostics.png"
    fig.savefig(png, dpi=150)
    print(f"Saved diagnostics to {png}")
    plt.show()


if __name__ == "__main__":
    main()
