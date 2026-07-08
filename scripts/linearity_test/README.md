# SNAP linearity test

Measures the linear range of the SNAP signal chain per frequency
channel and turns it into the `linear_range_file` calibration product
consumed by corr file headers and the live-status dashboard (dashed
min/max bounds on the corr spectrum plot).

Prior art: the April 2026 total-power test (memo in
`memos/linearity/`), which eyeballed the linear range from log-log
plots. This workflow supersedes it with per-channel line fits.

## Workflow

1. **Sweep** (lab, manual step attenuator on the noise source):

   ```
   python corr_linearity.py --redis-host 10.10.10.10 --outfile sweep
   ```

   At each attenuation step, type the dB value and press Enter; the
   script records `--nsamples` integrations of the full per-channel
   auto spectra for inputs 0 and 1, plus the corr header — the
   operating-point provenance (adc_gain, fft_shift, corr_scalar, ...)
   the product is validated against downstream. If your noise source
   or LPF differ from the defaults, record them with
   `--noise-density-dbm-hz` / `--bandwidth-hz`.

2. **Fit**:

   ```
   python fit_linearity.py sweep.npz --plot
   ```

   Per input and channel this fits log10(counts) vs input power (dBm)
   on a sigma-clipped middle region; steps within `--threshold-db`
   (default 1 dB) of the fit are "linear", and the min/max bounds are
   the measured counts at the lowest/highest linear step. Channels
   with too little dynamic range (e.g. above the LPF cutoff) get NaN
   bounds. Bounds are median-smoothed across frequency
   (`--smooth-window`) and combined into a conservative envelope
   across inputs. Inspect the `*_diagnostics.png` before deploying.

3. **Deploy**: commit the product npz to
   `src/eigsep_observing/data/` and set `linear_range_file` in
   `config/corr_config.yaml`. From then on corr files carry
   `linear_range_min`/`linear_range_max` header datasets and
   live-status draws the dashed bounds. Both consumers validate the
   product's operating point against the live corr header and omit
   the bounds (with an ERROR log) on mismatch — re-fit after any
   operating-point change.

`plot_linear.py` plots total power vs input power for both the new
per-channel sweeps and the archived April 2026 total-power npz files.
