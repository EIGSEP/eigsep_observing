# Instrument data files

Fixed characterizations of the deployed hardware, shipped as package data
(`pyproject.toml` `[tool.setuptools.package-data]`). This README is not
shipped.

| File | What it is |
| --- | --- |
| `eigsep_fengine_1g_v2_3_2024-07-08_1858.fpg` | SNAP F-engine bitstream, v2.3 |
| `eigsep_fengine_1g_v2_4_2026-06-23_2052.fpg` | SNAP F-engine bitstream, v2.4 (single-spectrum) |
| `corr_linear_range_v2_4_2026-07-08.npz` | Per-channel linear-range product; see `linear_range.py` |
| `vna_internal_osl_20260629T090825Z.npz` | Lab characterization of the VNA's internal OSL standards |
| `switch_sparams.npz` | Lab-characterized S-parameters of the RF-switch paths |

## VNA lab calibration materials

Two files that take a raw field S11 sweep (`ants11_*` / `recs11_*.h5`,
written by `vna_writer`) to plane P, the receiver input. The field
calibration in `vna_calibration.py` assumes ideal OSL standards; these
are the inputs for the precise version. Consumers:
`data-analysis/scripts/calibrate_field_s11.py` and Christian's
`d5_yfactor.py` (`calibrate_s11`), both via `cmt_vna.calkit`.

Both are on the VNA's 1000-point grid, 1–250 MHz (`header/freqs` of the
field S11 files). Values are complex128 reflection quantities, unitless.

**`vna_internal_osl_20260629T090825Z.npz`** — captured 2026-06-29 09:08 UTC.
Keys, each `(1000,)`: `freqs` (Hz, float64); `MANUALO`, `MANUALS`,
`MANUALL` (uncalibrated traces of the S911T manual kit); `VNAO`, `VNAS`,
`VNAL` (uncalibrated traces of the internal standards). Solve the VNA error
network from the manual traces against `cmt_vna.calkit.S911T`, then
de-embed it from the internal traces to get the internal standards' true Γ.

**`switch_sparams.npz`** — no `freqs` key; it assumes the grid above.
Keys, each `(3, 1000)` in `cmt_vna.calkit` order `[S11, S12·S21, S22]`:
`VNAANT`, `VNAAMB`, `VNASP1`, `VNARF` (VNA-leg paths, de-embedded) and
`RFANT`, `RFAMB`, `RFSP1` (RF-leg paths, embedded). Per DUT:
`ant` = VNAANT→RFANT, `amb` = VNAAMB→RFAMB, `sp1_open`/`sp1_short` =
VNASP1→RFSP1, `rec` = VNARF only. Measurement date not recorded; the file
was last written 2026-08-31.

Provenance: received from Christian Hellum Bye on 2026-09-25 as
`field_cal_data_2026/cal_materials/` in his D5 Y-factor bundle, copied
unchanged.

| File | SHA-256 |
| --- | --- |
| `vna_internal_osl_20260629T090825Z.npz` | `3a0e93ff130e42871a633e0bc8c5624de3d7cd3441fe53a8d8fa7107b064b92d` |
| `switch_sparams.npz` | `8b4418f07187c965ce7a2424e9b8f25c7aef3b2b0e8aa550d1e874b2ae72c53d` |
