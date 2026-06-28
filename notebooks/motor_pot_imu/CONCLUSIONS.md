# Azimuth sensor cross-check — motor vs potmon vs IMU

**Data:** `metadata_20260627_173152.h5` (recorder metadata file, ~386 s, azimuth
motor only). **Analysis date:** 2026-06-28. **Notebooks:** `01`–`05` in this
directory.

## Headline

**During real antenna motion, all three azimuth sensors agree to ~2°.** The
motor step count (`az_pos`), the potentiometer (`pot_az_voltage`), and the IMU
heading (`imu_az` `yaw`) are mutually consistent and individually trustworthy
for azimuth. There is **no measurable backlash or hysteresis**.

| comparison | residual (RMS) | notes |
|---|---|---|
| potmon ↔ IMU | 2.7° | + a 0.53 s publish-time offset (timing only, not mechanical) |
| motor ↔ pot/IMU (clean motion) | **2.0°** | refit on moving, non-stalled samples |
| up-sweep vs down-sweep branch gap | < 1° (median −0.4°) | ⇒ no backlash |

So motor `az_pos` *is* a faithful linear proxy for azimuth once calibrated
against pot or IMU.

## How we got there

1. **`01_raw_streams`** — raw motor / potmon / imu_az vs time. All three clearly
   track the same motion; IMU `yaw` wraps at ±180° (needs `np.unwrap`).
2. **`02_overlay`** — put all three on a common degrees axis (IMU yaw as the
   reference; linear fits of steps→deg and volts→deg). pot sits on top of IMU;
   a global motor fit showed a large structured residual (19.4° RMS).
3. **`03_time_lag`** — tested whether the motor residual was a time lag. It is
   **not**: the best constant lag explains 0.2% of the disagreement. pot↔IMU,
   by contrast, have a clean 0.53 s offset (publish/ADC delay).
4. **`04_hysteresis`** — motor vs true azimuth, colored by sweep direction. Up
   and down branches coincide (no backlash). Refit on clean motion → 2.0° RMS.
   All the disagreement collapses onto one localized region.
5. **`05_limit_event`** — zoom on that region (see below).

## The one anomaly — an isolated stall (not part of the headline)

In the last ~50 s of the record, `az_target_pos` commanded a move and `az_pos`
(the open-loop step counter) faithfully followed it, **but the antenna did not
move** — both pot and IMU stayed flat at ~58° while the counter logged ~80–90°
of travel. The motor is open-loop, so it recorded phantom motion the antenna
never made.

This is treated as an **isolated hardware failure to be investigated
separately**, *not* an expected behavior of the system. Plausible causes
include the motor losing power/torque, a stalled stepper, or a transient
drivetrain decoupling — a single file cannot distinguish them, and there is no
basis yet to assume it recurs.

The practical takeaway it *does* establish: the motor's open-loop count is **not
self-validating**. A stall is invisible in the motor stream alone — detecting it
requires cross-checking `az_pos` against the pot or IMU. (See deferred live-status
action below.)

## Caveats on absolute numbers

- **potmon is uncalibrated in this file** (`pot_az_cal_slope/intercept/angle`
  are `null`); only raw voltage is available. Degree conversions lean on the IMU
  fit, so absolute steps/deg and mV/deg are uncertain. The *relative* agreement
  conclusions do not depend on this.
- **IMU `yaw` is a relative heading** that wraps ±180° and may drift slowly over
  long records; fine over this 386 s window.
- Streams are **not hardware-synced** — motor ~3.8 Hz (±63 ms jitter), pot/imu
  ~5 Hz. Cross-sensor comparison requires resampling onto a common time base
  (we interpolated onto IMU timestamps).

## Follow-ups

1. ✅ This writeup.
2. ✅ Quick-run field-debug notebook (`notebooks/field_debug/`) that loads a
   recorder `metadata_*.h5` **or** a corr `*.h5` (metadata in header) and plots
   imu/pot/motor.
3. ⏸ **Deferred** — live-status warning on motor/antenna drift/stall (cross-check
   `az_pos` deltas vs pot/imu). Blocked on pico-firmware landing az/el
   conversions for raw IMU readings.
4. Investigate the stall as a hardware issue (check motor power/coupling);
   re-test with fresh data to see whether it reproduces.
