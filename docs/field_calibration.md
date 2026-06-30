# Antenna azimuth/elevation calibration & zeroing

Operator runbook for calibrating and zeroing the antenna pointing system
(motor + potentiometer + two IMUs). It covers what to do **in the lab**, what
to do **in the field** (box hanging ~100 m up, can't be leveled by hand), and
what to repeat **routinely** (between power cycles / after a remount), plus a
**recovery** appendix.

---

## The one principle: invariant geometry vs. drifting zeros

Two classes of quantity, and they map directly onto lab / field / routine:

| Quantity | Changes when | Where it's set |
|---|---|---|
| `steps/deg` (firmware motor geometry) | gearbox/hardware only | **Lab, once** (protractor) |
| pot slope `m` | pot remount / regearing | **Lab, once** (`calibrate-pot azimuth`) |
| IMU mount `M`, az/el conversion | IMU remount | **Lab, once** (`calibrate-imu`) |
| pot intercept `b`, motor step origin | **every power cycle / remount** | **Routine** (`field_zero.py`) |

The geometry/mount terms are properties of the hardware and are calibrated once
in the lab. The **zeros** are what you re-establish in the field and after every
power cycle.

### Calibration chain (do it in this order)

Each stage is the truth standard for the next, so the order is load-bearing:

```
protractor (external truth)
  └─> firmware steps/deg   [GATE — fix before anything downstream]
        └─> pot slope m    (calibrate-pot azimuth: motor drives, pot fits)
              └─> IMU mount + az/el conversion (calibrate-imu: pot is az truth)
                    └─> live az/el for every sensor
```

### What each sensor can and can't tell you

- **Elevation is absolute** — both IMUs derive `el` from gravity
  (`imu_el.el_deg` signed, `imu_az.el_deg` = |θ|). Trustworthy regardless of
  heading.
- **Azimuth is relative** — the IMU is in RVC mode with **no magnetometer**, so
  its yaw drifts and has no absolute north. IMU azimuth is *registered to the
  pot*. **The IMU cannot set an absolute azimuth zero.**
- The **absolute azimuth datum** therefore comes from the **potentiometer**
  (absolute within its range) or a mechanical hard stop — never the IMU.

---

## LAB — one-time geometry & calibration (box on a level surface)

Do these on the bench/level surface where you can use a protractor and a
spirit level. Confirm the box is level for steps 1–2.

### 1. Geometry gate (protractor) — **do this first**

The firmware converts steps↔degrees with `step_angle_deg · steps /
(microstep · gear_teeth)`. If that constant is wrong, **every** downstream
number inherits the error (a "360°" move under-rotates, and the pot slope comes
out wrong by the same factor — they are one bug, not two).

1. `python scripts/motor_manual.py` — jog azimuth and command a **45°** move.
2. Measure the **physical** rotation with a protractor / level.
3. **Gate:** a commanded 45° must read **45° ± ~1°** physically. If it doesn't,
   correct `gear_teeth` (or `microstep`/`step_angle_deg`) in pico-firmware,
   reflash, and re-test until a commanded angle matches reality.

Nothing below is trusted until this passes — the protractor, not a sensor, is
ground truth here.

### 2. Pot slope — `calibrate-pot --mode azimuth`

1. Jog to the position you want to call azimuth zero and **home** there.
2. Run `calibrate-pot --mode azimuth`, stepping the motor in ~45° increments
   through a full rotation; it fits `angle = m·V + b` and **pins the intercept
   to motor-home** (so the pot zero = the motor zero).
3. **Gate:** residual RMS < ~2°, and the ADC has headroom (no railing across
   the travel).

### 3. IMU mount — `calibrate-imu`

Requires the pot calibrated first (calibrate-imu treats the **pot as azimuth
truth** and gravity as elevation truth).

1. Mount `imu_az` as vertical and `imu_el` as horizontal as practical — the
   calibration fits the *actual* mount (rotation matrix + nearest signed
   permutation), so it tolerates an imperfect mount and reports
   `mount_misalign_deg`. A level mount makes pitch/roll ≈ 0 and yaw a clean 1:1
   azimuth, so it's still worth doing.
2. Run `calibrate-imu --mode all` (elevation sweep, azimuth-near-level sweep
   with yaw, azimuth-tilted sweep).
3. **Gate:** `mount_misalign_deg` is reported and sane (small).

After the lab pass, all three sensors agree and the geometry/mount terms are
fixed until the hardware changes.

---

## FIELD — deploy & set the operational zero (box hanging)

The mount and geometry are invariant, so **do not recalibrate in the field** —
verify, then zero.

### 4. Verify (don't recalibrate)

1. Start the dashboard: `python scripts/live_status.py`.
2. Do a short azimuth sweep and watch the **Antenna pointing** card:
   - **Gate:** the az/el **spread** tiles stay green (sensors agree to within a
     few degrees). A red spread on deploy means the IMU mount shifted in transit
     (re-run `calibrate-imu`) or the pot is slipping (next step will catch it).

### 5. Zero — `python scripts/field_zero.py`

This is the one field step that *sets* something. It is a self-contained active
driver — **stop `panda_observe` first** (it refuses to run while another driver
owns the motor).

It runs in order:
1. **Pot-slip pre-check** — a known there-and-back move vs. the expected pot
   voltage swing. ≥5% short → **warn**; ≥10% short → **abort** (this is the
   over-tightened/slipping-pot failure mode — back the pot mount off so the
   shaft turns freely, then retry).
2. **Jog** to the operational az/el zero (live motor/pot/IMU readout).
3. **Confirm** (Enter, then `y`) → resets the motor step origin **and** re-pins
   the pot intercept (`b = -m·v0`), then `BGSAVE`s and pushes the new cal live.

Tuning: `--move-deg` (slip-probe size, default 30) and `--deg` (initial jog
step).

---

## ROUTINE — between power cycles / after a remount

- **Power cycle, same Pi:** the motor zero auto-reseeds from Redis (firmware
  `boot_id`), and pot/IMU calibration persist via `dump.rdb`. So usually you
  **just verify** on the orientation card. Re-run `field_zero.py` only if the
  box was remounted or the pot-slip check fails.
- **After a pot remount or a failed slip check:** re-run `field_zero.py`
  (re-pins the intercept; the slope from the lab still holds unless the gearing
  changed).
- **After an IMU remount:** re-run `calibrate-imu` (the mount changed).

---

## RECOVERY — Pi swap / Redis loss (should never happen)

Calibration and the motor zero live in Redis (`pot_calibration`,
`imu_calibration`, `motor_position`), persisted to `dump.rdb` on the Redis
host. They survive a power cycle of that host, but **a Pi swap loses them**
unless you copy `dump.rdb`.

1. **Preferred:** restore `dump.rdb` onto the new Pi.
2. **Pot cal, by hand:** every recorded corr `.h5` embeds
   `pot_az_cal_slope` / `pot_az_cal_intercept` at record time. Read them off a
   recent file and re-enter them:
   `calibrate-pot --mode manual --slope <m> --intercept <b>`.
3. **IMU cal, from file:** every recorded corr *and* VNA `.h5` embeds the full
   `imu_calibration` blob at `header["imu_calibration"]` (and
   `header["imu_calibration_upload_unix"]`, unix seconds, for staleness).
   Read it off a recent file and re-upload via picohost `ImuCalStore` — this
   is now preferred since the mount matrix isn't hand-typeable. If
   `imu_calibration == {}` or `imu_calibration_upload_unix == 0.0` (that file
   caught the panda down or uncalibrated), re-run `calibrate-imu` instead.
   Note: standalone `metadata_*.h5` files do not carry this blob (no header
   group).
4. Then re-zero with `field_zero.py`.

---

## Live monitoring

The **Antenna pointing** card on the live-status dashboard shows each sensor's
az/el in degrees, a consensus, and an az/el **spread** tile that turns amber
(>3°) / red (>10°) when the sensors disagree. That spread is the live
drift/stall/slip alarm — green means motor, pot, and IMU agree on where the
antenna is. (The authoritative weighted az/el estimate is a post-processing
step, not a live decision.)
