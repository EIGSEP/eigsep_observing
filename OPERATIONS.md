# EIGSEP Observing Operations Guide

## Quick Reference Card

```
NETWORK
  Ground computer  ──── LattePanda (10.10.10.11, Redis 6379)
                   ──── RPi + SNAP  (10.10.10.10, Redis 6379)

STARTUP ORDER
  1. SSH into panda:     ssh eigsep@10.10.10.11
  2. Start panda client: eigsep-panda
       (operator-launched, not a systemd service — run inside
        tmux/screen for detachable sessions)
  3. On ground RPi:  eigsep-observe

STOP
  Ctrl-C on either script. Threads shut down gracefully.

CONFIG
  Panda owns the observing config (switch schedule, VNA, motor).
  Edit obs_config.yaml on the panda, then restart eigsep-panda —
  it uploads the yaml to Redis on launch. The observer reads the
  observing config from Redis; its only CLI knobs are host-local
  (IPs, save dir, ntimes).

LOGS
  Rotating log file: eigsep.log (10 MB, 5 backups)
```

### Common commands

```bash
# Normal startup
eigsep-panda                  # on panda
eigsep-observe                # on ground

# SNAP-only is automatic: if the panda Redis is unreachable at startup
# (or drops mid-run), eigsep-observe logs a WARNING and keeps writing
# corr files. Header overlays become sentinels (obs_config={},
# run_tag="UNKNOWN") and the metadata sidecar is empty until the panda
# is back — corr data itself is unaffected.

# Dry run with fake hardware
eigsep-panda --dummy          # terminal 1
eigsep-observe --dummy        # terminal 2

# Test-bench data collection (no SNAP): save pico metadata while you
# exercise actuators with motor_manual / tempctrl_manual / etc. The
# panda's PicoManager service must already be running; record_metadata
# is a consumer that drains the streams to its own HDF5 file (same JSON
# metadata format as a corr file's sidecar). Read it back with
# eigsep_observing.io.read_metadata_hdf5(fname), which returns
# {stream: [sample_dict, ...]} (each dict carries _ts_unix for joining).
python scripts/record_metadata.py --save-dir /tmp/runs

# Same idea for VNA: loop ant/rec bundles at a fixed interval and
# save each via save_vna_manual_h5 (also publishes to Redis so the
# live-status panes update).
python scripts/record_vna.py --save-dir /tmp/runs --interval 300

# Custom observing config (switch schedule / VNA / motor) — panda side
eigsep-panda --cfg_file /path/to/my_config.yaml

# Override host-local knobs — observer side (no yaml, CLI flags only)
eigsep-observe --rpi-ip 10.0.0.5 --panda-ip 10.0.0.6 \
    --corr-save-dir /mnt/data --corr-ntimes 120
```

### If something crashes

| What crashed | What to do |
|---|---|
| Observer only | Restart `eigsep-observe`. Panda keeps running. You may lose some data that overflowed the Redis stream buffer. |
| Panda only | SSH into panda, restart `eigsep-panda`. It reads config from Redis automatically. Observer keeps writing SNAP data (without metadata sidecar / overlays) until panda reconnects; on reconnect, metadata stream positions are skipped to the current tail so resumed sidecar averages stay aligned with the corr integration window (no replayed backlog). |
| Both | Start panda first, then observer. |
| Redis | Everything stops. Restart Redis, then panda, then observer. |

### Changing configuration

1. SSH into panda
2. Edit `obs_config.yaml` (or pass `--cfg_file` at launch)
3. Ctrl-C on `eigsep-panda`
4. Restart `eigsep-panda` (reads new yaml, uploads to Redis)
5. Optionally restart `eigsep-observe` (it will read the new config from
   Redis for VNA save dir and `use_vna` gating; host-local knobs stay as
   CLI flags)

---

## Detailed System Flowchart

### Architecture

```
                         GROUND COMPUTER
                    ┌─────────────────────────┐
                    │      observe.py          │
                    │  ┌───────────────────┐   │
                    │  │   EigObserver      │   │
                    │  │                   │   │
                    │  │  status_logger ─────────── reads stream:status
                    │  │  record_corr_data ─────── reads stream:corr ──────┐
                    │  │  record_vna_data  ─────── reads stream:vna ───┐  │
                    │  │                   │   │                       │  │
                    │  │  writes HDF5 files │   │                       │  │
                    │  └───────────────────┘   │                       │  │
                    └─────────────────────────┘                       │  │
                                                                      │  │
                    ┌─────────── REDIS (on panda) ──────────────┐     │  │
                    │                                           │     │  │
                    │  "config"          ← obs config (JSON)    │     │  │
                    │  "metadata"        ← live sensor hash     │     │  │
                    │  "heartbeat:client"← panda alive flag     │     │  │
                    │  stream:status     ← panda log messages   │     │  │
                    │  stream:vna        ← VNA S11 data     ◄───┼─────┘  │
                    │  stream:{sensor}   ← sensor time series   │        │
                    │                                           │        │
                    └───────────────────────────────────────────┘        │
                                                                         │
                    ┌─────────── REDIS (on RPi) ────────────────┐       │
                    │                                           │       │
                    │  "corr_config"     ← SNAP config (JSON)   │       │
                    │  "corr_header"     ← correlator header    │       │
                    │  stream:corr       ← correlator data  ◄───┼───────┘
                    │                                           │
                    └───────────────────────────────────────────┘

                         LATTEPANDA (suspended box)
                    ┌─────────────────────────┐
                    │      eigsep-panda        │
                    │  ┌───────────────────┐   │
                    │  │   PandaClient     │   │
                    │  │                   │   │
                    │  │  heartbeat_thd ────────── sets heartbeat:client
                    │  │  switch_loop   ────────── cycles RFANT/RFNOFF/RFNON/RFAMB/RFSP1
                    │  │  vna_loop      ────────── measures S11, writes stream:vna
                    │  │  pico threads  ────────── reads sensors, writes metadata
                    │  │                   │   │
                    │  └───────────────────┘   │
                    │         │                │
                    │    ┌────┴────┐           │
                    │    │ Hardware │           │
                    │    │ RF switch│           │
                    │    │ VNA      │           │
                    │    │ Picos    │           │
                    │    └─────────┘           │
                    └─────────────────────────┘
```

### Startup Sequence

```
STEP 1: Start Panda                    STEP 2: Start Observer
─────────────────────                  ──────────────────────

eigsep-panda                           eigsep-observe
  │                                      │
  ├─ Connect to local Redis              ├─ Parse CLI flags
  ├─ Load --cfg_file yaml                │    (IPs, save dir, ntimes)
  ├─ Upload yaml to Redis (authoritative)│
  ├─ PandaClient.__init__()              ├─ Connect to RPi Redis
  │   ├─ Read config from Redis          ├─ Connect to Panda Redis
  │   ├─ Discover picos on serial        │
  │   ├─ Add pico info to config         ├─ EigObserver.__init__()
  │   ├─ Upload enriched config          │   ├─ Read corr_config from RPi Redis
  │   │   back to Redis                  │   ├─ Read obs config from Panda Redis
  │   ├─ Init RF switch network          │   └─ Start status_logger thread
  │   ├─ Init VNA                        │
  │   └─ Start heartbeat thread          ├─ Wait for panda heartbeat
  │                                      │
  ├─ Start switch_loop thread            ├─ Start record_corr_data thread
  ├─ Start vna_loop thread               ├─ Start record_vna_data thread
  │                                      │
  └─ Block on thread.join()              └─ Block on thread.join()
```

### Panda Threads (autonomous)

```
switch_loop                          vna_loop
───────────                          ────────
while not stopped:                   while not stopped:
  for mode in schedule:                acquire switch_lock
    ┌─────────────────────────────────────────────────────┐         ├─ save current switch state
    │ RFANT  (sky)                                        │ 3600s   ├─ measure_s11("ant")
    │  lock → switch                                      │         │   ├─ set VNA power for antenna
    │  unlock                                             │         │   ├─ OSL calibration (O, S, L)
    │  wait 3600s                                         │         │   ├─ measure antenna + noise + load
    │  (VNA can                                           │         │   ├─ get live metadata from Redis
    │   interrupt here)                                   │         │   └─ write to stream:vna
    ├─────────────────────────────────────────────────────┤         ├─ measure_s11("rec")
    │ RFNOFF (noise off — offline cross-check)            │ 60s     │   ├─ set VNA power for receiver
    │  lock → switch                                      │         │   ├─ OSL calibration
    │  wait with lock                                     │         │   ├─ measure receiver
    │  (VNA blocked)                                      │         │   └─ write to stream:vna
    ├─────────────────────────────────────────────────────┤         ├─ restore previous switch state
    │ RFNON  (noise on — Y-factor hot)                    │ 60s     release switch_lock
    │  lock → switch                                      │         wait vna_interval (3600s)
    │  wait with lock                                     │
    │  (VNA blocked)                                      │
    ├─────────────────────────────────────────────────────┤
    │ RFAMB  (ambient load — Y-factor cold)               │ 60s
    │  lock → switch                                      │
    │  wait with lock                                     │
    │  (VNA blocked)                                      │
    ├─────────────────────────────────────────────────────┤
    │ RFSP1  (Spare-1 open cable, see switch_connections) │ 60s
    │  lock → switch                                      │
    │  wait with lock                                     │
    │  (VNA blocked)                                      │
    └─────────────────────────────────────────────────────┘

heartbeat_thd                        pico threads (per device)
─────────────                        ────────────────────────
while not stopped:                   Managed by picohost library.
  set heartbeat:client = 1           Each pico reads its sensor
  (expires after 60s)                and calls redis.add_metadata()
  sleep 1s                           to update the metadata hash
                                     and sensor streams.
```

### Observer Threads

```
record_corr_data                     record_vna_data
────────────────                     ───────────────
while not stopped:                   while not stopped:
  blocking read stream:corr            blocking read stream:vna
  (timeout 10s)                        (blocks indefinitely)
  │                                    │
  ├─ unpack bytes → numpy arrays       ├─ unpack bytes → numpy arrays
  ├─ read live metadata from panda     ├─ decode header + metadata
  │   (or None if disconnected)        └─ write HDF5 file
  └─ accumulate into File object
     (writes HDF5 every ntimes
      integrations = ~240 * t_int)

status_logger
─────────────
while not stopped:
  check panda heartbeat
  if disconnected: log warning every 10s, wait
  if connected: read stream:status
    log any messages from panda
```

### Data Flow

```
SNAP correlator                     VNA
  │                                   │
  │ raw bytes (>i4)                   │ complex arrays
  ▼                                   ▼
stream:corr (maxlen 5000)           stream:vna (maxlen 1000)
  │                                   │
  │ read by observer                  │ read by observer
  ▼                                   ▼
HDF5 files                          HDF5 files
  corr_save_dir/                      vna_save_dir/
  ├─ {timestamp}.hdf5                 ├─ {timestamp}.hdf5
  │   ├─ corr data (per pair)         │   ├─ S11 data (ant/rec/cal)
  │   ├─ corr_header                  │   ├─ VNA header (freqs, power)
  │   ├─ corr_config                  │   └─ metadata snapshot
  │   └─ metadata snapshots           │
  └─ ...                              └─ ...


Sensor metadata
  │
  │ picohost → add_metadata()
  ▼
  Two destinations:
  1. "metadata" hash  ← latest values (live)
  2. stream:{key}     ← time series (for file headers)
```

### Config Ownership

```
obs_config.yaml (on panda disk, selected by --cfg_file)
  │
  │ read at eigsep-panda startup, uploaded to Redis
  ▼
Redis "config" key (authoritative; overwritten on each launch)
  │
  │ PandaClient enriches with pico info,
  │ re-uploads with timestamp
  │
  │ read by EigObserver for VNA settings
  │ saved in data file headers
  ▼
Official config record
  (what the system was actually running)


To change config:
  1. Edit obs_config.yaml on panda (or pass --cfg_file PATH)
  2. Restart eigsep-panda
     └─ uploads YAML to Redis, discovers picos, re-uploads with pico info
  3. Restart eigsep-observe if host-local knobs changed
     (--corr-save-dir, --corr-ntimes, --rpi-ip, --panda-ip)
     └─ observer reads the observing config from Redis automatically
```

### Network Addresses

Panda-side defaults live in `obs_config.yaml`. Ground-side defaults are
baked into `eigsep-observe` as CLI flag defaults (`--rpi-ip`, `--panda-ip`).
Actual IPs may differ per deployment — pass them explicitly if so.

```
Ground computer:  user machine (runs eigsep-observe)
RPi + SNAP:       10.10.10.10 (Redis port 6379)
LattePanda:       10.10.10.11 (Redis port 6379)
VNA:              127.0.0.1:5025 (local to panda)
```

## Tempctrl channel descope and hot-swap

The tempctrl Pico's two Peltier channels are independent config knobs:
`tempctrl_settings.{LNA,LOAD}.installed` in `obs_config.yaml`. A channel
marked `installed: false` is descoped: firmware never samples its
thermistor (no ADC-mux switch to the dead divider, so it cannot
crosstalk into the live channel) or drives its Peltier, and it publishes
**no Redis stream** — its corr-file columns, dashboard tiles, threshold
bands, and health checks all disappear cleanly rather than streaming
`status="error"` forever. `installed: false` must be paired with
`enable: false` (rejected at init otherwise). Flip back to `true` when
the module returns.

**Hot-swap (LOAD connector fails in the field → move the LOAD module to
the LNA connector):**

1. Stop `panda_observe`. Peltier control replay stays owned by
   `pico-manager.service`, so the LOAD side stays controlled until you
   push new state.
2. In `tempctrl_manual`: disable LOAD (`O`), mark it uninstalled (`U`).
   Physically move the Peltier+thermistor module to the LNA connector.
   Mark LNA installed (`t`) and confirm the `tempctrl_lna` row comes
   alive with a sane `T_now` within a tick.
3. Edit `obs_config.yaml`:
   - `LOAD: {installed: false, enable: false}`
   - `LNA: {installed: true, enable: true}` plus a copy of the LOAD
     block's `target_C` / `hysteresis_C` / `clamp` / `cooling_enabled` /
     `Kp` / `Ki` — the physical module is the same, only the channel
     (pins + stream name) changed.
   - `calibration.t_load_stream: tempctrl_lna` — the Y-factor
     calibration's load-temperature reference now rides the
     LNA-connector stream.
4. One-time Redis cleanup so the retired `tempctrl_load` stream doesn't
   emit throttled staleness warnings on the ground side: delete its
   stream key (`stream:tempctrl_load`), its `metadata` hash fields
   (`tempctrl_load`, `tempctrl_load_ts`), and its `metadata_streams`
   set entry.
5. Restart `panda_observe`, and restart the live-status dashboard after
   updating **its** copy of `obs_config.yaml` too — signal gating reads
   the dashboard host's local file, not the panda's upload.
6. Verify with `pico_preflight` (the retired row reads "no stream
   (channel uninstalled or producer fault)") and the dashboard cal
   panel.

Note: after any tempctrl Pico reboot, firmware defaults to
`installed: true` for a few 200 ms ticks until `pico-manager` replays
the cached flags — a brief `status="error"` burst on the retired stream
is expected and harmless (schema-valid rows, at most one spurious
health-check warning).

## Orientation calibration recipe

Azimuth is pot-referenced (`potmon`); elevation is IMU-referenced
(`imu_el`, plus `imu_az`'s el-only `|θ|` as a cross-check). The
picohost-4.3 az descope retired accel-derived `imu_az` azimuth — at
level, azimuth rotation is rotation about gravity and unobservable to
an accelerometer (2026-07-08 field data: ~20x noise amplification for
a few degrees of tilt). See CLAUDE.md's "IMU mode" section for the
schema-level detail.

Three commands, run in order (all picohost CLI entry points, run
against the running `pico-manager`; step 3 is this repo's
`motor_manual`):

1. **`calibrate-pot --mode auto`** — in-box, motor-driven pot sweep.
   Defines az 0 = pot 0° (writes the slope/intercept to `PotCalStore`).
2. **`calibrate-imu`** — auto-driven single elevation sweep. Defines
   el 0 = the pose where the IMUs read most "down", derived from the
   sweep itself (no operator-eyeballed level needed). **The az
   turntable must be parked at az home during this sweep** — the
   `imu_az` `el_deg` section of the fit is gated on the pot reading
   within ~10° of the calibrated zero; `imu_el` is azimuth-invariant
   and calibrates regardless of az position.
3. **`motor_manual` → `h` + confirm** — drives the closed-loop
   `MotorHomer` onto the cal-defined home (pot 0° az / IMU-level el)
   and re-zeros the step counters there.

**Order matters**: run step 1 before step 2. `calibrate-imu`'s
`imu_az` section needs a calibrated, home-parked pot to gate against.
Running it before `calibrate-pot`, or with the turntable off home,
does **not** silently drop the `imu_az` section: the gate prints a
warning to stderr and prompts `Continue imu_el-only? [y / Enter to
abort]:`. The default (Enter) **aborts the entire calibration**,
including `imu_el`; only an explicit `y` continues with `imu_el`
alone, dropping the `imu_az` cross-check section from the saved fit.

**A stale motor zero is a warning, not a failure.** If step 2's
derived level sits more than ~10° from the motor's current zero
position, it logs a warning ("motor zero may be stale; home after
saving"), but the warning does not bypass or auto-trigger saving —
saving a calibration always goes through the same `Save this
calibration? [y/N]:` confirm, warning or not; the fit is independent
of the motor's step-counter zero. Run step 3 afterward to re-zero the
counters against the newly-saved cal-defined home; it is not a
prerequisite for steps 1–2 to succeed.

**Expect cross-check FLAG rows near el 0 and ±180°.** `imu_az`'s
`|θ|` estimator has an intrinsic near-pole floor (~10–20° with a
single-sweep cal, dominated by the along-axis accel-bias component a
single el sweep cannot observe) — `imu_el` is the el authority, and
`imu_az` is a cross-check/failover only.
