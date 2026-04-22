# EIGSEP Observing Operations Guide

## Quick Reference Card

```
NETWORK
  Ground computer  ──── LattePanda (10.10.10.11, Redis 6379)
                   ──── RPi + SNAP  (10.10.10.10, Redis 6379)

STARTUP ORDER
  1. SSH into panda:     ssh eigsep@10.10.10.11
  2. Start panda client: python scripts/panda_observe.py
  3. On ground RPi:  python scripts/observe.py

STOP
  Ctrl-C on either script. Threads shut down gracefully.

CONFIG
  Panda owns the observing config (switch schedule, VNA, motor).
  Edit obs_config.yaml on the panda, then restart panda_observe.py
  — it uploads the yaml to Redis on launch. The observer reads the
  observing config from Redis; its only CLI knobs are host-local
  (IPs, save dir, ntimes).

LOGS
  Rotating log file: eigsep.log (10 MB, 5 backups)
```

### Common commands

```bash
# Normal startup
python scripts/panda_observe.py          # on panda
python scripts/observe.py                # on ground

# Observer only (no panda connection, SNAP correlator only)
python scripts/observe.py --no-panda

# Panda only (no SNAP)
python scripts/observe.py --no-snap

# Dry run with fake hardware
python scripts/panda_observe.py --dummy  # terminal 1
python scripts/observe.py --dummy        # terminal 2

# Custom observing config (switch schedule / VNA / motor) — panda side
python scripts/panda_observe.py --cfg_file /path/to/my_config.yaml

# Override host-local knobs — observer side (no yaml, CLI flags only)
python scripts/observe.py --rpi-ip 10.0.0.5 --panda-ip 10.0.0.6 \
    --corr-save-dir /mnt/data --corr-ntimes 120
```

### If something crashes

| What crashed | What to do |
|---|---|
| Observer only | Restart `observe.py`. Panda keeps running. You may lose some data that overflowed the Redis stream buffer. |
| Panda only | SSH into panda, restart `panda_observe.py`. It reads config from Redis automatically. Observer keeps writing SNAP data (without metadata) until panda reconnects. |
| Both | Start panda first, then observer. |
| Redis | Everything stops. Restart Redis, then panda, then observer. |

### Changing configuration

1. SSH into panda
2. Edit `obs_config.yaml` (or pass `--cfg_file` at launch)
3. Ctrl-C on `panda_observe.py`
4. Restart `panda_observe.py` (reads new yaml, uploads to Redis)
5. Optionally restart `observe.py` (it will read the new config from Redis
   for VNA save dir and `use_vna` gating; host-local knobs stay as CLI flags)

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
                    │    panda_observe.py      │
                    │  ┌───────────────────┐   │
                    │  │   PandaClient     │   │
                    │  │                   │   │
                    │  │  heartbeat_thd ────────── sets heartbeat:client
                    │  │  switch_loop   ────────── cycles RFANT/RFNOFF/RFNON
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

panda_observe.py                       observe.py
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
    ┌──────────────────┐               ├─ save current switch state
    │ RFANT  (sky)     │ 3600s         ├─ measure_s11("ant")
    │  lock → switch   │               │   ├─ set VNA power for antenna
    │  unlock          │               │   ├─ OSL calibration (O, S, L)
    │  wait 3600s      │               │   ├─ measure antenna + noise + load
    │  (VNA can        │               │   ├─ get live metadata from Redis
    │   interrupt here)│               │   └─ write to stream:vna
    ├──────────────────┤               ├─ measure_s11("rec")
    │ RFNOFF (load)    │ 60s           │   ├─ set VNA power for receiver
    │  lock → switch   │               │   ├─ OSL calibration
    │  wait with lock  │               │   ├─ measure receiver
    │  (VNA blocked)   │               │   └─ write to stream:vna
    ├──────────────────┤               ├─ restore previous switch state
    │ RFNON  (noise)   │ 60s           release switch_lock
    │  lock → switch   │               wait vna_interval (3600s)
    │  wait with lock  │
    │  (VNA blocked)   │
    └──────────────────┘

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
  │ read at panda_observe.py startup, uploaded to Redis
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
  2. Restart panda_observe.py
     └─ uploads YAML to Redis, discovers picos, re-uploads with pico info
  3. Restart observe.py if host-local knobs changed
     (--corr-save-dir, --corr-ntimes, --rpi-ip, --panda-ip)
     └─ observer reads the observing config from Redis automatically
```

### Network Addresses

Panda-side defaults live in `obs_config.yaml`. Ground-side defaults are
baked into `observe.py` as CLI flag defaults (`--rpi-ip`, `--panda-ip`).
Actual IPs may differ per deployment — pass them explicitly if so.

```
Ground computer:  user machine (runs observe.py)
RPi + SNAP:       10.10.10.10 (Redis port 6379)
LattePanda:       10.10.10.11 (Redis port 6379)
VNA:              127.0.0.1:5025 (local to panda)
```
