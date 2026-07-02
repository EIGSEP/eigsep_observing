# RF switch EEPROM paths + PCB thermistors — consumer-side design

**Date:** 2026-07-02
**Repos:** `eigsep_observing` (primary), `pico-firmware` branch `feat/rfswitch-eeprom-paths` (coordinated producer change)
**Status:** approved design, pending plan

## Motivation

The `pico-firmware` branch `feat/rfswitch-eeprom-paths` reworks the RF switch
Pico in two ways that reach `eigsep_observing`:

1. **EEPROM path addressing.** `PicoRFSwitch.path_str` (name → binary GPIO
   string) and the `rbin()` helper are gone, replaced by `PicoRFSwitch.PATHS`
   (name → EEPROM address int, `0x00`–`0x0F`). `sw_state` now means "EEPROM
   path address" instead of "raw 8-bit GPIO bitmask." 10 legacy path names are
   retained; 6 new ones are added (`VNAAMB`, `VNASP1`, `VNASP2`, `RFAMB`,
   `RFSP1`, `RFSP2`).
2. **Three PCB thermistors** on the RF switch board (ADC0–2 / GP26–28),
   reported as raw averaged pin voltages `volt_therm0/1/2` (volts). Not yet
   calibrated — voltage→temperature conversion is deferred to host-side once
   the divider + Steinhart–Hart constants are measured. Firmware currently
   emits them on the rfswitch status line.

Goal: reflect both on the consumer side with the **smallest public-API change
on the `eigsep_observing` side**, surfacing the thermistors in `watch_sensors`
and the live-status dashboard.

## Decision summary

- **Item 2 (PATHS rename):** follow the firmware rename mechanically. No
  firmware conform needed; `eigsep_observing` already sources state names from
  the firmware class.
- **Item 1 (thermistors):** **make firmware conform** — fan the thermistors
  into a **separate `rfswitch_therm` metadata stream** (the established
  `system_current` fan-out pattern). This keeps the consumer change purely
  additive and keeps the rfswitch stream a pure categorical (switch-state)
  stream. Surface the three temps **folded into the existing rfswitch pane** on
  the dashboard.

Both decisions confirmed with the operator on 2026-07-02.

---

## Item 2 — EEPROM path rename (eigsep_observing only)

`PicoRFSwitch.path_str` / `rbin()` are removed upstream. Three call sites break
at import with `AttributeError`:

| File:line | Current | Change |
|-----------|---------|--------|
| `src/eigsep_observing/client.py:26` | `VALID_SWITCH_STATES = set(PicoRFSwitch.path_str)` | `set(PicoRFSwitch.PATHS)` |
| `scripts/rfswitch_manual.py:37` | `STATES = list(PicoRFSwitch.path_str)` | `list(PicoRFSwitch.PATHS)` |
| `tests/test_io.py:1336, 3070` | `PicoRFSwitch.rbin(PicoRFSwitch.path_str[name])` | `PicoRFSwitch.PATHS[name]` |

Notes:

- `client.py:26` is the load-bearing break: it runs at import and takes all of
  `PandaClient` down. Fixing it restores switching end-to-end.
- The `sw_state` *numeric* semantics change (bitmask → address) is **inert** on
  the consumer side — a repo-wide sweep confirms nothing reads the raw
  `sw_state` int; only `sw_state_name` (the string) is consumed
  (`client._read_switch_mode_from_redis`, `io._avg_rfswitch_metadata`,
  `aggregator` rfswitch-transition latch). The test-fixture `sw_state` values
  change automatically because they are derived from the paths dict.
- No path names were removed, so nothing referencing a specific state name
  breaks; the 6 new names simply widen `VALID_SWITCH_STATES` and the
  `rfswitch_manual` menu.
- **Opportunistic fix:** `client.py:285` docstring example `sw("RFLOAD")`
  references a state that has never existed in `PATHS`. Correct it to a real
  path (e.g. `RFAMB`, the LNA→Amb/Hot-Load path) while touching the file.

After these swaps, `rfswitch_manual.py` and the production switching path work
unchanged.

---

## Item 1 — thermistors as a separate `rfswitch_therm` stream

### Why not ride on the rfswitch stream

The rfswitch stream has a **bespoke** consumer: `io.avg_metadata` dispatches
`rfswitch` to `_avg_rfswitch_metadata`, which returns only the switch-state
**name string**, discarding every numeric field. Consequences of leaving
`volt_therm` on the rfswitch line:

- Thermistors never reach the corr HDF5 file (dropped by the string reducer).
- `io._validate_metadata` flags **extra keys** → "extra keys" WARNING at the
  corr loop's ~4 Hz drain, and the producer-contract test fails.
- Carrying them would force `_avg_rfswitch_metadata` to return a dict — a
  **breaking** change to the rfswitch-as-string contract (corr file field,
  `read_hdf5`, `EigObserver` transition-window logic, `File._insert_sample`,
  and downstream analysis).

A separate stream avoids all of this and mirrors how `system_current` is fanned
out of the lidar line and how `tempctrl_lna` / `tempctrl_load` are per-channel
streams.

### Producer contract (pico-firmware)

In `picohost.base.PicoRFSwitch._rfswitch_redis_handler`, after adding
`sw_state_name`, **pop** `volt_therm0/1/2` from the rfswitch payload and
re-publish them as a second metadata entry — exactly the two-publish shape of
`PicoLidar._lidar_redis_handler`:

```python
# rfswitch entry (unchanged shape): {sensor_name: "rfswitch", status,
#   app_id, sw_state, sw_state_name}
# fanned entry:
{
    "sensor_name": "rfswitch_therm",
    "status": "update",
    "volt_therm0": float,
    "volt_therm1": float,
    "volt_therm2": float,
}
```

- The C firmware / `RFSwitchEmulator.get_status()` still emit `volt_therm` on
  the raw status line — no emulator change (mirrors lidar, whose emulator emits
  `current_voltage` merged and the handler splits it).
- `rfswitch_therm` carries **no `app_id`**, matching `system_current` (a
  fanned/derived stream whose sensor key is not the pico's canonical app name).
- picohost's rfswitch redis-handler test updates to expect **two** publishes
  and assert the rfswitch line no longer carries `volt_therm`.

This firmware change and the `eigsep_observing` schema change **must land
together** — the `eigsep_observing` producer-contract test composes the locally
installed picohost emulator + handler, so a mismatch fails CI.

### Consumer changes (eigsep_observing)

**A. Schema — `src/eigsep_observing/io.py` (`SENSOR_SCHEMAS`, ~line 861).**
Add, mirroring `system_current`:

```python
"rfswitch_therm": {
    "sensor_name": str,
    "status": str,
    "volt_therm0": float,
    "volt_therm1": float,
    "volt_therm2": float,
},
```

Flows through the generic `_avg_sensor_values` (float→mean) path — no
`_avg_rfswitch_metadata` change — so the three voltages land in the corr file
automatically (desirable: PCB temperature affects the calibration network).
`None`-when-a-read-fails is handled by the existing None short-circuit.

**B. Producer-contract test —
`src/eigsep_observing/contract_tests/test_producer_contracts.py`.**
Refactor `_rfswitch_post_handler_reading` into a two-publish
`_rfswitch_post_handler_readings()` (capture a *list*, like
`_lidar_post_handler_readings`), then register both in `SENSOR_EMULATORS`
(~line 252):

```python
"rfswitch":       lambda: _rfswitch_post_handler_readings()[0],
"rfswitch_therm": lambda: _rfswitch_post_handler_readings()[1],
```

This satisfies `test_every_schema_has_conforming_emulator`, which parametrizes
over `SENSOR_SCHEMAS` and fails CI for any schema key lacking an emulator.

**C. `watch_sensors` — no code change.** `_PANDA_STREAMS` derives from
`SENSOR_SCHEMAS`, `_render` prints all fields, and `--plot` traces the three
`volt_therm*` floats via the all-float fallback in `_plot_fields_for`.
(Optional: add `"rfswitch_therm": ("volt_therm0","volt_therm1","volt_therm2")`
to `_PLOT_FIELDS` for an explicit label — not required.)

**D. Live-status dashboard — fold into the rfswitch pane.**

Backend is already generic (`_metadata_payload` at `app.py:326` iterates the
snapshot hash and matches registered signals by dotted domain; `/api/metadata`
serves it). No `app.py`, `aggregator.py`, or `thresholds.py` change.

1. `src/eigsep_observing/live_status/signals.py` (`SIGNAL_REGISTRY`): register
   three signals `rfswitch_therm.volt_therm0/1/2`, `enabled_by=None`,
   `unit="V"`, `max_age_s≈30`. With no threshold band (uncalibrated) they
   classify as `"unknown"` → grey info tiles, exactly like uncalibrated potmon.
   No YAML entry required.
2. `static/js/dashboard.js`: extend `renderRfswitch(rf, metaEntry)` to accept
   the `rfswitch_therm` metadata entry and append three rows (value + classify
   tile) below the existing `state` / `next change` rows. Update the call site
   `dashboard.js:1013` to pass `metadata.data["rfswitch_therm"]`.
3. `templates/index.html`: no new `<section>` — reuse the existing `#rfswitch`
   pane. CSS already provides `.tile.unknown` / `.metadata-row`; no change.

**E. Tests (additive, nothing existing breaks).** Mirror the `system_current`
cases: a `signals`/`classify → "unknown"` test in
`tests/test_live_status_thresholds.py` and a `_metadata_payload` test in
`tests/test_live_status_app.py`. Update the two `test_io.py` rfswitch fixtures
per Item 2; add an end-to-end averaging assertion for `rfswitch_therm` if
convenient (round-trip pattern in `test_io.py`).

### Forward compatibility

When the thermistors are calibrated, firmware adds derived `temp_c0/1/2` (and
optionally cal scalars) to the `rfswitch_therm` publish, the schema grows
additively (floats, `None` when uncalibrated — the potmon/system_current
precedent), the dashboard rows switch to showing °C, and a healthy/danger band
is added in `live_status_thresholds.yaml`. No structural change.

---

## Scope / non-goals

- No change to `_avg_rfswitch_metadata` or the rfswitch-as-string contract.
- No change to the corr data path, VNA path, or any other sensor stream.
- The picohost fan-out change lives in `pico-firmware`; this repo consumes it.
  It is a hard prerequisite for thermistor data to appear and must land in
  lockstep with the schema change to keep the contract test green.

## Files touched (eigsep_observing)

- `src/eigsep_observing/client.py` (rename + docstring)
- `scripts/rfswitch_manual.py` (rename)
- `src/eigsep_observing/io.py` (`rfswitch_therm` schema)
- `src/eigsep_observing/contract_tests/test_producer_contracts.py` (two-publish
  fixture + registry)
- `src/eigsep_observing/live_status/signals.py` (3 signals)
- `src/eigsep_observing/live_status/static/js/dashboard.js` (rfswitch pane +
  call site)
- `tests/test_io.py` (fixtures), `tests/test_live_status_*.py` (additive)

## Coordinated files (pico-firmware, `feat/rfswitch-eeprom-paths`)

- `picohost/src/picohost/base.py` (`_rfswitch_redis_handler` fan-out)
- `picohost/tests/…` (rfswitch redis-handler two-publish assertions)
