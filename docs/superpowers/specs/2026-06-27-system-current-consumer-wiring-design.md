# System-current consumer wiring — design

**Date:** 2026-06-27
**Status:** Approved design, pending implementation plan
**Repo:** `eigsep_observing` (consumer side)
**Producer:** `pico-firmware` branch `feat/system-current-monitor` (not yet
merged) + the matching `picohost` release (not yet cut).

## Goal

Wire the new whole-system current measurement — published by the producer as a
clean `system_current` metadata stream — into the three consumer surfaces it
needs to reach:

1. **Sensor schema + corr-file writes** so every corr integration's metadata
   sidecar carries the averaged system current.
2. **The live-status dashboard** so an operator sees the current at a glance and
   in detail, colored against a healthy/danger band.
3. **`watch_sensors`** so the passive terminal monitor shows and plots it.

The current-based **safety / auto-shutdown** behaviour (shed power-hungry
sensors above a danger threshold; global pause on a sustained high reading) is
**explicitly out of scope** here and gets its own spec once 1–3 are verified on
hardware. See *Out of scope* below.

## Producer contract (fixed upstream — we conform, we do not define)

The ACS724 current sensor piggybacks on the **lidar Pico** (ADC2/GP28). The
host-side `PicoLidar._lidar_redis_handler` fans the merged firmware line into
**two** metadata publishes:

- `metadata['lidar']` — the existing distance reading, with `current_voltage`
  **stripped** (so the existing `lidar` schema is unchanged in *content*, but
  its emulator-derived test fixture changes — see §2).
- `metadata['system_current']` — a **new** stream:

  | field            | type  | notes                                            |
  |------------------|-------|--------------------------------------------------|
  | `sensor_name`    | str   | `"system_current"`                               |
  | `status`         | str   | hard-set `"update"` (decoupled from lidar I2C)   |
  | `current_voltage`| float | raw ADC-pin voltage (diagnostic)                 |
  | `current_a`      | float | derived amps (the meaningful value)              |

  No `app_id` — like `adc_stats`, this is a fanned-out stream, not a 1:1 Pico
  app. Reference: `pico-firmware/docs/superpowers/specs/2026-06-24-system-current-monitor-design.md`
  and `picohost.base.PicoLidar`.

## Components

### 1. Sensor schema + corr-file writes (`src/eigsep_observing/io.py`)

Add one entry to `SENSOR_SCHEMAS`, mirroring the producer exactly:

```python
"system_current": {
    "sensor_name": str,
    "status": str,
    "current_voltage": float,
    "current_a": float,
},
```

No other `io.py` change is required. `avg_metadata` dispatches on the
producer-emitted `sensor_name`, validates against this schema, and runs both
floats through the generic `_avg_sensor_values` float→mean reduction. With no
`app_id` and no extra invariant fields beyond `sensor_name`, it behaves exactly
like `adc_stats` on the reduction path. Because the producer hard-sets
`status:"update"`, rows never collapse to `"error"` from this stream.

This is the entire "wire into file writes" task: once the stream is in
`SENSOR_SCHEMAS`, `EigObserver.record_corr_data` → `metadata_stream.drain` →
`avg_metadata` already carries it into the per-integration metadata sidecar.

### 2. Producer-contract test (`src/eigsep_observing/contract_tests/test_producer_contracts.py`)

Two changes, both driven by the producer composing lidar + current into one
firmware line that the handler splits:

- **New helper `_lidar_post_handler_readings()`** — compose
  `LidarEmulator().get_status()` through `PicoLidar._lidar_redis_handler`
  (bypassing `__init__` via `__new__`, capturing *both* fan-out publishes in
  order), returning `(lidar_dict, system_current_dict)`. Same pattern as the
  existing `_rfswitch_post_handler_reading` / `_potmon_post_handler_reading`.
  `PicoLidar`'s conversion constants are class attributes, so `_v_to_current`
  works on a `__new__`-constructed instance.

- **Fix the existing `lidar` registration.** `SENSOR_EMULATORS["lidar"]` is
  currently `lambda: LidarEmulator().get_status()`. After the picohost bump the
  emulator's raw `get_status()` gains `current_voltage`, which the unchanged
  `lidar` schema rejects as an extra key — so the existing contract test would
  fail. Re-point `lidar` at the post-handler lidar dict (distance only, current
  stripped), and register `"system_current"` at the second fan-out dict.

`test_every_schema_has_conforming_emulator` then covers `system_current`
automatically: it parametrizes over `SENSOR_SCHEMAS` keys, asserts a registered
emulator exists, asserts `sensor_name == "system_current"`, and validates the
reading against the schema.

### 3. Live-status dashboard

`/api/metadata` already projects any stream present in the panda snapshot hash
(`_metadata_payload` iterates `state.metadata_snapshot`), so **no `app.py` or
`aggregator.py` change** is needed for the data to reach the front-end — it
arrives the moment the producer publishes. The remaining work is the signal,
the band, and the (not-data-driven) front-end tiles.

- **Signal** (`live_status/signals.py`): register `system_current.current_a`
  (`description="System current"`, `unit="A"`, `max_age_s=30.0`,
  `enabled_by=None` — a system-wide vital, never gated). `current_voltage` is
  **not** a separate signal; it is shown on the card as a raw diagnostic in
  parentheses, exactly as potmon shows `pot_az_voltage` alongside the
  `pot_az_angle` signal.

- **Threshold** (`config/live_status_thresholds.yaml`): a real band —
  `healthy: [0.0, 5.0]`, `danger: [0.0, 8.0]` (amps). Nominal only: panda + both
  Peltiers ≈ 3 A, ACS724-10AB saturates ≈ 12.5 A. To be tuned against a measured
  baseline; tunable in this YAML with no code change. (Confirmed acceptable as a
  nominal at design time; exact numbers locked later.)

- **Front-end** (`live_status/templates/index.html`,
  `live_status/static/js/dashboard.js`) — the dashboard's metadata tiles are
  rendered by explicit per-sensor functions, not a loop, so a new stream needs:

  1. **Glanceable header tile.** Add `<span class="tile" id="tile-system-current">`
     to the `#health-summary` row in the header (alongside `tile-corr-loop`,
     `tile-panda-observe`, …). This is the "easily visible at a glance" element
     the operator asked for — colored by the `current_a` classify, showing e.g.
     `Current: 3.2 A`.
  2. **Detail card.** Add a dedicated `<section class="card"><h2>System current</h2>
     <div id="system-current-block"></div></section>` as the **first card** in
     `<main class="grid">` (immediately under the header, before *Correlation
     spectra*) so the detail is prominent, not buried at the bottom with
     lidar/potmon.
  3. **Renderer `renderSystemCurrent(meta)`** — populate the card (status header
     via `makePaneStatusHeader("system current", entry)`; `current_a` as the
     primary value in A, `current_voltage` in parens as the diagnostic) **and**
     set the header tile's text + color from `entry.classify["system_current.current_a"]`
     and `entry.value.current_a`. Both updates live in this one function because
     the value comes from `/api/metadata` (not `/api/health`), so the header
     tile is fed from metadata rather than `updateHealth`.
  4. **Call it in `tick()`** next to `renderLidar(metadata.data)`.

  Rationale for the dedicated card over folding into the Lidar card: although
  they share a Pico, "lidar" is an implementation detail the producer
  deliberately hides — the user-facing surface must read as *system current*,
  and a system-wide vital deserves top-of-grid placement, not a sub-line of a
  range sensor.

### 4. `watch_sensors` (`scripts/watch_sensors.py`)

**Automatic** for the text display: `_PANDA_STREAMS = [s for s in SENSOR_SCHEMAS
if s != "adc_stats"]`, and `system_current` is a panda stream (only `adc_stats`,
which is SNAP-side, is excluded). It appears in the default `--streams` set and
the in-place table with no change.

One line of polish: add `_PLOT_FIELDS["system_current"] = ("current_a",)` so
`--plot` traces amps only; without it the fallback would also plot the raw ADC
`current_voltage`, which is not operator-meaningful.

### 5. Dependency / sequencing

- Install the local producer for testing with **uv** (the repo's package
  manager — `uv.lock` is the source of truth):
  `uv pip install -e ../pico-firmware/picohost` from the
  `feat/system-current-monitor` branch into `.venv`, so the new + fixed
  contract tests run green locally before the picohost release exists. This is
  a local-only dev install (not committed); do not add a `[tool.uv.sources]`
  local-path pin to `pyproject.toml`, which would leak the dev path into the PR.
- Bump `pyproject.toml`: `picohost>=3.8.0` → the release that ships the current
  monitor (expected `>=3.9.0`; pin to the actual version once cut). Update
  `uv.lock` to match.
- **Hold PR merge** until that picohost release is published — an external
  blocker, which the self-contained-PR rule explicitly permits. The branch is
  otherwise complete and green against the local editable install.

## Testing

Targeted tests only (CI runs the full suite):

- **Contract** (`test_producer_contracts.py`): `lidar` post-handler reading
  validates against the unchanged `lidar` schema with `current_voltage`
  stripped; `system_current` post-handler reading validates against the new
  schema; `test_every_schema_has_conforming_emulator` is green for the new key.
- **Averaging / round-trip** (`tests/test_io.py`): a `system_current` case
  through `avg_metadata` (float→mean over multiple raw samples; `status:"update"`
  row stays clean) and, if it fits the existing end-to-end metadata round-trip
  fixture, through `write_hdf5` → `read_hdf5`.
- **Live status**: `system_current.current_a` is in the signal registry and
  enabled regardless of config flags; `Thresholds.classify` returns `ok`/`warn`/
  `danger` at the band edges from the YAML; `_metadata_payload` emits a
  `classify` entry for `system_current.current_a` when the stream is in the
  snapshot.
- **watch_sensors**: `_plot_fields_for("system_current") == ["current_a"]`.

## Out of scope (future spec)

Current-triggered safety, to be designed separately after 1–3 are verified on
hardware:

- In `panda_observe` (or a dedicated background watcher), shed power-hungry,
  run-away-prone subsystems (tempctrl, motor) when `current_a` crosses a danger
  threshold.
- If current stays high after shedding (suspected short / voltage hazard),
  globally pause and surface loudly on the live-status dashboard (field
  deployment has no wifi — local dashboard is the only alerting channel).

These need their own decisions (threshold hysteresis, who owns the kill switch,
how it interacts with `run_tag` / manual sessions, firmware-vs-host
responsibility) and are not built here.
