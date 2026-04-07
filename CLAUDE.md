# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EIGSEP Observing is control code for the EIGSEP radio astronomy experiment. It manages a distributed system with a ground control computer and a suspended LattePanda computer, communicating via Redis streams. The system controls a SNAP correlator, VNA (Vector Network Analyzer), environmental sensors (via Pico microcontrollers), and RF switch network for calibration.

## Development Commands

```bash
pip install -e ".[dev]"           # Install with dev dependencies
pytest                            # Run all tests (includes coverage)
pytest -k "test_name"             # Run specific test
pytest -x                         # Stop on first failure
ruff check .                      # Lint
ruff format --check .             # Check formatting (line length 79)
```

## Architecture

**Source layout**: `src/eigsep_observing/` with `src`-layout setuptools packaging.

### Core classes (all use Redis for communication):

- **EigsepRedis** (`eig_redis.py`) - Redis message bus wrapping `redis.Redis`. Manages streams: `stream:ctrl` (commands), `stream:status` (status updates), `stream:data:{sensor}` (sensor data). Large file (~1000 lines).
- **EigObserver** (`observer.py`) - Main orchestrator on the ground computer. Takes two Redis connections (`redis_snap` for SNAP correlator, `redis_panda` for LattePanda). Manages observation schedules, data collection, and file writing.
- **PandaClient** (`client.py`) - Runs on the suspended LattePanda. Pulls sensor data, pushes to Redis, listens for control commands. Manages Pico devices (IMU, thermometers, peltier, lidar, RF switch) via `picohost` library.
- **EigsepFpga** (`fpga.py`) - Extends `eigsep_corr.fpga.EigsepFpga` for SNAP FPGA/correlator interface.

### Testing architecture (`testing/` subpackage):

Each core class has a `Dummy*` counterpart (`DummyEigsepRedis`, `DummyPandaClient`, `DummyEigObserver`, `DummyEigsepFpga`) for hardware-free testing. Tests use these dummy classes instead of mocks. `DummyEigsepRedis` uses `fakeredis` instead of a real Redis server.

### Key dependencies:

- `eigsep_corr` - SNAP correlator library (config loading via `eigsep_corr.config.load_config()`)
- `cmt_vna` - VNA control library
- `picohost` - Pico microcontroller communication
- `fakeredis` - In-memory Redis for testing

### Configuration (`config/*.yaml`):

- `obs_config.yaml` - Observation parameters (switch schedules, VNA settings)
- `corr_config.yaml` / `corr_config_snap122.yaml` - SNAP correlator settings
- `dummy_config.yaml` / `test_config.yaml` - Hardware-free development configs

## Code Style

- Ruff with line length 79 (linting and formatting)
- Python 3.9+ compatibility

## Design Philosophy: Contract-Based and Defensive

`eigsep_observing` is the **architect** of the data pipeline. It consumes data from
producers (Picos, VNA, SNAP) but defines the contracts those producers must conform to
(see `SENSOR_SCHEMAS` in `io.py`). Producers should be fixed when they violate
contracts; do not silently absorb violations on the consumer side.

Two principles, in priority order:

1. **Corr data is sacred.** Under no circumstances should failures in any other data
   product (metadata, sensors, VNA, etc.) prevent corr data from being saved. Narrow
   safety nets around non-corr processing are acceptable *only if* they log loudly at
   ERROR level so the upstream contract violation is visible and actionable.
2. **Enforce contracts loudly, don't paper over them.** Schemas, validation, and
   warnings are the primary defense. Reserve `try/except` for narrow, specific
   safety nets — never as a way to make tests pass or hide producer bugs. If a
   contract is wrong, fix the schema; if a producer is wrong, fix the producer.

## Metadata flow: streaming for corr, snapshot for VNA

`EigsepRedis` exposes two metadata-fetching mechanisms with deliberately different
semantics. They look inconsistent, but the inconsistency is intentional and not
something to "fix" by unifying them.

- **`get_metadata()` — streaming, used by corr.** Drains all sensor readings since
  the last call from per-stream Redis streams (advances a position pointer). Used by
  `EigObserver.record_corr_data` per integration. Each integration is a sub-second
  window, and the corr loop averages all sensor readings within that window down to
  one entry per spectrum (via `io.avg_metadata`). The streaming path matches the corr
  cadence and gives cadence-correct averages. Because it advances a position pointer,
  only one consumer per `EigsepRedis` instance can call it per stream.

- **`get_live_metadata()` — snapshot, used by VNA.** Reads the latest values from
  the Redis metadata hash (no position pointer, no draining). Used by
  `PandaClient.execute_vna` when packaging a VNA measurement. A VNA reading is
  point-in-time, taken at ~1/hour cadence; the right semantic is "what was the
  latest sensor reading at the moment the VNA was triggered," not "average everything
  since the last VNA an hour ago." The VNA file header includes
  `metadata_snapshot_unix` so downstream can sanity-check the snapshot's recency.

**Do not unify these.** Using `get_metadata` for VNA would compete with the corr
loop for stream position (consumer race) and would average over an irrelevantly long
window. Using `get_live_metadata` for corr would lose the cadence-matched averaging
and would mix in stale readings. The two paths reflect different physical semantics
(integration window vs point-in-time) and run in different processes
(`EigObserver` on the ground PC vs `PandaClient` on the panda).

**Known weakness:** `get_live_metadata` has no freshness check. A dead sensor
silently returns its last reading. The `metadata_snapshot_unix` header field lets
downstream detect this at file inspection time, but a runtime warning would require
panda-side timestamping in `add_metadata` (no firmware change needed). Tracked
informally; not yet implemented.

## Metadata averaging: per-type reduction policy

When `EigObserver.record_corr_data` calls `get_metadata` it gets a list of raw
sensor dicts since the last call (one entry per producer push, ~5 per integration
at typical pico cadence). Those get reduced to one entry per integration via
`io.avg_metadata` → `_avg_sensor_values`. The reduction is **per-type**, derived
directly from `SENSOR_SCHEMAS`:

| schema type | reduction                                       | rationale |
|-------------|-------------------------------------------------|-----------|
| `float`     | `np.mean` over non-error survivors              | the actual averaging path; matches the integration's physical meaning |
| `int`       | `min` over non-error survivors                  | every int field today is either a constant (`app_id`, `watchdog_timeout_ms`) or a worst-case-wins quality level (`accel_cal`/`mag_cal` 0–3); `min` preserves the worst case |
| `bool`      | `any` over non-error survivors                  | bool fields are fault flags (`watchdog_tripped`); `any` preserves a fault that occurred mid-integration |
| `str`       | first value if unanimous, else `"UNKNOWN"`      | matches the rfswitch convention |

All four reductions filter samples whose own `status` is `"error"` — an errored
sample's data is junk. Schemas in `SENSOR_SCHEMAS` are therefore *load-bearing
for the output*, not just for input validation: if you change a field's schema
type, you change its reduction.

**Per-row fault flag.** Before the per-key loop, the integration's own `status`
field collapses to `"error"` if *any* raw sample errored. This gives downstream
a single per-integration "this row is suspect" signal instead of having to
inspect every numeric field for `None` to infer that errors happened. A row
with `status: "update"` is fully clean; a row with `status: "error"` had at
least one bad raw sample, and the data fields shown are the average (or min,
or any, etc.) of the survivors.

**Invariant fields.** A few fields should be physical constants for the
lifetime of a stream — `sensor_name`, `app_id`, `watchdog_timeout_ms`. If two
raw samples in a single integration disagree on one of these, that's a
producer-side bug (Pico misconfiguration, stream cross-talk, memory
corruption). It's logged at ERROR, throttled to once per 60s per (stream,
field) so a chronic bug doesn't drown the log file at the corr loop's ~4 Hz.
The reduction itself still produces a value (`min` for int, `"UNKNOWN"` for
str), so the file stays well-formed and the row doesn't get dropped.

**What is *not* logged.** Disagreement on non-invariant fields (cal levels,
fault flags, mode strings) is silent — the saved value already encodes the
disagreement (`min`/`any`/`"UNKNOWN"`) and downstream can detect it from the
file. Logging every cal-level wobble would generate ~14k events/hour during
normal operation.

**Two paths that bypass `_avg_sensor_values`:**

- `_avg_rfswitch_metadata` returns the bare `sw_state` int or `"UNKNOWN"`
  (not a dict). See the `RFSWITCH_TRANSITION_WINDOW_S` block in `io.py` for the
  additional forward-window flagging that fires on consecutive-sample switch
  state changes.
- `_avg_temp_metadata` first averages the top-level (non-prefixed) fields via
  `_avg_sensor_values`, then splits the `A_*`/`B_*` channel keys into
  `temp_mon_a`/`temp_mon_b` (etc.) sub-dicts and runs `_avg_sensor_values` on
  each. The split happens in `File.add_data`, not here, so the streams in the
  saved file are flat per-channel.

**Documented quirk:** the IMU's `calibrated` field is a `bool` event flag
(set briefly when a user-triggered BNO085 calibration completes), not a state.
With the `bool → any` reduction, an integration that contains the completion
event reports `True`, which is mildly weird semantically. It's scheduled for
removal in the next picohost PR; not worth special-casing.

## Testing philosophy: fixtures must match production data

Test fixtures should mirror the **shape, types, and cardinality** of real
production data. A fixture that diverges from what producers actually emit
is worse than useless — it hides contract drift (producers change, the
consumer adapts, the fixture stays static, the test keeps passing) and
silently normalizes bugs. Two rules:

1. **Fixtures look like real data.** When building a fixture for something
   a producer emits, trace one real end-to-end example and match it
   exactly — field names, nested dict shapes, numeric precision, integer
   vs float (the schemas in `SENSOR_SCHEMAS` are load-bearing and
   `avg_metadata` preserves them: int fields stay int, float fields are
   averaged), string vs bool, per-sample list length, `None` gap-fill
   entries, sentinel strings like `"UNKNOWN"`. If the real data has a
   nasty edge case (e.g. the producer emits a ragged dict, or a sensor
   dropout produces `None`), the fixture must include it, because the
   edge case is part of the contract the consumer has to handle.

2. **Deviations are called out and justified.** If a fixture can't match
   real data — because the real producer hasn't been written yet, because
   mimicking it would require an unreasonable amount of scaffolding, or
   because the test is deliberately exercising a boundary condition — say
   so in a comment at the fixture or at the call site, and explain *why*
   the deviation is acceptable for this specific test. Silent deviations
   are bugs in the test suite.

These rules apply doubly to "golden" fixtures shared across many tests
(e.g. `HEADER`, `CORR_METADATA`, `VNA_METADATA` in `tests/test_io.py`).
Shared fixtures amplify drift: one wrong value rots every test that
touches it. Related values should be derived from a single source of
truth — e.g. `FILE_TIME = NTIMES * INTEGRATION_TIME` rather than two
independently-set numbers that can drift apart.

When the consumer of a fixture is itself a pipeline (producer → averager
→ writer → reader), the preferred shape of test is an **end-to-end
round-trip**: feed the pipeline raw-producer-shaped input, let it run,
and assert the output matches the fixture. See
`test_metadata_end_to_end_round_trip` in `tests/test_io.py` for the
canonical pattern — it's the guard rail that ties the raw stream format,
`avg_metadata`, `File._insert_sample`, `write_hdf5`, and `read_hdf5`
together into one contract.
