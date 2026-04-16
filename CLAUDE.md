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

- **EigsepRedis** (`eigsep_redis/eig_redis.py`) - Thin composition object over a shared `Transport`. Exposes per-bus writer/reader attributes, not a god-class of methods. Surfaces: `metadata` (writer), `metadata_snapshot` / `metadata_stream` (readers), `status` (writer), `status_reader`, `heartbeat` (writer), `heartbeat_reader`, `config` (store). Picohost and other external producers consume this; it lives in the shared `eigsep_redis` package so everyone sees the same wire format.
- **EigsepObsRedis** (`eigsep_observing/eig_redis.py`) - Observer-side subclass. Adds `corr_config` (store — config + header), `corr` (writer), `corr_reader`, `vna` (writer), `vna_reader`. The observer-specific classes live in `corr.py` and `vna.py`.
- **EigObserver** (`observer.py`) - Main orchestrator on the ground computer. Takes two Redis connections (`redis_snap` for SNAP correlator, `redis_panda` for LattePanda). Manages observation schedules, data collection, and file writing.
- **PandaClient** (`client.py`) - Runs on the suspended LattePanda. Pulls sensor data, pushes to Redis, listens for control commands. Manages Pico devices (IMU, thermometers, peltier, lidar, RF switch) via `picohost` library.
- **EigsepFpga** (`fpga.py`) - SNAP FPGA/correlator driver. Owns the register blocks (`blocks.py`), the `.fpg` bitstream (`data/`), and the corr-bus publication path (`redis.corr.add`, `redis.corr_config.upload_header`). Was historically a subclass of `eigsep_corr.fpga.EigsepFpga`; the two were merged in-tree when `eigsep_corr` was archived.

### Per-bus class split (why `EigsepRedis` is not a god-class)

Writer and reader classes are separated per bus so that **wrong-stream writes are structurally impossible**, not runtime-checked. `MetadataWriter` has no method that accepts a VNA payload; `CorrWriter` has no method that accepts a metadata payload; etc. Structural-impossibility guards in `tests/test_redis.py` enforce this at test time. Motivated by a real VNA→metadata leak bug caught in PR review (fixed in `b8cc1ed` / `ba42f1f`, then made structural in this refactor).

`EigsepRedis.add_metadata` remains as a one-line shim with `DeprecationWarning`, narrowly for picohost (`pico-firmware/picohost/src/picohost/base.py:65` until the monorepo merge). In-tree code must use `redis.metadata.add(...)` directly.

### Testing architecture (`testing/` subpackage):

Each core class has a `Dummy*` counterpart (`DummyEigsepRedis`, `DummyPandaClient`, `DummyEigObserver`, `DummyEigsepFpga`) for hardware-free testing. Tests use these dummy classes instead of mocks. `DummyEigsepRedis` uses `fakeredis` instead of a real Redis server.

### Key dependencies:

- `casperfpga` - SNAP board driver (lazy optional import; only required on the ground computer that actually talks to the FPGA)
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

Two reader classes expose metadata with deliberately different semantics. They
look inconsistent, but the inconsistency is intentional and not something to
"fix" by unifying them.

- **`MetadataStreamReader.drain()` — streaming, used by corr.** Drains all
  sensor readings since the last call from per-stream Redis streams (advances a
  position pointer). Used by `EigObserver.record_corr_data` per integration.
  Each integration is a sub-second window, and the corr loop averages all sensor
  readings within that window down to one entry per spectrum (via
  `io.avg_metadata`). The streaming path matches the corr cadence and gives
  cadence-correct averages. Because it advances a position pointer, only one
  consumer per `Transport` can call it per stream.

- **`MetadataSnapshotReader.get()` — snapshot, used by VNA.** Reads the latest
  values from the Redis metadata hash (no position pointer, no draining). Used
  by `PandaClient.execute_vna` when packaging a VNA measurement. A VNA reading
  is point-in-time, taken at ~1/hour cadence; the right semantic is "what was
  the latest sensor reading at the moment the VNA was triggered," not "average
  everything since the last VNA an hour ago." The VNA file header includes
  `metadata_snapshot_unix` so downstream can sanity-check the snapshot's
  recency.

**Do not unify these.** Using the stream reader for VNA would compete with the
corr loop for stream position (consumer race) and would average over an
irrelevantly long window. Using the snapshot reader for corr would lose the
cadence-matched averaging and would mix in stale readings. The two paths
reflect different physical semantics (integration window vs point-in-time) and
run in different processes (`EigObserver` on the ground PC vs `PandaClient` on
the panda).

**Known weakness:** the snapshot reader has no freshness check. A dead sensor
silently returns its last reading. The `metadata_snapshot_unix` header field
lets downstream detect this at file inspection time, but a runtime warning
would require panda-side timestamping in `MetadataWriter.add` (no firmware
change needed). Tracked informally; not yet implemented.

## corr `sync_time` lives on the corr header, not metadata

`sync_time` is a per-sync invariant (set once when the SNAP is synchronized),
not a per-integration sensor reading. It rides on the corr header
(`redis.corr_config.get_header()["sync_time"]`), published on every
state-changing call in `EigsepFpga` — `initialize`, `synchronize`,
`set_pam_atten` / `set_pam_atten_all`, `set_pol_delay`. There is no periodic
heartbeat re-upload; the header persists in Redis until overwritten by the
next state change. Every `CorrConfigStore.upload_header` call also stamps a
`header_upload_unix` field so file headers record when the producer last
re-published — offline you can check consistency between `sync_time` and
`header_upload_unix` to detect a SNAP that was reconfigured without
re-publishing. `EigObserver.record_corr_data` caches `sync_time` from the
header at file-start; a mid-file re-sync is an edge case that warrants a
new file anyway. Historically `sync_time` was pushed through `add_metadata`
and fetched inline by `read_corr_data` — that was the last cross-bus read
inside a reader, removed in this refactor.

## Metadata averaging: per-type reduction policy

When `EigObserver.record_corr_data` calls `redis.metadata_stream.drain` it gets a list of raw
sensor dicts since the last call (one entry per producer push, ~5 per integration
at typical pico cadence). Those get reduced to one entry per integration via
`io.avg_metadata` → `_avg_sensor_values`. The reduction is **per-type**, derived
directly from `SENSOR_SCHEMAS`:

| schema type | reduction                                       | rationale |
|-------------|-------------------------------------------------|-----------|
| `float`     | `np.mean` over non-error survivors              | the actual averaging path; matches the integration's physical meaning |
| `int`       | `min` over non-error survivors                  | every int field today is an invariant constant (`app_id`, `watchdog_timeout_ms`) — `min` is a no-op on agreement, and a disagreement is caught by the throttled invariant ERROR log path |
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
  `_avg_sensor_values`, then splits the `LNA_*`/`LOAD_*` channel keys into
  `tempctrl_lna`/`tempctrl_load` sub-dicts and runs `_avg_sensor_values` on
  each. The split happens in `File.add_data`, not here, so the streams in the
  saved file are flat per-channel. Only `tempctrl` exercises this helper —
  the standalone `temp_mon` Pico app was retired in picohost 1.0.0.

**IMU mode (picohost 1.0.0).** The two IMU picos (`imu_el` panda elevation,
app_id 3; `imu_az` antenna azimuth, app_id 6) emit BNO085 UART RVC payloads:
just `yaw`/`pitch`/`roll` (degrees) and `accel_x/y/z` (m/s²). No quaternion,
linear-accel, gyro, mag, or calibration-level fields. Both share the
`_IMU_SCHEMA` body in `io.py`.

**`potmon` producer-contract quirk.** The `potmon` schema is enforced
against the **post-`_pot_redis_handler`** shape, not the raw
`PotMonEmulator.get_status()` output. The emulator only emits voltages;
the slope/intercept and derived angle fields are added by
`PicoPotentiometer._pot_redis_handler` at Redis-publish time. The
producer-contract test (`tests/test_producer_contracts.py`) composes the
two by bypassing `PicoPotentiometer.__init__` and calling
`_pot_redis_handler` directly with a calibrated cal dict — see
`_potmon_post_handler_reading` there. The picohost scalar-only contract
(documented on `picohost.base.redis_handler`) means every field is
scalar, including the cal slope/intercept which were flattened from the
old `[m, b]` list shape.

## File I/O: write/read symmetry

`write_hdf5` and `read_hdf5` are a matched pair. Any transformation
applied when writing must be inverted when reading so that the consumer
API is stable regardless of the on-disk representation. Concretely:

- **Corr data is stored as int32.** `reshape_data(avg_even_odd=True)`
  averages even/odd spectra via banker's rounding (`np.rint`) and
  returns int32 arrays. Auto-correlations are `(ntimes, nchan)` int32.
  Cross-correlations are `(ntimes, nchan, 2)` int32, where `[..., 0]`
  is real and `[..., 1]` is imaginary.
- **`read_hdf5` reconstructs complex crosses.** On read, 3-D integer
  datasets whose last axis is 2 are converted back to complex128 via
  `arr[..., 0] + 1j * arr[..., 1]`. Old files that already store
  complex128 are returned as-is. This keeps the consumer-facing API
  (complex arrays for crosses) unchanged.
- **VNA / S11 data is unaffected.** It is natively complex128 from the
  VNA and is stored and read as complex128.

If you change the on-disk format in the future, update `read_hdf5` to
invert the transformation so that callers never need to know about the
storage representation.

## Testing philosophy: dummies over mocks

When testing classes that interact with hardware, Redis, or other
external systems, prefer the project's `Dummy*` classes over
replacing attributes with `Mock()`. The dummies are not stubs — they
are working in-process implementations. `DummyEigsepFpga`, for
example, exposes a real `DummyFpga` (in `eigsep_corr.testing`) with
realistic register defaults and a wallclock-driven `corr_acc_cnt`
counter; replacing `fpga.fpga` with `Mock()` clobbers all of that and
forces every test that touches the fixture to manually re-supply
values that were already there.

**Rule of thumb**: if you're about to write `fixture_obj.attr = Mock()`,
check whether the dummy already provides a working `attr`. If it does,
scope a per-test `patch.object(fixture_obj, "attr", ...)` to the
*specific* method or value the test needs to control, instead of a
fixture-wide Mock that hides what's available.

**When the dummy doesn't do what you need**: first try to set the
attribute directly (e.g. `fpga.pfb.fft_shift = fpga.cfg["fft_shift"]`)
or call the dummy's own setter. If that's not enough, extending the
dummy class itself is preferable to fixture-level Mocking — the
extension benefits every other test, where a fixture Mock only
papers over the gap locally.

**Loggers**: prefer pytest's `caplog` fixture over replacing
`obj.logger = Mock()`. The dummies use real Python loggers and
`caplog` captures records natively. If a test needs to interrupt a
loop after a specific log message, drive the loop's actual control
variable (e.g. set the event in a counting closure on the patched
read method) — the logger is not a control plane.

**Bad**: `fpga.fpga = Mock()` — replaces the whole DummyFpga, breaks
the realistic counter, register defaults, and downstream tests.
**Good**: `with patch.object(fpga.fpga, "read_int", side_effect=[5, 6, 6]):`
— scopes the patch to the one method the test needs to control.

`tests/test_fpga.py` follows this pattern; refer to it for the
canonical shape of producer-side and consumer-side observe() tests.

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
