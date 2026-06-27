# System-current consumer wiring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the producer's `system_current` metadata stream (ACS724 on the lidar Pico) into the consumer's sensor schema + corr-file sidecar, the live-status dashboard (header tile + detail card), and the `watch_sensors` monitor.

**Architecture:** The producer (`pico-firmware feat/system-current-monitor` + matching `picohost`) fans the lidar Pico's merged line into a clean `system_current` stream `{sensor_name, status, current_voltage, current_a}` — no `app_id`, like `adc_stats`. The consumer is already generic on the data path (`avg_metadata` dispatches by `sensor_name`; `/api/metadata` projects any snapshot-hash stream), so the work is: one schema entry, a producer-contract test (plus a fix to the now-coupled lidar test), one dashboard signal + band, hand-written front-end tiles (the JS is not data-driven), and one `watch_sensors` plot-curation line.

**Tech Stack:** Python 3.9+, pytest, ruff (line length 79), `eigsep_redis` (DummyTransport/fakeredis), `picohost` (producer emulators), Flask + vanilla JS (live-status), uv (package manager).

**Spec:** `docs/superpowers/specs/2026-06-27-system-current-consumer-wiring-design.md`

## Global Constraints

- **Python 3.9+ compatibility; ruff line length 79** (lint + format). Run `ruff check .` and `ruff format --check .` before each commit.
- **Producer is unreleased.** Dev/test against an editable install of the local `pico-firmware/picohost` branch (`feat/system-current-monitor`) via **uv** — `uv pip install -e ../pico-firmware/picohost`. Always activate `.venv` first.
- **Do NOT bump the `picohost` pin in `pyproject.toml` or touch `uv.lock` now.** The branch still declares `picohost 3.8.0`; pinning to an unreleased version breaks resolution. The pin bump is the final, release-gated task (Task 7) and the PR holds for it.
- **Run targeted tests only** (CI runs the full suite). Each task lists its exact `pytest` selector.
- **Dummies over mocks; `caplog` over `logger=Mock()`** (project testing philosophy).
- **Producer shape is the contract — conform, don't redefine.** The `system_current` fields and types are fixed upstream; match them exactly.
- Work on branch `feat/system-current-consumer-wiring` (already created; spec already committed there).

---

### Task 1: Editable-install the local producer + verify its surface

Environment setup, no code change, no commit. Everything downstream needs the producer's `PicoLidar._lidar_redis_handler` + the emulator's `current_voltage`, which the installed `picohost 3.8.0` lacks.

**Files:** none (environment only).

**Interfaces:**
- Produces (for later tasks): `picohost.base.PicoLidar._lidar_redis_handler(self, data)` fans one merged lidar dict into two `self._base_redis_handler(...)` calls — first `metadata['lidar']` (distance, `current_voltage` stripped), then `metadata['system_current']` (`{sensor_name:"system_current", status:"update", current_voltage:<float>, current_a:<float>}`). `picohost.testing.LidarEmulator().get_status()` includes a `current_voltage` float.

- [ ] **Step 1: Ensure the producer branch is checked out**

Run:
```bash
git -C ../pico-firmware branch --show-current
```
Expected: `feat/system-current-monitor`. If not, `git -C ../pico-firmware checkout feat/system-current-monitor`.

- [ ] **Step 2: Editable-install picohost with uv into the active venv**

Run:
```bash
source .venv/bin/activate
uv pip install -e ../pico-firmware/picohost
```
Expected: installs `picohost 3.8.0` from the local editable path (replacing the index build).

- [ ] **Step 3: Verify the producer surface exists**

Run:
```bash
source .venv/bin/activate
python -c "
from picohost.base import PicoLidar
from picohost.testing import LidarEmulator
assert hasattr(PicoLidar, '_lidar_redis_handler'), 'no fan-out handler'
captured = []
lidar = PicoLidar.__new__(PicoLidar)
lidar._current_cal = None  # bypass __init__: nominal conversion
lidar._base_redis_handler = lambda d: captured.append(dict(d))
lidar._lidar_redis_handler(LidarEmulator().get_status())
assert len(captured) == 2, captured
assert captured[0]['sensor_name'] == 'lidar' and 'current_voltage' not in captured[0]
sc = captured[1]
assert sc['sensor_name'] == 'system_current'
assert sc['status'] == 'update'
assert isinstance(sc['current_voltage'], float)
assert isinstance(sc['current_a'], float)
print('OK', sorted(sc))
"
```
Expected: `OK ['current_a', 'current_voltage', 'sensor_name', 'status']`. If this fails, stop — the rest of the plan depends on it.

---

### Task 2: Add `system_current` to `SENSOR_SCHEMAS` (schema + corr-file sidecar)

**Files:**
- Modify: `src/eigsep_observing/io.py` (the `SENSOR_SCHEMAS` dict, ends ~line 829)
- Test: `tests/test_io.py`

**Interfaces:**
- Consumes: nothing (data path is generic).
- Produces: `io.SENSOR_SCHEMAS["system_current"] == {"sensor_name": str, "status": str, "current_voltage": float, "current_a": float}`. Once registered, `avg_metadata` validates + float→mean-reduces it into the per-integration metadata sidecar, and the producer-contract test (Task 3) parametrizes over it.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_io.py` (the module already imports `io` and `pytest`):

```python
def test_system_current_schema_registered_and_validates():
    """system_current is a fanned-out stream (no app_id, like adc_stats).
    The schema's job is loud validation: current_a/current_voltage must be
    real floats so the float->mean reducer doesn't silently drop an
    int-emitting producer to None."""
    schema = io.SENSOR_SCHEMAS["system_current"]
    assert schema == {
        "sensor_name": str,
        "status": str,
        "current_voltage": float,
        "current_a": float,
    }
    good = {
        "sensor_name": "system_current",
        "status": "update",
        "current_voltage": 1.70,
        "current_a": 2.0,
    }
    assert io._validate_metadata(good, schema) == []
    # int current_a violates the strict float check (would be dropped to
    # None by the float reducer) -> must surface as a contract violation.
    bad = {**good, "current_a": 2}
    violations = io._validate_metadata(bad, schema)
    assert any("current_a" in v for v in violations)


def test_avg_metadata_system_current():
    """Both floats reduce via the generic float->mean path; the
    producer-fixed status='update' row stays clean."""
    data = [
        {
            "sensor_name": "system_current",
            "status": "update",
            "current_voltage": 1.70,
            "current_a": 2.0,
        },
        {
            "sensor_name": "system_current",
            "status": "update",
            "current_voltage": 1.74,
            "current_a": 4.0,
        },
    ]
    result = io.avg_metadata(data)
    assert result["sensor_name"] == "system_current"
    assert result["status"] == "update"
    assert result["current_a"] == pytest.approx(3.0)
    assert result["current_voltage"] == pytest.approx(1.72)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_io.py::test_system_current_schema_registered_and_validates -v`
Expected: FAIL — `KeyError: 'system_current'` (schema not registered yet).

- [ ] **Step 3: Add the schema entry**

In `src/eigsep_observing/io.py`, inside `SENSOR_SCHEMAS`, add (place it after the `"lidar"` entry — same Pico — or anywhere in the dict; keep it adjacent to `lidar` for readability):

```python
    # `system_current`: whole-system current draw, fanned out from the
    # lidar Pico's ACS724 by picohost's PicoLidar._lidar_redis_handler.
    # Like `adc_stats`, it is a derived stream with no `app_id` (not a
    # 1:1 pico app). `status` is producer-fixed to "update" (the ADC read
    # is decoupled from lidar's I2C result). `current_a` is the meaningful
    # value (amps); `current_voltage` is the raw ADC-pin voltage diagnostic.
    # Both floats reduce via the standard float->mean path.
    "system_current": {
        "sensor_name": str,
        "status": str,
        "current_voltage": float,
        "current_a": float,
    },
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_io.py::test_system_current_schema_registered_and_validates tests/test_io.py::test_avg_metadata_system_current -v`
Expected: PASS (both).

- [ ] **Step 5: Lint + commit**

Run:
```bash
ruff check src/eigsep_observing/io.py tests/test_io.py && ruff format --check src/eigsep_observing/io.py tests/test_io.py
git add src/eigsep_observing/io.py tests/test_io.py
git commit -m "feat(io): add system_current sensor schema

Register the producer's fanned-out system_current stream (ACS724 on the
lidar Pico) in SENSOR_SCHEMAS so avg_metadata validates it and float->mean
reduces current_a/current_voltage into the per-integration corr-file
metadata sidecar. No app_id, like adc_stats.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Producer-contract test — lidar post-handler fix + system_current registration

The lidar emulator now also emits `current_voltage`, so the existing `lidar` registration (raw `get_status()`) gains an extra key the unchanged `lidar` schema rejects. Re-point `lidar` at the post-handler (current-stripped) dict and register `system_current` at the fan-out dict.

**Files:**
- Modify: `src/eigsep_observing/contract_tests/test_producer_contracts.py`

**Interfaces:**
- Consumes: `io.SENSOR_SCHEMAS["system_current"]` (Task 2); `PicoLidar._lidar_redis_handler` (Task 1).
- Produces: `SENSOR_EMULATORS["lidar"]` returns the post-handler lidar dict (no `current_voltage`); `SENSOR_EMULATORS["system_current"]` returns the fan-out dict. `test_every_schema_has_conforming_emulator` then covers `system_current`.

- [ ] **Step 1: Run the contract suite to see the new failures**

Run: `pytest src/eigsep_observing/contract_tests/test_producer_contracts.py -k "lidar or system_current" -v`
Expected: FAIL —
- `test_every_schema_has_conforming_emulator[system_current]`: `AssertionError: SENSOR_SCHEMAS has 'system_current' but no emulator is registered`.
- `test_every_schema_has_conforming_emulator[lidar]`: `lidar producer drift: ["extra keys: ['current_voltage']"]` (the emulator's raw `get_status()` now carries `current_voltage`).

- [ ] **Step 2: Add the `PicoLidar` import**

In `test_producer_contracts.py`, extend the existing picohost.base import:

```python
from picohost.base import PicoLidar, PicoRFSwitch
```

(`LidarEmulator` is already imported from `picohost.testing`.)

- [ ] **Step 3: Add the post-handler helper**

Add near the other `_*_post_handler_reading` helpers:

```python
def _lidar_post_handler_readings():
    """Return (lidar_dict, system_current_dict) after _lidar_redis_handler.

    The lidar Pico's firmware emits one merged line (distance +
    current_voltage); ``PicoLidar._lidar_redis_handler`` splits it into
    two metadata publishes — ``metadata['lidar']`` (distance, current
    stripped) and ``metadata['system_current']`` (current_voltage +
    derived current_a). The contract these tests enforce is the
    post-handler shape of each, so compose ``LidarEmulator.get_status()``
    through the real handler and capture both publishes in order. Mirrors
    ``_rfswitch_post_handler_reading`` / ``_motor_post_handler_reading``.
    ``_current_cal`` is normally set in ``PicoLidar.__init__`` (two-point
    current cal, picohost); ``__new__`` bypasses that, so set it to ``None``
    to select the nominal ACS724 conversion — same pattern as
    ``_motor_post_handler_reading`` setting ``_motor_pos_store = None``.
    """
    lidar = PicoLidar.__new__(PicoLidar)
    lidar._current_cal = None  # bypass __init__: nominal conversion
    captured = []
    lidar._base_redis_handler = lambda d: captured.append(dict(d))
    lidar._lidar_redis_handler(LidarEmulator().get_status())
    assert len(captured) == 2, (
        f"expected lidar + system_current publishes, got {len(captured)}"
    )
    return captured[0], captured[1]
```

- [ ] **Step 4: Re-point `lidar` and register `system_current` in the registry**

In `SENSOR_EMULATORS`, replace the `"lidar"` line and add `"system_current"`:

```python
    "lidar": lambda: _lidar_post_handler_readings()[0],
    "potmon": _potmon_post_handler_reading,
    "motor": _motor_post_handler_reading,
    "adc_stats": _adc_stats_post_publish_reading,
    "system_current": lambda: _lidar_post_handler_readings()[1],
```

(Keep the existing entries; only `lidar` changes and `system_current` is added.)

- [ ] **Step 5: Run the contract suite to verify it passes**

Run: `pytest src/eigsep_observing/contract_tests/test_producer_contracts.py -k "lidar or system_current" -v`
Expected: PASS — `[lidar]` and `[system_current]` both green (the latter also asserts `sensor_name == "system_current"`).

- [ ] **Step 6: Lint + commit**

Run:
```bash
ruff check src/eigsep_observing/contract_tests/test_producer_contracts.py && ruff format --check src/eigsep_observing/contract_tests/test_producer_contracts.py
git add src/eigsep_observing/contract_tests/test_producer_contracts.py
git commit -m "test(contracts): cover system_current; fix lidar post-handler shape

The lidar emulator now also emits current_voltage (stripped by
PicoLidar._lidar_redis_handler), so re-point the lidar contract emulator
at the post-handler distance-only dict and register system_current at the
fan-out dict. test_every_schema_has_conforming_emulator now covers the new
stream by construction.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Dashboard data layer — signal, threshold band, classify

**Files:**
- Modify: `src/eigsep_observing/live_status/signals.py` (the `SIGNAL_REGISTRY` dict)
- Modify: `src/eigsep_observing/config/live_status_thresholds.yaml`
- Test: `tests/test_live_status_thresholds.py`, `tests/test_live_status_app.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `SIGNAL_REGISTRY["system_current.current_a"]` (`unit="A"`, `enabled_by=None`, `max_age_s=30.0`); bundled YAML band `system_current.current_a: healthy [0,5], danger [0,8]`. `_metadata_payload` then emits `classify["system_current.current_a"]` for a `system_current` snapshot entry.

- [ ] **Step 1: Write the failing signal + threshold tests**

Add to `tests/test_live_status_thresholds.py`:

```python
def test_system_current_signal_registered_and_always_enabled():
    sig = SIGNAL_REGISTRY["system_current.current_a"]
    assert sig.unit == "A"
    assert sig.enabled_by is None  # system-wide vital, never gated
    assert sig.max_age_s == 30.0
    # Present even when tempctrl (and other optional subsystems) are off.
    assert "system_current.current_a" in enabled_signals(OBS_CFG_TEMPCTRL_OFF)


def test_system_current_band_from_bundled_yaml():
    th = Thresholds.from_yaml(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    assert th.bands["system_current.current_a"]["healthy"] == [0.0, 5.0]
    assert th.bands["system_current.current_a"]["danger"] == [0.0, 8.0]
    assert th.classify("system_current.current_a", 3.0) == "ok"
    assert th.classify("system_current.current_a", 6.0) == "warn"
    assert th.classify("system_current.current_a", 9.0) == "danger"
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest tests/test_live_status_thresholds.py -k system_current -v`
Expected: FAIL — `KeyError: 'system_current.current_a'` (signal not registered).

- [ ] **Step 3: Register the signal**

In `src/eigsep_observing/live_status/signals.py`, add to `SIGNAL_REGISTRY` (next to the `lidar.distance_m` / `potmon.pot_az_angle` site-geometry block, but note this one is always-enabled):

```python
    # Whole-system current draw (ACS724 on the lidar Pico, fanned out to
    # the system_current stream). Always enabled — a system-wide vital,
    # not gated by a subsystem flag. current_voltage is a raw ADC
    # diagnostic shown on the card but not a classified signal.
    "system_current.current_a": Signal(
        "system_current.current_a",
        "System current",
        unit="A",
    ),
```

- [ ] **Step 4: Add the bundled threshold band**

In `src/eigsep_observing/config/live_status_thresholds.yaml`, add (after the ADC block, before the site-geometry TODO block):

```yaml
# Whole-system current (ACS724-10AB inline on the shared 5 V feed).
# Nominal bands: panda + both Peltiers draws ~3 A; the sensor saturates
# ~12.5 A. Tune against a measured baseline — these are starting values.
system_current.current_a:
  healthy: [0.0, 5.0]
  danger:  [0.0, 8.0]
```

- [ ] **Step 5: Run the threshold tests to verify they pass**

Run: `pytest tests/test_live_status_thresholds.py -k system_current -v`
Expected: PASS (both).

- [ ] **Step 6: Write the failing app-route classify test**

Add to `tests/test_live_status_app.py` (the module already imports `OBS_CFG`; the helpers below mirror the existing `_payload_thresholds` / direct `_metadata_payload` tests):

```python
def test_metadata_payload_classifies_system_current():
    """A system_current snapshot entry is classified against the
    current_a band by the existing /api/metadata projection (no app.py
    change needed — the route is generic over snapshot-hash streams)."""
    from eigsep_observing.live_status.app import _metadata_payload
    from eigsep_observing.live_status.aggregator import StateSnapshot

    now = 1000.0
    state = StateSnapshot()
    state.metadata_snapshot = {
        "system_current": {
            "sensor_name": "system_current",
            "status": "update",
            "current_voltage": 1.70,
            "current_a": 3.0,
        },
        "system_current_ts": now,
    }
    state.metadata_snapshot_read_unix = now
    payload = _metadata_payload(state, _payload_thresholds())
    entry = payload["system_current"]
    assert entry["classify"]["system_current.current_a"] == "ok"
    assert entry["status"] == "update"
```

- [ ] **Step 7: Run the app test to verify it passes**

Run: `pytest tests/test_live_status_app.py::test_metadata_payload_classifies_system_current -v`
Expected: PASS (no production code change in app.py — this test characterizes that the generic route already classifies the new stream once the signal + band exist). `_payload_thresholds()` loads the bundled YAML added in Step 4, so the `current_a=3.0` reading lands in `[0,5]` → `ok`.

- [ ] **Step 8: Lint + commit**

Run:
```bash
ruff check src/eigsep_observing/live_status/signals.py tests/test_live_status_thresholds.py tests/test_live_status_app.py && ruff format --check src/eigsep_observing/live_status/signals.py tests/test_live_status_thresholds.py tests/test_live_status_app.py
git add src/eigsep_observing/live_status/signals.py src/eigsep_observing/config/live_status_thresholds.yaml tests/test_live_status_thresholds.py tests/test_live_status_app.py
git commit -m "feat(live-status): classify system_current.current_a

Register system_current.current_a as an always-enabled dashboard signal
with a nominal healthy/danger amp band in the bundled thresholds YAML. The
generic /api/metadata route classifies it with no app.py change.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Front-end — glanceable header tile + detail card

The dashboard's metadata tiles are rendered by explicit per-sensor JS functions (not a loop), so a new stream needs an HTML container, a renderer, and a `tick()` call. There is no JS unit-test harness in this repo; verification is via the dummy live-status server + a structural grep.

**Files:**
- Modify: `src/eigsep_observing/live_status/templates/index.html`
- Modify: `src/eigsep_observing/live_status/static/js/dashboard.js`

**Interfaces:**
- Consumes: `/api/metadata` JSON `system_current` entry `{value:{current_a,current_voltage}, status, age_s, classify:{"system_current.current_a": <tag>}}` (Task 4). Uses existing JS helpers `tileClass(classify)`, `fmt(v,d)`, `makePaneStatusHeader(name,entry)`, `appendTileRow(container,label,tileClass(cls),text)`, `appendValueRow(container,label,text)`.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the glanceable header tile**

In `src/eigsep_observing/live_status/templates/index.html`, inside `<div id="health-summary" class="health">`, add a tile immediately after the `tile-corr-loop` span (so the system vital sits high in the row):

```html
    <span class="tile" id="tile-system-current" title="Whole-system current draw (ACS724 on the lidar Pico). Colored by the system_current.current_a band in live_status_thresholds.yaml.">Current: ?</span>
```

- [ ] **Step 2: Add the detail card as the first grid card**

In the same file, immediately after `<main class="grid">` and before the `Correlation spectra` section, add:

```html
  <section class="card">
    <h2>System current</h2>
    <div id="system-current-block"></div>
  </section>
```

- [ ] **Step 3: Add the `renderSystemCurrent` renderer**

In `src/eigsep_observing/live_status/static/js/dashboard.js`, add next to `renderLidar` (in the "new per-pico renderers" block):

```javascript
// Whole-system current. Updates both the glanceable header tile and the
// detail card from the same /api/metadata entry (the value comes from
// metadata, not /api/health, so the header tile is driven here rather
// than in updateHealth). Colored by the current_a classify band.
function renderSystemCurrent(meta) {
  const entry = meta["system_current"];
  const cls = entry && entry.classify
    ? entry.classify["system_current.current_a"]
    : undefined;
  const value = (entry && entry.value) || {};

  const tile = document.getElementById("tile-system-current");
  if (tile) {
    if (!entry) {
      tile.className = "tile unknown";
      tile.textContent = "Current: —";
    } else {
      tile.className = tileClass(cls);
      tile.textContent = `Current: ${fmt(value.current_a, 2)} A`;
    }
  }

  const container = document.getElementById("system-current-block");
  if (container) {
    container.replaceChildren();
    if (!entry) {
      container.textContent = "no system_current data";
    } else {
      container.appendChild(makePaneStatusHeader("system current", entry));
      appendTileRow(
        container, "current", tileClass(cls), `${fmt(value.current_a, 2)} A`,
      );
      appendValueRow(
        container, "voltage", `${fmt(value.current_voltage, 3)} V`,
      );
    }
  }
}
```

- [ ] **Step 4: Call the renderer in `tick()`**

In `tick()`, add the call immediately after `renderLidar(metadata.data);`:

```javascript
    renderLidar(metadata.data);
    renderSystemCurrent(metadata.data);
```

- [ ] **Step 5: Structural verification (ids + call are present)**

Run:
```bash
grep -q 'id="tile-system-current"' src/eigsep_observing/live_status/templates/index.html \
 && grep -q 'id="system-current-block"' src/eigsep_observing/live_status/templates/index.html \
 && grep -q 'function renderSystemCurrent' src/eigsep_observing/live_status/static/js/dashboard.js \
 && grep -q 'renderSystemCurrent(metadata.data)' src/eigsep_observing/live_status/static/js/dashboard.js \
 && echo "WIRED OK"
```
Expected: `WIRED OK`.

- [ ] **Step 6: Live smoke test against the dummy dashboard**

Run (background the server, hit the API, then stop it):
```bash
source .venv/bin/activate
python scripts/live_status.py --dummy --port 5099 &
SRV=$!
sleep 3
curl -s localhost:5099/api/metadata | python -c "import sys,json; d=json.load(sys.stdin)['data']; print('system_current present:', 'system_current' in d)"
kill $SRV
```
Expected: prints `system_current present: True` if the dummy publishes the stream. (If the dummy `picohost` lidar emulator does not publish `system_current` in this environment, the value will be `False` — that is acceptable; the data path is covered by Tasks 2–4 and the front-end is verified structurally in Step 5. Note the discrepancy in the commit body if so.) Then open `http://localhost:5099/` in a browser to confirm the header tile and card render (optional, requires a display).

- [ ] **Step 7: Lint + commit**

Run:
```bash
ruff check scripts/live_status.py 2>/dev/null; true
git add src/eigsep_observing/live_status/templates/index.html src/eigsep_observing/live_status/static/js/dashboard.js
git commit -m "feat(live-status): system_current header tile + detail card

Add a glanceable 'Current: X A' tile to the health-summary row and a
dedicated 'System current' card at the top of the grid, both colored by the
current_a band. renderSystemCurrent drives both from /api/metadata.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: `watch_sensors` — curate the plotted field

Text display is automatic (`system_current` is a panda stream in `SENSOR_SCHEMAS`, not excluded like `adc_stats`). Only the `--plot` field list needs curation so it traces amps, not the raw ADC voltage.

**Files:**
- Modify: `scripts/watch_sensors.py` (the `_PLOT_FIELDS` dict, ~line 41)
- Test: `tests/test_watch_sensors.py` (new)

**Interfaces:**
- Consumes: `io.SENSOR_SCHEMAS["system_current"]` (Task 2) — without it, `system_current` would not be in `_PANDA_STREAMS`.
- Produces: `_plot_fields_for("system_current") == ["current_a"]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_watch_sensors.py`:

```python
"""Smoke test for the watch_sensors bring-up script.

watch_sensors lives under scripts/ (not an importable package), so load
it by file path the same way tests/test_motor_scripts.py does.
"""

import importlib.util
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _load(name):
    path = _REPO_ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_system_current_in_default_panda_streams():
    ws = _load("watch_sensors")
    # Panda stream (not adc_stats, which is SNAP-side) -> shown by default.
    assert "system_current" in ws._PANDA_STREAMS


def test_system_current_plots_current_a_only():
    ws = _load("watch_sensors")
    # The raw ADC current_voltage is not operator-meaningful; only amps
    # are traced in --plot.
    assert ws._plot_fields_for("system_current") == ["current_a"]
```

- [ ] **Step 2: Run to verify the plot test fails**

Run: `pytest tests/test_watch_sensors.py -v`
Expected: `test_system_current_in_default_panda_streams` PASSES (automatic via the schema), `test_system_current_plots_current_a_only` FAILS — fallback returns `["current_voltage", "current_a"]` (all float fields) instead of `["current_a"]`.

- [ ] **Step 3: Add the plot-curation entry**

In `scripts/watch_sensors.py`, add to the `_PLOT_FIELDS` dict (alongside `"lidar": ("distance_m",)`):

```python
    "system_current": ("current_a",),
```

- [ ] **Step 4: Run to verify both pass**

Run: `pytest tests/test_watch_sensors.py -v`
Expected: PASS (both).

- [ ] **Step 5: Lint + commit**

Run:
```bash
ruff check scripts/watch_sensors.py tests/test_watch_sensors.py && ruff format --check scripts/watch_sensors.py tests/test_watch_sensors.py
git add scripts/watch_sensors.py tests/test_watch_sensors.py
git commit -m "feat(watch_sensors): trace current_a only for system_current

system_current shows in the text table automatically (panda stream). Curate
the --plot field list so it traces amps, not the raw ADC current_voltage.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Bump the `picohost` pin — RELEASE-GATED, do not run now

**Blocked on:** a published `picohost` release that ships `PicoLidar._lidar_redis_handler` + the emulator `current_voltage` (a version > 3.8.0). Until then the PR holds (external blocker, per the self-contained-PR rule). The branch is otherwise complete and green against the Task 1 editable install.

**Files:**
- Modify: `pyproject.toml` (the `picohost>=3.8.0` line)
- Modify: `uv.lock`

- [ ] **Step 1 (when the release exists): bump the floor**

In `pyproject.toml`, change `"picohost>=3.8.0",` to the released version that ships the current monitor, e.g. `"picohost>=3.9.0",` (use the actual cut version).

- [ ] **Step 2: Re-lock and reinstall from the index**

Run:
```bash
source .venv/bin/activate
uv lock
uv pip install -e ".[dev]"
```
Expected: resolves `picohost` to the released version (replacing the local editable install).

- [ ] **Step 3: Full targeted re-run against the released producer**

Run:
```bash
pytest src/eigsep_observing/contract_tests/test_producer_contracts.py -k "lidar or system_current" tests/test_io.py -k system_current tests/test_live_status_thresholds.py -k system_current tests/test_live_status_app.py::test_metadata_payload_classifies_system_current tests/test_watch_sensors.py -v
```
Expected: all PASS against the released picohost.

- [ ] **Step 4: Commit + ready the PR**

Run:
```bash
git add pyproject.toml uv.lock
git commit -m "chore(deps): require picohost with the system_current producer

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**1. Spec coverage:**
- Schema + corr-file sidecar → Task 2. ✓
- Producer-contract test + lidar fix → Task 3. ✓
- Live-status signal + band + classify → Task 4. ✓
- Live-status front-end (header tile + detail card, prominent) → Task 5. ✓
- watch_sensors → Task 6. ✓
- Dependency/sequencing (uv editable install, pin bump held for release) → Task 1 + Task 7. ✓
- Out-of-scope safety feature → intentionally excluded (spec defers it). ✓

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to Task N" — every code/test block is complete. The only deferred content is Task 7, which is explicitly release-gated with concrete steps (the exact version is the one external unknown, flagged as such). ✓

**3. Type consistency:** `system_current` schema fields (`sensor_name`, `status`, `current_voltage`, `current_a`) are identical across Tasks 2/3/4/5/6. Signal name `system_current.current_a`, container ids `tile-system-current` / `system-current-block`, and function `renderSystemCurrent` are spelled identically wherever referenced. Helper signatures (`tileClass`, `fmt`, `makePaneStatusHeader`, `appendTileRow`, `appendValueRow`) match their definitions in `dashboard.js`. ✓
