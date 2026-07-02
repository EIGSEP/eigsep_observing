# RF switch EEPROM paths + PCB thermistors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reflect the `pico-firmware` `feat/rfswitch-eeprom-paths` branch in `eigsep_observing`: follow the `PicoRFSwitch.path_str`→`PATHS` rename, and surface the three RF-switch PCB thermistors (raw volts + host-derived °C) in `watch_sensors` and the live-status dashboard.

**Architecture:** The thermistors ride a **new `rfswitch_therm` metadata stream** fanned out of the switch-state line in `PicoRFSwitch._rfswitch_redis_handler` (the `PicoLidar`→`system_current` pattern), keeping the categorical rfswitch stream pure. Voltage→temperature conversion is done host-side in that handler with hardcoded datasheet Beta constants. On the consumer side the change is purely additive: a `SENSOR_SCHEMAS` entry, a contract-test fixture, three dashboard signals, and a fold-in of three rows into the existing rfswitch pane.

**Tech Stack:** Python 3.9+, pytest, ruff (line length 79), Redis (fakeredis in tests), Flask + vanilla JS dashboard, picohost (sibling repo), uv-managed venvs.

## Global Constraints

- Ruff line length **79**; run `ruff check` and `ruff format --check` before every commit.
- Python **3.9+** compatibility (no 3.10+ syntax).
- **Contract-based:** every producer stream in `SENSOR_SCHEMAS` must have a matching `SENSOR_EMULATORS` entry (enforced by `test_every_schema_has_conforming_emulator`).
- **Corr data is sacred:** never let non-corr processing changes touch the corr path.
- Datasheet thermistor constants (verbatim): 10k NTC, **R₀ = 10000 Ω @ 25 °C**, **B = 3380** (25–50 °C), divider = **10 kΩ pullup to 5.0 V**, thermistor to GND. Firmware `volt_therm` is the ADC-pin voltage referenced to the RP2040's **internal 3.3 V ADC full-scale** (the external harness is 5 V-only; there is no 3.3 V rail in the sensor circuit).
- **Post-execution addendum (circuit confirmed 2026-07-02):** the 5 V pullup on a 3.3 V ADC saturates below **~8.5 °C** (R≈19.4 kΩ, pin=3.3 V). `_therm_temp_c` gained `THERM_ADC_MAX_VOLTS = 3.3` and returns **`None`** at/above it (saturated → untrustworthy) rather than a fake ~8.5 °C floor (picohost commit `aca2f45`, io.py comment `8d49797`). Hardware caveat flagged to operator: RP2040 ADC pin is not 5 V-tolerant.
- **Two repos, coordinated:** the picohost change (Phase A) must be installed into the `eigsep_observing` venv before its tests can pass. The two must land together.

### Environments / commands

- `eigsep_observing` tests: **`.venv/bin/pytest`** from `/home/eigsep/eigsep/eigsep_observing` (do **not** use `uv run pytest` — it re-syncs from the lock and clobbers the editable picohost).
- `eigsep_observing` lint: `.venv/bin/ruff check .` and `.venv/bin/ruff format --check .`
- picohost tests: from `/home/eigsep/eigsep/pico-firmware/picohost`, run **`.venv/bin/pytest tests/`**.
- Install branch picohost into the eigsep venv (Phase A → B bridge): from `/home/eigsep/eigsep/eigsep_observing`, `uv pip install -e /home/eigsep/eigsep/pico-firmware/picohost`.
- pico-firmware branch: `feat/rfswitch-eeprom-paths`. eigsep_observing branch: `feat/rfswitch-eeprom-thermistors` (already created).

---

## Task 1: picohost — fan thermistors into `rfswitch_therm` with host-side °C

**Repo:** `/home/eigsep/eigsep/pico-firmware` (branch `feat/rfswitch-eeprom-paths`)

**Files:**
- Modify: `picohost/src/picohost/base.py` (`PicoRFSwitch`, ~lines 441–546; add `import math` if absent)
- Modify: `picohost/src/picohost/emulators/rfswitch.py` (`DEFAULT_THERM_VOLTS` 1.65→2.5, ~line 44)
- Modify: `README.md` (thermistor note, RF Switch Wiring section)
- Test: `picohost/tests/test_base.py` (new `TestRFSwitchThermistorFanout`)
- Test: `picohost/tests/test_emulators.py` (update any `1.65`/`DEFAULT_THERM_VOLTS` assertion to `2.5`)

**Interfaces:**
- Produces: `PicoRFSwitch._rfswitch_redis_handler(data)` now emits **two** publishes when `volt_therm*` present — `{sensor_name:"rfswitch", …, sw_state_name}` (thermistor keys removed) then `{sensor_name:"rfswitch_therm", status:"update", volt_therm0/1/2: float, temp_therm0/1/2: float|None}`. One publish when no `volt_therm*`.
- Produces: `PicoRFSwitch._therm_temp_c(v) -> float|None` (classmethod) and class constants `THERM_SUPPLY_VOLTS=5.0`, `THERM_PULLUP_OHMS=10000.0`, `THERM_R0_OHMS=10000.0`, `THERM_T0_KELVIN=298.15`, `THERM_B=3380.0`, `THERM_NUM=3`.

- [ ] **Step 1: Write the failing tests**

Add to `picohost/tests/test_base.py` (after the existing `TestRFSwitchRedisHandler` class):

```python
class TestRFSwitchThermistorFanout:
    """The three PCB thermistors fan out of the switch-state line into a
    separate rfswitch_therm stream (mirrors PicoLidar -> system_current),
    carrying raw volts + host-derived degrees C. The switch-state line
    stays a pure categorical signal (no thermistor keys)."""

    def _capture(self, switch, data):
        published = []
        switch._base_redis_handler = lambda d: published.append(dict(d))
        switch._rfswitch_redis_handler(data)
        return published

    def test_fans_thermistors_into_separate_stream(self):
        switch = DummyPicoRFSwitch("/dev/dummy")
        try:
            pub = self._capture(
                switch,
                {
                    "sensor_name": "rfswitch",
                    "status": "update",
                    "app_id": 5,
                    "sw_state": switch.PATHS["RFANT"],
                    "volt_therm0": 2.5,
                    "volt_therm1": 2.5,
                    "volt_therm2": 2.5,
                },
            )
            assert [p["sensor_name"] for p in pub] == [
                "rfswitch",
                "rfswitch_therm",
            ]
            # switch-state line is pure: thermistor keys stripped
            assert "volt_therm0" not in pub[0]
            assert pub[0]["sw_state_name"] == "RFANT"
            therm = pub[1]
            assert therm["status"] == "update"
            assert "app_id" not in therm  # fanned/derived stream, cf. system_current
            for i in range(3):
                assert therm[f"volt_therm{i}"] == 2.5
                # 2.5 V over the 5 V / 10k divider -> R = 10k -> 25 C
                assert therm[f"temp_therm{i}"] == pytest.approx(25.0, abs=0.05)
        finally:
            switch.disconnect()

    def test_out_of_range_voltage_maps_to_none_temp(self):
        switch = DummyPicoRFSwitch("/dev/dummy")
        try:
            pub = self._capture(
                switch,
                {
                    "sensor_name": "rfswitch",
                    "sw_state": switch.PATHS["RFANT"],
                    "volt_therm0": 0.0,   # v <= 0  -> None
                    "volt_therm1": 5.0,   # v >= SUPPLY -> None
                    "volt_therm2": 2.5,   # valid -> 25 C
                },
            )
            therm = pub[1]
            assert therm["temp_therm0"] is None
            assert therm["temp_therm1"] is None
            assert therm["temp_therm2"] == pytest.approx(25.0, abs=0.05)
            # raw volts always pass through, even when temp is None
            assert therm["volt_therm0"] == 0.0
        finally:
            switch.disconnect()

    def test_no_thermistor_fields_publishes_only_rfswitch(self):
        switch = DummyPicoRFSwitch("/dev/dummy")
        try:
            pub = self._capture(
                switch,
                {"sensor_name": "rfswitch", "sw_state": switch.PATHS["RFANT"]},
            )
            assert [p["sensor_name"] for p in pub] == ["rfswitch"]
        finally:
            switch.disconnect()

    def test_therm_temp_c_known_points(self):
        # 2.5 V -> R=10k -> exactly 25 C; monotonic: lower V (hotter) -> higher C
        assert PicoRFSwitch._therm_temp_c(2.5) == pytest.approx(25.0, abs=0.05)
        assert PicoRFSwitch._therm_temp_c(1.169) == pytest.approx(60.0, abs=1.0)
        assert PicoRFSwitch._therm_temp_c(0.0) is None
        assert PicoRFSwitch._therm_temp_c(5.0) is None
        assert PicoRFSwitch._therm_temp_c(None) is None
```

Confirm `DummyPicoRFSwitch` and `PicoRFSwitch` are already imported at the top of `test_base.py` (they are — used by `TestRFSwitchRedisHandler`); `pytest` is imported too.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/eigsep/eigsep/pico-firmware/picohost && .venv/bin/pytest tests/test_base.py::TestRFSwitchThermistorFanout -v`
Expected: FAIL — `AttributeError: … has no attribute '_therm_temp_c'` and single-publish assertion mismatches.

- [ ] **Step 3: Implement the conversion + fan-out**

In `picohost/src/picohost/base.py`, ensure `import math` is present near the top (add it if not). Then in `class PicoRFSwitch`, add the constants and helper (place after `SW_STATE_UNKNOWN_NAME`), and rewrite `_rfswitch_redis_handler`:

```python
    # --- PCB thermistor conversion (host-side) --------------------------
    # Three 10k NTC thermistors on the RF switch PCB (ADC0-2). The C
    # firmware reports the raw ADC-pin voltage (counts * 3.3/4095,
    # referenced to the 3.3V ADC rail). The divider is powered from 5.0V
    # with a 10k pullup and the thermistor to GND, so
    #   v = SUPPLY * R / (PULLUP + R)  =>  R = PULLUP * v / (SUPPLY - v).
    # Datasheet Beta model R = R0*exp(B*(1/T - 1/T0)), hardcoded like
    # tempctrl's Steinhart-Hart constants: 10k NTC, R0 = 10k @ 25C,
    # B = 3380 (25-50C). Nominal ~+/-1-2C; refine with a measured cal
    # later without changing the stream shape.
    THERM_SUPPLY_VOLTS = 5.0        # divider pullup rail (NOT the 3.3V ADC ref)
    THERM_PULLUP_OHMS = 10_000.0
    THERM_R0_OHMS = 10_000.0        # at 25 C
    THERM_T0_KELVIN = 298.15
    THERM_B = 3380.0
    THERM_NUM = 3

    @classmethod
    def _therm_temp_c(cls, v):
        """Convert one thermistor ADC-pin voltage (volts) to degrees C.

        Returns None when ``v`` is None, non-finite, or outside
        ``(0, THERM_SUPPLY_VOLTS)`` (open / shorted / ADC-clipped
        channel) — mirrors potmon's None-when-invalid derived field.
        """
        if (
            v is None
            or not math.isfinite(v)
            or v <= 0.0
            or v >= cls.THERM_SUPPLY_VOLTS
        ):
            return None
        r = cls.THERM_PULLUP_OHMS * v / (cls.THERM_SUPPLY_VOLTS - v)
        inv_t = (
            1.0 / cls.THERM_T0_KELVIN
            + math.log(r / cls.THERM_R0_OHMS) / cls.THERM_B
        )
        return 1.0 / inv_t - 273.15
```

Replace the body of `_rfswitch_redis_handler` with:

```python
    def _rfswitch_redis_handler(self, data):
        """Add sw_state_name and fan the PCB thermistors into their own stream.

        Firmware reports ``sw_state`` as an EEPROM path address (or
        :attr:`SW_STATE_UNKNOWN` while settling) plus the three raw PCB
        thermistor voltages ``volt_therm0/1/2`` on the same status line.
        The switch-state entry stays categorical (thermistor keys
        removed); the thermistors are re-published on a separate
        ``rfswitch_therm`` stream carrying raw volts + host-derived degrees
        C — the same two-publish fan-out as PicoLidar -> system_current.
        """
        data = data.copy()
        sw_state = data.get("sw_state")
        if sw_state == self.SW_STATE_UNKNOWN:
            data["sw_state_name"] = self.SW_STATE_UNKNOWN_NAME
        else:
            data["sw_state_name"] = self._name_by_state.get(sw_state)
        volts = [data.pop(f"volt_therm{i}", None) for i in range(self.THERM_NUM)]
        self._base_redis_handler(data)
        if any(v is not None for v in volts):
            therm = {"sensor_name": "rfswitch_therm", "status": "update"}
            for i, v in enumerate(volts):
                therm[f"volt_therm{i}"] = v
                therm[f"temp_therm{i}"] = self._therm_temp_c(v)
            self._base_redis_handler(therm)
```

- [ ] **Step 4: Bump the emulator placeholder**

In `picohost/src/picohost/emulators/rfswitch.py`, change `DEFAULT_THERM_VOLTS = 1.65` to:

```python
    # 2.5 V over the 5.0 V / 10k-pullup divider inverts to R = 10k = R0,
    # i.e. exactly 25 C at the emulator's default, so tests read a clean
    # midpoint. (Was 1.65, chosen for a 3.3 V midpoint before the 5 V rail
    # was confirmed.)
    DEFAULT_THERM_VOLTS = 2.5
```

Update any assertion in `picohost/tests/test_emulators.py` that expects `1.65` (or `DEFAULT_THERM_VOLTS == 1.65`) to `2.5` (grep: `grep -n "1.65\|DEFAULT_THERM_VOLTS" picohost/tests/test_emulators.py`).

- [ ] **Step 5: Run the picohost suite to verify pass**

Run: `cd /home/eigsep/eigsep/pico-firmware/picohost && .venv/bin/pytest tests/test_base.py tests/test_emulators.py -q`
Expected: PASS (new fanout tests + existing rfswitch handler tests + emulator parity).

Then the full picohost suite: `.venv/bin/pytest tests/ -q` — Expected: PASS.

- [ ] **Step 6: Update the firmware README**

In `README.md`, in the RF Switch Wiring section, replace the "Thermistors" bullet with:

```markdown
- **Thermistors**: firmware reports the raw averaged ADC pin voltage (`volt_therm<i>`, volts, 3.3V-referenced). Voltage→temperature is done host-side in `PicoRFSwitch._rfswitch_redis_handler`, which re-publishes the three channels on a separate `rfswitch_therm` metadata stream carrying `volt_therm<i>` plus derived `temp_therm<i>` (°C). Conversion uses a datasheet Beta model (10k NTC, R0=10k@25°C, B=3380) over a 10kΩ-pullup-to-5.0V divider; swap in measured constants when characterized (no firmware change).
```

- [ ] **Step 7: Lint + commit (pico-firmware)**

Run: `cd /home/eigsep/eigsep/pico-firmware/picohost && .venv/bin/ruff check src/ tests/ && .venv/bin/ruff format --check src/ tests/` (fix if the repo's ruff config flags anything).

```bash
cd /home/eigsep/eigsep/pico-firmware
git add picohost/src/picohost/base.py picohost/src/picohost/emulators/rfswitch.py picohost/tests/test_base.py picohost/tests/test_emulators.py README.md
git commit -m "feat(rfswitch): fan PCB thermistors into rfswitch_therm stream with host-side degC

Split volt_therm0/1/2 out of the switch-state line into a separate
rfswitch_therm publish (PicoLidar->system_current pattern), adding
host-side Beta conversion to temp_therm0/1/2 (5V/10k divider, R0=10k@25C,
B=3380). Keeps the rfswitch stream a pure categorical signal. Emulator
default 1.65->2.5V (=25C)."
```

---

## Bridge: install the branch picohost into the eigsep venv

- [ ] **Step 1: Editable-install picohost from the branch**

Run: `cd /home/eigsep/eigsep/eigsep_observing && uv pip install -e /home/eigsep/eigsep/pico-firmware/picohost`

- [ ] **Step 2: Verify the new API + handler are live**

Run:
```bash
cd /home/eigsep/eigsep/eigsep_observing
.venv/bin/python -c "from picohost.base import PicoRFSwitch as P; print('PATHS', hasattr(P,'PATHS'), 'path_str', hasattr(P,'path_str')); print('temp@2.5V', round(P._therm_temp_c(2.5),3))"
```
Expected: `PATHS True path_str False` and `temp@2.5V 25.0`.

Do **not** run `uv run pytest` / `uv sync` after this (they re-resolve from the lock and revert to PyPI picohost 3.11.0). Use `.venv/bin/pytest` for all eigsep tests.

---

## Task 2: eigsep_observing — follow the `path_str`→`PATHS` rename

**Repo:** `/home/eigsep/eigsep/eigsep_observing` (branch `feat/rfswitch-eeprom-thermistors`)

**Files:**
- Modify: `src/eigsep_observing/client.py:26` (and docstring at `:285`)
- Modify: `scripts/rfswitch_manual.py:37`
- Modify: `tests/test_io.py:1336, 3070` (fixture expressions)

**Interfaces:**
- Produces: `client.VALID_SWITCH_STATES = set(PicoRFSwitch.PATHS)` (16 states incl. new `VNAAMB`, `VNASP1/2`, `RFAMB`, `RFSP1/2`).

- [ ] **Step 1: Confirm the breakage (test as spec)**

Run: `.venv/bin/pytest tests/test_client.py -q --co`
Expected: FAIL — collection error `AttributeError: type object 'PicoRFSwitch' has no attribute 'path_str'` (from `client.py:26` at import).

- [ ] **Step 2: Fix `client.py`**

`src/eigsep_observing/client.py:26` — change:

```python
VALID_SWITCH_STATES = set(PicoRFSwitch.PATHS)
```

Also fix the stale docstring example at `src/eigsep_observing/client.py:285` — replace `sw("RFLOAD")` (never a valid state) with a real path:

```python
        ...     sw("RFAMB")
```

- [ ] **Step 3: Fix `rfswitch_manual.py`**

`scripts/rfswitch_manual.py:37` — change:

```python
STATES = list(PicoRFSwitch.PATHS)
```

- [ ] **Step 4: Fix the `test_io.py` fixtures**

At `tests/test_io.py:1336` and `tests/test_io.py:3070`, replace:

```python
                    "sw_state": PicoRFSwitch.rbin(PicoRFSwitch.path_str[name]),
```

with (the address is already the int — no `rbin`):

```python
                    "sw_state": PicoRFSwitch.PATHS[name],
```

- [ ] **Step 5: Run the affected tests**

Run:
```bash
.venv/bin/pytest tests/test_client.py -q
.venv/bin/pytest tests/test_io.py -k "rfswitch or metadata_end_to_end or switch" -q
```
Expected: PASS (import restored; rfswitch round-trip still asserts the state-name string, which is unchanged).

- [ ] **Step 6: Lint + commit**

Run: `.venv/bin/ruff check . && .venv/bin/ruff format --check .`

```bash
git add src/eigsep_observing/client.py scripts/rfswitch_manual.py tests/test_io.py
git commit -m "fix(rfswitch): follow PicoRFSwitch.path_str -> PATHS rename

path_str/rbin removed upstream (EEPROM path addressing). Swap the three
call sites to PATHS (address int, no binary conversion); fix stale
sw(\"RFLOAD\") docstring example. No public-API change: state names are
sourced from the firmware class."
```

---

## Task 3: eigsep_observing — `rfswitch_therm` schema + producer-contract fixture

**Files:**
- Modify: `src/eigsep_observing/io.py` (`SENSOR_SCHEMAS`, after the `rfswitch` entry ~line 881)
- Modify: `src/eigsep_observing/contract_tests/test_producer_contracts.py` (`_rfswitch_post_handler_reading` ~line 143 → two-publish; `SENSOR_EMULATORS` ~line 252)

**Interfaces:**
- Consumes: `PicoRFSwitch._rfswitch_redis_handler` two-publish behavior (Task 1).
- Produces: `SENSOR_SCHEMAS["rfswitch_therm"]` with `sensor_name, status: str` and `volt_therm0/1/2, temp_therm0/1/2: float`; `SENSOR_EMULATORS["rfswitch_therm"]`.

- [ ] **Step 1: Show the contract test fails (emulator now fans out)**

Run: `.venv/bin/pytest "src/eigsep_observing/contract_tests/test_producer_contracts.py::test_every_schema_has_conforming_emulator[rfswitch]" -q`
Expected: FAIL — the current single-dict `_rfswitch_post_handler_reading` merges both publishes via `.update`, so `sensor_name` becomes `"rfswitch_therm"` and validation reports extra/missing keys.

- [ ] **Step 2: Add the schema**

In `src/eigsep_observing/io.py`, immediately after the `"rfswitch": { … }` entry in `SENSOR_SCHEMAS`, add:

```python
    # `rfswitch_therm`: three PCB thermistors on the RF switch board,
    # fanned out of the switch-state line by
    # PicoRFSwitch._rfswitch_redis_handler (the system_current pattern) so
    # the categorical rfswitch stream stays pure. Like system_current it is
    # a derived stream with no `app_id`. `volt_therm<i>` is the raw
    # 3.3V-referenced ADC-pin voltage; `temp_therm<i>` is the host-side
    # datasheet-Beta conversion in degrees C (None when the channel reads
    # out of range). All six floats reduce via the standard float->mean
    # path and land in the corr file (PCB temperature affects the cal
    # network); None short-circuits in _validate_metadata / _avg_sensor_values.
    "rfswitch_therm": {
        "sensor_name": str,
        "status": str,
        "volt_therm0": float,
        "volt_therm1": float,
        "volt_therm2": float,
        "temp_therm0": float,
        "temp_therm1": float,
        "temp_therm2": float,
    },
```

- [ ] **Step 3: Refactor the contract fixture to two publishes**

In `src/eigsep_observing/contract_tests/test_producer_contracts.py`, replace `_rfswitch_post_handler_reading` (~line 143) with a two-publish version (mirrors `_lidar_post_handler_readings`):

```python
def _rfswitch_post_handler_readings():
    """Return [rfswitch, rfswitch_therm] after _rfswitch_redis_handler.

    The rfswitch producer fans the three PCB thermistors out of the
    switch-state line into a separate rfswitch_therm stream (raw volts +
    host-derived degrees C), the same two-publish shape as
    PicoLidar -> system_current. Compose RFSwitchEmulator.get_status()
    through the real handler and capture both publishes in order. Mirrors
    _lidar_post_handler_readings.
    """
    sw = PicoRFSwitch.__new__(PicoRFSwitch)
    sw._name_by_state = {v: k for k, v in sw.__class__.paths.fget(sw).items()}
    captured = []
    sw._base_redis_handler = lambda d: captured.append(dict(d))
    sw._rfswitch_redis_handler(RFSwitchEmulator().get_status())
    assert len(captured) == 2, (
        f"expected rfswitch + rfswitch_therm publishes, got {len(captured)}"
    )
    return captured[0], captured[1]
```

- [ ] **Step 4: Register both streams in `SENSOR_EMULATORS`**

In the `SENSOR_EMULATORS` dict (~line 252), replace the line
`"rfswitch": _rfswitch_post_handler_reading,` with:

```python
    "rfswitch": lambda: _rfswitch_post_handler_readings()[0],
    "rfswitch_therm": lambda: _rfswitch_post_handler_readings()[1],
```

- [ ] **Step 5: Run the contract + io tests**

Run:
```bash
.venv/bin/pytest src/eigsep_observing/contract_tests/test_producer_contracts.py -q
.venv/bin/pytest tests/test_io.py -q
```
Expected: PASS — both `[rfswitch]` and `[rfswitch_therm]` parametrizations validate (2.5 V → 25.0 °C float), and rfswitch schema still validates the pure switch-state line.

- [ ] **Step 6: Lint + commit**

```bash
.venv/bin/ruff check . && .venv/bin/ruff format --check .
git add src/eigsep_observing/io.py src/eigsep_observing/contract_tests/test_producer_contracts.py
git commit -m "feat(io): add rfswitch_therm schema + two-publish producer contract

New rfswitch_therm sensor stream (volt_therm0/1/2 + host-derived
temp_therm0/1/2, no app_id) fanned out of the rfswitch line. Refactor the
rfswitch contract fixture to capture both publishes (lidar pattern) and
register both in SENSOR_EMULATORS."
```

---

## Task 4: eigsep_observing — register three dashboard thermistor signals

**Files:**
- Modify: `src/eigsep_observing/live_status/signals.py` (`SIGNAL_REGISTRY`, after `system_current`/host signals ~line 171)
- Test: `tests/test_live_status_thresholds.py` (new registration + classify test)

**Interfaces:**
- Produces: signals `rfswitch_therm.temp_therm0/1/2` (`unit="C"`, `enabled_by=None`, `max_age_s=30.0`, no derived/YAML band → classifies `"unknown"`).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_live_status_thresholds.py` (near the `system_current` block ~line 322). It reuses that file's existing imports (`SIGNAL_REGISTRY`, `enabled_signals`, `Thresholds`, `OBS_CFG_TEMPCTRL_OFF`, `CORR_HEADER`):

```python
# rfswitch_therm PCB thermistor signals
# ---------------------------------------------------------------------


def test_rfswitch_therm_signals_registered_and_always_enabled():
    for i in range(3):
        name = f"rfswitch_therm.temp_therm{i}"
        sig = SIGNAL_REGISTRY[name]
        assert sig.unit == "C"
        assert sig.enabled_by is None  # board vital, never gated
        # present even with optional subsystems off
        assert name in enabled_signals(OBS_CFG_TEMPCTRL_OFF)


def test_rfswitch_therm_classifies_unknown_without_band():
    # No config-derived and no bundled-YAML band -> grey "unknown" tile.
    th = Thresholds(OBS_CFG_TEMPCTRL_OFF, CORR_HEADER)
    assert th.classify("rfswitch_therm.temp_therm0", 30.0) == "unknown"
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_live_status_thresholds.py -k rfswitch_therm -v`
Expected: FAIL — `KeyError: 'rfswitch_therm.temp_therm0'` (not registered).

- [ ] **Step 3: Register the signals**

In `src/eigsep_observing/live_status/signals.py`, add to `SIGNAL_REGISTRY` (after the `host_panda.temp_c` entry, before the site-geometry block):

```python
    # RF switch PCB thermistors — three 10k NTC on the switch board, fanned
    # out of the switch-state line into the rfswitch_therm stream. Host-side
    # datasheet-Beta conversion (~+/-1-2C), so no config-derived band —
    # grey "unknown" tiles until an empirical band is added to the YAML
    # override. Always enabled (a board vital, like system_current).
    "rfswitch_therm.temp_therm0": Signal(
        "rfswitch_therm.temp_therm0",
        "RF switch PCB temp 0",
        unit="C",
    ),
    "rfswitch_therm.temp_therm1": Signal(
        "rfswitch_therm.temp_therm1",
        "RF switch PCB temp 1",
        unit="C",
    ),
    "rfswitch_therm.temp_therm2": Signal(
        "rfswitch_therm.temp_therm2",
        "RF switch PCB temp 2",
        unit="C",
    ),
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/pytest tests/test_live_status_thresholds.py -q`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```bash
.venv/bin/ruff check . && .venv/bin/ruff format --check .
git add src/eigsep_observing/live_status/signals.py tests/test_live_status_thresholds.py
git commit -m "feat(live_status): register rfswitch_therm PCB thermistor signals

Three temp_therm0/1/2 signals (degC), always enabled, no derived band ->
grey 'unknown' tiles until an empirical band is tuned in the YAML override."
```

---

## Task 5: eigsep_observing — fold thermistor rows into the rfswitch pane

**Files:**
- Modify: `src/eigsep_observing/live_status/static/js/dashboard.js` (`renderRfswitch` ~line 916; call site ~line 1013)
- Test: `tests/test_live_status_app.py` (new `_metadata_payload` test)

**Interfaces:**
- Consumes: `SENSOR_SCHEMAS["rfswitch_therm"]` (Task 3), signals (Task 4), the generic `/api/metadata` projection.
- Produces: three °C rows in the `#rfswitch` pane.

- [ ] **Step 1: Write the backend projection guard test**

Add to `tests/test_live_status_app.py` (after `test_metadata_payload_classifies_system_current` ~line 1567). This is a **regression guard** proving the generic `/api/metadata` projection already serves `rfswitch_therm` with a per-channel classify tag once the schema (Task 3) and signals (Task 4) exist — there is no `app.py`/backend change in this task. The JS render is the real deliverable, verified end-to-end in Step 5 (no JS unit harness exists). It reuses that file's `_payload_thresholds()` helper and `StateSnapshot`/`_metadata_payload` imports:

```python
def test_metadata_payload_classifies_rfswitch_therm():
    """rfswitch_therm rides the generic /api/metadata projection: each
    temp_therm* field is classified against its registered signal (no band
    -> 'unknown'), and the raw volts pass through in `value`."""
    from eigsep_observing.live_status.app import _metadata_payload
    from eigsep_observing.live_status.aggregator import StateSnapshot

    now = 1000.0
    state = StateSnapshot()
    state.metadata_snapshot = {
        "rfswitch_therm": {
            "sensor_name": "rfswitch_therm",
            "status": "update",
            "volt_therm0": 2.5,
            "volt_therm1": 2.5,
            "volt_therm2": 2.5,
            "temp_therm0": 25.0,
            "temp_therm1": 25.0,
            "temp_therm2": 25.0,
        },
        "rfswitch_therm_ts": now,
    }
    state.metadata_snapshot_read_unix = now
    payload = _metadata_payload(state, _payload_thresholds())
    entry = payload["rfswitch_therm"]
    assert entry["status"] == "update"
    assert entry["value"]["temp_therm0"] == 25.0
    assert entry["classify"]["rfswitch_therm.temp_therm0"] == "unknown"
```

- [ ] **Step 2: Run the guard test**

Run: `.venv/bin/pytest tests/test_live_status_app.py -k rfswitch_therm -v`
Expected: **PASS** — with Task 3 (schema) and Task 4 (signals) already merged, the generic projection serves `rfswitch_therm` and classifies `temp_therm0` as `"unknown"` (no band). This confirms no backend change is needed; the JS fold-in below is verified manually in Step 5. (If it fails with `KeyError`/classify-missing, Task 3 or Task 4 was not applied — fix that first.)

- [ ] **Step 3: Extend `renderRfswitch` in `dashboard.js`**

In `src/eigsep_observing/live_status/static/js/dashboard.js`, change the `renderRfswitch` signature and append thermistor rows before `el.append(row1, row2);`. Replace the function's signature line and the final `el.append(row1, row2);`:

```javascript
function renderRfswitch(rf, metaEntry, thermEntry) {
```

and, immediately before `el.append(row1, row2);`, insert:

```javascript
  // PCB thermistors (rfswitch_therm stream): three °C rows, raw V in
  // parens, classify tile per channel (grey "unknown" until a band is set).
  const rows = [row1, row2];
  const tv = (thermEntry && thermEntry.value) || null;
  const tc = (thermEntry && thermEntry.classify) || {};
  if (tv) {
    for (let i = 0; i < 3; i++) {
      const t = tv[`temp_therm${i}`];
      const v = tv[`volt_therm${i}`];
      const label = t !== null && t !== undefined
        ? `${fmt(t, 1)}°C (${fmt(v, 3)} V)`
        : `— (${fmt(v, 3)} V)`;
      const cls = tc[`rfswitch_therm.temp_therm${i}`] || "unknown";
      const trow = document.createElement("div");
      trow.className = "metadata-row";
      trow.append(
        makeSpan("label", `PCB temp ${i}`),
        makeSpan(tileClass(cls), label),
        makeSpan("value", "")
      );
      rows.push(trow);
    }
  }
  el.append(...rows);
```

Remove the now-redundant original `el.append(row1, row2);` line (replaced by `el.append(...rows);`).

- [ ] **Step 4: Update the call site**

At `dashboard.js:1013`, change:

```javascript
    renderRfswitch(rfswitch.data, metadata.data["rfswitch"], metadata.data["rfswitch_therm"]);
```

- [ ] **Step 5: Run backend test + verify the app renders**

Run: `.venv/bin/pytest tests/test_live_status_app.py -q`
Expected: PASS.

Then drive the dashboard end-to-end (the JS has no unit harness) using the project's run pattern:
```bash
.venv/bin/python scripts/live_status.py --dummy   # or the documented dummy/live invocation
```
Load the dashboard, confirm the RF switch card shows three "PCB temp N" rows at ~25 °C (dummy emulator default 2.5 V). Stop the app. (If `live_status.py` needs specific flags, check `scripts/CLAUDE.md` / `--help`.)

- [ ] **Step 6: Lint + commit**

```bash
.venv/bin/ruff check . && .venv/bin/ruff format --check .
git add src/eigsep_observing/live_status/static/js/dashboard.js tests/test_live_status_app.py
git commit -m "feat(live_status): show RF switch PCB thermistor temps in the rfswitch pane

Fold three temp_therm rows (degC, raw V in parens, per-channel classify
tile) into renderRfswitch from the rfswitch_therm metadata stream. Backend
/api/metadata projection is already generic; no app.py change."
```

---

## Task 6: Full-suite verification + watch_sensors spot-check + follow-up note

**Files:**
- Modify: `docs/superpowers/plans/2026-07-02-rfswitch-eeprom-thermistors.md` (append a "picohost release follow-up" note) — optional
- No source changes expected; this task is the integration gate.

- [ ] **Step 1: Full eigsep_observing suite**

Run: `.venv/bin/pytest -q`
Expected: PASS (whole suite, including coverage). Investigate any failure before proceeding.

- [ ] **Step 2: `watch_sensors` renders the new stream (no code change expected)**

Confirm `rfswitch_therm` is discoverable and displays all six fields:
```bash
.venv/bin/python -c "from eigsep_observing.io import SENSOR_SCHEMAS; print('rfswitch_therm' in SENSOR_SCHEMAS, list(SENSOR_SCHEMAS['rfswitch_therm']))"
```
Expected: `True ['sensor_name', 'status', 'volt_therm0', 'volt_therm1', 'volt_therm2', 'temp_therm0', 'temp_therm1', 'temp_therm2']`.
Optionally run `.venv/bin/python scripts/watch_sensors.py --dummy` and confirm an `=== rfswitch_therm ===` block appears with `volt_therm*` and `temp_therm*`.

- [ ] **Step 3: Lint gate (whole repo)**

Run: `.venv/bin/ruff check . && .venv/bin/ruff format --check .`
Expected: clean.

- [ ] **Step 4: Record the picohost release follow-up**

The eigsep_observing venv currently runs the branch picohost via editable install; `pyproject.toml` still pins `picohost>=3.11.0` and `uv.lock` resolves PyPI 3.11.0. **Do not** bump the floor or re-lock in this branch — that happens once picohost cuts the release containing the fan-out (e.g. 3.12.0). Add a one-line reminder to the PR description: "Depends on picohost >= <release with rfswitch_therm fan-out>; bump the floor + `uv lock` when it publishes."

- [ ] **Step 5: Commit (if the plan note was edited)**

```bash
git add docs/superpowers/plans/2026-07-02-rfswitch-eeprom-thermistors.md
git commit -m "docs: note picohost release follow-up for rfswitch_therm dependency"
```

---

## Self-review notes

- **Spec coverage:** Item 2 rename → Task 2. Item 1 producer fan-out + conversion → Task 1. Schema + contract → Task 3. `watch_sensors` (zero code) → Task 6 Step 2. Dashboard signals → Task 4. Dashboard pane fold-in → Task 5. Cross-repo lockstep + version-floor → Bridge + Task 6 Step 4.
- **Corr file:** `rfswitch_therm` floats flow through `_avg_sensor_values` automatically (no task needed beyond the schema in Task 3); covered by `tests/test_io.py` running green in Task 3 Step 5.
- **Type consistency:** `_therm_temp_c` returns `float|None`; schema types `temp_therm*`/`volt_therm*` as `float` (None tolerated by `_validate_metadata`'s None short-circuit). `renderRfswitch(rf, metaEntry, thermEntry)` arity matches the updated call site.
