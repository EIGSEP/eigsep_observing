# eigsep_observing.testing

Dummy/fixture implementations of the hardware interfaces `eigsep_observing`
talks to, for running the observing software's tests (and dry-run demos)
without real FPGA/Panda/Pico hardware attached.

## Layout

| Module | Purpose |
|---|---|
| `fpga.py` | `DummyEigsepFpga` — stand-in for the real FPGA/correlator interface. |
| `client.py` | `DummyPandaClient`, `start_dummy_pico_manager` — stand-ins for the Panda/picohost hardware clients. Picohost naming here must track the installed `picohost` version (see `REPO_AUDIT.md` BF-2 — a naming-skew break between this module and real `picohost` cascaded into `eigsep_data` import failures once). |
| `observer.py` | `DummyEigObserver` — stand-in for the observing-session driver. |
| `utils.py` | Shared helpers for the fixtures above. |

Imported at package level by `eigsep_observing`'s test fixtures, so a naming
mismatch against the real hardware client packages breaks `import
eigsep_observing` entirely, not just the tests — keep this module's names in
sync with whatever hardware-client version is pinned.

## Recent changes

- 2026-09-15 (`software-engineer`): added this file (README-convention
  retrofit, fleet-wide consolidation pass).
