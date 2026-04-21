# EIGSEP Observing

[![codecov](https://codecov.io/gh/EIGSEP/eigsep_observing/graph/badge.svg?token=GK8ZZOJ57W)](https://codecov.io/gh/EIGSEP/eigsep_observing)

Control code needed to take EIGSEP data.

## Installation

```bash
pip install -e ".[dev]"
```

Pulls [`eigsep_redis`](https://github.com/EIGSEP/eigsep_redis) as a
sibling runtime dependency (Redis transport + bus primitives, also
consumed by `picohost`).

### Hardware dependency

Talking to the SNAP board requires
[casperfpga](https://github.com/EIGSEP/casperfpga), which is **not** on PyPI
and must be installed from source. It is a lazy optional import, so the test
suite and any dummy-mode / panda-side install does not need it. On the ground
computer that actually drives the correlator, install the pinned version from
`hardware-requirements.txt`:

```bash
pip install -r hardware-requirements.txt
```

See that file for the current tag (currently **v0.6.0**).

## Scripts

Observing loops and the startup flow live in `OPERATIONS.md`. Motor
operations run through `PicoManager` via Redis, so the manager service
stays up during scans.

```bash
# Az/el beam scan
python scripts/motor_control.py [--dummy] [--el_first] [--count N] \
                                [--pause_s S] [--sleep_s S]

# Interactive zeroing UI (curses)
python scripts/motor_manual.py  [--dummy] [--deg D]
```
