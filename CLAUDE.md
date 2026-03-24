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
