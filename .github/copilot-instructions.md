# GitHub Copilot Instructions for EIGSEP Observing

This repository contains control code for EIGSEP radio astronomy observations using a distributed system architecture.

## Project Overview

EIGSEP Observing is a distributed radio astronomy control system that manages:
- **SNAP correlator** for primary data acquisition
- **VNA (Vector Network Analyzer)** for secondary measurements
- **Environmental sensors** (temperature, humidity, etc.)
- **Automated calibration** through RF switch network

The system coordinates between a ground control computer and a suspended PANDA computer using Redis streams for communication.

## Development Setup

### Installation
```bash
pip install -e .              # Install in development mode
pip install -e ".[dev]"       # Install with dev dependencies
```

### Testing
```bash
pytest                        # Run all tests
pytest -x                     # Stop on first failure
pytest -k "test_name"         # Run specific tests
pytest --cov                  # Run with coverage
```

### Code Quality
```bash
black .                       # Format code (line length 79)
flake8                        # Lint code
```

## Architecture

### Core Components
1. **EigObserver** (`observer.py`) - Main orchestrator managing observation schedules
2. **PandaClient** (`client.py`) - Remote client on suspended hardware
3. **EigsepRedis** (`eig_redis.py`) - Redis message bus for distributed communication
4. **EigsepFpga** (`fpga.py`) - FPGA/SNAP correlator interface

### Configuration Files
- `config/corr_config.yaml` - SNAP correlator settings (FPGA files, sample rates)
- `config/obs_config.yaml` - Observation parameters (switch schedules, VNA settings)
- `config/dummy_config.yaml` - Test configuration for hardware-free development

Load configurations using `eigsep_corr.config.load_config()`.

## Coding Guidelines

### Testing Strategy
- Use dummy implementations from `testing/` module for hardware-free tests
- Prefer dummy instances over excessive mocking
- All tests should work without physical hardware
- Use `pytest -x` for fail-fast debugging

### Redis Communication
All distributed control uses Redis streams with specific naming:
- `stream:ctrl` - Control commands to remote systems
- `stream:status` - Status updates from remote systems
- `stream:data:{sensor_name}` - Sensor data streams

### File I/O
- **Data format**: HDF5 for correlator data
- **Metadata**: JSON alongside data files
- **VNA data**: Separate S11 measurement files
- **Directories**: Configurable via YAML config files

### Hardware Abstraction
Sensors must inherit from abstract `Sensor` base class and implement:
- `from_sensor()` method
- Redis integration for data streaming
- Error handling for connectivity issues

### Network Configuration
- Raspberry Pi (SNAP): 10.10.10.10
- PANDA Computer: 10.10.10.12
- SNAP Board: 10.10.10.13
- VNA: 127.0.0.1:5025

## Common Patterns

### Observation Scheduling
Use `make_schedule()` to create cyclic observation patterns with keys:
- `vna` - Number of VNA measurements
- `snap_repeat` - Number of SNAP measurements per cycle
- `sky`, `load`, `noise` - Calibration state repetitions

### Code Style
- Follow PEP 8 with line length 79 (enforced by Black)
- Use type hints where appropriate
- Match existing comment style in files
- Prefer existing libraries over adding new dependencies

## Important Notes

- **Dummy vs. Hardware**: Use dummy classes from `testing/` for development without hardware
- **MRO Inheritance**: `DummyEigsepFpga` requires specific inheritance order
- **Error Handling**: Use `_safe_redis_operation()` for Redis operations
- **Redis Streams**: Use proper structure: `[(stream_name, [(entry_id, fields)])]`

## Known Issues

- `eig_redis.py` is large (986 lines) and could benefit from splitting
- Some sensor API inconsistencies between tests and implementation
- Not all Redis operations consistently use error handling wrappers
