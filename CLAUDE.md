# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
```bash
pytest                    # Run all tests
pytest tests/test_*.py    # Run specific test file
pytest -k "test_name"     # Run tests matching pattern
```

### Code Quality
```bash
black .                   # Format code (line length 79)
flake8                    # Lint code
```

### Package Management
```bash
pip install -e .          # Install in development mode
pip install -e ".[dev]"   # Install with development dependencies
```

## Architecture Overview

This is a distributed radio astronomy control system for EIGSEP observations with these key architectural components:

### Core Control Flow
1. **EigObserver** (`observer.py`) - Main orchestrator that manages observation schedules
2. **PandaClient** (`client.py`) - Remote client running on suspended hardware (PANDA computer)
3. **EigsepRedis** (`redis.py`) - Redis-based message bus for distributed communication
4. **EigsepFpga** (`fpga.py`) - FPGA/SNAP correlator interface and control

### Distributed System Design
- **Main Computer (Ground)**: Runs EigObserver, controls overall observation schedule
- **PANDA Computer (Suspended Box)**: Runs PandaClient, interfaces with hardware sensors
- **Communication**: Redis streams for real-time data and control commands

### Hardware Integration
- **SNAP Correlator**: Primary data acquisition from radio telescope
- **VNA (Vector Network Analyzer)**: Secondary measurement system
- **Switch Network**: Automated switching between sky/load/noise calibration states
- **Sensors**: Temperature, humidity, and other environmental monitoring

### Key Data Flow
1. EigObserver creates observation schedules with `make_schedule()`
2. Commands sent via Redis streams to PandaClient
3. PandaClient executes hardware operations (VNA, switches, sensors)
4. Data flows back through Redis for storage and real-time monitoring
5. Live web interface available via `live_status.py` Flask server

### Configuration System
- **Correlator Config**: SNAP correlator settings in `config/corr_config.yaml` (FPGA files, sample rates, etc.)
- **Observation Config**: Observation parameters in `config/obs_config.yaml` (switch schedules, file handling, VNA settings)
- **Dummy Config**: Test configuration in `config/dummy_config.yaml` for hardware-free development
- Configurations loaded using `eigsep_corr.config.load_config()` from YAML files

### Testing Strategy
- Dummy implementations in `testing/` module for hardware-free development
- DummyEigsepRedis and DummySensor classes for unit testing
- Tests cover Redis communication, sensor integration, and observation logic

## Important Development Notes

### Redis Communication Pattern
All distributed control uses Redis streams with specific naming:
- `stream:ctrl` - Control commands to remote systems
- `stream:status` - Status updates from remote systems  
- `stream:data:{sensor_name}` - Sensor data streams

### Observation Scheduling
The `make_schedule()` function creates cyclic observation patterns with keys:
- `vna`: Number of VNA measurements
- `snap_repeat`: Number of SNAP measurements per cycle
- `sky`, `load`, `noise`: Calibration state repetitions

### Hardware Abstraction
Sensors inherit from abstract `Sensor` base class requiring:
- `from_sensor()` method implementation
- Redis integration for data streaming
- Error handling for hardware connectivity issues

### File I/O Patterns  
- HDF5 format for correlator data storage
- JSON metadata alongside data files  
- VNA S11 measurements saved separately
- Configurable save directories via YAML config files (corr_config.yaml, obs_config.yaml)

### Network Configuration
- **Raspberry Pi (SNAP)**: 10.10.10.10 - Controls SNAP correlator
- **PANDA Computer**: 10.10.10.12 - Runs sensors and VNA in suspended box  
- **SNAP Board**: 10.10.10.13 - FPGA correlator hardware
- **VNA**: 127.0.0.1:5025 - Vector Network Analyzer interface

### Hardware Interface Details
- **Sensor Picos**: Multiple Raspberry Pi Pico devices for different sensors (IMU, temperature, etc.)
- **Switch Control**: Automated RF switching via dedicated Pico controller
- **VNA Integration**: S11 measurements with configurable frequency range and power settings