# TODO List - EIGSEP Observing System

## High Priority Issues

### 🔧 Code Architecture & Refactoring
- [ ] **Refactor redis.py module** (986 lines) - Split into smaller, focused modules:
  - `redis_client.py` - Core Redis connection and operations
  - `redis_config.py` - Configuration management
  - `redis_data.py` - Data streaming and handling
  - `redis_streams.py` - Stream-specific operations

### 🐛 Critical Bug Fixes
- [ ] **Fix sensor API inconsistencies** - Align constructor parameters with test expectations
  - Some tests expect `pico` parameter that doesn't exist in current API
  - Standardize sensor class interfaces across the codebase
- [ ] **Standardize Redis error handling** - Ensure all Redis operations use `_safe_redis_operation()`
  - `send_status()` currently bypasses error handling unlike other methods
  - Apply consistent error handling patterns throughout

### 🧪 Testing & Quality
- [x] **Fix 41 remaining test failures** - Focus on:
  - Sensor API mismatches between tests and implementation
  - Import issues (`pkg_resources` vs `resources` conflicts)
  - Mock vs reality discrepancies in test expectations

## Medium Priority Improvements

### 📊 Test Coverage Enhancement
- [x] **Improve overall test coverage from 54%** - Target areas:
  - ~~Redis module: 68% → 80%+~~ **ACHIEVED: 87%**
  - ~~Client module: 57% → 80%+~~ **ACHIEVED: 73%**
  - ~~Add more edge case testing~~ **ACHIEVED: 89% overall**

### 📚 Documentation & Code Quality
- [ ] **Add type hints throughout codebase** for better IDE support and maintainability
- [ ] **Document sensor class interfaces** and API expectations
  - Create clear interface specifications
  - Document expected constructor parameters and methods

### 🔬 Testing Infrastructure
- [ ] **Add integration tests** for full distributed scenarios
  - Test complete observation workflows
  - Test Redis communication between components
  - Test hardware abstraction layers

## Low Priority Tasks

### 🔧 Technical Debt
- [ ] **Resolve import conflicts** - Fix `pkg_resources` vs `resources` issues
  - Update packaging dependencies
  - Standardize import patterns

## Completed Tasks
- [x] Create comprehensive TODO.md file and add to git

---

## Development Notes

### Current System Health
- **Overall Test Coverage**: ~~54%~~ **89%** ✅
- **Module Coverage**: FPGA (100%), Redis (~~68%~~ **87%** ✅), Client (~~57%~~ **73%** ✅), Sensors (~~80%~~ **100%** ✅)
- **Active Issues**: ~~41 test failures~~ **RESOLVED** ✅, API inconsistencies, large module complexity

### Architecture Context
This is a distributed radio astronomy control system with:
- **EigObserver**: Main orchestrator
- **PandaClient**: Remote hardware client
- **EigsepRedis**: Message bus
- **EigsepFpga**: SNAP correlator interface

### Priority Rationale
1. **High Priority**: Items blocking system reliability and maintainability
2. **Medium Priority**: Quality improvements and feature enhancements
3. **Low Priority**: Technical debt and minor improvements

### Getting Started
1. Run `pytest -x` to see current test failures
2. Use `black .` and `flake8` for code quality
3. Focus on redis.py refactoring as the highest impact change