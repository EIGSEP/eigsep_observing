# EIGSEP Live Status Dashboard

A real-time web dashboard for monitoring the distributed EIGSEP radio astronomy observation system.

## Overview

The Live Status App provides a comprehensive view of your EIGSEP observation system by connecting to both `redis_panda` (PANDA computer) and `redis_snap` (SNAP correlator) to display:

- **System Health**: Overall status of PANDA and SNAP components
- **Sensor Data**: Real-time readings from IMU, temperature, humidity, and other sensors
- **Correlator Status**: SNAP correlator performance and configuration
- **Connection Status**: Live monitoring of Redis connections

## Features

- 🔄 **Auto-refresh**: Updates every 5 seconds automatically
- 📱 **Responsive Design**: Works on desktop and mobile devices
- 🚨 **Error Handling**: Graceful handling of connection failures
- 📊 **Multiple Views**: Organized data in intuitive cards
- 🎨 **Modern UI**: Clean, professional interface
- ⚡ **Real-time**: Live data from Redis streams

## Quick Start

### Basic Usage

```bash
# Run with default network settings
python live_status.py

# Specify custom Redis hosts
python live_status.py --panda-host 10.10.10.12 --snap-host 10.10.10.10

# Run on different web port
python live_status.py --web-port 8080

# Production mode (disable debug)
python live_status.py --no-debug --web-host 0.0.0.0
```

### Network Configuration

The app expects the following default network setup:
- **redis_panda**: `10.10.10.12:6379` (PANDA computer sensors/client data)
- **redis_snap**: `10.10.10.10:6379` (SNAP correlator data)
- **Web server**: `localhost:5000` (dashboard interface)

## Command Line Options

```bash
usage: live_status.py [-h] [--panda-host PANDA_HOST] [--snap-host SNAP_HOST]
                      [--redis-port REDIS_PORT] [--web-host WEB_HOST]
                      [--web-port WEB_PORT] [--no-debug]

options:
  --panda-host PANDA_HOST    IP address of redis_panda (default: 10.10.10.12)
  --snap-host SNAP_HOST      IP address of redis_snap (default: 10.10.10.10)
  --redis-port REDIS_PORT    Redis port number (default: 6379)
  --web-host WEB_HOST        Web server host address (default: localhost)
  --web-port WEB_PORT        Web server port (default: 5000)
  --no-debug                 Disable Flask debug mode
```

## API Endpoints

The app provides REST API endpoints for programmatic access:

- `GET /api/status` - Complete system status from both Redis instances
- `GET /api/health` - System health check and component status
- `GET /api/sensors` - Detailed sensor data from PANDA
- `GET /api/correlator` - SNAP correlator status and configuration

### Example API Usage

```bash
# Get overall system status
curl http://localhost:5000/api/status

# Check system health
curl http://localhost:5000/api/health

# Get sensor readings
curl http://localhost:5000/api/sensors
```

## Data Sources

### PANDA System (redis_panda)
- **IMU Sensors**: Azimuth/elevation orientation data
- **Temperature**: Multiple thermistor readings
- **Environment**: Humidity, pressure, LIDAR distance
- **Control Systems**: Peltier, switches, VNA status
- **Client Status**: Heartbeat and connectivity

### SNAP Correlator (redis_snap)
- **Correlation Data**: Accumulation counts and data rates
- **Configuration**: FPGA correlator settings
- **Performance**: Real-time processing metrics

## Installation Requirements

```bash
# Install required Python packages
pip install flask

# The app uses the existing eigsep_observing package
# No additional dependencies required
```

## Testing

Test the app without live Redis connections:

```bash
# Run the test suite
python test_live_status.py

# This will verify all API endpoints work with dummy data
```

## Troubleshooting

### Connection Issues

If you see Redis connection errors:

1. **Check network connectivity**:
   ```bash
   ping 10.10.10.12  # PANDA
   ping 10.10.10.10  # SNAP
   ```

2. **Verify Redis is running**:
   ```bash
   redis-cli -h 10.10.10.12 ping
   redis-cli -h 10.10.10.10 ping
   ```

3. **Test with local Redis** (for development):
   ```bash
   python live_status.py --panda-host localhost --snap-host localhost
   ```

### Performance

- The app auto-pauses updates when the browser tab is not visible
- Each update fetches data from all APIs in parallel for efficiency
- Connection failures are handled gracefully without crashing

### Firewall/Network

Make sure the following ports are accessible:
- Redis: `6379` (both PANDA and SNAP)
- Web dashboard: `5000` (or your custom port)

## Architecture

```
┌─────────────────┐    ┌─────────────────┐
│   Web Browser   │◄───┤  Live Status    │
│   Dashboard     │    │   Flask App     │
└─────────────────┘    └─────────┬───────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
            ┌───────▼────────┐       ┌────────▼──────┐
            │ redis_panda    │       │ redis_snap    │
            │ (10.10.10.12)  │       │ (10.10.10.10) │
            │                │       │               │
            │ • Sensors      │       │ • Correlator  │
            │ • Client       │       │ • SNAP FPGA   │
            │ • Environment  │       │ • Config      │
            └────────────────┘       └───────────────┘
```

## Development

### Adding New Metrics

To add new sensor types or metrics:

1. **Backend**: Update the data categorization in `get_sensor_data()`
2. **Frontend**: Modify the sensor card rendering in `updateSensorCard()`
3. **Styling**: Add CSS classes for new sensor categories

### Customizing the Interface

- **Refresh Rate**: Change the interval in the JavaScript (currently 5000ms)
- **Styling**: Modify the CSS in `templates/index.html`
- **New Cards**: Add new dashboard cards by extending the grid layout

## Security Notes

- The app runs in debug mode by default for development
- Use `--no-debug` and `--web-host 0.0.0.0` for production
- Consider adding authentication for production deployments
- Redis connections are not encrypted (use VPN/secure network)

## Support

For issues with the Live Status App:
1. Check the console output for error messages
2. Verify Redis connectivity with the troubleshooting steps
3. Test with dummy data using `test_live_status.py`
4. Check the browser developer console for JavaScript errors