#!/usr/bin/env python3
"""
Live Status App for EIGSEP Observations

This app provides a real-time web dashboard to monitor the distributed
radio astronomy system by connecting to both redis_panda and redis_snap
to display current observation status, sensor data, and system health.
"""

import json
import time
from datetime import datetime, timezone
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

import flask
from flask import Flask, render_template, jsonify, request

from eigsep_observing import EigsepRedis


class LiveStatusApp:
    """Live status monitoring application for EIGSEP observations."""
    
    def __init__(self, panda_host="10.10.10.12", snap_host="10.10.10.10", port=6379):
        """
        Initialize the live status app.
        
        Parameters
        ----------
        panda_host : str
            IP address for redis_panda (sensor/client data)
        snap_host : str  
            IP address for redis_snap (correlator data)
        port : int
            Redis port number
        """
        self.app = Flask(__name__, template_folder='templates')
        self.panda_host = panda_host
        self.snap_host = snap_host
        self.port = port
        
        # Initialize Redis connections with error handling
        self.redis_panda = None
        self.redis_snap = None
        self._init_redis_connections()
        
        # Setup Flask routes
        self._setup_routes()
    
    def _init_redis_connections(self):
        """Initialize Redis connections with fallback for testing."""
        try:
            self.redis_panda = EigsepRedis(host=self.panda_host, port=self.port)
            print(f"✓ Connected to redis_panda at {self.panda_host}:{self.port}")
        except Exception as e:
            print(f"⚠ Failed to connect to redis_panda: {e}")
            
        try:
            self.redis_snap = EigsepRedis(host=self.snap_host, port=self.port)
            print(f"✓ Connected to redis_snap at {self.snap_host}:{self.port}")
        except Exception as e:
            print(f"⚠ Failed to connect to redis_snap: {e}")
    
    def _setup_routes(self):
        """Setup Flask application routes."""
        
        @self.app.route('/')
        def index():
            """Main dashboard page."""
            return render_template('index.html')
        
        @self.app.route('/api/status')
        def api_status():
            """API endpoint for current system status."""
            return jsonify(self.get_system_status())
        
        @self.app.route('/api/sensors')
        def api_sensors():
            """API endpoint for sensor data."""
            return jsonify(self.get_sensor_data())
        
        @self.app.route('/api/correlator')
        def api_correlator():
            """API endpoint for correlator status."""
            return jsonify(self.get_correlator_status())
        
        @self.app.route('/api/health')
        def api_health():
            """API endpoint for system health check."""
            return jsonify(self.get_health_status())
    
    def get_system_status(self):
        """Get overall system status from both Redis instances."""
        status = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'panda_connected': self.redis_panda is not None,
            'snap_connected': self.redis_snap is not None,
            'panda_data': {},
            'snap_data': {}
        }
        
        # Get PANDA system data
        if self.redis_panda:
            try:
                panda_metadata = self.redis_panda.get_live_metadata()
                status['panda_data'] = {
                    'metadata': panda_metadata,
                    'client_alive': self._check_client_heartbeat(),
                    'active_streams': list(self.redis_panda.data_streams.keys())
                }
            except Exception as e:
                status['panda_data'] = {'error': str(e)}
        
        # Get SNAP correlator data
        if self.redis_snap:
            try:
                snap_metadata = self.redis_snap.get_live_metadata()
                status['snap_data'] = {
                    'metadata': snap_metadata,
                    'correlator_config': self._get_correlator_config(),
                    'active_streams': list(self.redis_snap.data_streams.keys())
                }
            except Exception as e:
                status['snap_data'] = {'error': str(e)}
        
        return status
    
    def get_sensor_data(self):
        """Get detailed sensor data from PANDA."""
        if not self.redis_panda:
            return {'error': 'Redis PANDA not connected'}
        
        try:
            metadata = self.redis_panda.get_live_metadata()
            
            # Organize sensor data by type
            sensors = {
                'imu': {},
                'temperature': {},
                'environment': {},
                'control': {}
            }
            
            for key, value in metadata.items():
                if key.endswith('_ts'):
                    continue  # Skip timestamp keys
                    
                if key.startswith('imu_'):
                    sensors['imu'][key] = value
                elif 'therm' in key.lower() or 'temp' in key.lower():
                    sensors['temperature'][key] = value
                elif key in ['humidity', 'pressure', 'lidar']:
                    sensors['environment'][key] = value
                elif key in ['peltier', 'switch', 'vna']:
                    sensors['control'][key] = value
                else:
                    sensors.setdefault('other', {})[key] = value
            
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'sensors': sensors,
                'client_heartbeat': self._check_client_heartbeat()
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def get_correlator_status(self):
        """Get correlator status from SNAP Redis."""
        if not self.redis_snap:
            return {'error': 'Redis SNAP not connected'}
        
        try:
            metadata = self.redis_snap.get_live_metadata()
            
            # Get correlator-specific data
            correlator_data = {}
            for key, value in metadata.items():
                if 'acc_cnt' in key or 'corr_' in key:
                    correlator_data[key] = value
            
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'correlator_data': correlator_data,
                'config': self._get_correlator_config(),
                'data_rate': self._calculate_data_rate(),
                'active_pairs': self._get_correlation_pairs()
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def get_health_status(self):
        """Get overall system health status."""
        health = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'overall_status': 'unknown',
            'components': {}
        }
        
        # Check PANDA health
        if self.redis_panda:
            try:
                client_alive = self._check_client_heartbeat()
                metadata_count = len(self.redis_panda.get_live_metadata())
                health['components']['panda'] = {
                    'status': 'healthy' if client_alive and metadata_count > 0 else 'degraded',
                    'client_alive': client_alive,
                    'metadata_count': metadata_count
                }
            except Exception as e:
                health['components']['panda'] = {'status': 'error', 'error': str(e)}
        else:
            health['components']['panda'] = {'status': 'disconnected'}
        
        # Check SNAP health
        if self.redis_snap:
            try:
                metadata_count = len(self.redis_snap.get_live_metadata())
                config_present = self._get_correlator_config() is not None
                health['components']['snap'] = {
                    'status': 'healthy' if config_present and metadata_count >= 0 else 'degraded',
                    'config_present': config_present,
                    'metadata_count': metadata_count
                }
            except Exception as e:
                health['components']['snap'] = {'status': 'error', 'error': str(e)}
        else:
            health['components']['snap'] = {'status': 'disconnected'}
        
        # Determine overall status
        component_statuses = [comp['status'] for comp in health['components'].values()]
        if all(status == 'healthy' for status in component_statuses):
            health['overall_status'] = 'healthy'
        elif any(status == 'error' for status in component_statuses):
            health['overall_status'] = 'error'
        elif any(status == 'disconnected' for status in component_statuses):
            health['overall_status'] = 'disconnected'
        else:
            health['overall_status'] = 'degraded'
        
        return health
    
    def _check_client_heartbeat(self):
        """Check if PANDA client is alive."""
        if not self.redis_panda:
            return False
        try:
            return self.redis_panda.client_heartbeat_check()
        except Exception:
            return False
    
    def _get_correlator_config(self):
        """Get correlator configuration from SNAP Redis."""
        if not self.redis_snap:
            return None
        try:
            return self.redis_snap.get_corr_config()
        except Exception:
            return None
    
    def _get_correlation_pairs(self):
        """Get active correlation pairs."""
        if not self.redis_snap:
            return []
        try:
            # Try to get correlation pairs from Redis set
            pairs = getattr(self.redis_snap, 'corr_pairs', set())
            return list(pairs) if pairs else []
        except Exception:
            return []
    
    def _calculate_data_rate(self):
        """Calculate approximate data rate from recent correlation data."""
        # This would require accessing stream data and calculating rate
        # For now, return placeholder
        return {'rate_mbps': 'unknown', 'last_update': 'unknown'}
    
    def run(self, host='localhost', port=5000, debug=True):
        """Run the Flask application."""
        print(f"\n🚀 EIGSEP Live Status App")
        print(f"📊 Dashboard: http://{host}:{port}")
        print(f"🔗 API Status: http://{host}:{port}/api/status")
        print(f"📡 PANDA: {self.panda_host}:{self.port}")
        print(f"🖥️  SNAP: {self.snap_host}:{self.port}")
        print()
        
        self.app.run(host=host, port=port, debug=debug)


def main():
    """Main entry point for the live status application."""
    parser = ArgumentParser(
        description="Live status dashboard for EIGSEP observations",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--panda-host",
        default="10.10.10.12",
        help="IP address of redis_panda (PANDA computer)",
    )
    parser.add_argument(
        "--snap-host", 
        default="10.10.10.10",
        help="IP address of redis_snap (SNAP/RPI)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port number",
    )
    parser.add_argument(
        "--web-host",
        default="localhost",
        help="Web server host address",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=5000,
        help="Web server port",
    )
    parser.add_argument(
        "--no-debug",
        action="store_true",
        help="Disable Flask debug mode",
    )
    
    args = parser.parse_args()
    
    # Create and run the live status app
    app = LiveStatusApp(
        panda_host=args.panda_host,
        snap_host=args.snap_host,
        port=args.redis_port
    )
    
    app.run(
        host=args.web_host,
        port=args.web_port,
        debug=not args.no_debug
    )


if __name__ == "__main__":
    main()