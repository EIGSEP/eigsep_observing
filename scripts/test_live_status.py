#!/usr/bin/env python3
"""
Test script for the Live Status App

This script tests the live status app with dummy data when Redis is not
available.
"""

import json
from unittest.mock import Mock
from live_status import LiveStatusApp


def create_mock_redis():
    """Create a mock Redis instance with dummy data."""
    mock_redis = Mock()

    # Mock live metadata with sensor data
    mock_redis.get_live_metadata.return_value = {
        "imu_az": 45.5,
        "imu_el": 30.2,
        "imu_az_ts": "2025-01-01T12:00:00Z",
        "imu_el_ts": "2025-01-01T12:00:00Z",
        "therm_1": 22.5,
        "therm_2": 23.1,
        "therm_1_ts": "2025-01-01T12:00:00Z",
        "therm_2_ts": "2025-01-01T12:00:00Z",
        "humidity": 65.2,
        "pressure": 1013.25,
        "lidar": 150.0,
        "acc_cnt": 12345,
        "corr_status": "running",
    }

    # Mock data streams
    mock_redis.data_streams = {
        "imu_az": "stream_id_1",
        "therm_1": "stream_id_2",
        "acc_cnt": "stream_id_3",
    }

    # Mock heartbeat
    mock_redis.client_heartbeat_check.return_value = True

    # Mock correlator config
    mock_redis.get_corr_config.return_value = {
        "n_antennas": 2,
        "integration_time": 1.0,
        "sample_rate": 250e6,
    }

    return mock_redis


def test_app_with_dummy_data():
    """Test the app with dummy Redis data."""
    print("Testing Live Status App with dummy data...")

    # Create mock Redis instances
    mock_panda = create_mock_redis()
    mock_snap = create_mock_redis()

    # Create app instance
    app = LiveStatusApp(panda_host="127.0.0.1", snap_host="127.0.0.1")

    # Replace Redis connections with mocks
    app.redis_panda = mock_panda
    app.redis_snap = mock_snap

    # Test API endpoints
    print("\nTesting API endpoints...")

    with app.app.test_client() as client:
        # Test status endpoint
        response = client.get("/api/status")
        assert response.status_code == 200
        status_data = json.loads(response.data)
        print(f"✓ Status API: {len(status_data)} keys returned")

        # Test health endpoint
        response = client.get("/api/health")
        assert response.status_code == 200
        health_data = json.loads(response.data)
        print(
            f"✓ Health API: Overall status = {health_data['overall_status']}"
        )

        # Test sensors endpoint
        response = client.get("/api/sensors")
        assert response.status_code == 200
        sensor_data = json.loads(response.data)
        print(
            f"✓ Sensors API: {len(sensor_data['sensors'])} sensor categories"
        )

        # Test correlator endpoint
        response = client.get("/api/correlator")
        assert response.status_code == 200
        corr_data = json.loads(response.data)
        print(
            f"Correlator API: {len(corr_data['correlator_data'])} "
            "correlator metrics"
        )

        # Test main dashboard page
        response = client.get("/")
        assert response.status_code == 200
        print("✓ Dashboard HTML page loads successfully")

    print("\n All tests passed! The Live Status App is working correctly.")
    print("\n To run the app with real Redis connections:")
    print(
        "   python live_status.py --panda-host 10.10.10.12 "
        "--snap-host 10.10.10.10"
    )
    print(
        "\nTo run with local testing (will show connection errors but "
        "serve dummy data):"
    )
    print(
        "   python live_status.py --panda-host localhost --snap-host localhost"
    )


if __name__ == "__main__":
    test_app_with_dummy_data()
