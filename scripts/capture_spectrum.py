#!/usr/bin/env python3
"""
Spectrum capture script for EIGSEP observing system.
Captures and saves correlation spectra from Redis data streams.
"""

import argparse
import sys

from eigsep_observing.redis import EigsepRedis
from eigsep_observing.capture import SpectrumCapture


def main():
    """Main function with command line interface."""
    parser = argparse.ArgumentParser(
        description="Capture correlation spectra from EIGSEP Redis"
    )
    parser.add_argument(
        "--pairs",
        nargs="+",
        default=["0", "1", "2", "3", "02", "13"],
        help="Correlation pairs to capture",
    )
    parser.add_argument(
        "--count",
        "-n",
        type=int,
        default=10,
        help="Number of spectra to capture",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Time interval between captures in seconds",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output filename (default: auto-generated)",
    )
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument(
        "--redis-port", type=int, default=6379, help="Redis port"
    )

    args = parser.parse_args()

    # Connect to Redis
    try:
        redis_client = EigsepRedis(host=args.redis_host, port=args.redis_port)
        print(f"Connected to Redis at {args.redis_host}:{args.redis_port}")
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        sys.exit(1)

    # Create spectrum capture instance
    capture = SpectrumCapture(redis_client)

    # Capture and save spectra
    try:
        filename = capture.save_last_n_spectra(
            n_spectra=args.count,
            pairs=args.pairs,
            filename=args.output,
            interval=args.interval,
        )
        print(f"Capture complete. Data saved to: {filename}")
    except KeyboardInterrupt:
        print("\nCapture interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error during capture: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
