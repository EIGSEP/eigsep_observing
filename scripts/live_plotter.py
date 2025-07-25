import argparse
import logging

from eigsep_observing import EigsepRedis
from eigsep_observing.plot import LivePlotter

logger = logging.getLogger(__name__)


def main():
    """Main function with command line interface."""
    parser = argparse.ArgumentParser(
        description="Live plotter for EIGSEP correlation spectra"
    )
    parser.add_argument(
        "--pairs",
        nargs="+",
        default=None,
        help="Correlation pairs to plot",
    )
    parser.add_argument(
        "--delay", action="store_true", help="Plot delay spectrum"
    )
    parser.add_argument(
        "--linear",
        action="store_true",
        help="Use linear scale (default is log)",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=50,
        help="Polling interval in milliseconds to check for acc_cnt changes",
    )
    parser.add_argument(
        "--redis-host", default="10.10.10.10", help="Redis host"
    )
    parser.add_argument(
        "--redis-port", type=int, default=6379, help="Redis port"
    )

    args = parser.parse_args()

    # Connect to Redis
    redis_client = EigsepRedis(host=args.redis_host, port=args.redis_port)
    print(f"Connected to Redis at {args.redis_host}:{args.redis_port}")

    # Create and start plotter
    plotter = LivePlotter(
        redis_client=redis_client,
        pairs=args.pairs,
        plot_delay=args.delay,
        log_scale=not args.linear,
        poll_interval=getattr(args, "poll_interval", 50),
    )

    plotter.start()


if __name__ == "__main__":
    main()
