import argparse
import logging

from eigsep_redis import Transport

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
        "--redis-host", default="10.10.10.10", help="Redis host (SNAP/rpi_ip)"
    )
    parser.add_argument(
        "--redis-port", type=int, default=6379, help="Redis port"
    )
    parser.add_argument(
        "--channel",
        type=int,
        default=500,
        help="Channel index to track over time in its own strip-chart "
        "subplot (retains history across integrations). Pass a "
        "negative value (e.g. -1) to disable this subplot.",
    )
    parser.add_argument(
        "--metadata-key",
        default="tempctrl_load",
        help="Pico sensor stream to plot alongside the corr spectra "
        "(read from the panda-side Redis). Pass an empty string "
        "to disable this subplot.",
    )
    parser.add_argument(
        "--metadata-field",
        default="T_now",
        help="Field within --metadata-key's snapshot dict to plot.",
    )
    parser.add_argument(
        "--panda-host",
        default="10.10.10.11",
        help="Redis host for --metadata-key (panda_ip).",
    )
    parser.add_argument(
        "--panda-port", type=int, default=6379, help="Panda Redis port"
    )
    parser.add_argument(
        "--history-len",
        type=int,
        default=200,
        help="Points retained in the channel-history and metadata "
        "strip charts.",
    )

    args = parser.parse_args()

    channel = args.channel if args.channel is not None and args.channel >= 0 else None
    metadata_key = args.metadata_key or None

    # Connect to Redis (SNAP side; required)
    transport = Transport(host=args.redis_host, port=args.redis_port)
    print(f"Connected to Redis at {args.redis_host}:{args.redis_port}")

    # Panda-side Redis is only needed for the metadata strip chart, and
    # is lazy + connection-failure-tolerant so a dead panda never blocks
    # the corr spectra (corr data is sacred).
    transport_panda = None
    if metadata_key is not None:
        transport_panda = Transport(
            host=args.panda_host, port=args.panda_port, lazy=True
        )
        print(
            f"Metadata ({metadata_key}.{args.metadata_field}) from "
            f"{args.panda_host}:{args.panda_port}"
        )

    # Create and start plotter
    plotter = LivePlotter(
        transport=transport,
        pairs=args.pairs,
        plot_delay=args.delay,
        log_scale=not args.linear,
        poll_interval=getattr(args, "poll_interval", 50),
        channel=channel,
        history_len=args.history_len,
        transport_panda=transport_panda,
        metadata_key=metadata_key,
        metadata_field=args.metadata_field,
    )

    plotter.start()


if __name__ == "__main__":
    main()
