"""Always-on Raspberry Pi vitals publisher (``eigsep-host-health``).

Runs as a small systemd service on *both* pis (backend pi at
``rpi_ip``, panda pi at ``panda_ip``) and publishes the SoC
temperature to the pi's **local** Redis every ``--interval`` seconds
via :mod:`eigsep_observing.host_health`. The live-status dashboard
drains both Redis servers, so the same key constant on each side maps
to the right host without any per-host configuration — one identical
unit file ships to both pis.

Deliberately independent of ``eigsep-observe`` / ``panda_observe`` /
``pico-manager``: host thermals must keep flowing during manual
bring-up sessions and corr-only (panda-down) operation, the same
reasoning that keeps the peltier control loop out of ``panda_observe``
(see "Manual sessions" in the top-level CLAUDE.md).

The transport is built lazily so the service comes up cleanly at boot
even if Redis isn't accepting connections yet; ``host_health.publish``
swallows and WARNs on transport errors, so the loop itself is the
retry path.
"""

from argparse import ArgumentParser
import logging
import socket
import threading
from pathlib import Path
from typing import Optional

from eigsep_redis import Transport

from ..host_health import THERMAL_ZONE_PATH, publish, read_cpu_temp_c
from ..utils import configure_eig_logger

logger = logging.getLogger(__name__)


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description=(
            "Publish this pi's CPU temperature to its local Redis for "
            "the live-status dashboard."
        )
    )
    parser.add_argument(
        "--redis-host",
        default="localhost",
        help=(
            "Redis host to publish to. Each pi publishes to its own "
            "local Redis; the dashboard reads both."
        ),
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Publish cadence in seconds.",
    )
    return parser


def run(
    transport,
    *,
    interval_s: float,
    stop_event: Optional[threading.Event] = None,
    thermal_path: Path = THERMAL_ZONE_PATH,
    hostname: Optional[str] = None,
) -> None:
    """Publish loop: temperature read + K/V publish every interval.

    Both failure modes keep the loop alive: an unreadable thermal zone
    publishes ``temp_c: None`` (fresh timestamp proves the service is
    up), and a Redis outage is swallowed+WARNed by ``publish`` (the
    next iteration is the retry).
    """
    stop_event = stop_event or threading.Event()
    hostname = socket.gethostname() if hostname is None else hostname
    while not stop_event.is_set():
        publish(
            transport,
            temp_c=read_cpu_temp_c(path=thermal_path),
            hostname=hostname,
        )
        stop_event.wait(interval_s)


def main() -> None:
    configure_eig_logger(level=logging.INFO)
    args = build_parser().parse_args()
    # Lazy: don't fail at boot if Redis isn't up yet — publish retries.
    transport = Transport(
        host=args.redis_host, port=args.redis_port, lazy=True
    )
    logger.info(
        "publishing host health to %s:%d every %.1f s",
        args.redis_host,
        args.redis_port,
        args.interval,
    )
    try:
        run(transport, interval_s=args.interval)
    except KeyboardInterrupt:
        logger.info("host health publisher stopped")


if __name__ == "__main__":
    main()
