"""Local-only live-status dashboard for EIGSEP.

Spins up two :class:`Transport` instances (SNAP + panda), a
:class:`LiveStatusAggregator` that drains them in the background, and
the Flask app that projects the aggregator's state to JSON.

Binds to 127.0.0.1 by default; remote viewing is out of scope (SSH
tunnel if needed). Plotly.js is served from the ``/plotly.min.js``
route, sourced from the installed ``plotly`` Python package — no
manual vendoring step on field deploys.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys

from eigsep_redis import Transport

from eigsep_observing import utils
from eigsep_observing.live_status import (
    LiveStatusAggregator,
    Thresholds,
    create_app,
)


logger = logging.getLogger(__name__)


def _parse_bind(spec: str) -> tuple[str, int]:
    host, _, port = spec.partition(":")
    if not host or not port:
        raise argparse.ArgumentTypeError(
            f"--bind must be HOST:PORT, got {spec!r}"
        )
    return host, int(port)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Local EIGSEP live-status dashboard. Binds to 127.0.0.1 "
            "by default — this is a field-deployment tool, not a "
            "remotely-accessible service."
        )
    )
    parser.add_argument(
        "--snap-host",
        default="10.10.10.10",
        help="SNAP-side Redis host (rpi_ip).",
    )
    parser.add_argument("--snap-port", type=int, default=6379)
    parser.add_argument(
        "--panda-host",
        default="10.10.10.11",
        help="Panda-side Redis host (panda_ip).",
    )
    parser.add_argument("--panda-port", type=int, default=6379)
    parser.add_argument(
        "--obs-config",
        default=None,
        help=(
            "Path to obs_config.yaml; default loads the bundled "
            "config/obs_config.yaml."
        ),
    )
    parser.add_argument(
        "--thresholds",
        default=None,
        help=(
            "Path to live_status_thresholds.yaml override file; "
            "default loads the bundled config/live_status_thresholds.yaml."
        ),
    )
    parser.add_argument(
        "--bind",
        type=_parse_bind,
        default=("127.0.0.1", 5000),
        help="HOST:PORT to bind the Flask server (default 127.0.0.1:5000).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable Flask debug mode (reloader off; aggregator would "
        "double-start otherwise).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    obs_cfg_path = args.obs_config or utils.get_config_path("obs_config.yaml")
    obs_cfg = utils.load_config(obs_cfg_path, compute_inttime=False)

    transport_snap = Transport(host=args.snap_host, port=args.snap_port)
    transport_panda = Transport(host=args.panda_host, port=args.panda_port)

    thresholds = Thresholds.from_yaml(
        obs_cfg, corr_header=None, yaml_path=args.thresholds
    )
    aggregator = LiveStatusAggregator(
        transport_snap=transport_snap,
        transport_panda=transport_panda,
        obs_cfg=obs_cfg,
        thresholds=thresholds,
    )

    app = create_app(aggregator)

    def _shutdown(signum, _frame):
        logger.info("received signal %s; stopping aggregator", signum)
        aggregator.stop(timeout=2.0)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    aggregator.start()
    host, port = args.bind
    logger.info("serving live-status at http://%s:%s", host, port)
    try:
        app.run(host=host, port=port, debug=args.debug, use_reloader=False)
    finally:
        aggregator.stop(timeout=2.0)


if __name__ == "__main__":
    main()
