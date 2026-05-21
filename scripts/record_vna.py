"""Looped VNA recorder for test-bench data collection.

Same building blocks as :mod:`scripts.vna_manual` — production
``PandaClient.measure_s11`` bundles, ``save_vna_manual_h5`` for the
local HDF5 — but the interactive REPL is replaced with a timed loop
that alternates the requested bundles at a fixed interval. Designed
for hardware tests where the operator wants a steady stream of VNA
captures running in the background while motor/tempctrl scripts
exercise other parts of the system.

Each iteration runs every selected bundle (default: ``ant`` then
``rec``) in order, prints the per-bundle summary, then sleeps until
the next tick. SIGINT / SIGTERM stop the loop after the current
bundle completes so the in-flight HDF5 is closed cleanly.
"""

from argparse import ArgumentParser
import logging
import signal
import sys
import threading
from pathlib import Path

from picohost.proxy import PicoProxy

from eigsep_observing._scripts_util import require_pico
from eigsep_observing._vna_manual_core import (
    build_vna_client,
    build_vna_transport,
    load_vna_cfg,
    run_bundle,
)
from eigsep_observing.utils import configure_eig_logger


logger = logging.getLogger(__name__)


_VALID_BUNDLES = ("ant", "rec")


def _parse_bundles(raw: str) -> list[str]:
    bundles = [b.strip() for b in raw.split(",") if b.strip()]
    if not bundles:
        raise SystemExit(
            "--bundles must list at least one of: " + ", ".join(_VALID_BUNDLES)
        )
    bad = [b for b in bundles if b not in _VALID_BUNDLES]
    if bad:
        raise SystemExit(
            f"--bundles has unknown entries {bad}; "
            f"valid options: {', '.join(_VALID_BUNDLES)}"
        )
    return bundles


def _parse_args():
    parser = ArgumentParser(
        description=(
            "Loop ant/rec VNA bundles at a fixed interval. Each bundle "
            "is written via save_vna_manual_h5 (same path as "
            "scripts/vna_manual.py) and also published to Redis so the "
            "live-status dashboard sees it."
        )
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient.",
    )
    parser.add_argument(
        "--save-dir",
        type=Path,
        default=Path("."),
        help="Directory for local HDF5 files.",
    )
    parser.add_argument(
        "--cfg-file",
        type=Path,
        default=None,
        help=(
            "Observing config yaml. Defaults to the packaged "
            "obs_config.yaml, or dummy_config.yaml with --dummy."
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=300.0,
        help="Seconds to sleep between bundle cycles.",
    )
    parser.add_argument(
        "--bundles",
        type=str,
        default="ant,rec",
        help=(
            "Comma-separated bundles to run each tick, in order. Valid "
            "entries: " + ", ".join(_VALID_BUNDLES)
        ),
    )
    return parser.parse_args()


def _loop(client, save_dir, bundles, interval, stop_event):
    while not stop_event.is_set():
        for mode in bundles:
            if stop_event.is_set():
                break
            try:
                summary = run_bundle(client, mode, save_dir)
            except KeyboardInterrupt:
                summary = f"!! {mode} bundle interrupted"
                stop_event.set()
            logger.info("%s", summary)
        if stop_event.is_set():
            break
        # ``wait`` returns True when the event fires mid-sleep, so the
        # next iteration exits the outer loop immediately.
        stop_event.wait(interval)


def main():
    configure_eig_logger(level=logging.INFO)
    args = _parse_args()
    bundles = _parse_bundles(args.bundles)
    cfg = load_vna_cfg(args.cfg_file, args.dummy)

    if not args.save_dir.exists():
        raise SystemExit(f"save-dir does not exist: {args.save_dir}")
    if not args.save_dir.is_dir():
        raise SystemExit(f"save-dir is not a directory: {args.save_dir}")

    transport = build_vna_transport(args.dummy)
    client = build_vna_client(transport, cfg, args.dummy)

    stop_event = threading.Event()

    def _handle(signum, _frame):
        logger.info(
            "Signal %s received, stopping after current bundle.", signum
        )
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    try:
        require_pico(PicoProxy("rfswitch", transport, source="record_vna"))
        if client.vna is None:
            raise SystemExit("VNA not initialized; check vna config block.")
        logger.info(
            "Looping bundles %s every %.1fs, saving to %s.",
            bundles,
            args.interval,
            args.save_dir.resolve(),
        )
        _loop(client, args.save_dir, bundles, args.interval, stop_event)
    finally:
        client.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
