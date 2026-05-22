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

import numpy as np
import yaml

from eigsep_redis import ConfigStore, Transport
from picohost.proxy import PicoProxy

from eigsep_observing import PandaClient
from eigsep_observing._scripts_util import require_pico
from eigsep_observing.utils import configure_eig_logger, get_config_path
from eigsep_observing.vna import save_vna_manual_h5


logger = logging.getLogger(__name__)


_VALID_BUNDLES = ("ant", "rec")


def _build_transport(dummy):
    """Bare transport for the VNA scripts. The shared
    ``_scripts_util.build_transport`` auto-attaches a no-cfg
    ``DummyPandaClient`` for the other manual scripts, which would
    collide with the cfg-aware ``DummyPandaClient`` ``_build_vna_client``
    constructs below."""
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        transport = Transport(host="localhost", port=6380)
        transport.reset()
        return transport
    return Transport(host="localhost", port=6379)


def _build_vna_client(transport, cfg, dummy):
    """VNA-only ``PandaClient``: switches/motor/tempctrl off,
    motion/switching serialization off (each bundle runs inside its
    own ``switch_section``)."""
    cfg = dict(cfg)
    cfg["use_vna"] = True
    cfg["use_switches"] = False
    cfg["use_motor"] = False
    cfg["use_tempctrl"] = False
    cfg["serialize_motion_and_switching"] = False
    ConfigStore(transport).upload(cfg)
    if dummy:
        from eigsep_observing.testing import DummyPandaClient

        return DummyPandaClient(transport=transport, cfg=cfg)
    return PandaClient(transport)


def _summary_db(arr):
    mag = np.abs(np.asarray(arr))
    mag = mag[mag > 0]
    if mag.size == 0:
        return float("nan")
    return float(20.0 * np.log10(np.mean(mag)))


def _run_bundle(client, mode, save_dir):
    """Run one OSL+DUT bundle and write the local HDF5 file. Catches
    measurement failures and local-save OS errors so the loop can keep
    going."""
    with client.coord.switch_section():
        try:
            payload = client.measure_s11(mode)
        except (RuntimeError, TimeoutError, ValueError) as exc:
            return f"!! {mode} bundle failed: {type(exc).__name__}: {exc}"
    s11, header, metadata = payload
    try:
        path = save_vna_manual_h5(
            s11, header, metadata, save_dir=save_dir, mode=mode
        )
    except OSError as exc:
        return (
            f"!! {mode} bundle measured (published to Redis) but local "
            f"save failed: {exc}"
        )
    db = _summary_db(s11[mode])
    return f"{mode} saved {path.name}  (|Γ|_{mode}_mean={db:.1f} dB)"


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
                summary = _run_bundle(client, mode, save_dir)
            except KeyboardInterrupt:
                summary = f"!! {mode} bundle interrupted"
                stop_event.set()
            if summary.startswith("!!"):
                logger.error("%s", summary)
            else:
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

    cfg_file = args.cfg_file
    if cfg_file is None:
        cfg_file = get_config_path(
            "dummy_config.yaml" if args.dummy else "obs_config.yaml"
        )
    with open(cfg_file, "r") as f:
        cfg = yaml.safe_load(f)

    if not args.save_dir.exists():
        raise SystemExit(f"save-dir does not exist: {args.save_dir}")
    if not args.save_dir.is_dir():
        raise SystemExit(f"save-dir is not a directory: {args.save_dir}")

    transport = _build_transport(args.dummy)
    client = _build_vna_client(transport, cfg, args.dummy)

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
