"""Looped VNA recorder for test-bench data collection.

Same building blocks as :mod:`scripts.vna_manual` — production
``eigsep_observing.vna.measure_s11`` protocol, ``save_vna_manual_h5``
for the local HDF5 — but the interactive REPL is replaced with a timed
loop that alternates the requested bundles at a fixed interval.
Designed for hardware tests where the operator wants a steady stream
of VNA captures running in the background while motor/tempctrl
scripts exercise other parts of the system.

Each iteration runs every selected bundle (default: ``ant`` then
``rec``) in order, prints the per-bundle summary, then sleeps until
the next tick. SIGINT / SIGTERM stop the loop after the current
bundle completes so the in-flight HDF5 is closed cleanly.

Like ``vna_manual``, this script follows the bring-up contract in
``scripts/CLAUDE.md``: it builds only the minimal VNA-producer
subsystem, never a :class:`PandaClient`. The Pico arbitrates rfswitch
state, so concurrent operator scripts (motor_manual, tempctrl_manual,
...) and the production observer all coexist without an in-process
coord lock.
"""

from argparse import ArgumentParser
import logging
import signal
import sys
import threading
from pathlib import Path

import numpy as np
import yaml

from picohost.proxy import PicoProxy

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport_bare,
    require_pico,
)
from eigsep_observing.utils import configure_eig_logger, get_config_path
from eigsep_observing.vna import (
    build_vna_subsystem,
    measure_s11,
    save_vna_manual_h5,
)


logger = logging.getLogger(__name__)


_VALID_BUNDLES = ("ant", "rec")


def _summary_db(arr):
    mag = np.abs(np.asarray(arr))
    mag = mag[mag > 0]
    if mag.size == 0:
        return float("nan")
    return float(20.0 * np.log10(np.mean(mag)))


def _run_bundle(subsystem, cfg, transport, mode, save_dir):
    """Run one OSL+DUT bundle and write the local HDF5 file. Catches
    measurement failures and local-save OS errors so the loop can keep
    going."""
    try:
        payload = measure_s11(
            subsystem.vna,
            mode,
            cfg=cfg,
            transport=transport,
            vna_writer=subsystem.vna_writer,
            metadata_snapshot=subsystem.metadata_snapshot,
            on_contract_violation=logger.warning,
        )
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
        help="Run against a fakeredis-backed DummyVNA + dummy PicoManager.",
    )
    add_redis_args(parser)
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


def _loop(subsystem, cfg, transport, save_dir, bundles, interval, stop_event):
    while not stop_event.is_set():
        for mode in bundles:
            if stop_event.is_set():
                break
            try:
                summary = _run_bundle(
                    subsystem, cfg, transport, mode, save_dir
                )
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

    transport = build_transport_bare(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    # require_pico runs again inside build_vna_subsystem, but checking
    # before subsystem assembly gives the operator a clearer error
    # path (no VNA setup attempted against a missing rfswitch).
    require_pico(PicoProxy("rfswitch", transport, source="record_vna"))
    subsystem = build_vna_subsystem(
        transport, cfg, source="record_vna", dummy=args.dummy
    )

    stop_event = threading.Event()

    def _handle(signum, _frame):
        logger.info(
            "Signal %s received, stopping after current bundle.", signum
        )
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    try:
        with run_tag.session(transport, "record_vna"):
            logger.info(
                "Looping bundles %s every %.1fs, saving to %s.",
                bundles,
                args.interval,
                args.save_dir.resolve(),
            )
            _loop(
                subsystem,
                cfg,
                transport,
                args.save_dir,
                bundles,
                args.interval,
                stop_event,
            )
    finally:
        subsystem.cleanup()
    return 0


if __name__ == "__main__":
    sys.exit(main())
