"""Interactive VNA bring-up tool.

Drives the production ``PandaClient.measure_s11`` path one bundle at
a time. Each Enter-pressed selection (``a`` = antenna, ``r`` =
receiver) runs the same OSL + DUT sequence the ``vna_loop`` would
run during normal observing, publishes through ``vna_writer`` so the
live-status VNA pane updates in the browser, and *also* writes a
self-contained local HDF5 file containing the raw arrays and the
first-order-calibrated S11 (ideal +1/-1/0 OSL — the same calibration
``eigsep_observing.vna_calibration.calibrate_s11`` applies on the
dashboard).

Run alongside ``scripts/live_status.py`` for visual confirmation, or
post-process the saved ``.h5`` files in a notebook.

Switches are not operator-controlled — ``cmt_vna`` routes through
``switch_fn`` automatically. The operator does not swap SMA cables
between bundles; the deployed RF switch network handles it.
"""

from argparse import ArgumentParser
import logging
from pathlib import Path


from picohost.proxy import PicoProxy

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import require_pico
from eigsep_observing._vna_manual_core import (
    build_vna_client,
    build_vna_transport,
    load_vna_cfg,
    run_bundle,
)
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


def _print_banner(cfg, save_dir):
    vna = cfg["vna_settings"]
    print()
    print("=== VNA manual ===")
    print(
        f"  freqs: {vna['fstart'] / 1e6:.1f}–{vna['fstop'] / 1e6:.1f} MHz, "
        f"{vna['npoints']} points, IFBW={vna['ifbw']} Hz"
    )
    print(
        f"  power_dBm: ant={vna['power_dBm']['ant']}, "
        f"rec={vna['power_dBm']['rec']}"
    )
    print(f"  save_dir: {save_dir.resolve()}")
    print(
        "  tip: run scripts/live_status.py in another terminal to "
        "watch the VNA panes update."
    )


def _print_menu(last_summary):
    print()
    if last_summary:
        print(f"Last bundle: {last_summary}")
    print("  [a] antenna bundle  (OSL + VNAANT + VNANOFF + VNANON)")
    print("  [r] receiver bundle (OSL + VNARF)")
    print("  [q] quit")


def _repl(client, save_dir):
    last = ""
    while True:
        _print_menu(last)
        try:
            choice = input("Select> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if choice == "":
            continue
        if choice == "q":
            return
        if choice in ("a", "r"):
            mode = "ant" if choice == "a" else "rec"
            try:
                last = run_bundle(client, mode, save_dir)
            except KeyboardInterrupt:
                last = f"!! {mode} bundle interrupted"
            print(last)
            continue
        print(f"  ?? unrecognized input: {choice!r}")


def _parse_args():
    parser = ArgumentParser(
        description=(
            "Interactive VNA bring-up: trigger ant/rec bundles by hand, "
            "publish through vna_writer (live-status panes update), and "
            "save raw + calibrated S11 locally."
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
        help="Directory for local HDF5 files (default: current dir).",
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
    return parser.parse_args()


def main():
    args = _parse_args()
    cfg = load_vna_cfg(args.cfg_file, args.dummy)
    if not args.save_dir.exists():
        raise SystemExit(f"save-dir does not exist: {args.save_dir}")
    if not args.save_dir.is_dir():
        raise SystemExit(f"save-dir is not a directory: {args.save_dir}")

    transport = build_vna_transport(args.dummy)

    with run_tag.session(transport, "vna_manual"):
        client = build_vna_client(transport, cfg, args.dummy)
        try:
            require_pico(PicoProxy("rfswitch", transport, source="vna_manual"))
            if client.vna is None:
                raise SystemExit(
                    "VNA not initialized; check vna config block."
                )
            _print_banner(cfg, args.save_dir)
            _repl(client, args.save_dir)
        finally:
            client.stop()


if __name__ == "__main__":
    main()
