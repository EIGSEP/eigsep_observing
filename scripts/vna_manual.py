"""Interactive VNA bring-up tool.

Drives the production ``PandaClient.measure_s11`` path one bundle at
a time. Each Enter-pressed selection (``a`` = antenna, ``r`` =
receiver) runs the same OSL + DUT sequence the ``vna_loop`` would
run during normal observing, publishes through ``vna_writer`` so the
live-status VNA pane updates in the browser, and *also* writes a
self-contained local HDF5 file containing the raw arrays and the
first-order-calibrated S11 (ideal +1/-1/0 OSL — the same calibration
``live_status.vna_calibration.calibrate_s11`` applies on the
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

import numpy as np
import yaml

from eigsep_redis import ConfigStore, Transport
from picohost.proxy import PicoProxy

from eigsep_observing import PandaClient
from eigsep_observing._scripts_util import require_pico
from eigsep_observing.utils import configure_eig_logger, get_config_path
from eigsep_observing.vna import save_vna_manual_h5


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


def _build_transport(dummy):
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        transport = Transport(host="localhost", port=6380)
        transport.reset()
        return transport
    return Transport(host="localhost", port=6379)


def _build_client(transport, cfg, dummy):
    cfg = dict(cfg)
    cfg["use_vna"] = True
    cfg["use_switches"] = False
    cfg["use_motor"] = False
    cfg["use_tempctrl"] = False
    cfg["serialize_motion_and_switching"] = False
    ConfigStore(transport).upload(cfg)
    if dummy:
        from eigsep_observing.testing import DummyPandaClient

        return DummyPandaClient(transport=transport, default_cfg=cfg)
    return PandaClient(transport)


def _summary_db(arr):
    mag = np.abs(np.asarray(arr))
    mag = mag[mag > 0]
    if mag.size == 0:
        return float("nan")
    return float(20.0 * np.log10(np.mean(mag)))


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


def _run_bundle(client, mode, save_dir):
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
                last = _run_bundle(client, mode, save_dir)
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
    if args.cfg_file is None:
        args.cfg_file = get_config_path(
            "dummy_config.yaml" if args.dummy else "obs_config.yaml"
        )
    with open(args.cfg_file, "r") as f:
        cfg = yaml.safe_load(f)
    if not args.save_dir.exists():
        raise SystemExit(f"save-dir does not exist: {args.save_dir}")

    transport = _build_transport(args.dummy)

    client = _build_client(transport, cfg, args.dummy)
    try:
        require_pico(PicoProxy("rfswitch", transport, source="vna_manual"))
        if client.vna is None:
            raise SystemExit("VNA not initialized; check vna config block.")
        _print_banner(cfg, args.save_dir)
        _repl(client, args.save_dir)
    finally:
        client.stop()


if __name__ == "__main__":
    main()
