"""Interactive VNA bring-up tool.

Drives the production ``eigsep_observing.vna.measure_s11`` protocol
one bundle at a time, without constructing a :class:`PandaClient`.
Each Enter-pressed selection (``a`` = antenna, ``r`` = receiver) runs
the same OSL + DUT sequence the production ``vna_loop`` would run,
publishes through ``vna_writer`` so the live-status VNA pane updates
in the browser, and *also* writes a self-contained local HDF5 file
containing the raw arrays and the first-order-calibrated S11 (ideal
+1/-1/0 OSL — the same calibration
``eigsep_observing.vna_calibration.calibrate_s11`` applies on the
dashboard).

Unlike a real panda boot, this script does **not** start a heartbeat
thread, does **not** force-RFANT the rig, does **not** upload an
obs_config to Redis, and does **not** build a motion/switch
coordinator. It's a side-channel operator harness, structurally
distinct from the production driver.

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

from cmt_vna import VNA
from eigsep_redis import MetadataSnapshotReader, Transport
from picohost.proxy import PicoProxy

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import require_pico
from eigsep_observing.utils import configure_eig_logger, get_config_path
from eigsep_observing.vna import (
    VnaWriter,
    measure_s11,
    save_vna_manual_h5,
)


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


def _build_transport(dummy):
    """Bare transport for the VNA scripts. ``_build_subsystem`` below
    attaches its own minimal ``start_dummy_pico_manager`` in dummy mode
    (rfswitch only), so this helper doesn't auto-attach the full
    ``DummyPandaClient`` that ``_scripts_util.build_transport`` would."""
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        transport = Transport(host="localhost", port=6380)
        transport.reset()
        return transport
    return Transport(host="localhost", port=6379)


def _switch_via_proxy(sw_proxy, state):
    """Raise on switch failure; matches cmt_vna's ``switch_fn`` contract."""
    if sw_proxy.send_command("switch", state=state) is None:
        raise RuntimeError(
            f"RF switch to {state} failed: rfswitch not registered "
            "with PicoManager."
        )


def _build_subsystem(transport, cfg, dummy):
    """Assemble the minimum VNA-producer surface for vna_manual.

    Returns ``(vna, vna_writer, metadata_snapshot, cleanup)``. The
    ``cfg`` is never uploaded to Redis — it only feeds local
    ``measure_s11`` parameters and the local file-header overlay.

    Unlike :class:`PandaClient`, this assembly does **not** start a
    panda heartbeat thread, does **not** force-RFANT the rig at
    startup, and does **not** build a motion/switch coordinator. It is
    deliberately not a panda boot — just enough to drive one
    ``measure_s11`` call at a time from an operator REPL.
    """
    sw_proxy = PicoProxy("rfswitch", transport, source="vna_manual")

    def switch_fn(state):
        _switch_via_proxy(sw_proxy, state)

    manager = None
    if dummy:
        from cmt_vna.testing import DummyVNA
        from eigsep_observing.testing import start_dummy_pico_manager

        manager = start_dummy_pico_manager(transport)
        vna = DummyVNA(
            ip=cfg["vna_ip"],
            port=cfg["vna_port"],
            timeout=cfg["vna_timeout"],
            switch_fn=switch_fn,
        )
    else:
        vna = VNA(
            ip=cfg["vna_ip"],
            port=cfg["vna_port"],
            timeout=cfg["vna_timeout"],
            switch_fn=switch_fn,
        )

    require_pico(sw_proxy)

    setup_kwargs = cfg["vna_settings"].copy()
    setup_kwargs["power_dBm"] = setup_kwargs["power_dBm"]["ant"]
    vna.setup(**setup_kwargs)

    def cleanup():
        if manager is not None:
            manager.stop()

    return (
        vna,
        VnaWriter(transport),
        MetadataSnapshotReader(transport),
        cleanup,
    )


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


def _run_bundle(subsystem, cfg, transport, mode, save_dir):
    vna, vna_writer, metadata_snapshot, _ = subsystem
    try:
        payload = measure_s11(
            vna,
            mode,
            cfg=cfg,
            transport=transport,
            vna_writer=vna_writer,
            metadata_snapshot=metadata_snapshot,
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


def _repl(subsystem, cfg, transport, save_dir):
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
                last = _run_bundle(subsystem, cfg, transport, mode, save_dir)
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
        help="Run against a fakeredis-backed DummyVNA + dummy PicoManager.",
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
    subsystem = _build_subsystem(transport, cfg, args.dummy)
    cleanup = subsystem[3]
    with run_tag.session(transport, "vna_manual"):
        try:
            _print_banner(cfg, args.save_dir)
            _repl(subsystem, cfg, transport, args.save_dir)
        finally:
            cleanup()


if __name__ == "__main__":
    main()
