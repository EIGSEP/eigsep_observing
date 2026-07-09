"""Interactive VNA bring-up tool.

Drives the production ``eigsep_observing.vna.measure_s11`` protocol
one bundle at a time. Each Enter-pressed selection (``a`` = antenna,
``r`` = receiver) runs the same OSL + DUT sequence the production
``vna_loop`` would run, publishes through ``vna_writer`` so the
live-status VNA pane updates in the browser, and *also* writes a
self-contained local HDF5 file containing the raw arrays and the
first-order-calibrated S11 (ideal +1/-1/0 OSL — the same calibration
``eigsep_observing.vna_calibration.calibrate_s11`` applies on the
dashboard).

``d STATE`` (e.g. ``d VNAANT``) takes a one-off raw probe of a single
switch path via ``cmt_vna.VNA.measure_dut`` — no OSL, saved locally
only (the VNA stream protocol is bundle-shaped, so probes are not
published). ``--once`` runs any one command non-interactively and
exits, e.g. ``vna_manual.py --once d VNAANT``.

Follows the bring-up contract in ``scripts/CLAUDE.md``: builds only
the minimal VNA producer subsystem via
:func:`eigsep_observing.vna.build_vna_subsystem`, never a
:class:`PandaClient`. This means no heartbeat thread, no boot-time
force-RFANT, no ``ConfigStore`` upload, no in-process coord lock —
the tool can run alongside a production observer (and other operator
scripts in sibling terminals) without competing for any of those.

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

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport_bare,
)
from eigsep_observing.utils import configure_eig_logger, get_config_path
from eigsep_observing.vna import (
    build_vna_subsystem,
    measure_dut,
    measure_s11,
    save_vna_dut_h5,
    save_vna_manual_h5,
)


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


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
    print(
        "  [a] antenna bundle  (OSL + VNAANT + VNANOFF + VNANON + "
        "VNAAMB + VNASP1)"
    )
    print("  [r] receiver bundle (OSL + VNARF)")
    print('  [d STATE] one-off probe of one switch path (e.g. "d VNAANT");')
    print("            raw-only local .h5, no OSL, not published to Redis")
    print("  [q] quit")


def _run_bundle(subsystem, cfg, transport, mode, save_dir):
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


def _run_dut(subsystem, cfg, transport, state, save_dir):
    try:
        s11, header, metadata = measure_dut(
            subsystem.vna,
            state,
            cfg=cfg,
            transport=transport,
            metadata_snapshot=subsystem.metadata_snapshot,
        )
    except (RuntimeError, TimeoutError, ValueError) as exc:
        return f"!! dut {state} probe failed: {type(exc).__name__}: {exc}"
    try:
        path = save_vna_dut_h5(
            s11, header, metadata, save_dir=save_dir, state=state
        )
    except OSError as exc:
        return f"!! dut {state} measured but local save failed: {exc}"
    db = _summary_db(s11)
    return f"dut {state} saved {path.name}  (|Γ|_mean={db:.1f} dB)"


def _dispatch(tokens, subsystem, cfg, transport, save_dir):
    """Run one command; ``tokens`` is the whitespace-split input.

    Shared by the REPL and ``--once``: ``("a",)`` / ``("r",)`` run a
    bundle, ``("d", "<STATE>")`` runs a one-off probe. Returns a
    one-line summary; failures start with ``!!``, unrecognized input
    with ``??``.
    """
    cmd = tokens[0].lower()
    if cmd in ("a", "r") and len(tokens) == 1:
        mode = "ant" if cmd == "a" else "rec"
        return _run_bundle(subsystem, cfg, transport, mode, save_dir)
    if cmd == "d" and len(tokens) == 2:
        state = tokens[1].upper()
        return _run_dut(subsystem, cfg, transport, state, save_dir)
    return f"?? unrecognized command: {' '.join(tokens)!r}"


def _repl(subsystem, cfg, transport, save_dir):
    last = ""
    while True:
        _print_menu(last)
        try:
            choice = input("Select> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if choice == "":
            continue
        if choice.lower() == "q":
            return
        try:
            last = _dispatch(
                tuple(choice.split()), subsystem, cfg, transport, save_dir
            )
        except KeyboardInterrupt:
            last = f"!! {choice} interrupted"
        print(last)


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
    add_redis_args(parser)
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
    parser.add_argument(
        "--once",
        nargs="+",
        default=None,
        metavar="CMD",
        help=(
            "Run one command non-interactively and exit (non-zero on "
            "failure). Same tokens as the REPL: 'a', 'r', or 'd STATE' "
            "— e.g. --once d VNAANT for a one-off antenna-path probe."
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

    transport = build_transport_bare(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    # build_vna_subsystem starts cmtvna.service (real mode) and its
    # cleanup() stops it, so the whole REPL runs in one service window.
    subsystem = build_vna_subsystem(
        transport, cfg, source="vna_manual", dummy=args.dummy
    )
    with run_tag.session(transport, "vna_manual"):
        try:
            if args.once is not None:
                line = _dispatch(
                    tuple(args.once),
                    subsystem,
                    cfg,
                    transport,
                    args.save_dir,
                )
                print(line)
                if line.startswith(("!!", "??")):
                    raise SystemExit(1)
            else:
                _print_banner(cfg, args.save_dir)
                _repl(subsystem, cfg, transport, args.save_dir)
        finally:
            subsystem.cleanup()


if __name__ == "__main__":
    main()
