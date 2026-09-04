"""Measure OSL calibration standards for the VNA path, either the
manual/external-calkit standards at the VNA's own test port, or the
RF switch's built-in VNAO / VNAS / VNAL cal standards -- pick one with
``--mode``, never both in the same run. Each mode writes its own,
separate output file.

``--mode manual``
    The operator connects an external OPEN / SHORT / LOAD calibrator
    straight at the VNA's own test-port cable -- bypassing the switch
    entirely -- and is prompted before each standard is swept (Enter
    to sweep, ``s`` to skip just that one, ``q`` to abort the rest).
    This is the same "direct-at-VNA reference" step
    ``vna_switch_calibration.py`` runs before its per-path
    characterization (see that script's docstring for the full
    rationale on why a real external calkit beats the switch's own
    positions). Saved under ``OPEN`` / ``SHORT`` / ``LOAD`` -- whichever
    standards were actually captured; one can be skipped and the rest
    still saved. Default output: ``vna_manual_osl_<timestamp>.npz``.

``--mode automatic``
    The RF switch's own built-in "generic SMA cap" VNAO / VNAS / VNAL
    positions -- see ``eigsep_observing.vna_calibration`` for why
    they're lower-accuracy than a real external calkit. Fully
    automatic, no prompts: switch to each state, sweep, save. Saved
    under ``VNAO`` / ``VNAS`` / ``VNAL``. Default output:
    ``vna_internal_osl_<timestamp>.npz``.

Every captured trace is defensively copied (``np.array(..., copy=True)``)
at the moment it's read out of the VNA, rather than passed through
with a plain ``np.asarray`` (which does not copy an already-ndarray
input). Some VNA drivers hand back a reference to one internal buffer
that gets overwritten in place on the next sweep -- without the copy,
every captured trace would silently alias the same memory and read
back as whatever the *last* sweep wrote, with no error anywhere.

Follows the bring-up-script contract in ``scripts/CLAUDE.md``: builds
only the minimal VNA producer subsystem via
``eigsep_observing.vna.build_vna_subsystem`` (never a ``PandaClient``),
and claims ``run_tag`` for the duration of the run.

Usage:

    python scripts/measure_vna_internal_osl.py --mode manual
    python scripts/measure_vna_internal_osl.py --mode automatic
    python scripts/measure_vna_internal_osl.py --mode automatic -o my_osl.npz
    python scripts/measure_vna_internal_osl.py --mode manual --dummy   # no hardware
"""

from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import yaml

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport_bare,
)
from eigsep_observing.utils import get_config_path
from eigsep_observing.vna import build_vna_subsystem, measure_dut

STATES = ["VNAO", "VNAS", "VNAL"]

# Manually-connected standard -> npz key, walked through in this
# order at the VNA's own test port.
MANUAL_STANDARDS = [("OPEN", "OPEN"), ("SHORT", "SHORT"), ("LOAD", "LOAD")]

# Per-mode default output filename stem (a UTC timestamp + ".npz" is
# appended, same as before).
DEFAULT_OUTFILE_STEM = {
    "manual": "vna_manual_osl",
    "automatic": "vna_internal_osl",
}


def _prompt(msg):
    """Block for operator input at one manual connection step.

    Returns ``"go"`` (Enter), ``"skip"`` (operator typed ``s`` -- skip
    just this standard), or ``"abort"`` (``q``, Ctrl+C, or EOF -- stop
    the whole run). Mirrors ``vna_switch_calibration.py``'s ``_prompt``
    (duplicated locally rather than imported, same as that script does
    for ``eigsep_observing.vna``'s helpers -- keeps each bring-up
    script only depending on public API).
    """
    try:
        choice = input(f"{msg} [Enter=continue, s=skip, q=abort] ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return "abort"
    choice = choice.lower()
    if choice == "q":
        return "abort"
    if choice == "s":
        return "skip"
    return "go"


def _measure_manual_standards(vna):
    """Walk the operator through OPEN/SHORT/LOAD at the VNA's own test
    port (no switch command issued at all).

    Returns ``(data, header)``. ``data`` has whichever of
    MANUAL_STANDARDS' npz keys were actually captured -- a standard
    can be skipped, or the run can be aborted partway through, and
    whatever was already captured is kept either way. ``header`` is
    from the last standard captured, or ``None`` if none were.
    """
    data = {}
    header = None
    for std_name, key in MANUAL_STANDARDS:
        result = _prompt(
            f"Connect the {std_name} standard at the VNA test port "
            "directly (bypass the switch entirely)."
        )
        if result == "abort":
            print(f"aborted before measuring {std_name}")
            break
        if result == "skip":
            print(f"skipped {std_name}")
            continue
        print(f"Measuring {std_name}...")
        # copy=True is deliberate: some VNA drivers hand back a
        # reference to one internal buffer that gets overwritten in
        # place on the next sweep. np.asarray() would NOT copy an
        # already-ndarray input, so every entry captured this way
        # would silently alias the same memory and all read back as
        # whatever the last sweep wrote (no error, just wrong data).
        data[key] = np.array(vna.measure_S11(), copy=True)
        header = vna.header
    return data, header


def _measure_automatic(vna, *, cfg, transport, metadata_snapshot):
    """Switch to each of VNAO/VNAS/VNAL and sweep. Fully automatic,
    no prompts.

    Returns ``(data, freqs)``.
    """
    data = {}
    freqs = None
    for state in STATES:
        print(f"Measuring {state}...")
        s11, header, _metadata = measure_dut(
            vna,
            state,
            cfg=cfg,
            transport=transport,
            metadata_snapshot=metadata_snapshot,
        )
        # Same defensive copy as the manual phase -- see the comment
        # in _measure_manual_standards.
        data[state] = np.array(s11, copy=True)
        freqs = np.asarray(header["freqs"], dtype=float)
    return data, freqs


def main():
    parser = ArgumentParser(
        description=(
            "Measure OSL calibration standards for the VNA path -- "
            "either the manual external-calkit standards at the VNA "
            "test port, or the switch's built-in VNAO/VNAS/VNAL cal "
            "standards. Pick one with --mode; each writes its own "
            "output file."
        )
    )
    parser.add_argument(
        "--mode",
        choices=["manual", "automatic"],
        required=True,
        help=(
            "'manual': prompt for hand-connected OPEN/SHORT/LOAD at "
            "the VNA test port. 'automatic': the switch's built-in "
            "VNAO/VNAS/VNAL sweep, no prompts."
        ),
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyVNA + dummy PicoManager.",
    )
    add_redis_args(parser)
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
        "-o",
        "--outfile",
        type=Path,
        default=None,
        help=(
            "Output .npz path (default: "
            "vna_manual_osl_<timestamp>.npz for --mode manual, "
            "vna_internal_osl_<timestamp>.npz for --mode automatic)."
        ),
    )
    args = parser.parse_args()

    cfg_file = args.cfg_file
    if cfg_file is None:
        cfg_file = get_config_path(
            "dummy_config.yaml" if args.dummy else "obs_config.yaml"
        )
    with open(cfg_file, "r") as f:
        cfg = yaml.safe_load(f)

    transport = build_transport_bare(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    subsystem = build_vna_subsystem(
        transport, cfg, source="measure_vna_internal_osl", dummy=args.dummy
    )
    data = {}
    freqs = None
    try:
        with run_tag.session(transport, "measure_vna_internal_osl"):
            if args.mode == "manual":
                print("=== Manual OSL standards (direct at VNA test port) ===")
                data, header = _measure_manual_standards(subsystem.vna)
                if header is not None:
                    freqs = np.asarray(header["freqs"], dtype=float)
            else:
                print("=== Automatic internal OSL (VNAO/VNAS/VNAL) ===")
                data, freqs = _measure_automatic(
                    subsystem.vna,
                    cfg=cfg,
                    transport=transport,
                    metadata_snapshot=subsystem.metadata_snapshot,
                )
    finally:
        subsystem.cleanup()

    if not data:
        print("nothing captured; no file written")
        return

    outfile = args.outfile
    if outfile is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        outfile = Path(f"{DEFAULT_OUTFILE_STEM[args.mode]}_{stamp}.npz")
    np.savez(outfile, freqs=freqs, **data)
    print(f"saved {outfile}  (keys: freqs, {', '.join(sorted(data))})")


if __name__ == "__main__":
    main()
