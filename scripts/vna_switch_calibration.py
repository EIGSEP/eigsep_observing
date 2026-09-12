"""Manual per-path OSL characterization of the RF switch, via the VNA.

The run has three phases:

1. **Direct-at-VNA reference** (no switch path set at all). The
   operator connects the external OPEN / SHORT / LOAD standards
   straight at the VNA's own test-port cable -- bypassing the switch
   entirely -- establishing the VNA+cable's own one-port reference
   before the switch is touched. Runs once, at the very start (only
   when the ``vna`` leg is selected -- see ``--legs``), since it
   characterizes the VNA+cable itself and is unaffected by which
   switch port that same cable is later plugged into.
2. **VNA-x paths** (``VNA_PORT_PATHS``): everything reachable while
   the VNA is connected at its normal, dedicated port on the switch --
   VNAANT / VNANOFF / VNANON / VNAAMB / VNASP1 / VNARF, plus the
   switch's own built-in cal positions VNAO / VNAS / VNAL (walked
   through the same external-OSL procedure as every other path, so
   they get a cross-check against the external standards too).
3. **LNA-x paths** (``LNA_PORT_PATHS``): the same physical antenna
   nodes, reached instead through the switch leg that normally feeds
   the LNA / correlator (RFANT / RFNOFF / RFNON / RFAMB / RFSP1). The
   script pauses and asks the operator to manually move the VNA cable
   from the VNA port to the LNA port before starting this leg.

For every path (phases 2 and 3) the RF switch is set to that path and
the operator is walked through three manual steps: connect the
external OPEN standard, then SHORT, then LOAD, at the path's far end
(pressing Enter after each connection triggers one VNA sweep). This
directly OSL-characterizes each physical switch path with a real
calkit, rather than relying on the switch's own internal "generic SMA
cap" OSL positions (see ``eigsep_observing.vna_calibration`` for why
those are lower-accuracy) or on a single calibration at the VNA's own
test port alone (which cannot capture per-path differences in cable
length / loss / connectors -- that's what phases 2/3 are for).

The SP1 far-end failsafe termination (normally toggled automatically
via the potmon pico between production SHORT/OPEN dwells) is left
alone here -- VNASP1 / RFSP1 are each characterized as a single path,
since the manual OSL swap already disconnects whatever is normally at
the far end.

**One file per O/S/L set.** Each set of standards -- the direct-at-VNA
trio, and each path's trio -- is saved together as a single HDF5 file
(``vna_oslset_<label>_<timestamp>.h5``, via the local
:func:`_save_osl_set_h5`) rather than one file per standard. A set
where a standard was skipped or failed still saves whatever was
captured. No bundle is published to the VNA Redis stream. Only
``rfswitch`` / ``rfswitch_therm`` metadata is stamped into each
standard's entry (the switch state and the RF-switch-board thermistor
readings) rather than the full panda sensor snapshot.

**VNA service warm-up.** ``build_vna_subsystem`` starts ``cmtvna``
and waits for the instrument to answer on its socket, but that has
been observed to not be enough on its own -- the very first sweep
right after can still come back all-NaN. Before anything else,
:func:`_warm_up_vna` sleeps for ``--vna-settle-s`` (default 10s) and
then takes throwaway warm-up sweeps (retried, with a short pause, if
they come back all-NaN) so an operator prompt never lands on the
sweep that silently fails.

Follows the bring-up-script contract in ``scripts/CLAUDE.md``: builds
only the minimal VNA producer subsystem via
:func:`eigsep_observing.vna.build_vna_subsystem`, never a
:class:`PandaClient`, and claims ``run_tag`` for the duration of the
run (active driver -- sends switch commands and writes files).

Run alongside ``scripts/live_status.py`` in another terminal to watch
the rfswitch tile confirm each transition.

``--legs`` / ``--paths`` let a run be scoped to a subset (e.g. to
resume after an earlier run already finished the VNA-port leg, or to
redo one path whose standard got knocked loose). The direct-at-VNA
reference (phase 1) only runs when the ``vna`` leg is selected:

    python scripts/vna_switch_calibration.py --legs lna
    python scripts/vna_switch_calibration.py --paths VNAAMB,VNASP1
"""

from argparse import ArgumentParser
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time

import h5py
import numpy as np
import yaml

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport_bare,
)
from eigsep_observing.utils import configure_eig_logger, get_config_path
from eigsep_observing.vna import build_vna_subsystem, measure_dut


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)


# Switch paths reachable while the VNA occupies its normal, dedicated
# switch port ("VNA-x"). VNAO/VNAS/VNAL are the switch's own built-in
# cal positions -- included so they get the same external-OSL
# characterization/cross-check as every other path.
VNA_PORT_PATHS = [
    "VNAANT",
    "VNANOFF",
    "VNANON",
    "VNAAMB",
    "VNASP1",
    "VNARF",
    "VNAO",
    "VNAS",
    "VNAL",
]

# Same physical antenna nodes, reached through the switch leg that
# normally faces the LNA / correlator. Requires manually moving the
# VNA cable to the switch's LNA port first -- see the "lna" prompt in
# main().
LNA_PORT_PATHS = ["RFANT", "RFNOFF", "RFNON", "RFAMB", "RFSP1"]

# (code, human label) for the three manually-connected standards, in
# the order the operator is walked through them for every set.
STANDARDS = [("O", "OPEN"), ("S", "SHORT"), ("L", "LOAD")]

# Label used for the direct-at-VNA reference set (phase 1) -- not a
# real switch-path name, so it can't collide with one.
VNA_DIRECT_LABEL = "VNA_DIRECT"


def _summary_db(arr):
    mag = np.abs(np.asarray(arr))
    mag = mag[mag > 0]
    if mag.size == 0:
        return float("nan")
    return float(20.0 * np.log10(np.mean(mag)))


def _prompt(msg):
    """Block for operator input at one manual connection step.

    Returns ``"go"`` (Enter), ``"skip"`` (operator typed ``s`` -- skip
    just this standard), or ``"abort"`` (``q``, Ctrl+C, or EOF -- stop
    the whole run).
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


def _trim_metadata(metadata):
    """Keep only ``rfswitch`` / ``rfswitch_therm`` from a panda snapshot.

    This calibration run only cares about the switch state that was
    active and the RF-switch-board thermistor readings (published by
    the same pico, on the ``rfswitch_therm`` stream) -- not the rest
    of the panda's sensor snapshot. Keys that aren't present in this
    snapshot are omitted rather than written as ``None`` -- h5py
    attrs can't hold ``None``.
    """
    metadata = metadata or {}
    trimmed = {}
    for key in ("rfswitch", "rfswitch_therm"):
        val = metadata.get(key)
        if val is not None:
            trimmed[key] = val
    return trimmed


def _write_json_attrs(target, mapping, skip=()):
    """Write a dict onto an h5 node's attrs, JSON-encoding nested values.

    Local copy of the same small pattern used by
    ``eigsep_observing.vna``'s save helpers -- duplicated here (rather
    than importing that module's private helper) so this script only
    depends on ``eigsep_observing.vna``'s public API.
    """
    for k, v in mapping.items():
        if k in skip:
            continue
        if isinstance(v, (dict, list, tuple)):
            target.attrs[k] = json.dumps(v)
        elif isinstance(v, np.ndarray):
            target.attrs[k] = json.dumps(v.tolist())
        else:
            target.attrs[k] = v


def _save_osl_set_h5(entries, *, save_dir, label):
    """Save one whole O/S/L set as a single local HDF5 file.

    ``entries`` is ``{code: (s11, header, metadata)}`` for whichever
    of O/S/L were actually captured -- a partial set (one standard
    skipped or failed) still saves the rest. The frequency axis and
    instrument header are taken from whichever standard was captured
    first (fstart/fstop/npoints/ifbw/power_dBm are fixed for the whole
    run, so they don't vary standard-to-standard); each standard keeps
    its own metadata snapshot, since the rfswitch thermistors can
    drift over the minutes it takes to swap connectors by hand.

    Layout: ``/freqs``, ``/raw/<code>`` for each captured standard,
    ``/metadata_snapshot/<code>`` attrs per standard, and the shared
    instrument header on root attrs.
    """
    save_dir = Path(save_dir)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = save_dir / f"vna_oslset_{label}_{stamp}.h5"
    first_header = next(iter(entries.values()))[1]
    with h5py.File(path, "w") as f:
        raw_grp = f.create_group("raw")
        meta_grp = f.create_group("metadata_snapshot")
        for code, (s11, _header, metadata) in entries.items():
            raw_grp.create_dataset(code, data=np.asarray(s11))
            _write_json_attrs(meta_grp.create_group(code), metadata)
        f.create_dataset(
            "freqs", data=np.asarray(first_header["freqs"], dtype=float)
        )
        _write_json_attrs(f, first_header, skip={"freqs", "mode"})
        f.attrs["mode"] = f"osl_set:{label}"
        f.attrs["vna_switch_calibration_script_version"] = "1"
        f.attrs["standards_captured"] = sorted(entries.keys())
    return path


def read_osl_set_h5(fname):
    """Read one file written by :func:`_save_osl_set_h5`.

    This script's ``.h5`` files are a bespoke, bring-up-local layout
    (like ``eigsep_observing.vna.save_vna_dut_h5`` /
    ``save_vna_manual_h5``, which this mirrors) -- **not** the
    production layout ``eigsep_observing.io.write_hdf5`` /
    ``read_s11_file`` read/write (that pair expects top-level
    ``data``/``header``/``metadata`` groups written by
    ``_write_header_item``; this file has ``raw``/``metadata_snapshot``
    groups with JSON-encoded attrs instead). ``read_s11_file`` will
    raise ``KeyError`` on a file this script wrote -- use this
    function instead. Inverts :func:`_save_osl_set_h5` /
    :func:`_write_json_attrs`: JSON-encoded attrs (the ones whose
    original value was a dict/list/tuple/ndarray) are decoded back;
    plain scalar attrs (str/int/float/bool) round-trip natively
    through h5py and are returned as-is.

    Parameters
    ----------
    fname : str or Path
        Path to a ``vna_oslset_*.h5`` file.

    Returns
    -------
    raw : dict[str, np.ndarray]
        Complex S11 arrays keyed by standard code (``"O"``/``"S"``/
        ``"L"`` -- only whichever were actually captured for this
        set).
    freqs : np.ndarray
        Frequency axis, Hz.
    header : dict
        Shared instrument header (nested dict/list values decoded
        from their JSON-encoded attrs).
    metadata : dict[str, dict]
        Per-standard metadata snapshot (``{"rfswitch": ...,
        "rfswitch_therm": ...}``, whichever were present at capture
        time), keyed the same way as ``raw``.
    """

    def _decode_attrs(attrs, skip=()):
        out = {}
        for k, v in attrs.items():
            if k in skip:
                continue
            if isinstance(v, str):
                try:
                    v = json.loads(v)
                except (json.JSONDecodeError, ValueError):
                    pass
            out[k] = v
        return out

    with h5py.File(fname, "r") as f:
        raw = {code: f["raw"][code][:] for code in f["raw"]}
        freqs = f["freqs"][:]
        header = _decode_attrs(f.attrs, skip={"freqs"})
        metadata = {
            code: _decode_attrs(f["metadata_snapshot"][code].attrs)
            for code in f["metadata_snapshot"]
        }
    return raw, freqs, header, metadata


def _measure_standard(vna, state, *, cfg, transport, metadata_snapshot):
    """One raw sweep for one standard.

    If ``state`` is given, switches to that path first via the public
    :func:`eigsep_observing.vna.measure_dut` (three calls per path,
    one per standard, is a harmless idempotent re-switch). If
    ``state`` is ``None``, takes a bare sweep with no switch command
    at all -- the direct-at-VNA reference (phase 1).
    """
    if state is not None:
        s11, header, metadata = measure_dut(
            vna,
            state,
            cfg=cfg,
            transport=transport,
            metadata_snapshot=metadata_snapshot,
        )
        return s11, header, _trim_metadata(metadata)

    s11 = vna.measure_S11()
    header = dict(vna.header)
    header["mode"] = f"osl:{VNA_DIRECT_LABEL}"
    tag = run_tag.read(transport)
    header["run_tag"] = (
        tag["run_tag"] if tag["run_tag"] is not None else "UNKNOWN"
    )
    metadata = _trim_metadata(metadata_snapshot.get())
    return s11, header, metadata


def _warm_up_vna(vna, *, settle_s, max_retries=3, retry_delay_s=2.0):
    """Give the VNA time to finish initializing before any real sweep.

    ``build_vna_subsystem`` starts ``cmtvna.service`` and waits for
    the instrument to answer on its socket, but that has been observed
    to not be enough on its own -- the very first sweep right after
    can still come back all-NaN. Sleep first, then take throwaway
    warm-up sweeps -- retried (with a short pause) if they come back
    all-NaN -- so an operator prompt never lands on the sweep that
    silently fails.
    """
    if settle_s > 0:
        print(f"Waiting {settle_s:.0f}s for the VNA service to settle...")
        time.sleep(settle_s)
    for attempt in range(1, max_retries + 1):
        s11 = np.asarray(vna.measure_S11())
        if not np.all(np.isnan(s11)):
            return
        print(
            f"  warm-up sweep {attempt}/{max_retries} came back all-NaN; "
            f"retrying in {retry_delay_s:.0f}s..."
        )
        time.sleep(retry_delay_s)
    print(
        "  !! warm-up sweeps kept returning all-NaN; proceeding anyway "
        "-- watch the first real capture closely."
    )


def _run_osl_set(vna, state, *, cfg, transport, metadata_snapshot, save_dir):
    """Walk the operator through OPEN/SHORT/LOAD for one set, then save
    the whole set as one HDF5 file.

    ``state`` is a switch-path name (the RF switch is set to it before
    each standard), or ``None`` for the direct-at-VNA reference (no
    switch command is issued at all).

    Returns ``"abort"`` if the operator quit mid-set, else ``"go"``. A
    set where every standard was skipped or failed writes no file.
    """
    label = state or VNA_DIRECT_LABEL
    where = (
        f"the {state} path's far end (switch is set to {state})"
        if state is not None
        else "the VNA test port directly (bypass the switch entirely, "
        "no switch path is set for this step)"
    )
    print(f"\n--- {label} ---")
    entries = {}
    aborted = False
    for code, std_name in STANDARDS:
        result = _prompt(f"  Connect the {std_name} calibrator at {where}.")
        if result == "abort":
            print(f"  aborted at {label} / {std_name}")
            aborted = True
            break
        if result == "skip":
            print(f"  skipped {label} / {std_name}")
            continue
        try:
            s11, header, metadata = _measure_standard(
                vna,
                state,
                cfg=cfg,
                transport=transport,
                metadata_snapshot=metadata_snapshot,
            )
        except (RuntimeError, TimeoutError, ValueError) as exc:
            print(
                f"  !! {label} {std_name} failed: {type(exc).__name__}: {exc}"
            )
            continue
        entries[code] = (s11, header, metadata)
        db = _summary_db(s11)
        nan_flag = (
            "  ⚠ contains NaNs" if np.any(np.isnan(np.asarray(s11))) else ""
        )
        print(f"  {std_name} measured  (|Γ|_mean={db:.1f} dB){nan_flag}")
    if entries:
        path = _save_osl_set_h5(entries, save_dir=save_dir, label=label)
        print(f"  saved {path.name}  (standards: {sorted(entries)})")
    else:
        print(f"  nothing captured for {label}; no file written")
    return "abort" if aborted else "go"


def _run_leg(vna, paths, *, cfg, transport, metadata_snapshot, save_dir):
    for i, state in enumerate(paths, start=1):
        print(f"\n[{i}/{len(paths)}]", end="")
        if (
            _run_osl_set(
                vna,
                state,
                cfg=cfg,
                transport=transport,
                metadata_snapshot=metadata_snapshot,
                save_dir=save_dir,
            )
            == "abort"
        ):
            return "abort"
    return "go"


def _filter_paths(paths, wanted):
    """Intersect one leg's full path list with a parsed --paths set.

    ``wanted`` is ``None`` (no filter -- return every path) or a set of
    upper-cased names already validated (in ``main``) against the
    union of paths across every *selected* leg. A name that's valid
    for the other leg but not this one simply yields no paths here,
    rather than erroring -- e.g. ``--paths VNAAMB`` with the default
    ``--legs vna,lna`` runs only the VNA-port leg, the LNA-port leg
    just has nothing to do.
    """
    if wanted is None:
        return paths
    return [p for p in paths if p in wanted]


def _parse_args():
    parser = ArgumentParser(
        description=(
            "Per-path OSL characterization of the RF switch: for every "
            "named switch path, manually connect external OPEN / SHORT "
            "/ LOAD standards at the path's far end and have the VNA "
            "sweep each. Starts with a direct-at-VNA reference (no "
            "switch path set), then runs the VNA-port leg, then pauses "
            "for a manual cable move to the LNA port and runs that leg."
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
        "--legs",
        default="vna,lna",
        help=(
            "Comma-separated subset of legs to run: 'vna', 'lna', or "
            "'vna,lna' (default). The direct-at-VNA reference (phase "
            "1) only runs when 'vna' is included."
        ),
    )
    parser.add_argument(
        "--paths",
        default=None,
        help=(
            "Comma-separated subset of switch-path names to run within "
            "the selected leg(s), e.g. --paths VNAAMB,VNASP1. Default: "
            "every path in each selected leg."
        ),
    )
    parser.add_argument(
        "--vna-settle-s",
        type=float,
        default=10.0,
        help=(
            "Seconds to wait after the VNA service starts, before "
            "attempting any sweep (default: 10). The service can "
            "report ready before the instrument is actually settled "
            "enough to return valid (non-NaN) data; increase this if "
            "warm-up sweeps keep coming back all-NaN."
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

    legs = [leg.strip().lower() for leg in args.legs.split(",") if leg.strip()]
    bad = set(legs) - {"vna", "lna"}
    if bad:
        raise SystemExit(f"--legs: unknown leg(s) {sorted(bad)}; use vna/lna")

    wanted = None
    if args.paths is not None:
        wanted = {
            p.strip().upper() for p in args.paths.split(",") if p.strip()
        }
        available = set()
        if "vna" in legs:
            available |= set(VNA_PORT_PATHS)
        if "lna" in legs:
            available |= set(LNA_PORT_PATHS)
        unknown = wanted - available
        if unknown:
            raise SystemExit(
                f"--paths: {sorted(unknown)} not found in the selected "
                f"leg(s) {legs}"
            )

    transport = build_transport_bare(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    # build_vna_subsystem starts cmtvna.service (real mode) and its
    # cleanup() stops it, so the whole run lives in one service window.
    subsystem = build_vna_subsystem(
        transport, cfg, source="vna_switch_calibration", dummy=args.dummy
    )
    try:
        with run_tag.session(transport, "vna_switch_calibration"):
            _warm_up_vna(subsystem.vna, settle_s=args.vna_settle_s)
            print("=== VNA switch-path OSL characterization ===")
            print(f"  save_dir: {args.save_dir.resolve()}")
            print(
                "  tip: run scripts/live_status.py in another terminal "
                "to watch the rfswitch tile confirm each transition."
            )

            if "vna" in legs:
                print("\n=== Phase 1: direct-at-VNA reference ===")
                if (
                    _run_osl_set(
                        subsystem.vna,
                        None,
                        cfg=cfg,
                        transport=transport,
                        metadata_snapshot=subsystem.metadata_snapshot,
                        save_dir=args.save_dir,
                    )
                    == "abort"
                ):
                    return

                paths = _filter_paths(VNA_PORT_PATHS, wanted)
                print(f"\n=== VNA-port leg: {len(paths)} path(s) ===")
                if (
                    _run_leg(
                        subsystem.vna,
                        paths,
                        cfg=cfg,
                        transport=transport,
                        metadata_snapshot=subsystem.metadata_snapshot,
                        save_dir=args.save_dir,
                    )
                    == "abort"
                ):
                    return

            if "lna" in legs:
                paths = _filter_paths(LNA_PORT_PATHS, wanted)
                print(f"\n=== LNA-port leg: {len(paths)} path(s) ===")
                if (
                    _prompt(
                        "Manually move the VNA cable from the VNA port "
                        "to the LNA port."
                    )
                    == "abort"
                ):
                    return
                _run_leg(
                    subsystem.vna,
                    paths,
                    cfg=cfg,
                    transport=transport,
                    metadata_snapshot=subsystem.metadata_snapshot,
                    save_dir=args.save_dir,
                )
    finally:
        subsystem.cleanup()


if __name__ == "__main__":
    main()
