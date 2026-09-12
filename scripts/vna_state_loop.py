"""Loop a fixed list of raw VNA switch-path probes indefinitely,
writing one production-format S11 HDF5 file (via
``eigsep_observing.io.write_s11_file`` -- the same reader/writer pair
as the normal ant/rec observing files) per cycle.

Unlike ``scripts/record_vna.py`` (which runs the full ant/rec bundle:
OSL standards, noise/load, SP1 short+open) this drives
``eigsep_observing.vna.measure_dut`` once per requested switch path,
with no SP1-termination toggling -- a plain probe of whatever paths
you ask for (default: VNAANT, VNAAMB, VNASP1, VNANON, VNANOFF). Each
cycle also reads the VNA's own internal OSL standards once, via
``cmt_vna.VNA.measure_OSL()`` (the same call ``measure_s11`` makes),
and writes them alongside the probed paths as ``cal:VNAO``/
``cal:VNAS``/``cal:VNAL`` -- so files from this script read back
through ``read_s11_file`` with a populated ``cal_data``, same as the
normal ant/rec observing files.

Each default switch path is renamed to the DUT name
``scripts/calibrate_field_s11.py`` expects (``STATE_TO_DUT``: VNAANT
-> ant, VNAAMB -> amb, VNASP1 -> sp1, VNANON -> noise, VNANOFF ->
load), and the header's ``mode`` is set to ``"ant"`` rather than a
script-specific label -- that script buckets internal-OSL captures by
``mode`` (only "ant"/"rec" are recognized), so files from this script
now calibrate the same way the normal ant-mode observing files do. A
switch path passed via ``--states`` that isn't in ``STATE_TO_DUT``
(e.g. a custom path, or VNARF/rec-side paths) is kept under its raw
switch-path name and won't be recognized by the calibration script.

All paths measured in one cycle are bundled into a single file; the
header/metadata snapshot attached to that file is taken after the
last path in the cycle, mirroring how
``eigsep_observing.vna.measure_s11`` snapshots once per bundle rather
than once per trace.

Follows the bring-up-script contract in ``scripts/CLAUDE.md``: builds
only the minimal VNA producer subsystem via
``eigsep_observing.vna.build_vna_subsystem`` (never a ``PandaClient``),
and claims ``run_tag`` for the duration of the run. SIGINT/SIGTERM stop
the loop after the in-flight cycle finishes so its HDF5 file is closed
cleanly.

Usage::

    python scripts/vna_state_loop.py
    python scripts/vna_state_loop.py --states VNAANT,VNAAMB --interval 60
    python scripts/vna_state_loop.py --dummy   # no hardware
"""

from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path
import logging
import signal
import sys
import threading

import yaml

from picohost.proxy import PicoProxy

from eigsep_observing import io, run_tag
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport_bare,
    require_pico,
)
from eigsep_observing.utils import configure_eig_logger, get_config_path
from eigsep_observing.vna import build_vna_subsystem, measure_dut

logger = logging.getLogger(__name__)

DEFAULT_STATES = ["VNAANT", "VNAAMB", "VNASP1", "VNANON", "VNANOFF"]

# Switch-path name -> DUT name, matching the keys
# scripts/calibrate_field_s11.py's DEEMBED_DICT/EMBED_DICT expect.
# Keep this in sync with that script if either changes.
STATE_TO_DUT = {
    "VNAANT": "ant",
    "VNAAMB": "amb",
    "VNASP1": "sp1",
    "VNANON": "noise",
    "VNANOFF": "load",
}

# calibrate_field_s11.py buckets internal-OSL captures by
# hdr["mode"], recognizing only "ant" or "rec". Every DUT this script
# measures by default lives on the VNA/antenna leg, so "ant" is the
# correct bucket; a state outside STATE_TO_DUT (e.g. a receiver-side
# path) would need "rec" instead, which this script doesn't handle.
BUNDLE_MODE = "ant"


def _parse_states(raw):
    states = [s.strip() for s in raw.split(",") if s.strip()]
    if not states:
        raise SystemExit("--states must list at least one switch path")
    return states


def _run_cycle(subsystem, cfg, transport, states, save_dir, fname_prefix):
    """Read the internal OSL standards, then probe every state in
    `states`, bundling everything into one file.

    Header/metadata come from whichever state was measured last --
    the same one-snapshot-per-bundle choice `measure_s11` makes for
    the ant/rec bundles, rather than one snapshot per trace.
    """
    logger.info("Measuring internal OSL standards")
    osl_s11 = subsystem.vna.measure_OSL()

    data = {}
    header = None
    metadata = None
    for state in states:
        logger.info("Measuring %s", state)
        s11, header, metadata = measure_dut(
            subsystem.vna,
            state,
            cfg=cfg,
            transport=transport,
            metadata_snapshot=subsystem.metadata_snapshot,
        )
        data[STATE_TO_DUT.get(state, state)] = s11
    header["mode"] = BUNDLE_MODE

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fname = f"{fname_prefix}_{stamp}.h5"
    # io.write_s11_file returns None (not the path it wrote), so the
    # filename for the summary below comes from `fname`, not a return
    # value.
    io.write_s11_file(
        data,
        header,
        metadata=metadata,
        cal_data=osl_s11,
        fname=fname,
        save_dir=save_dir,
    )
    return (
        f"saved {fname}  (duts: {', '.join(sorted(data))}, "
        f"OSL: {', '.join(sorted(osl_s11))})"
    )


def _loop(
    subsystem,
    cfg,
    transport,
    states,
    save_dir,
    fname_prefix,
    interval,
    stop_event,
):
    while not stop_event.is_set():
        try:
            summary = _run_cycle(
                subsystem, cfg, transport, states, save_dir, fname_prefix
            )
            logger.info("%s", summary)
        except KeyboardInterrupt:
            stop_event.set()
            break
        except Exception:
            # Catch-all is deliberate: this loop is meant to run
            # indefinitely, so one bad cycle (a VNA socket hiccup, a
            # transient switch failure, ...) must never kill the
            # whole process. Full traceback logged for diagnosis.
            logger.exception("cycle failed; continuing to next cycle")
        if stop_event.is_set():
            break
        if interval > 0:
            # `wait` returns True when the event fires mid-sleep, so
            # the next iteration exits the outer loop immediately.
            stop_event.wait(interval)


def _parse_args():
    parser = ArgumentParser(
        description=(
            "Loop a fixed list of raw VNA switch-path probes "
            "indefinitely, writing one production-format S11 HDF5 "
            "file per cycle."
        )
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
        "--save-dir",
        type=Path,
        default=Path("."),
        help="Directory for the per-cycle HDF5 files.",
    )
    parser.add_argument(
        "--states",
        type=str,
        default=",".join(DEFAULT_STATES),
        help=(
            "Comma-separated switch paths to probe each cycle, in "
            f"order. Default: {','.join(DEFAULT_STATES)}"
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help=(
            "Seconds to sleep between cycles (default: 0 -- start "
            "the next cycle immediately)."
        ),
    )
    parser.add_argument(
        "--fname-prefix",
        type=str,
        default="vna_states",
        help="Filename prefix for each cycle's HDF5 file.",
    )
    return parser.parse_args()


def main():
    configure_eig_logger(level=logging.INFO)
    args = _parse_args()
    states = _parse_states(args.states)

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
    require_pico(PicoProxy("rfswitch", transport, source="vna_state_loop"))
    subsystem = build_vna_subsystem(
        transport, cfg, source="vna_state_loop", dummy=args.dummy
    )

    stop_event = threading.Event()

    def _handle(signum, _frame):
        logger.info(
            "Signal %s received, stopping after current cycle.", signum
        )
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    try:
        with run_tag.session(transport, "vna_state_loop"):
            logger.info(
                "Looping states %s every %.1fs, saving to %s.",
                states,
                args.interval,
                args.save_dir.resolve(),
            )
            _loop(
                subsystem,
                cfg,
                transport,
                states,
                args.save_dir,
                args.fname_prefix,
                args.interval,
                stop_event,
            )
    finally:
        subsystem.cleanup()
    return 0


if __name__ == "__main__":
    sys.exit(main())
