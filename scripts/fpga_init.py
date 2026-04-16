import logging
from eigsep_observing.utils import configure_eig_logger

logger = configure_eig_logger(level=logging.INFO)

import argparse  # noqa: E402
from threading import Thread  # noqa: E402

import IPython  # noqa: E402
from eigsep_observing import EigsepFpga  # noqa: E402
from eigsep_observing.testing import DummyEigsepFpga  # noqa: E402
from eigsep_observing.utils import get_config_path, load_config  # noqa: E402

parser = argparse.ArgumentParser(
    description=(
        "SNAP observing with Eigsep FPGA. "
        "With no flags, attach to a SNAP that is already running and "
        "synced (sync_time is rehydrated from the corr header). "
        "Use --reinit for a fresh observing block (ADC + FPGA regs + "
        "sync); add -p/-P to also (re)program the bitstream."
    ),
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-p",
    dest="program",
    action="store_true",
    default=False,
    help="Program Eigsep correlator (requires --reinit).",
)
parser.add_argument(
    "-P",
    dest="force_program",
    action="store_true",
    default=False,
    help="Force program Eigsep correlator (requires --reinit).",
)
parser.add_argument(
    "--reinit",
    dest="reinit",
    action="store_true",
    default=False,
    help=(
        "Full re-init: ADC + FPGA registers + sync. Starts a fresh "
        "observing block and invalidates any prior sync_time."
    ),
)
parser.add_argument(
    "--config_file",
    dest="config_file",
    default=str(get_config_path("corr_config.yaml")),
    help="Configuration file for Eigsep Fpga.",
)
parser.add_argument(
    "--dummy",
    dest="dummy_mode",
    action="store_true",
    default=False,
    help="Run with a dummy SNAP interface",
)
parser.add_argument(
    "-i",
    "--interactive",
    action="store_true",
    default=False,
    help="Drop into an IPython shell with live access to the fpga object.",
)
args = parser.parse_args()

if (args.program or args.force_program) and not args.reinit:
    parser.error(
        "-p/-P requires --reinit: a fresh bitstream leaves all FPGA "
        "registers at zero and needs a full init + sync."
    )

cfg = load_config(args.config_file)

if args.force_program:
    program = "force"
else:
    program = args.program

if args.dummy_mode:
    logger.warning("Running in DUMMY mode.")
    fpga = DummyEigsepFpga(cfg=cfg, program=program)
else:
    snap_ip = cfg["snap_ip"]
    logger.info(f"Connecting to Eigsep correlator at {snap_ip}.")
    fpga = EigsepFpga(cfg=cfg, program=program)

if args.reinit:
    # Fresh observing block: full init + sync.
    fpga.initialize(initialize_adc=True, initialize_fpga=True, sync=True)
    # Validate cfg against freshly-initialized hardware and publish.
    fpga.upload_config(validate=True)
else:
    # Attach path: SNAP is already running; recover sync_time from the
    # header so CorrWriter.add doesn't drop every integration.
    logger.info("Attaching to running SNAP (no --reinit).")
    if not fpga.rehydrate_sync_from_header():
        parser.error(
            "No valid sync_time on corr header; refusing to attach. "
            "Run with --reinit to start a fresh observing block."
        )
    # validate_config against hardware is vacuous on attach (the
    # header echoes cfg for blocks this process didn't initialize).
    # Use Redis as source of truth: a cfg diff means the yaml was
    # edited without a matching reinit — refuse rather than silently
    # push the edited cfg to Redis.
    try:
        fpga.assert_config_matches_redis()
    except RuntimeError as e:
        parser.error(str(e))

# start observing
logger.info("Starting observation.")
if args.interactive:
    thd = Thread(
        target=fpga.observe,
        kwargs={"pairs": None, "timeout": 10},
        daemon=True,
    )
    thd.start()
    try:
        IPython.embed(
            banner1=(
                "EIGSEP interactive shell. The `fpga` object is live.\n"
                "Type `exit` or Ctrl-D to stop observing and exit."
            ),
        )
    finally:
        fpga.end_observing()
        thd.join(timeout=5)
        logger.info("Observing done.")
else:
    try:
        fpga.observe(pairs=None, timeout=10)
    except KeyboardInterrupt:
        logger.info("Observation interrupted by user.")
    finally:
        fpga.end_observing()
        logger.info("Observing done.")
