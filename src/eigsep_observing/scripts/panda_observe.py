"""Panda observing client entry point.

Starts the steady-state observing loops on the suspended LattePanda:
``switch_loop`` (RF calibration schedule), ``vna_loop`` (periodic S11),
``motor_loop`` (periodic az/el pointing scans), and ``tempctrl_loop``
Each loop is gated by a ``use_*`` flag in the observing config so the
panda can run with any subset.

Operator-launched (not systemd-supervised) — the loops control physical
actuators (motors, RF switches, Peltier controllers) and already
self-recover from per-iteration hardware faults, so auto-restart would
add risk without buying liveness. Run inside ``tmux``/``screen`` for
detachable sessions.

Installed as the ``eigsep-panda`` console script.

Dedicated observing modes that need cross-loop coordination — beam
mapping (rfswitch pinned to RFANT), VNA-at-positions, or motion/switch
sync — are deferred to separate top-level scripts; running them
through this entry point would couple the steady-state loops to
mode-specific orchestration.
"""

import argparse
import logging
import sys
from pathlib import Path
from threading import Thread

import yaml

from eigsep_redis import ConfigStore, Transport

from eigsep_observing import PandaClient, run_tag
from eigsep_observing.obs_config_owner import publish_owner

try:
    from eigsep_observing.testing import DummyPandaClient
except ImportError:
    _HAS_DUMMY = False
else:
    _HAS_DUMMY = True
from eigsep_observing.utils import configure_eig_logger, get_config_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Panda observing client",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--cfg_file",
        dest="cfg_file",
        type=Path,
        default=None,
        help=(
            "Observing config yaml (switch schedule, VNA settings, motor "
            "params). Published to Redis on launch and consumed by the "
            "ground-side observer. Defaults to the packaged "
            "obs_config.yaml, or dummy_config.yaml with --dummy."
        ),
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run in dummy mode (no hardware)",
    )
    return parser


def main() -> int:
    configure_eig_logger(level=logging.INFO)
    logger = logging.getLogger(__name__)

    parser = _build_parser()
    args = parser.parse_args()

    if args.cfg_file is None:
        args.cfg_file = get_config_path(
            "dummy_config.yaml" if args.dummy else "obs_config.yaml"
        )

    logger.info(f"Loading observing config from {args.cfg_file}")
    with open(args.cfg_file, "r") as f:
        cfg = yaml.safe_load(f)

    if args.dummy:
        if not _HAS_DUMMY:
            parser.error(
                "Running in dummy mode, but testing module is not "
                "available. Do pip install .[dev] to get required "
                "dependencies."
            )
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        transport = Transport(host="localhost", port=6380)
        transport.reset()  # reset test redis database
    else:
        transport = Transport(host="localhost", port=6379)

    client = None
    thds = {}
    try:
        with run_tag.session(transport, "panda_observe"):
            # Upload obs_config + claim ownership inside the session so
            # that if another driver currently owns the run_tag, the
            # RuntimeError from session() fires before we overwrite the
            # legitimate owner's obs_config / obs_config_owner stamps
            # (obs_config_owner has no clear() — once written, it
            # persists until the next uploader).
            ConfigStore(transport).upload(cfg)
            publish_owner(transport, "panda_observe")
            if args.dummy:
                client = DummyPandaClient(transport=transport, cfg=cfg)
            else:
                client = PandaClient(transport)
            logger.info(f"Client configuration: {client.cfg}")

            # Apply RFI-standby defaults once at startup (best-effort;
            # a down device is logged and skipped — never blocks
            # observing). See obs_config `standby:` and PandaClient.
            client.apply_standby_defaults()

            # switches
            if client.cfg["use_switches"]:
                switch_thd = Thread(target=client.switch_loop)
                thds["switch"] = switch_thd
                logger.info("Starting switch thread")
                switch_thd.start()

            # VNA
            if client.cfg["use_vna"]:
                vna_thd = Thread(target=client.vna_loop)
                thds["vna"] = vna_thd
                logger.info("Starting VNA thread")
                vna_thd.start()

            # motor (periodic az/el scans)
            if client.cfg.get("use_motor", False):
                motor_thd = Thread(target=client.motor_loop)
                thds["motor"] = motor_thd
                logger.info("Starting motor thread")
                motor_thd.start()

            # tempctrl
            if client.cfg.get("use_tempctrl", False):
                tempctrl_thd = Thread(target=client.tempctrl_loop)
                thds["tempctrl"] = tempctrl_thd
                logger.info("Starting tempctrl thread")
                tempctrl_thd.start()

            client.stop_client.wait()  # wait until stop signal is set
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, stopping threads")
    finally:
        if client is not None:
            client.stop()
        for name, t in thds.items():
            logger.info(f"Joining thread {name}")
            t.join()
            logger.info(f"Thread {name} joined")
        logger.info("All threads joined, exiting.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
