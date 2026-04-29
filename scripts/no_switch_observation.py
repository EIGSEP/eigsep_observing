"""Bracketed motor scan with no in-flight switching.

Flow:

1. Calibration block at home — VNA sweep (ant + rec) plus dwells on
   every non-RFANT mode in ``switch_schedule``.
2. Motor scan with the rfswitch pinned to RFANT for the entire scan
   duration. ``switch_loop`` and ``vna_loop`` are not started, so
   nothing else can drive the switch out of RFANT during the scan.
3. Calibration block at home again, so post-processing can interpolate
   the calibration solution across the scan.

The scan-phase pin is implemented by holding ``client.switch_session``
for the entire ``MotorClient.scan`` call. With per-move
``coord.motion_section`` re-acquiring the same lock, this only works
because the panda's switch lock is an ``RLock``. A plain ``Lock`` would
deadlock on the first move.
"""

from argparse import ArgumentParser
import logging
from pathlib import Path

import yaml

from eigsep_redis import ConfigStore, StatusWriter, Transport

from eigsep_observing import PandaClient
from eigsep_observing.run_tag import clear as clear_run_tag
from eigsep_observing.run_tag import publish as publish_run_tag
from eigsep_observing.testing import DummyPandaClient
from eigsep_observing.utils import configure_eig_logger, get_config_path


configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


def _build_transport(dummy):
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        transport = Transport(host="localhost", port=6380)
        transport.reset()
        return transport
    return Transport(host="localhost", port=6379)


def _build_client(transport, cfg, dummy):
    """Build a panda client with motor + VNA on, steady-state loops off."""
    cfg = dict(cfg)
    cfg["use_motor"] = True
    cfg["use_vna"] = True
    cfg["use_switches"] = False
    cfg["serialize_motion_and_switching"] = True
    ConfigStore(transport).upload(cfg)
    if dummy:
        return DummyPandaClient(transport=transport, default_cfg=cfg)
    return PandaClient(transport)


def _calibration(client, status, label):
    status.send(f"no_switch_observation: calibration {label} starting")
    logger.info(f"calibration {label} starting")
    completed = client.run_calibration_sequence()
    if not completed:
        logger.warning(f"calibration {label} aborted by stop_client")
    status.send(
        f"no_switch_observation: calibration {label} "
        f"{'complete' if completed else 'aborted'}"
    )
    return completed


def main(transport, args):
    if args.cfg_file is None:
        args.cfg_file = get_config_path(
            "dummy_config.yaml" if args.dummy else "obs_config.yaml"
        )
    logger.info(f"Loading observing config from {args.cfg_file}")
    with open(args.cfg_file, "r") as f:
        cfg = yaml.safe_load(f)

    scan_kwargs = cfg.get("motor_scan") or {}
    if not isinstance(scan_kwargs, dict):
        raise ValueError(
            f"motor_scan must be a dict; got {type(scan_kwargs).__name__}."
        )

    status = StatusWriter(transport)
    client = None
    try:
        client = _build_client(transport, cfg, args.dummy)
        if client.motor_client is None:
            raise RuntimeError(
                "Motor client not initialized; check motor pico registration."
            )
        if client.vna is None:
            raise RuntimeError("VNA not initialized; check vna config block.")

        publish_run_tag(transport, "no_switch_observation")
        status.send("no_switch_observation started")
        logger.info("no_switch_observation started")

        client.motor_client.set_delay()
        client.motor_client.halt()
        client.motor_client.home()

        if not _calibration(client, status, "pre-scan"):
            return

        # Pin RFANT for the duration of the scan. switch_session
        # holds coord.switch_section() across the whole block;
        # per-move motion_section calls re-acquire the same RLock
        # from this thread, which is the load-bearing reason the
        # underlying lock is an RLock and not a plain Lock.
        with client.switch_session() as sw:
            if not sw("RFANT"):
                raise RuntimeError("Failed to pin rfswitch to RFANT for scan.")
            status.send(
                "no_switch_observation: rfswitch pinned RFANT, scanning"
            )
            logger.info("rfswitch pinned RFANT; starting scan")
            client.motor_client.scan(
                stop_event=client.stop_client, **scan_kwargs
            )

        client.motor_client.home()

        if client.stop_client.is_set():
            logger.warning(
                "stop_client set after scan; skipping post-scan calibration"
            )
            return
        _calibration(client, status, "post-scan")
    except KeyboardInterrupt:
        logger.info("Observation interrupted by user")
    except (TimeoutError, RuntimeError) as exc:
        logger.error("no_switch_observation aborted: %s", exc)
        status.send(
            f"no_switch_observation aborted ({type(exc).__name__}: {exc})",
            level=logging.ERROR,
        )
    finally:
        if client is not None:
            if client.motor_client is not None:
                client.motor_client.halt()
            client.stop()
        clear_run_tag(transport)
        status.send("no_switch_observation ended")
        logger.info("no_switch_observation ended")


def _parse_args():
    parser = ArgumentParser(
        description=(
            "Bracketed motor scan with calibration on either side and "
            "rfswitch pinned to RFANT during the scan."
        )
    )
    parser.add_argument(
        "--cfg_file",
        type=Path,
        default=None,
        help=(
            "Observing config yaml. Defaults to the packaged "
            "obs_config.yaml, or dummy_config.yaml with --dummy."
        ),
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    transport = _build_transport(args.dummy)
    main(transport, args)
