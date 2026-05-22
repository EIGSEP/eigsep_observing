"""Standalone VNA position-sweep characterization.

Walks the motors through a coarse az/el grid (configured under
``vna_position_sweep`` in the observing yaml) and runs a full S11
measurement at each grid point. The "full" measurement is the same
``measure_s11("ant") -> measure_s11("rec")`` pair the regular
``vna_loop`` runs, so each grid point produces:

* OSL calibration (open / short / load),
* antenna + noise + load measurement,
* OSL calibration again,
* receiver measurement.

These are bundled and published through ``vna_writer`` exactly as
``vna_loop`` would. Per-position correlation downstream is by motor
position metadata in the snapshot — no new "vna_position_sweep"
record type is added.

This script does **not** start ``switch_loop``, ``vna_loop``, or
``motor_loop`` — it owns the picohost-mediated devices for its run
and forces ``coord.serialize = True`` on the panda so motor moves and
RF switching never overlap during characterization.
"""

from argparse import ArgumentParser
import logging
from pathlib import Path

import yaml

from eigsep_redis import ConfigStore, StatusWriter, Transport

from eigsep_observing import PandaClient, run_tag
from eigsep_observing.obs_config_owner import publish_owner
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
    """Build a panda client with motor + VNA forced on and serialize forced True.

    The script's whole purpose is "characterize antenna under controlled
    conditions" — running with switching racing motor moves would
    defeat that. Forcing the flag here means the deployed yaml stays
    untouched (default-off) but this script is always serialized.
    """
    cfg = dict(cfg)
    cfg["use_motor"] = True
    cfg["use_vna"] = True
    # Don't start the steady-state loops — we drive everything inline.
    cfg["use_switches"] = False
    cfg["use_tempctrl"] = cfg.get("use_tempctrl", False)
    cfg["serialize_motion_and_switching"] = True
    ConfigStore(transport).upload(cfg)
    publish_owner(transport, "vna_position_sweep")
    if dummy:
        from eigsep_observing.testing import DummyPandaClient

        return DummyPandaClient(transport=transport, cfg=cfg)
    return PandaClient(transport)


def _grid(sweep_cfg):
    az_grid = list(sweep_cfg.get("az_grid_deg", []))
    el_grid = list(sweep_cfg.get("el_grid_deg", []))
    if not az_grid or not el_grid:
        raise ValueError(
            "vna_position_sweep requires non-empty az_grid_deg and "
            f"el_grid_deg; got az={az_grid}, el={el_grid}."
        )
    return [(float(az), float(el)) for az in az_grid for el in el_grid]


def main(transport, args):
    if args.cfg_file is None:
        args.cfg_file = get_config_path(
            "dummy_config.yaml" if args.dummy else "obs_config.yaml"
        )
    logger.info(f"Loading observing config from {args.cfg_file}")
    with open(args.cfg_file, "r") as f:
        cfg = yaml.safe_load(f)

    sweep_cfg = cfg.get("vna_position_sweep")
    if not isinstance(sweep_cfg, dict):
        raise ValueError(
            "vna_position_sweep block missing or malformed in "
            f"{args.cfg_file}."
        )
    grid = _grid(sweep_cfg)
    settle_s = float(sweep_cfg.get("settle_s", 0.0))

    status = StatusWriter(transport)
    client = None
    try:
        with run_tag.session(transport, "vna_position_sweep"):
            client = _build_client(transport, cfg, args.dummy)
            if client.motor_client is None:
                raise RuntimeError(
                    "Motor client not initialized; check motor pico "
                    "registration."
                )
            if client.vna is None:
                raise RuntimeError(
                    "VNA not initialized; check vna config block."
                )

            status.send(
                f"vna_position_sweep started ({len(grid)} grid points, "
                f"settle_s={settle_s})"
            )
            logger.info(
                f"vna_position_sweep started ({len(grid)} grid points, "
                f"settle_s={settle_s})"
            )

            client.motor_client.set_delay()
            client.motor_client.halt()
            client.motor_client.home()
            for idx, (az, el) in enumerate(grid):
                if client.stop_client.is_set():
                    logger.info("stop_client set; aborting sweep")
                    break
                logger.info(
                    f"[{idx + 1}/{len(grid)}] move_to az={az}, el={el}"
                )
                client.motor_client.move_to(az_deg=az, el_deg=el)
                if settle_s > 0 and client.stop_client.wait(settle_s):
                    break
                with client.coord.switch_section():
                    for mode in ("ant", "rec"):
                        logger.info(
                            f"[{idx + 1}/{len(grid)}] measure_s11({mode!r})"
                        )
                        client.measure_s11(mode)
            client.motor_client.home()
    except KeyboardInterrupt:
        logger.info("Sweep interrupted by user")
    except (TimeoutError, RuntimeError) as exc:
        logger.error("vna_position_sweep aborted: %s", exc)
        status.send(
            f"vna_position_sweep aborted ({type(exc).__name__}: {exc})",
            level=logging.ERROR,
        )
    finally:
        if client is not None:
            if client.motor_client is not None:
                client.motor_client.halt()
            client.stop()
        status.send("vna_position_sweep ended")
        logger.info("vna_position_sweep ended")


def _parse_args():
    parser = ArgumentParser(
        description=(
            "Standalone VNA-at-positions characterization (motor grid + "
            "S11 at each position)."
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
