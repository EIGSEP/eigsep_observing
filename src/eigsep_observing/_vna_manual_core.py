"""Shared helpers for ``scripts/vna_manual.py`` and ``scripts/record_vna.py``.

Both scripts drive the same production ``PandaClient.measure_s11``
bundle path and write the resulting payload through
:func:`eigsep_observing.vna.save_vna_manual_h5`. The only difference
is the *trigger*: ``vna_manual`` reads bundle selections from an
interactive REPL, ``record_vna`` runs the same selections on a fixed
interval. The bundle-running, client-building, and transport-building
machinery is identical, so it lives here.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import yaml

from eigsep_redis import ConfigStore, Transport

from . import PandaClient
from .utils import get_config_path
from .vna import save_vna_manual_h5

logger = logging.getLogger(__name__)


def build_vna_transport(dummy: bool) -> Transport:
    """Same transport convention as ``scripts/vna_manual.py``: real
    Redis on ``localhost:6379`` in production; fakeredis-backed real
    Redis on ``localhost:6380`` in dummy mode, with a flush at the
    start so each test session starts clean."""
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        transport = Transport(host="localhost", port=6380)
        transport.reset()
        return transport
    return Transport(host="localhost", port=6379)


def build_vna_client(
    transport: Transport, cfg: dict, dummy: bool
) -> PandaClient:
    """Build a VNA-only ``PandaClient``: switches/motor/tempctrl off,
    motion/switching serialization off (the manual bring-up runs each
    bundle inside its own ``switch_section``)."""
    cfg = dict(cfg)
    cfg["use_vna"] = True
    cfg["use_switches"] = False
    cfg["use_motor"] = False
    cfg["use_tempctrl"] = False
    cfg["serialize_motion_and_switching"] = False
    ConfigStore(transport).upload(cfg)
    if dummy:
        from .testing import DummyPandaClient

        return DummyPandaClient(transport=transport, default_cfg=cfg)
    return PandaClient(transport)


def load_vna_cfg(cfg_file: Path | None, dummy: bool) -> dict:
    """Resolve the obs-config yaml the same way the manual scripts do."""
    if cfg_file is None:
        cfg_file = get_config_path(
            "dummy_config.yaml" if dummy else "obs_config.yaml"
        )
    with open(cfg_file, "r") as f:
        return yaml.safe_load(f)


def summary_db(arr) -> float:
    """Average |Γ| in dB over non-zero magnitudes; ``nan`` if all-zero."""
    mag = np.abs(np.asarray(arr))
    mag = mag[mag > 0]
    if mag.size == 0:
        return float("nan")
    return float(20.0 * np.log10(np.mean(mag)))


def run_bundle(client: PandaClient, mode: str, save_dir: Path) -> str:
    """Run one OSL+DUT bundle and write the local HDF5 file.

    Mirrors ``scripts/vna_manual._run_bundle``: catches measurement
    failures and local-save OS errors so the caller (REPL or loop) can
    keep going. Returns a one-line summary suitable for stdout.
    """
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
    db = summary_db(s11[mode])
    return f"{mode} saved {path.name}  (|Γ|_{mode}_mean={db:.1f} dB)"
