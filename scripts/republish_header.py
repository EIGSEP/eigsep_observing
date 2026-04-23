"""Correct the wiring block of the in-Redis corr header.

Standalone tool for operator-driven header corrections. Does NOT touch
the FPGA, does NOT import ``EigsepFpga``, does NOT mutate ``corr_config``
in Redis. Loads the wiring manifest from disk, fetches the current
header from Redis, replaces its ``wiring`` field with the yaml contents,
and re-uploads. The upload re-stamps ``header_upload_unix`` so file
headers see a fresh publication time.

Effect on running processes:

- ``fpga_init.py``: unaffected; this script does not change any state
  the SNAP-setup script reads.
- ``observe.py``: the in-flight corr file keeps its file-start header
  snapshot (by design); the next file opened by
  ``EigObserver.record_corr_data`` picks up the corrected wiring via
  ``CorrConfigStore.get_header()``.
- ``panda_observe.py`` (motors, tempctrl, VNA, RF switch): does not
  read the corr header at all — zero impact.

Refuses:
- No corr header in Redis → cold boot. Run ``fpga_init.py --reinit``.

Does NOT check:
- Whether ``corr_config.yaml`` on disk matches cfg in Redis. If the
  operator also edited that, this script will not apply it; run
  ``fpga_init.py --reinit`` when ready.
- Whether the wiring declares PAMs that aren't actually initialized.
  ``EigsepFpga.header`` emits a throttled warning for that case when
  a process attaches.
"""

import argparse
import logging
import sys

import yaml

from eigsep_observing.corr import CorrConfigStore
from eigsep_observing.utils import (
    configure_eig_logger,
    get_config_path,
    load_config,
)
from eigsep_redis import Transport


def _build_parser():
    p = argparse.ArgumentParser(
        description=(
            "Correct the wiring block of the in-Redis corr header "
            "without touching the FPGA. Next corr file opened by the "
            "observer will carry the new wiring; in-flight file keeps "
            "its file-start snapshot."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--wiring_file",
        default=str(get_config_path("wiring.yaml")),
        help="Hardware wiring manifest to publish.",
    )
    p.add_argument(
        "--config_file",
        default=str(get_config_path("corr_config.yaml")),
        help="corr_config; only used to resolve Redis host/port.",
    )
    return p


def main(argv=None, transport=None):
    """Run the republish. ``transport`` is an optional override used by
    tests (pass a ``DummyTransport``); production callers leave it
    ``None`` to build a real :class:`Transport` from the cfg's redis
    block."""
    logger = configure_eig_logger(level=logging.INFO)
    args = _build_parser().parse_args(argv)

    cfg = load_config(args.config_file)
    with open(args.wiring_file, "r") as f:
        wiring = yaml.safe_load(f)
    if not isinstance(wiring, dict) or "ants" not in wiring:
        sys.exit(
            f"Invalid wiring in {args.wiring_file}: expected a dict "
            "with an 'ants' key."
        )

    if transport is None:
        rcfg = cfg["redis"]
        transport = Transport(host=rcfg["host"], port=rcfg["port"])
    store = CorrConfigStore(transport)
    try:
        header = store.get_header()
    except ValueError:
        sys.exit(
            "No corr header in Redis; cannot republish. Run "
            "fpga_init.py --reinit first."
        )

    old_wiring = header.get("wiring")
    header["wiring"] = wiring
    store.upload_header(header)

    if old_wiring == wiring:
        logger.info(
            "Republished corr header (wiring unchanged from Redis; "
            "only header_upload_unix was re-stamped)."
        )
    else:
        logger.info(
            "Republished corr header with updated wiring from "
            f"{args.wiring_file}."
        )


if __name__ == "__main__":
    main()
