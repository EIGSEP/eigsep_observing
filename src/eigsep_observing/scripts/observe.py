"""Observing-side file writer.

Writes correlation and VNA data to disk. SNAP is required — the corr
thread is the writer's reason to exist. The panda is optional and
opportunistic: ``obs_config`` / ``run_tag`` overlays and the per-
integration metadata sidecar are read whenever the panda is reachable
(see :meth:`EigObserver._with_header_overlays` and the corr loop's
drain at ``observer.record_corr_data``), and a panda that is missing
at startup or drops mid-run is logged at WARNING and degrades to
corr-only without raising. Corr data is sacred; a panda failure must
never block a SNAP-side write.

The VNA file thread runs unconditionally — it polls ``panda_connected``
internally and idles on an empty VNA stream, so it's harmless when no
panda is publishing.

Installed as the ``eigsep-observe`` console script and run by the
``eigsep-observe-writer.service`` systemd unit (see ``deploy/systemd/``).
"""

import argparse
import logging
import sys
import threading

import redis.exceptions
from eigsep_redis import Transport

from eigsep_observing import EigObserver

try:
    from eigsep_observing.testing import DummyEigObserver
except ImportError:
    _HAVE_DUMMY = False
else:
    _HAVE_DUMMY = True
from eigsep_observing.utils import configure_eig_logger


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Eigsep Observer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--rpi-ip",
        dest="rpi_ip",
        default="10.10.10.10",
        help="IP of the RPi hosting the SNAP correlator Redis.",
    )
    parser.add_argument(
        "--panda-ip",
        dest="panda_ip",
        default="10.10.10.11",
        help="IP of the LattePanda hosting the observing-config Redis.",
    )
    parser.add_argument(
        "--corr-save-dir",
        dest="corr_save_dir",
        default="/media/eigsep/T7/data",
        help="Directory for correlator HDF5 files.",
    )
    parser.add_argument(
        "--vna-save-dir",
        dest="vna_save_dir",
        default="/media/eigsep/T7/data/s11_data",
        help="Directory for VNA S11 HDF5 files.",
    )
    parser.add_argument(
        "--corr-ntimes",
        dest="corr_ntimes",
        type=int,
        default=240,
        help="Integrations per correlator HDF5 file.",
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run in dummy mode, using mock Redis instances.",
    )
    return parser


def _connect_panda(panda_ip: str, redis_port: int, logger) -> Transport | None:
    """Attempt the panda transport; warn and degrade on failure.

    The corr loop's only hard dependency is the SNAP transport. Header
    overlays (``obs_config`` / ``run_tag``) and the metadata sidecar
    are best-effort, with the observer already wired to handle
    ``transport_panda=None`` (sentinel header values, empty metadata).
    A non-fatal startup-time WARNING preserves "corr data is sacred"
    when the panda is down for a SNAP-only test session.
    """
    logger.info(f"Connecting to LattePanda at {panda_ip}.")
    try:
        return Transport(host=panda_ip, port=redis_port)
    except redis.exceptions.ConnectionError as exc:
        logger.warning(
            "Panda Redis unreachable at %s:%s (%s). Running corr-only — "
            "restart the observer once the panda is back to re-enable "
            "metadata sidecar and header overlays.",
            panda_ip,
            redis_port,
            exc,
        )
        return None


def main() -> int:
    # logger with rotating file handler
    configure_eig_logger(level=logging.INFO)
    logger = logging.getLogger(__name__)

    parser = _build_parser()
    args = parser.parse_args()
    if args.dummy:
        if not _HAVE_DUMMY:
            parser.error(
                "Dummy mode requires eigsep_observing.testing. "
                "Please install with `pip install eigsep-observing[dev]`."
            )
        logger.warning(
            "Running in DUMMY mode, using mock Redis instances. "
            "No actual data will be recorded."
        )
        redis_port = 6380  # test port for mock Redis
        # Dummy mode always targets the fakeredis instance started by
        # panda_observe --dummy on the same machine.
        rpi_ip = "localhost"
        panda_ip = "localhost"
    else:
        redis_port = 6379
        rpi_ip = args.rpi_ip
        panda_ip = args.panda_ip

    # SNAP transport is required — there is no "observer without SNAP"
    # mode any more (use scripts/record_metadata.py for test bench data
    # collection that needs no correlator). A SNAP-side failure here
    # is fatal; nothing else this script does makes sense without it.
    logger.info(f"Connecting to RPi Redis instance at {rpi_ip}.")
    transport_snap = Transport(host=rpi_ip, port=redis_port)

    # Panda transport is opportunistic — see _connect_panda.
    transport_panda = _connect_panda(panda_ip, redis_port, logger)

    if args.dummy:
        observer = DummyEigObserver(
            transport_snap=transport_snap,
            transport_panda=transport_panda,
        )
    else:
        observer = EigObserver(
            transport_snap=transport_snap,
            transport_panda=transport_panda,
        )

    thds = {}
    thds["status"] = observer.status_thread

    # crash observing if the corr thread dies for any reason
    corr_crashed = [False]

    def _corr_target():
        try:
            observer.record_corr_data(
                args.corr_save_dir,
                ntimes=args.corr_ntimes,
                timeout=10,
            )
        except Exception:
            logger.exception(
                "Correlator recording crashed. Stopping observer."
            )
            corr_crashed[0] = True
            observer.stop_event.set()

    corr_thd = threading.Thread(target=_corr_target)
    thds["corr"] = corr_thd
    logger.info("Starting correlation file writing thread.")
    corr_thd.start()

    # VNA file writing: always spawn. The thread polls
    # ``panda_connected`` internally and idles on an empty VNA stream,
    # so it's harmless when no panda is publishing or the panda is
    # unreachable.
    vna_thd = threading.Thread(
        target=observer.record_vna_data,
        args=(args.vna_save_dir,),
    )
    thds["vna"] = vna_thd
    logger.info("Starting VNA file writing thread.")
    vna_thd.start()

    try:
        observer.stop_event.wait()  # wait until stop event
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, stopping observer.")
    finally:
        observer.close()
        # close() joined the status thread; join the rest.
        for name in thds:
            if name == "status":
                continue
            logger.info(f"Stopping thread: {name}")
            thds[name].join()
            logger.info(f"Thread {name} stopped.")
        logger.info("All threads stopped. Exiting observer.")

    if args.dummy:
        # reset Redis instances
        transport_snap.reset()
        if transport_panda is not None:
            transport_panda.reset()

    return 1 if corr_crashed[0] else 0


if __name__ == "__main__":
    sys.exit(main())
