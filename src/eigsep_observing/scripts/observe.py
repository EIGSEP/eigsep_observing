"""Observing-side file writer.

Writes correlation and VNA data to disk. Corr writing starts as soon as
the SNAP is producing — independent of any panda-side state — and the
``obs_config`` / ``run_tag`` overlays stamped into each corr file header
are read opportunistically per file boundary (see
:meth:`EigObserver._with_header_overlays`). This decouples the writer
from ``panda_observe`` so any panda-side script (or none at all) can run
alongside it.

The VNA thread is spawned unconditionally whenever ``--panda`` is on;
it polls ``panda_connected`` internally and sits idle if no VNA data
appears on the stream.

Installed as the ``eigsep-observe`` console script and run by the
``eigsep-observe-writer.service`` systemd unit (see ``deploy/systemd/``).
"""

import argparse
import logging
import sys
import threading

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
        "--snap",
        dest="use_snap",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Read correlation data from RPi Redis in box.",
    )
    parser.add_argument(
        "--panda",
        dest="use_panda",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Connect to LattePanda in box.",
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run in dummy mode, using mock Redis instances.",
    )
    return parser


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

    # initialize the Redis transports
    if args.use_snap:
        logger.info(f"Connecting to RPi Redis instance at {rpi_ip}.")
        transport_snap = Transport(host=rpi_ip, port=redis_port)
    else:
        logger.warning("Not connecting to RPi Redis instance.")
        transport_snap = None
    if args.use_panda:
        logger.info(f"Connecting to LattePanda at {panda_ip}.")
        transport_panda = Transport(host=panda_ip, port=redis_port)
    else:
        logger.warning("Not connecting to LattePanda")
        transport_panda = None

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

    # set up file writing: corr_thd for correlation data, panda_thd for s11
    if args.use_snap:
        corr_thd = threading.Thread(target=_corr_target)
        thds["corr"] = corr_thd
        logger.info("Starting correlation file writing thread.")
        corr_thd.start()

    # VNA file writing: spawn unconditionally when --panda is on. The
    # thread polls ``panda_connected`` internally and idles on an empty
    # VNA stream, so it's harmless when no panda script is publishing.
    if args.use_panda:
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
        if args.use_snap:
            transport_snap.reset()
        if args.use_panda:
            transport_panda.reset()

    return 1 if corr_crashed[0] else 0


if __name__ == "__main__":
    sys.exit(main())
