"""Panda observing client entry point.

Starts the steady-state observing loops on the suspended LattePanda:
``switch_loop`` (RF calibration schedule), ``vna_loop`` (periodic S11),
and ``motor_loop`` (periodic az/el pointing scans). Each loop is gated
by a ``use_*`` flag in the observing config so the panda can run with
any subset.

Dedicated observing modes that need cross-loop coordination — beam
mapping (rfswitch pinned to RFANT), VNA-at-positions, or motion/switch
sync — are deferred to separate top-level scripts; running them
through this entry point would couple the steady-state loops to
mode-specific orchestration.
"""

from argparse import ArgumentParser
import logging
from threading import Thread

from eigsep_redis import Transport

from eigsep_observing import PandaClient
from eigsep_observing.testing import DummyPandaClient
from eigsep_observing.utils import configure_eig_logger

# logger with rotating file handler
configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = ArgumentParser(description="Panda observing client")
parser.add_argument(
    "--dummy", action="store_true", help="Run in dummy mode (no hardware)"
)
args = parser.parse_args()


if args.dummy:
    logger.warning("Running in DUMMY mode, no hardware will be used.")
    transport = Transport(host="localhost", port=6380)
    transport.reset()  # reset test redis database
    client = DummyPandaClient(transport=transport)
else:
    transport = Transport(host="localhost", port=6379)
    client = PandaClient(transport)

logger.info(f"Client configuration: {client.cfg}")
thds = {}
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

try:
    client.stop_client.wait()  # wait until stop signal is set
except KeyboardInterrupt:
    logger.info("Keyboard interrupt received, stopping threads")
finally:
    client.stop()
    for name, t in thds.items():
        logger.info(f"Joining thread {name}")
        t.join()
        logger.info(f"Thread {name} joined")
    logger.info("All threads joined, exiting.")
