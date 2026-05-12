__author__ = "Christian Hellum Bye"
__version__ = "2.1.0"

from .client import PandaClient
from .motion_switch import MotionSwitchCoordinator
from .observer import EigObserver
from .fpga import EigsepFpga
from .motor_client import MotorClient
from .motor_zeroer import MotorZeroer
from .status_log_handler import StatusStreamHandler
from .tempctrl_client import TempCtrlClient

try:
    from . import testing
except ImportError as e:
    import logging

    logging.warning(
        f"Could not import testing module: {e}, use pip install .[dev] to "
        "install the required dependencies for testing if needed."
    )
