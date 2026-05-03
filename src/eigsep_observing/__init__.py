__author__ = "Christian Hellum Bye"
__version__ = "2.0.1"

from .client import PandaClient
from .motion_switch import MotionSwitchCoordinator
from .observer import EigObserver
from .fpga import EigsepFpga
from .motor_client import MotorClient
from .motor_zeroer import MotorZeroer
from .tempctrl_client import TempCtrlClient

try:
    from . import testing
except ImportError as e:
    import logging

    logging.warning(
        f"Could not import testing module: {e}, use pip install .[dev] to "
        "install the required dependencies for testing if needed."
    )
