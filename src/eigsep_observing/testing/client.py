from cmt_vna.testing import DummyVNA
import picohost

from .. import PandaClient


class DummyPandaClient(PandaClient):
    """
    Mock up of PandaClient for testing purposes, that uses dummy
    implementations of the VNA and PicoHost.
    """

    # override pico classes with dummies
    PICO_CLASSES = {
        "imu": None,  # XXX we're here
    }
