import threading
from unittest.mock import MagicMock
import logging

logger = logging.getLogger(__name__)


class DummyPico:
    def __init__(self, port, timeout=5):
        self.port = port
        self.timeout = timeout
        self.is_connected = False
        self.response_handler = None
        self._thread = None
        self.ser = MagicMock()
        self.ser.is_open = True

    def connect(self):
        logger.debug(f"DummyPico connecting to {self.port}")
        self.is_connected = True
        return True

    def set_response_handler(self, handler):
        self.response_handler = handler

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        # Simulate periodic data sending
        if self.response_handler:
            # Send some dummy data
            self.response_handler({"test": "data"})

    def is_alive(self):
        return self._thread and self._thread.is_alive()


class DummyPicoDevice(DummyPico):
    pass


class DummyPicoRFSwitch(DummyPico):
    def __init__(self, port, timeout=5):
        super().__init__(port, timeout)
        self.redis = None

    def switch(self, mode):
        return True


class DummyPicoPeltier(DummyPico):
    pass


class DummyPicoMotor(DummyPico):
    pass
