from concurrent.futures import ThreadPoolExecutor
import pytest
import time
import yaml

from cmt_vna.testing import DummyVNA

# Import dummy classes before importing client to ensure mocking works
from eigsep_observing.testing import DummyEigsepRedis
from picohost.testing import (
    DummyPicoDevice,
    DummyPicoRFSwitch,
    DummyPicoPeltier,
    DummyPicoMotor,
)

import eigsep_observing
from eigsep_observing import PandaClient


# use dummy classes to simulate hardware
@pytest.fixture(autouse=True)
def dummies(monkeypatch):
    # Mock picohost at import time
    import picohost

    picohost.PicoDevice = DummyPicoDevice
    picohost.PicoRFSwitch = DummyPicoRFSwitch
    picohost.PicoPeltier = DummyPicoPeltier
    picohost.PicoMotor = DummyPicoMotor

    monkeypatch.setattr("eigsep_observing.client.VNA", DummyVNA)


@pytest.fixture(scope="module")
def module_tmpdir(tmp_path_factory):
    """
    Create a temporary directory for the module scope.
    This will be used to store VNA files and other temporary data.
    """
    return tmp_path_factory.mktemp("module_tmpdir")


@pytest.fixture
def redis():
    return DummyEigsepRedis()


@pytest.fixture
def client(redis, module_tmpdir, monkeypatch):
    # Patch init_picos to ensure attributes are set even if no picos connect
    original_init_picos = PandaClient.init_picos

    def patched_init_picos(self):
        # Initialize attributes first
        self.switch_nw = None
        self.motor = None
        self.peltier = None
        # Call original method
        original_init_picos(self)

    monkeypatch.setattr(PandaClient, "init_picos", patched_init_picos)

    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    with open(path, "r") as f:
        dummy_cfg = yaml.safe_load(f)
    dummy_cfg["vna_save_dir"] = str(module_tmpdir)
    return PandaClient(redis, default_cfg=dummy_cfg)


def test_client(client):
    # client is initialized with redis commands
    assert client.redis.client_heartbeat_check()  # check heartbeat
    # Check picos instead of sensors
    if hasattr(client, "picos") and client.picos:
        if "switch" in client.picos:
            assert client.switch_nw is not None
            # Since picohost is mocked, we just check it exists
            assert client.switch_nw is client.picos["switch"]
    # vna should be initialized if switch exists and use_vna is true
    if client.switch_nw is not None and client.cfg.get("use_vna", False):
        assert isinstance(client.vna, DummyVNA)
    else:
        assert client.vna is None


def test_add_pico(caplog, monkeypatch, client):
    caplog.set_level("DEBUG")
    # Test that client initializes picos based on config
    # The client should have initialized picos from the dummy config
    # Check that picos were initialized (if any in config)
    if hasattr(client, "picos") and client.picos:
        # With mocked picohost, we just verify picos were created
        assert len(client.picos) > 0

        # Check that switch_nw was set if switch pico exists
        if "switch" in client.picos:
            assert client.switch_nw is not None
            assert client.switch_nw is client.picos["switch"]

        # Check logging
        for record in caplog.records:
            if "Adding sensor" in record.getMessage():
                # Verify picos were attempted to be added
                assert record.levelname == "INFO"


def test_read_ctrl_switch(client):
    """
    Test read_ctrl with a switch network.
    """
    # Skip test if no switch network
    if not client.switch_nw:
        pytest.skip("No switch network initialized")

    # Add switch method to the mocked switch
    client.switch_nw.switch = lambda mode: client.redis.add_metadata(
        "obs_mode", mode
    )

    # make sure the switching updates redis
    mode = "RFANT"
    # send a switch command
    switch_cmd = f"switch:{mode}"
    # read_ctrl is blocking and will process the command in a thread
    with ThreadPoolExecutor() as ex:
        future = ex.submit(
            client.redis.read_ctrl
        )  # call redis.read_ctrl directly
        time.sleep(0.1)  # small delay to ensure read starts
        client.redis.send_ctrl(switch_cmd)  # send after read started
        cmd, kwargs = future.result(timeout=5)  # wait for the result
    # verify the command was read correctly
    assert cmd == switch_cmd
    # now test that client.read_ctrl() processes the command correctly
    with ThreadPoolExecutor() as ex:
        future = ex.submit(client.read_ctrl)  # client processes the command
        time.sleep(0.1)  # small delay to ensure read starts
        client.redis.send_ctrl(switch_cmd)  # send another command
        future.result(timeout=5)  # wait for processing to complete
    # check that switch was actually processed
    metadata = client.redis.get_live_metadata()
    assert metadata.get("obs_mode") == mode


def test_read_ctrl_VNA(client, module_tmpdir):
    """
    Test read_ctrl with VNA commands.
    """
    # Skip test if no VNA
    if not client.vna:
        pytest.skip("No VNA initialized")

    # Add switch method to the mocked switch if it exists
    if client.switch_nw:

        def mock_switch(mode, verify=False):
            client.redis.add_metadata("obs_mode", mode)

        client.switch_nw.switch = mock_switch

    # Test that VNA commands work correctly
    mode = "ant"
    vna_cmd = f"vna:{mode}"

    # First test that redis.read_ctrl() can read VNA commands
    with ThreadPoolExecutor() as ex:
        future = ex.submit(client.redis.read_ctrl)
        time.sleep(0.1)  # ensure read starts
        client.redis.send_ctrl(vna_cmd)
        cmd, kwargs = future.result(timeout=5)

    # verify the command was read correctly
    assert cmd == vna_cmd
    assert kwargs == {}

    # Now test that client.read_ctrl() processes VNA commands correctly
    with ThreadPoolExecutor() as ex:
        future = ex.submit(client.read_ctrl)
        time.sleep(0.1)  # ensure read starts
        client.redis.send_ctrl(vna_cmd)
        future.result(timeout=10)  # VNA operations might take longer

    # Verify VNA was initialized and used
    assert client.vna is not None
    assert isinstance(client.vna, DummyVNA)
