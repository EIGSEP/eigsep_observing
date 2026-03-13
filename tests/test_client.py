from concurrent.futures import ThreadPoolExecutor
import json
import pytest
import time
import yaml

from cmt_vna.testing import DummyVNA

# Import dummy classes before importing client to ensure mocking works
from eigsep_observing.testing import DummyEigsepRedis

import eigsep_observing
from eigsep_observing.testing import DummyPandaClient
from eigsep_observing.testing.utils import compare_dicts


@pytest.fixture(scope="module")
def module_tmpdir(tmp_path_factory):
    """
    Create a temporary directory for the module scope.
    This will be used to store VNA files and other temporary data.
    """
    return tmp_path_factory.mktemp("module_tmpdir")


@pytest.fixture()
def dummy_cfg(module_tmpdir):
    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg["vna_save_dir"] = str(module_tmpdir)
    return cfg


@pytest.fixture
def redis():
    return DummyEigsepRedis()


@pytest.fixture
def client(redis, dummy_cfg):
    return DummyPandaClient(redis, default_cfg=dummy_cfg)


def test_client(client):
    # client is initialized with redis commands
    assert client.redis.client_heartbeat_check()  # check heartbeat
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


def test_get_cfg(caplog, dummy_cfg):
    caplog.set_level("INFO")

    # should be no config in redis at start
    r = DummyEigsepRedis(port=6380)  # different port to avoid conflicts
    with pytest.raises(ValueError):
        r.get_config()
    client2 = DummyPandaClient(r, default_cfg={})
    # should have created a logger warning about missing config
    for record in caplog.records:
        if "No configuration found in Redis" in record.getMessage():
            assert record.levelname == "WARNING"
    # after init of client2, the cfg should be in redis
    # it is appended with a timestamp and empty pico config
    cfg_in_redis = client2._get_cfg()
    assert len(cfg_in_redis) == 2  # timestamp and picos
    assert "upload_time" in cfg_in_redis
    assert "picos" in cfg_in_redis
    assert cfg_in_redis["picos"] == {}

    # upload the dummy config to client2's redis
    client2.redis.upload_config(dummy_cfg, from_file=False)

    # check that they're the same
    retrieved_cfg = client2._get_cfg()
    retrieved_cfg_copy = retrieved_cfg.copy()
    del retrieved_cfg_copy["upload_time"]
    dummy_cfg_serialized = json.loads(json.dumps(dummy_cfg))
    compare_dicts(dummy_cfg_serialized, retrieved_cfg_copy)

    # if reinit client2, it should get the config from redis
    client3 = DummyPandaClient(r, default_cfg={})
    retrieved_cfg2 = client3._get_cfg()
    compare_dicts(client3.cfg, retrieved_cfg2)
    # retrieved_cfg was directly uploaded so didn't have picos
    del retrieved_cfg2["picos"]
    compare_dicts(retrieved_cfg, retrieved_cfg2)

    # check logging
    for record in caplog.records:
        if "Using config from Redis" in record.getMessage():
            assert record.levelname == "INFO"


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

@pytest.mark.skip("read ctrl are deprecated")
def test_read_ctrl_switch(client):
    """
    Test read_ctrl with a switch network.
    """
    # make sure the switching updates redis
    mode = "RFANT"
    # send a switch command
    switch_cmd = f"switch:{mode}"
    # read_ctrl is blocking and will process the command in a thread
    with ThreadPoolExecutor() as ex:
        future = ex.submit(client.redis.read_ctrl)
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


@pytest.mark.skip("read ctrl are deprecated")
def test_read_ctrl_VNA(client, module_tmpdir):
    """
    Test read_ctrl with VNA commands.
    """
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
