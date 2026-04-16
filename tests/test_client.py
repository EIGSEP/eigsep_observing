import json
import pytest
import yaml

from cmt_vna.testing import DummyVNA
from picohost.proxy import PicoProxy

from eigsep_observing.testing import DummyEigsepObsRedis

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
    return DummyEigsepObsRedis()


@pytest.fixture
def client(redis, dummy_cfg):
    c = DummyPandaClient(redis, default_cfg=dummy_cfg)
    yield c
    c.stop()


def test_client(client):
    # client is initialized with redis commands
    assert client.redis.heartbeat_reader.check()  # check heartbeat
    # sw_proxy is always created as a generic PicoProxy
    assert client.sw_proxy is not None
    assert isinstance(client.sw_proxy, PicoProxy)
    # vna should be initialized if use_vna is true in config
    if client.cfg.get("use_vna", False):
        assert isinstance(client.vna, DummyVNA)
    else:
        assert client.vna is None


def test_get_cfg(caplog, dummy_cfg):
    caplog.set_level("INFO")

    # should be no config in redis at start
    r = DummyEigsepObsRedis(port=6380)  # different port to avoid conflicts
    with pytest.raises(ValueError):
        r.config.get()
    client2 = DummyPandaClient(r, default_cfg={})
    client3 = None
    try:
        # should have created a logger warning about missing config
        for record in caplog.records:
            if "No configuration found in Redis" in record.getMessage():
                assert record.levelname == "WARNING"
        # after init of client2, the cfg should be in redis
        cfg_in_redis = client2._get_cfg()
        assert "upload_time" in cfg_in_redis

        # upload the dummy config to client2's redis
        client2.redis.config.upload(dummy_cfg)

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

        # check logging
        for record in caplog.records:
            if "Using config from Redis" in record.getMessage():
                assert record.levelname == "INFO"
    finally:
        client2.stop()
        if client3 is not None:
            client3.stop()


def test_switch_proxy_created(client):
    """sw_proxy is a PicoProxy that can see PicoManager's rfswitch."""
    assert isinstance(client.sw_proxy, PicoProxy)
    assert client.sw_proxy.is_available
    assert client.sw_proxy.name == "rfswitch"


def test_pico_manager_devices_visible(client):
    """PicoManager's registered devices are visible in Redis."""
    available = client.redis.r.smembers("picos")
    names = {n.decode() if isinstance(n, bytes) else n for n in available}
    expected = {
        "tempctrl",
        "potmon",
        "imu_el",
        "imu_az",
        "lidar",
        "rfswitch",
        "motor",
    }
    assert names == expected
