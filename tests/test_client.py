import copy
import json
import time
from unittest.mock import patch

import pytest
import yaml

from cmt_vna.testing import DummyVNA
from picohost.base import PicoRFSwitch
from picohost.proxy import PicoProxy

from eigsep_observing.testing import DummyEigsepObsRedis

import eigsep_observing
from eigsep_observing.client import _SW_INT_TO_MODE
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


def test_vna_loop_returns_when_vna_is_none(caplog, client):
    """vna_loop must return promptly when self.vna is None — no
    polling. Regression for a bare `threading.Event().wait(5)` that
    ignored stop_client."""
    caplog.set_level("WARNING")
    assert client.vna is None  # dummy_config has use_vna: false
    t0 = time.monotonic()
    client.vna_loop()
    elapsed = time.monotonic() - t0
    assert elapsed < 1.0, f"vna_loop did not return promptly ({elapsed}s)"
    assert any("VNA not initialized" in r.getMessage() for r in caplog.records)


def test_switch_loop_does_not_mutate_cfg_schedule(client):
    """switch_loop must not mutate self.cfg['switch_schedule'] when
    filtering out zero-wait modes. Regression for `del schedule[mode]`
    on the live cfg reference."""
    original = copy.deepcopy(client.cfg["switch_schedule"])
    assert any(v == 0 for v in original.values()), (
        "test needs a zero-wait mode in dummy_config to exercise the filter"
    )
    client.stop_client.set()  # bypass the main while loop after validation
    client.switch_loop()
    assert client.cfg["switch_schedule"] == original


def test_cfg_is_get_cfg_result_without_extra_roundtrip(client):
    """``self.cfg`` must equal what ``_get_cfg`` returns. The previous
    ``json.loads(json.dumps(cfg))`` step was dead code — ``config.get``
    already returns a JSON-normalized dict (via ``json.loads`` on the
    serialized payload) — and would silently re-introduce drift if
    re-added on top of a different storage path."""
    assert client.cfg == client._get_cfg()


def test_sw_int_to_mode_inverts_pico_path_str():
    """The module-level inverse map must round-trip every PicoRFSwitch
    path string so a firmware change to ``path_str`` is caught at
    import-time mismatch rather than silently dropping a mode."""
    assert set(_SW_INT_TO_MODE.values()) == set(PicoRFSwitch.path_str)
    for mode, bits in PicoRFSwitch.path_str.items():
        assert _SW_INT_TO_MODE[PicoRFSwitch.rbin(bits)] == mode


def test_read_switch_mode_from_redis_returns_published_mode(client):
    """The helper maps the rfswitch's last-published ``sw_state`` int
    back to a mode string. This is the reconcile path that replaces the
    panda-side shadow ``current_switch_state`` — the published state is
    the single source of truth across PandaClient/PicoManager restarts.
    """
    # Drive the rfswitch to a non-default mode and wait for the firmware
    # status publish to land in Redis.
    assert client._switch_to("RFNON")
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if client._read_switch_mode_from_redis() == "RFNON":
            break
        time.sleep(0.05)
    assert client._read_switch_mode_from_redis() == "RFNON"


def test_read_switch_mode_from_redis_no_rfswitch_data(client):
    """Returns ``None`` if the rfswitch hasn't published yet — caller
    decides the fallback (``vna_loop`` falls back to RFANT with a
    warning)."""
    # Wipe the rfswitch entry from the metadata snapshot.
    client.redis.r.hdel("metadata", "rfswitch")
    assert client._read_switch_mode_from_redis() is None


def test_read_switch_mode_from_redis_unmapped_sw_state(client):
    """Returns ``None`` if the published ``sw_state`` doesn't map to a
    known mode — guards against firmware drift."""
    bogus = json.dumps({"sensor_name": "rfswitch", "sw_state": 99999}).encode()
    client.redis.r.hset("metadata", "rfswitch", bogus)
    assert client._read_switch_mode_from_redis() is None


def test_vna_loop_uses_redis_published_mode_for_switch_back(
    redis, dummy_cfg, caplog
):
    """vna_loop reads ``prev_mode`` from Redis (PicoManager truth), not
    from a panda-side shadow. After a PandaClient restart that finds
    the rfswitch already in RFNOFF, the post-VNA switch-back must
    target RFNOFF — not the previously-shadowed RFANT default."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 60  # long: only one iteration before stop
    client = DummyPandaClient(redis, default_cfg=cfg)
    try:
        # Pre-seed the rfswitch in Redis to simulate a state PicoManager
        # set before this PandaClient process started.
        assert client._switch_to("RFNOFF")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if client._read_switch_mode_from_redis() == "RFNOFF":
                break
            time.sleep(0.05)
        assert client._read_switch_mode_from_redis() == "RFNOFF"

        switch_calls = []
        original_switch_to = client._switch_to

        # Stop after the post-VNA switch-back. The VNA's internal
        # switch_fn only touches VNA* modes (VNAANT, VNARF, ...);
        # RFNOFF is uniquely vna_loop's switch-back target. Setting
        # stop_client here (instead of patching stop_client.wait)
        # avoids racing the heartbeat thread, which shares the same
        # Event and would otherwise see the patched wait().
        def recording_switch_to(state):
            switch_calls.append(state)
            result = original_switch_to(state)
            if state == "RFNOFF":
                client.stop_client.set()
            return result

        with patch.object(
            client, "_switch_to", side_effect=recording_switch_to
        ):
            caplog.set_level("INFO")
            client.vna_loop()

        # The last _switch_to call from vna_loop itself is the
        # switch-back; intermediate calls come from VNA OSL/ant/rec.
        assert switch_calls, "vna_loop made no switch calls"
        assert switch_calls[-1] == "RFNOFF", (
            f"expected switch-back to RFNOFF (Redis truth), got "
            f"{switch_calls[-1]!r}; full sequence: {switch_calls}"
        )
        assert any(
            "previous mode: RFNOFF" in r.getMessage() for r in caplog.records
        )
    finally:
        client.stop()


def test_vna_loop_warns_and_defaults_when_rfswitch_absent(
    redis, dummy_cfg, caplog
):
    """If the rfswitch hasn't published, vna_loop logs a WARNING and
    falls back to RFANT — making the contract violation visible
    instead of silently switching to the wrong place."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 60
    client = DummyPandaClient(redis, default_cfg=cfg)
    try:
        # Wipe the rfswitch entry so the helper returns None.
        client.redis.r.hdel("metadata", "rfswitch")

        switch_calls = []
        original_switch_to = client._switch_to

        # See sibling test: RFANT is the fallback switch-back target and
        # isn't hit by the VNA's internal VNA*-mode switching, so it's
        # safe to key the stop on it without racing the heartbeat.
        def recording_switch_to(state):
            switch_calls.append(state)
            result = original_switch_to(state)
            if state == "RFANT":
                client.stop_client.set()
            return result

        with patch.object(
            client, "_read_switch_mode_from_redis", return_value=None
        ):
            with patch.object(
                client, "_switch_to", side_effect=recording_switch_to
            ):
                caplog.set_level("WARNING")
                client.vna_loop()

        assert switch_calls[-1] == "RFANT"
        assert any(
            "rfswitch state unavailable in Redis" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        )
    finally:
        client.stop()


def test_vna_loop_warns_on_failed_switch_back(redis, dummy_cfg, caplog):
    """If the post-VNA ``_switch_to(prev_mode)`` returns falsy, vna_loop
    logs a WARNING — mirrors switch_loop's "Failed to switch" pattern so
    a hardware-stuck calibrator is operator-visible instead of silent."""
    cfg = dict(dummy_cfg)
    cfg["use_vna"] = True
    cfg["vna_interval"] = 60  # long: only one iteration before stop
    client = DummyPandaClient(redis, default_cfg=cfg)
    try:
        assert client._switch_to("RFNOFF")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if client._read_switch_mode_from_redis() == "RFNOFF":
                break
            time.sleep(0.05)
        assert client._read_switch_mode_from_redis() == "RFNOFF"

        original_switch_to = client._switch_to

        # Only fail the switch-back (RFNOFF). The VNA's internal
        # switch_fn touches VNA* modes and must continue to succeed, or
        # the test would short-circuit before reaching the tail.
        def failing_switch_back(state):
            if state == "RFNOFF":
                client.stop_client.set()
                return None
            return original_switch_to(state)

        with patch.object(
            client, "_switch_to", side_effect=failing_switch_back
        ):
            caplog.set_level("WARNING")
            client.vna_loop()

        assert any(
            "Failed to switch back to RFNOFF" in r.getMessage()
            and r.levelname == "WARNING"
            for r in caplog.records
        ), (
            "expected 'Failed to switch back to RFNOFF' warning; "
            f"got records: {[r.getMessage() for r in caplog.records]}"
        )
    finally:
        client.stop()


def test_no_current_switch_state_attribute(client):
    """Regression: the panda-side shadow ``current_switch_state`` is
    gone — its replacement is :meth:`_read_switch_mode_from_redis`.
    A new attribute creeping back in would re-introduce the drift."""
    assert not hasattr(client, "current_switch_state")


def test_stop_joins_heartbeat_and_emits_goodbye(client):
    """stop() sets stop_client, joins the heartbeat thread, and the
    thread's final alive=False is visible to readers. Idempotent."""
    assert client.heartbeat_thd.is_alive()
    assert client.redis.heartbeat_reader.check() is True
    client.stop()
    assert not client.heartbeat_thd.is_alive()
    assert client.redis.heartbeat_reader.check() is False
    client.stop()  # idempotent — must not raise
