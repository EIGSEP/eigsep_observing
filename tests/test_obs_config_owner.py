"""Tests for eigsep_observing.obs_config_owner.

Mirrors :mod:`tests.test_run_tag` but for the persistent
``eigsep:obs_config_owner`` key (no ``clear`` — the owner stamp
outlives the uploader script's process; it's only displaced by the
next legitimate upload).
"""

from __future__ import annotations

import json

from eigsep_redis.testing import DummyTransport

from eigsep_observing.obs_config_owner import (
    OBS_CONFIG_OWNER_KEY,
    publish_owner,
    read_owner,
)


def test_read_empty_returns_none_fields():
    t = DummyTransport()
    assert read_owner(t) == {"owner": None, "uploaded_at_unix": None}


def test_publish_then_read_roundtrip():
    t = DummyTransport()
    publish_owner(t, "panda_observe", uploaded_at_unix=1000.0)
    assert read_owner(t) == {
        "owner": "panda_observe",
        "uploaded_at_unix": 1000.0,
    }


def test_publish_default_uploaded_unix_uses_now(monkeypatch):
    t = DummyTransport()
    monkeypatch.setattr(
        "eigsep_observing.obs_config_owner.time.time", lambda: 4242.0
    )
    publish_owner(t, "no_switch_observation")
    assert read_owner(t) == {
        "owner": "no_switch_observation",
        "uploaded_at_unix": 4242.0,
    }


def test_publish_overwrites_previous_owner():
    """Owner key is the most-recent uploader by design (no cross-check)."""
    t = DummyTransport()
    publish_owner(t, "panda_observe", uploaded_at_unix=1.0)
    publish_owner(t, "vna_position_sweep", uploaded_at_unix=2.0)
    assert read_owner(t) == {
        "owner": "vna_position_sweep",
        "uploaded_at_unix": 2.0,
    }


def test_module_has_no_clear():
    """No ``clear`` API: the owner record persists across uploader exits.

    Clearing on exit would falsely declare the still-resident cfg
    unowned. The key only changes when the next legitimate uploader
    publishes.
    """
    import eigsep_observing.obs_config_owner as mod

    assert not hasattr(mod, "clear"), (
        "obs_config_owner intentionally has no clear(); persists across "
        "uploader script exits."
    )


def test_read_malformed_payload_returns_empty(caplog):
    t = DummyTransport()
    t.add_raw(OBS_CONFIG_OWNER_KEY, b"not-json-at-all")
    with caplog.at_level("WARNING"):
        out = read_owner(t)
    assert out == {"owner": None, "uploaded_at_unix": None}
    assert any(
        "malformed obs_config_owner" in rec.message for rec in caplog.records
    )


def test_read_non_numeric_uploaded_unix_returns_empty(caplog):
    """Regression: float coercion used to live outside the parse
    try-block, so a junk timestamp raised an uncaught ValueError out
    of read_owner() (issue #149)."""
    t = DummyTransport()
    t.add_raw(
        OBS_CONFIG_OWNER_KEY,
        json.dumps({"owner": "panda_observe", "uploaded_at_unix": "abc"}),
    )
    with caplog.at_level("WARNING"):
        out = read_owner(t)
    assert out == {"owner": None, "uploaded_at_unix": None}
    assert any(
        "malformed obs_config_owner" in rec.message for rec in caplog.records
    )


def test_read_partial_null_payload_warns(caplog):
    t = DummyTransport()
    t.add_raw(
        OBS_CONFIG_OWNER_KEY,
        json.dumps({"owner": "panda_observe", "uploaded_at_unix": None}),
    )
    with caplog.at_level("WARNING"):
        out = read_owner(t)
    assert out == {"owner": None, "uploaded_at_unix": None}
    assert any(
        "partial obs_config_owner" in rec.message for rec in caplog.records
    )


def test_publish_swallows_transport_error(caplog):
    class BoomTransport:
        def add_raw(self, key, value, ex=None):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        publish_owner(BoomTransport(), "panda_observe", uploaded_at_unix=1.0)
    assert any(
        "failed to publish obs_config_owner" in rec.message
        for rec in caplog.records
    )


def test_read_swallows_transport_error(caplog):
    class BoomTransport:
        def get_raw(self, key):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        out = read_owner(BoomTransport())
    assert out == {"owner": None, "uploaded_at_unix": None}
    assert any(
        "failed to read obs_config_owner" in rec.message
        for rec in caplog.records
    )
