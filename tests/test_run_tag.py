"""Tests for eigsep_observing.run_tag (Redis K/V tag for active script).

Mirrors :mod:`tests.test_file_heartbeat`: panda-side scripts publish a
small JSON blob, observers/clients read it. All paths drive against a
fakeredis-backed ``DummyTransport``.
"""

from __future__ import annotations

import json

import pytest
from eigsep_redis.testing import DummyTransport

from eigsep_observing.run_tag import (
    RUN_TAG_KEY,
    clear,
    publish,
    read,
    session,
)


def test_read_empty_returns_none_fields():
    t = DummyTransport()
    assert read(t) == {"run_tag": None, "run_started_at_unix": None}


def test_publish_then_read_roundtrip():
    t = DummyTransport()
    publish(t, "panda_observe", started_unix=1000.0)
    out = read(t)
    assert out == {"run_tag": "panda_observe", "run_started_at_unix": 1000.0}


def test_publish_default_started_unix_uses_now(monkeypatch):
    t = DummyTransport()
    monkeypatch.setattr("eigsep_observing.run_tag.time.time", lambda: 4242.0)
    publish(t, "no_switch_observation")
    out = read(t)
    assert out == {
        "run_tag": "no_switch_observation",
        "run_started_at_unix": 4242.0,
    }


def test_publish_overwrites_previous_tag():
    t = DummyTransport()
    publish(t, "panda_observe", started_unix=1000.0)
    publish(t, "vna_position_sweep", started_unix=2000.0)
    out = read(t)
    assert out == {
        "run_tag": "vna_position_sweep",
        "run_started_at_unix": 2000.0,
    }


def test_clear_resets_to_empty_sentinel():
    t = DummyTransport()
    publish(t, "panda_observe", started_unix=1000.0)
    clear(t)
    assert read(t) == {"run_tag": None, "run_started_at_unix": None}


def test_read_malformed_payload_returns_empty(caplog):
    t = DummyTransport()
    t.add_raw(RUN_TAG_KEY, b"not-json-at-all")
    with caplog.at_level("WARNING"):
        out = read(t)
    assert out == {"run_tag": None, "run_started_at_unix": None}
    assert any("malformed run_tag" in rec.message for rec in caplog.records)


def test_publish_swallows_transport_error(caplog):
    class BoomTransport:
        def add_raw(self, key, value, ex=None):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        publish(BoomTransport(), "panda_observe", started_unix=1.0)
    assert any(
        "failed to publish run_tag" in rec.message for rec in caplog.records
    )


def test_clear_swallows_transport_error(caplog):
    class BoomTransport:
        def add_raw(self, key, value, ex=None):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        clear(BoomTransport())
    assert any(
        "failed to clear run_tag" in rec.message for rec in caplog.records
    )


def test_read_swallows_transport_error(caplog):
    class BoomTransport:
        def get_raw(self, key):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        out = read(BoomTransport())
    assert out == {"run_tag": None, "run_started_at_unix": None}
    assert any(
        "failed to read run_tag" in rec.message for rec in caplog.records
    )


@pytest.mark.parametrize(
    "missing_key",
    ["run_tag", "run_started_at_unix"],
)
def test_read_missing_required_field_returns_empty(missing_key):
    t = DummyTransport()
    payload = {"run_tag": "panda_observe", "run_started_at_unix": 1.0}
    payload.pop(missing_key)
    t.add_raw(RUN_TAG_KEY, json.dumps(payload))
    out = read(t)
    assert out == {"run_tag": None, "run_started_at_unix": None}


def test_read_partial_null_payload_warns(caplog):
    t = DummyTransport()
    t.add_raw(
        RUN_TAG_KEY,
        json.dumps({"run_tag": "panda_observe", "run_started_at_unix": None}),
    )
    with caplog.at_level("WARNING"):
        out = read(t)
    assert out == {"run_tag": None, "run_started_at_unix": None}
    assert any("partial run_tag" in rec.message for rec in caplog.records)


def test_publish_overwrite_with_different_tag_warns(caplog):
    """publish-time second-line audit: WARN when overwriting another tag."""
    t = DummyTransport()
    publish(t, "panda_observe", started_unix=1.0)
    with caplog.at_level("WARNING"):
        publish(t, "vna_manual", started_unix=2.0)
    assert read(t)["run_tag"] == "vna_manual"
    assert any(
        "is overwriting existing" in rec.message
        and "panda_observe" in rec.message
        for rec in caplog.records
    )


def test_publish_same_tag_no_overwrite_warning(caplog):
    """Re-publishing the same tag is a no-op for the overwrite WARN."""
    t = DummyTransport()
    publish(t, "panda_observe", started_unix=1.0)
    with caplog.at_level("WARNING"):
        publish(t, "panda_observe", started_unix=2.0)
    assert not any(
        "is overwriting existing" in rec.message for rec in caplog.records
    )


def test_session_publishes_and_clears_on_exit():
    t = DummyTransport()
    assert read(t) == {"run_tag": None, "run_started_at_unix": None}
    with session(t, "panda_observe"):
        assert read(t)["run_tag"] == "panda_observe"
    assert read(t) == {"run_tag": None, "run_started_at_unix": None}


def test_session_refuses_when_other_tag_already_published():
    t = DummyTransport()
    publish(t, "panda_observe", started_unix=1.0)
    with pytest.raises(RuntimeError, match="panda_observe"):
        with session(t, "vna_manual"):
            pass
    assert read(t)["run_tag"] == "panda_observe"


def test_session_allows_reentry_with_same_tag():
    t = DummyTransport()
    publish(t, "vna_manual", started_unix=1.0)
    with session(t, "vna_manual"):
        assert read(t)["run_tag"] == "vna_manual"
    assert read(t) == {"run_tag": None, "run_started_at_unix": None}


def test_session_safe_clear_does_not_trample_overwriter():
    """If another script overwrites our tag mid-session (refuse-on-conflict
    race lost), __exit__ must not clear — that other script is now the
    legitimate driver."""
    t = DummyTransport()
    with session(t, "panda_observe"):
        publish(t, "vna_manual", started_unix=5.0)  # simulated overwrite
    assert read(t)["run_tag"] == "vna_manual"


def test_session_clears_even_when_block_raises():
    t = DummyTransport()
    with pytest.raises(ValueError):
        with session(t, "panda_observe"):
            assert read(t)["run_tag"] == "panda_observe"
            raise ValueError("work failed")
    assert read(t) == {"run_tag": None, "run_started_at_unix": None}
