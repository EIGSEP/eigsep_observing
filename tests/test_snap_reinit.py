"""Tests for eigsep_observing.snap_reinit (Redis K/V heartbeat).

Mirrors :mod:`tests.test_file_heartbeat` — the dashboard runs on a
different host than ``fpga_init.py``, so the heartbeat must flow
through Redis. Tests drive the publish/read pair end-to-end against
``DummyTransport`` (fakeredis-backed).
"""

from __future__ import annotations

import json

import pytest
from eigsep_redis.testing import DummyTransport

from eigsep_observing.snap_reinit import REINIT_KEY, publish, read


def test_read_empty_returns_none_fields():
    t = DummyTransport()
    out = read(t, now=2000.0)
    assert out == {
        "count": None,
        "last_reinit_unix": None,
        "seconds_since_reinit": None,
    }


def test_publish_then_read_roundtrip(monkeypatch):
    monkeypatch.setattr(
        "eigsep_observing.snap_reinit.time.time", lambda: 1000.0
    )
    t = DummyTransport()
    publish(t)
    out = read(t, now=1500.0)
    assert out["count"] == 1
    assert out["last_reinit_unix"] == 1000.0
    assert out["seconds_since_reinit"] == 500.0


def test_publish_increments_existing_count(monkeypatch):
    monkeypatch.setattr(
        "eigsep_observing.snap_reinit.time.time", lambda: 1000.0
    )
    t = DummyTransport()
    publish(t)
    publish(t)
    publish(t)
    out = read(t, now=1000.0)
    assert out["count"] == 3
    assert out["last_reinit_unix"] == 1000.0


def test_publish_on_corrupt_payload_resets_count_to_one(caplog):
    t = DummyTransport()
    t.add_raw(REINIT_KEY, b"not-json")
    with caplog.at_level("WARNING"):
        publish(t)
    out = read(t, now=0.0)
    # Corrupt prior payload → count restarts at 1, never raises.
    assert out["count"] == 1
    assert any(
        "malformed snap reinit" in rec.message for rec in caplog.records
    )


def test_read_clamps_negative_seconds_since_reinit(monkeypatch):
    monkeypatch.setattr(
        "eigsep_observing.snap_reinit.time.time", lambda: 10_000.0
    )
    t = DummyTransport()
    publish(t)
    # now < last_reinit_unix (clock skew) — clamp to 0.
    out = read(t, now=9_000.0)
    assert out["seconds_since_reinit"] == 0.0


def test_read_malformed_payload_returns_empty(caplog):
    t = DummyTransport()
    t.add_raw(REINIT_KEY, b"not-json-at-all")
    with caplog.at_level("WARNING"):
        out = read(t, now=100.0)
    assert out == {
        "count": None,
        "last_reinit_unix": None,
        "seconds_since_reinit": None,
    }
    assert any(
        "malformed snap reinit" in rec.message for rec in caplog.records
    )


def test_publish_swallows_transport_error(caplog):
    class BoomTransport:
        def get_raw(self, key):
            raise RuntimeError("redis down")

        def add_raw(self, key, value, ex=None):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        publish(BoomTransport())
    assert any(
        "failed to publish snap reinit heartbeat" in rec.message
        for rec in caplog.records
    )


def test_read_swallows_transport_error(caplog):
    class BoomTransport:
        def get_raw(self, key):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        out = read(BoomTransport(), now=100.0)
    assert out["count"] is None
    assert any(
        "failed to read snap reinit heartbeat" in rec.message
        for rec in caplog.records
    )


@pytest.mark.parametrize("missing_key", ["count", "last_reinit_unix"])
def test_read_missing_required_field_returns_empty(missing_key):
    t = DummyTransport()
    payload = {"count": 5, "last_reinit_unix": 1.0}
    payload.pop(missing_key)
    t.add_raw(REINIT_KEY, json.dumps(payload))
    out = read(t, now=100.0)
    assert out["count"] is None
    assert out["last_reinit_unix"] is None
