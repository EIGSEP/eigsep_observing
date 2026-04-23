"""Tests for eigsep_observing.file_heartbeat (Redis K/V heartbeat).

The dashboard runs on a different host than the writer, so the
heartbeat must flow through Redis (``transport.add_raw`` /
``transport.get_raw``). These tests drive that path end-to-end against
``DummyTransport`` (fakeredis-backed).
"""

from __future__ import annotations

import pytest
from eigsep_redis.testing import DummyTransport

from eigsep_observing.file_heartbeat import (
    FILE_HEARTBEAT_KEY,
    publish,
    read,
)


def test_read_empty_returns_none_fields():
    t = DummyTransport()
    out = read(t, now=2000.0)
    assert out == {
        "newest_h5_path": None,
        "mtime_unix": None,
        "seconds_since_write": None,
    }


def test_publish_then_read_roundtrip():
    t = DummyTransport()
    publish(t, "/data/corr_20260424_120000.h5", 1000.0)
    out = read(t, now=1500.0)
    assert out["newest_h5_path"] == "/data/corr_20260424_120000.h5"
    assert out["mtime_unix"] == 1000.0
    assert out["seconds_since_write"] == 500.0


def test_publish_overwrites_previous_write():
    t = DummyTransport()
    publish(t, "/data/old.h5", 1000.0)
    publish(t, "/data/new.h5", 2000.0)
    out = read(t, now=2500.0)
    assert out["newest_h5_path"] == "/data/new.h5"
    assert out["mtime_unix"] == 2000.0
    assert out["seconds_since_write"] == 500.0


def test_read_clamps_negative_seconds_since_write():
    t = DummyTransport()
    publish(t, "/data/a.h5", 10_000.0)
    # now < mtime (clock skew) — should clamp to 0, not go negative.
    out = read(t, now=9_000.0)
    assert out["seconds_since_write"] == 0.0


def test_read_malformed_payload_returns_empty(caplog):
    t = DummyTransport()
    t.add_raw(FILE_HEARTBEAT_KEY, b"not-json-at-all")
    with caplog.at_level("WARNING"):
        out = read(t, now=100.0)
    assert out["newest_h5_path"] is None
    assert out["mtime_unix"] is None
    assert any(
        "malformed file heartbeat" in rec.message for rec in caplog.records
    )


def test_publish_swallows_transport_error(caplog):
    class BoomTransport:
        def add_raw(self, key, value, ex=None):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        publish(BoomTransport(), "/data/a.h5", 1.0)
    assert any(
        "failed to publish file heartbeat" in rec.message
        for rec in caplog.records
    )


def test_read_swallows_transport_error(caplog):
    class BoomTransport:
        def get_raw(self, key):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        out = read(BoomTransport(), now=100.0)
    assert out["newest_h5_path"] is None
    assert any(
        "failed to read file heartbeat" in rec.message
        for rec in caplog.records
    )


@pytest.mark.parametrize(
    "missing_key",
    ["path", "mtime_unix"],
)
def test_read_missing_required_field_returns_empty(missing_key):
    import json as _json

    t = DummyTransport()
    payload = {"path": "/data/a.h5", "mtime_unix": 1.0}
    payload.pop(missing_key)
    t.add_raw(FILE_HEARTBEAT_KEY, _json.dumps(payload))
    out = read(t, now=100.0)
    assert out["newest_h5_path"] is None
    assert out["mtime_unix"] is None
