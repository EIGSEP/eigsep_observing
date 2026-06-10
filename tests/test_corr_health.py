"""Tests for eigsep_observing.corr_health (Redis K/V diagnostic).

The dashboard runs on a different host than the SNAP observe loop, so
the health snapshot must flow through Redis (``transport.add_raw`` /
``transport.get_raw``). These tests drive that path end-to-end against
``DummyTransport`` (fakeredis-backed).

Unlike ``file_heartbeat.publish``, ``corr_health.publish`` does NOT
swallow transport errors — it runs at the diagnostics cadence (~1 Hz),
so the caller (``EigsepFpga._publish_corr_health``) owns the failure
policy (disable-on-first-failure, tested in ``tests/test_adc.py``).
"""

from __future__ import annotations

import pytest
from eigsep_redis.testing import DummyTransport

from eigsep_observing.corr_health import (
    CORR_HEALTH_KEY,
    _EMPTY,
    publish,
    read,
)


def test_read_empty_returns_none_fields():
    t = DummyTransport()
    assert read(t, now=2000.0) == _EMPTY


def test_publish_then_read_roundtrip():
    t = DummyTransport()
    publish(t, dropped_integrations=4, readout_time_ms=87.5, now=1000.0)
    out = read(t, now=1002.0)
    assert out["dropped_integrations"] == 4
    assert out["readout_time_ms"] == pytest.approx(87.5)
    assert out["published_unix"] == 1000.0
    assert out["seconds_since_publish"] == 2.0


def test_publish_null_readout_before_first_read():
    """Until the first readout completes there is no wall-time to
    report; the payload ships null honestly so the dashboard omits the
    readout suffix rather than rendering a fake 0 ms."""
    t = DummyTransport()
    publish(t, dropped_integrations=0, readout_time_ms=None, now=1000.0)
    out = read(t, now=1001.0)
    assert out["dropped_integrations"] == 0
    assert out["readout_time_ms"] is None


def test_publish_overwrites_previous_write():
    t = DummyTransport()
    publish(t, dropped_integrations=1, readout_time_ms=40.0, now=1000.0)
    publish(t, dropped_integrations=3, readout_time_ms=55.0, now=2000.0)
    out = read(t, now=2500.0)
    assert out["dropped_integrations"] == 3
    assert out["readout_time_ms"] == pytest.approx(55.0)
    assert out["seconds_since_publish"] == 500.0


def test_read_clamps_negative_seconds_since_publish():
    t = DummyTransport()
    publish(t, dropped_integrations=0, readout_time_ms=1.0, now=10_000.0)
    # now < published_unix (clock skew) — should clamp to 0.
    out = read(t, now=9_000.0)
    assert out["seconds_since_publish"] == 0.0


def test_read_malformed_payload_returns_empty(caplog):
    t = DummyTransport()
    t.add_raw(CORR_HEALTH_KEY, b"not-json-at-all")
    with caplog.at_level("WARNING"):
        out = read(t, now=100.0)
    assert out == _EMPTY
    assert any(
        "malformed corr health" in rec.message for rec in caplog.records
    )


def test_read_swallows_transport_error(caplog):
    class BoomTransport:
        def get_raw(self, key):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        out = read(BoomTransport(), now=100.0)
    assert out == _EMPTY
    assert any(
        "failed to read corr health" in rec.message for rec in caplog.records
    )


def test_publish_raises_on_transport_error():
    """The caller owns the failure policy; publish must not swallow."""

    class BoomTransport:
        def add_raw(self, key, value, ex=None):
            raise RuntimeError("redis down")

    with pytest.raises(RuntimeError, match="redis down"):
        publish(BoomTransport(), dropped_integrations=0, readout_time_ms=None)
