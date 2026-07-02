"""Tests for eigsep_observing._redis_json_kv (shared K/V helper).

The seven sibling K/V modules (``run_tag``, ``obs_config_owner``,
``file_heartbeat``, ``snap_reinit``, ``corr_health``, ``host_health``,
``imu_calibration``) share one publish/read shape; this module is the
single home for it. Behavior differences (swallow-vs-raise publish,
lifecycle, derived fields) stay in the sibling modules — see issue #149.
"""

from __future__ import annotations

import json
import logging

import pytest
from eigsep_redis.testing import DummyTransport

from eigsep_observing._redis_json_kv import publish_json, read_json

logger = logging.getLogger("test_redis_json_kv")

KEY = "eigsep:test_kv"


def _parse(obj):
    return {"name": str(obj["name"]), "value": float(obj["value"])}


def test_publish_then_read_roundtrip():
    t = DummyTransport()
    publish_json(t, KEY, {"name": "x", "value": 1.5})
    out = read_json(t, KEY, label="test kv", logger=logger, parse=_parse)
    assert out == {"name": "x", "value": 1.5}


def test_read_missing_key_returns_none_without_warning(caplog):
    t = DummyTransport()
    with caplog.at_level("WARNING"):
        out = read_json(t, KEY, label="test kv", logger=logger, parse=_parse)
    assert out is None
    assert not caplog.records


def test_read_transport_error_returns_none_and_warns(caplog):
    class BoomTransport:
        def get_raw(self, key):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        out = read_json(
            BoomTransport(), KEY, label="test kv", logger=logger, parse=_parse
        )
    assert out is None
    assert any(
        "failed to read test kv" in rec.message for rec in caplog.records
    )


def test_read_malformed_json_returns_none_and_warns(caplog):
    t = DummyTransport()
    t.add_raw(KEY, b"not-json-at-all")
    with caplog.at_level("WARNING"):
        out = read_json(t, KEY, label="test kv", logger=logger, parse=_parse)
    assert out is None
    assert any(
        "malformed test kv payload" in rec.message for rec in caplog.records
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"value": 1.0},  # missing field -> KeyError
        {"name": "x", "value": "abc"},  # non-numeric -> ValueError
        {"name": "x", "value": None},  # None -> TypeError
        ["not", "a", "dict"],  # list indexing by str -> TypeError
    ],
)
def test_read_parse_failure_returns_none_and_warns(payload, caplog):
    t = DummyTransport()
    t.add_raw(KEY, json.dumps(payload))
    with caplog.at_level("WARNING"):
        out = read_json(t, KEY, label="test kv", logger=logger, parse=_parse)
    assert out is None
    assert any(
        "malformed test kv payload" in rec.message for rec in caplog.records
    )


def test_read_warns_on_caller_logger(caplog):
    """Warnings carry the caller's logger name, so per-module log
    provenance (eigsep_observing.run_tag etc.) survives the extraction."""
    t = DummyTransport()
    t.add_raw(KEY, b"not-json-at-all")
    with caplog.at_level("WARNING"):
        read_json(t, KEY, label="test kv", logger=logger, parse=_parse)
    assert caplog.records[0].name == "test_redis_json_kv"


def test_read_decodes_bytes_payload():
    t = DummyTransport()
    t.add_raw(KEY, json.dumps({"name": "x", "value": 2.0}).encode())
    out = read_json(t, KEY, label="test kv", logger=logger, parse=_parse)
    assert out == {"name": "x", "value": 2.0}


def test_publish_raises_on_transport_error():
    """publish_json does NOT swallow — each sibling owns its failure
    policy (five swallow+WARN, corr_health raises to its caller)."""

    class BoomTransport:
        def add_raw(self, key, value, ex=None):
            raise RuntimeError("redis down")

    with pytest.raises(RuntimeError, match="redis down"):
        publish_json(BoomTransport(), KEY, {"name": "x", "value": 1.0})
