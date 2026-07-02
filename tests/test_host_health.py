"""Tests for eigsep_observing.host_health (Redis K/V pi vitals).

Each Raspberry Pi (backend pi at ``rpi_ip``, panda pi at ``panda_ip``)
runs its own Redis server, and the ``eigsep-host-health`` publisher on
each pi writes to its *local* Redis — so a single key constant
suffices and the transport identity disambiguates which pi a reading
belongs to. These tests drive the publish/read path end-to-end against
``DummyTransport`` (fakeredis-backed).

``publish`` follows the ``file_heartbeat`` failure policy
(swallow + WARN): the publisher is an always-on systemd service and a
transient Redis outage must not crash it — systemd would restart it
anyway, but the retry loop is already the recovery path.
"""

from __future__ import annotations

import threading
import time

import pytest
from eigsep_redis.testing import DummyTransport

from eigsep_observing.host_health import (
    HOST_HEALTH_KEY,
    _EMPTY,
    publish,
    read,
    read_cpu_temp_c,
)
from eigsep_observing.scripts.host_health import build_parser, run


def test_read_empty_returns_none_fields():
    t = DummyTransport()
    assert read(t, now=2000.0) == _EMPTY


def test_publish_then_read_roundtrip():
    t = DummyTransport()
    publish(t, temp_c=52.341, hostname="eigsep-backend", now=1000.0)
    out = read(t, now=1002.0)
    assert out["hostname"] == "eigsep-backend"
    assert out["temp_c"] == pytest.approx(52.341)
    assert out["published_unix"] == 1000.0
    assert out["seconds_since_publish"] == 2.0


def test_publish_none_temp_c_keeps_freshness_alive():
    """A failed thermal-zone read still publishes ``temp_c: None`` —
    the service proves it is alive (fresh ``published_unix``) while the
    dashboard renders the value as unknown, instead of the tile aging
    out as if the whole pi were down."""
    t = DummyTransport()
    publish(t, temp_c=None, hostname="eigsep-panda", now=1000.0)
    out = read(t, now=1001.0)
    assert out["hostname"] == "eigsep-panda"
    assert out["temp_c"] is None
    assert out["seconds_since_publish"] == 1.0


def test_publish_overwrites_previous_write():
    t = DummyTransport()
    publish(t, temp_c=50.0, hostname="pi", now=1000.0)
    publish(t, temp_c=61.5, hostname="pi", now=2000.0)
    out = read(t, now=2500.0)
    assert out["temp_c"] == pytest.approx(61.5)
    assert out["seconds_since_publish"] == 500.0


def test_read_clamps_negative_seconds_since_publish():
    t = DummyTransport()
    publish(t, temp_c=50.0, hostname="pi", now=10_000.0)
    # now < published_unix (clock skew) — should clamp to 0.
    out = read(t, now=9_000.0)
    assert out["seconds_since_publish"] == 0.0


def test_read_malformed_payload_returns_empty(caplog):
    t = DummyTransport()
    t.add_raw(HOST_HEALTH_KEY, b"not-json-at-all")
    with caplog.at_level("WARNING"):
        out = read(t, now=100.0)
    assert out == _EMPTY
    assert any(
        "malformed host health" in rec.message for rec in caplog.records
    )


def test_read_swallows_transport_error(caplog):
    class BoomTransport:
        def get_raw(self, key):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        out = read(BoomTransport(), now=100.0)
    assert out == _EMPTY
    assert any(
        "failed to read host health" in rec.message for rec in caplog.records
    )


def test_publish_swallows_transport_error(caplog):
    """The publisher is an always-on service; a Redis restart must not
    kill it (file_heartbeat policy)."""

    class BoomTransport:
        def add_raw(self, key, value, ex=None):
            raise RuntimeError("redis down")

    with caplog.at_level("WARNING"):
        publish(BoomTransport(), temp_c=50.0, hostname="pi", now=1000.0)
    assert any(
        "failed to publish host health" in rec.message
        for rec in caplog.records
    )


# -- read_cpu_temp_c ---------------------------------------------------


def test_read_cpu_temp_c_parses_millidegrees(tmp_path):
    # Real sysfs shape: integer millidegrees with a trailing newline
    # (verified on both a Pi 4B and a Pi 5).
    zone = tmp_path / "temp"
    zone.write_text("52341\n")
    assert read_cpu_temp_c(path=zone) == pytest.approx(52.341)


def test_read_cpu_temp_c_missing_file_returns_none(tmp_path, caplog):
    with caplog.at_level("WARNING"):
        out = read_cpu_temp_c(path=tmp_path / "nope")
    assert out is None
    assert any(
        "failed to read CPU temperature" in rec.message
        for rec in caplog.records
    )


def test_read_cpu_temp_c_garbage_returns_none(tmp_path, caplog):
    zone = tmp_path / "temp"
    zone.write_text("banana\n")
    with caplog.at_level("WARNING"):
        out = read_cpu_temp_c(path=zone)
    assert out is None
    assert any(
        "failed to read CPU temperature" in rec.message
        for rec in caplog.records
    )


# -- eigsep-host-health publisher loop ---------------------------------


def test_run_publishes_until_stopped(tmp_path):
    t = DummyTransport()
    zone = tmp_path / "temp"
    zone.write_text("48000\n")
    stop = threading.Event()
    thread = threading.Thread(
        target=run,
        kwargs=dict(
            transport=t,
            interval_s=0.01,
            stop_event=stop,
            thermal_path=zone,
            hostname="pi-test",
        ),
    )
    thread.start()
    try:
        deadline = time.time() + 2.0
        out = dict(_EMPTY)
        while time.time() < deadline:
            out = read(t)
            if out["temp_c"] is not None:
                break
            time.sleep(0.01)
    finally:
        stop.set()
        thread.join(timeout=1.0)
    assert not thread.is_alive()
    assert out["temp_c"] == pytest.approx(48.0)
    assert out["hostname"] == "pi-test"


def test_run_keeps_publishing_when_thermal_zone_missing(tmp_path):
    """A missing thermal zone (non-pi dev host, sysfs hiccup) must not
    kill the loop: the publish still lands with ``temp_c: None`` so the
    dashboard can distinguish 'sensor read failed' from 'publisher
    down'."""
    t = DummyTransport()
    stop = threading.Event()
    thread = threading.Thread(
        target=run,
        kwargs=dict(
            transport=t,
            interval_s=0.01,
            stop_event=stop,
            thermal_path=tmp_path / "nope",
            hostname="pi-test",
        ),
    )
    thread.start()
    try:
        deadline = time.time() + 2.0
        out = dict(_EMPTY)
        while time.time() < deadline:
            out = read(t)
            if out["published_unix"] is not None:
                break
            time.sleep(0.01)
    finally:
        stop.set()
        thread.join(timeout=1.0)
    assert not thread.is_alive()
    assert out["published_unix"] is not None
    assert out["temp_c"] is None


def test_build_parser_defaults():
    """Each pi publishes to its *local* Redis — localhost is the
    correct default on both, and the systemd unit relies on it (one
    identical unit file for both pis)."""
    args = build_parser().parse_args([])
    assert args.redis_host == "localhost"
    assert args.redis_port == 6379
    assert args.interval == 10.0
