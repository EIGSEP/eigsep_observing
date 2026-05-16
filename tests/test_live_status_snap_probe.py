"""Tests for SNAP FPGA TCP reachability probe.

Uses real loopback sockets (no mocks) so we exercise the actual
``socket.create_connection`` failure modes — connection refused
from an unbound port vs success against a real listener.
"""

from __future__ import annotations

import socket

import pytest

from eigsep_observing.live_status.snap_probe import probe_snap_fpga


@pytest.fixture
def listening_port():
    """Bind a real TCP socket on loopback and yield its port.

    The listener accepts no connections but the OS completes the
    SYN/ACK, so ``create_connection`` succeeds — which is all the
    probe checks for.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    s.listen(1)
    yield s.getsockname()[1]
    s.close()


def test_probe_returns_true_when_port_open(listening_port):
    assert probe_snap_fpga("127.0.0.1", port=listening_port) is True


def test_probe_returns_false_when_port_closed():
    # Port 1 on loopback is reserved/unbound on Linux — connect refused.
    assert probe_snap_fpga("127.0.0.1", port=1, timeout=0.5) is False


def test_probe_returns_false_on_unroutable_host():
    # TEST-NET-1 (RFC 5737) — must not route anywhere.
    assert probe_snap_fpga("192.0.2.1", port=7147, timeout=0.5) is False


def test_probe_returns_false_when_host_is_none():
    assert probe_snap_fpga(None) is False


def test_probe_returns_false_when_host_is_empty_string():
    assert probe_snap_fpga("") is False
