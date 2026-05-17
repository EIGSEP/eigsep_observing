"""Tests for the shared manual-script helpers.

The helpers are tiny but every manual test script under ``scripts/``
imports them, so the tests guard the two contract surfaces that an
operator running a manual script depends on:

- ``build_transport`` constructs the right Transport for the mode and,
  in dummy mode, leaves a fully-emulated ``DummyPandaClient`` attached
  so proxies are immediately available.
- ``require_pico`` exits with code 2 (not 1, not 0) on a missing pico
  so a wrapper script can distinguish "pico unavailable" from other
  exit conditions, and writes an actionable message to stderr.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from eigsep_redis.testing import DummyTransport

from eigsep_observing._scripts_util import build_transport, require_pico
from eigsep_observing.testing import DummyPandaClient


class _FakeProxy:
    """Minimal stand-in for picohost.proxy.PicoProxy.

    Only ``name`` and ``is_available`` are read by ``require_pico``, so
    the test fake exposes just those — using a real PicoProxy would
    require a transport and a Redis stream, both of which are
    orthogonal to what's being tested here.
    """

    def __init__(self, name, available):
        self.name = name
        self.is_available = available


def test_require_pico_passes_when_available():
    proxy = _FakeProxy("rfswitch", available=True)
    assert require_pico(proxy) is None


def test_require_pico_exits_when_unavailable(capsys):
    proxy = _FakeProxy("rfswitch", available=False)
    with pytest.raises(SystemExit) as excinfo:
        require_pico(proxy)
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "rfswitch" in err
    assert "pico-manager.service" in err
    # The hint script defaults to pico_preflight so operators have one
    # well-known command to run before re-trying.
    assert "pico_preflight.py" in err


def test_require_pico_custom_hint_script(capsys):
    proxy = _FakeProxy("motor", available=False)
    with pytest.raises(SystemExit):
        require_pico(proxy, hint_script="some_other.py")
    err = capsys.readouterr().err
    assert "some_other.py" in err
    assert "pico_preflight.py" not in err


def test_build_transport_real_uses_real_port():
    """Real mode should construct a plain Transport — no dummy plumbing.

    Substitute ``DummyTransport`` (a real working subclass of
    ``Transport`` backed by fakeredis) for ``Transport`` at the import
    site so the test exercises the real construction path without
    needing a Redis server. The host/port end up on the returned
    instance as ordinary attributes, which is what we verify.
    """
    with patch("eigsep_observing._scripts_util.Transport", DummyTransport):
        result = build_transport(False, host="example.org", real_port=4321)
    try:
        assert isinstance(result, DummyTransport)
        assert result.host == "example.org"
        assert result.port == 4321
        # Real mode must not attach a dummy client — that would mask a
        # misconfigured production deployment.
        assert not hasattr(result, "_dummy_client")
    finally:
        result.close()


def test_build_transport_dummy_attaches_client_and_resets():
    """Dummy mode must reset fakeredis and attach a real DummyPandaClient.

    The ``_dummy_client`` attribute is load-bearing — without it the
    embedded PicoManager threads would be garbage-collected and every
    proxy on the transport would report ``is_available=False``. Run
    the real ``DummyPandaClient`` (not a Mock) so that contract is
    actually exercised: the test fails loudly if the embedded manager
    ever stops registering picos on construction.

    ``Transport`` is substituted with a ``DummyTransport`` subclass
    that records ``reset()`` calls — we can't observe reset from the
    fakeredis end-state because ``DummyPandaClient`` immediately
    repopulates the DB.
    """
    reset_calls: list[tuple[str, int]] = []

    class _SpyDummyTransport(DummyTransport):
        def reset(self):
            reset_calls.append((self.host, self.port))
            super().reset()

    with patch("eigsep_observing._scripts_util.Transport", _SpyDummyTransport):
        result = build_transport(True, host="localhost", dummy_port=6380)
    try:
        assert isinstance(result, _SpyDummyTransport)
        assert result.host == "localhost"
        assert result.port == 6380
        assert reset_calls == [("localhost", 6380)]
        assert isinstance(result._dummy_client, DummyPandaClient)
        # The embedded PicoManager must have registered its picos
        # before build_transport returned — that's the whole reason
        # the dummy client gets stashed on the transport. If this
        # ever regresses, manual scripts would silently see
        # ``is_available=False`` on every proxy.
        registered = {
            n.decode() if isinstance(n, bytes) else n
            for n in result.r.smembers("picos")
        }
        assert "rfswitch" in registered
    finally:
        result._dummy_client.stop()
        result.close()
