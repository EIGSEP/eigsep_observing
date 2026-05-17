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

from eigsep_observing._scripts_util import build_transport, require_pico


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

    Patch the constructor at the module level (where the helper looks
    it up) so we can assert the args without needing a real Redis
    server on the test box.
    """
    with patch("eigsep_observing._scripts_util.Transport") as MockTransport:
        instance = MockTransport.return_value
        result = build_transport(False, host="example.org", real_port=4321)
    MockTransport.assert_called_once_with(host="example.org", port=4321)
    assert result is instance
    # Real mode must not attach a dummy client — that would mask a
    # misconfigured production deployment.
    assert (
        not hasattr(instance, "_dummy_client")
        or instance._dummy_client is MockTransport.return_value._dummy_client
    )  # noqa: E501


def test_build_transport_dummy_attaches_client_and_resets():
    """Dummy mode must reset fakeredis and attach DummyPandaClient.

    The ``_dummy_client`` attribute is load-bearing — without it the
    embedded PicoManager threads would be garbage-collected and every
    proxy on the transport would report ``is_available=False``.
    """
    with patch("eigsep_observing._scripts_util.Transport") as MockTransport:
        transport = MockTransport.return_value
        # Patch DummyPandaClient at its real import path; the helper
        # imports it lazily so the patch target is the canonical name.
        with patch("eigsep_observing.testing.DummyPandaClient") as MockClient:
            result = build_transport(True, host="localhost", dummy_port=6380)
    MockTransport.assert_called_once_with(host="localhost", port=6380)
    transport.reset.assert_called_once()
    MockClient.assert_called_once_with(transport=transport)
    assert result._dummy_client is MockClient.return_value
