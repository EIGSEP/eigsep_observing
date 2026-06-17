"""Ctrl-C responsiveness guard for the live-status dashboard.

``scripts/live_status.py`` builds its SNAP/panda transports lazily so the
dashboard can boot — and keep running — while a bus is down. The connect
timeout on those transports is load-bearing for *shutdown*: a drain
thread parked mid-connect to an unreachable bus can only be joined once
its connect attempt returns, so an unbounded ``socket_connect_timeout``
turns Ctrl-C into a multi-second wait (per drain thread, and ``stop()``
runs twice — once in the SIGINT handler, once in the ``finally``).

This test pins the timeout finite and well under the ``stop()`` join
budget, so a regression — e.g. reintroducing the old ``_LazyTransport``
override that hardcoded ``socket_connect_timeout=None`` — fails loudly.
"""

from __future__ import annotations

import importlib.util
import types
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load(name):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _connect_timeout(transport):
    """The ``socket_connect_timeout`` actually handed to ``redis.Redis``.

    Read from the live client (not ``transport.connect_timeout``): the
    bug being guarded was a ``_make_redis`` override whose hardcoded
    ``None`` diverged from the stored attribute, so only the real client
    kwargs reveal it.
    """
    return transport.r.connection_pool.connection_kwargs[
        "socket_connect_timeout"
    ]


def test_dashboard_transports_have_bounded_connect_timeout(monkeypatch):
    mod = _load("live_status")

    captured = {}

    class _FakeAgg:
        def __init__(self, *, transport_snap, transport_panda, **_kw):
            captured["snap"] = transport_snap
            captured["panda"] = transport_panda

        def start(self):
            pass

        def stop(self, timeout=2.0):
            pass

    # Keep main() from blocking in Flask or clobbering pytest's signal
    # handlers; we only care about how the transports are built.
    monkeypatch.setattr(mod, "LiveStatusAggregator", _FakeAgg)
    monkeypatch.setattr(
        mod,
        "create_app",
        lambda _agg: types.SimpleNamespace(run=lambda **_kw: None),
    )
    monkeypatch.setattr(mod.signal, "signal", lambda *_a: None)

    mod.main(["--bind", "127.0.0.1:5999"])

    for side in ("snap", "panda"):
        ct = _connect_timeout(captured[side])
        assert ct is not None, (
            f"{side} transport has an unbounded socket_connect_timeout; "
            "a worker stuck mid-connect to a down bus would stall Ctrl-C"
        )
        # Must be comfortably under the aggregator.stop() join timeout
        # (2.0 s) so a connecting drain thread exits before the join
        # deadline, keeping shutdown to ~1 s.
        assert 0 < ct < 2.0, (
            f"{side} connect timeout {ct!r} is too large for a snappy Ctrl-C"
        )
