"""Shared helpers for the standalone manual test scripts under ``scripts/``.

Two responsibilities, both deliberately small:

- :func:`build_transport` mirrors the ``--dummy`` bootstrap used by
  ``motor_manual.py`` / ``motor_control.py`` so every per-app manual
  script can share one well-tested transport path.
- :func:`require_pico` is a structural availability check the manual
  scripts run before issuing any command, so an operator who forgot to
  start ``pico-manager.service`` (or who flashed the wrong pico) gets a
  one-line actionable error instead of a silent ``send_command`` no-op.
"""

from __future__ import annotations

import logging
import sys

from eigsep_redis import Transport


logger = logging.getLogger(__name__)


def build_transport(
    dummy: bool,
    *,
    host: str = "localhost",
    real_port: int = 6379,
    dummy_port: int = 6380,
) -> Transport:
    """Return a :class:`Transport` matching the manual-script convention.

    Real mode just constructs a ``Transport(host, real_port)``.

    Dummy mode constructs a transport against a separate fakeredis port
    (``dummy_port``, conventionally 6380), resets the fakeredis state,
    and attaches a fully-emulated ``DummyPandaClient`` to the transport
    so every dummy pico (motor, rfswitch, tempctrl, potmon, imu_el,
    imu_az, lidar) is registered and emitting status before the caller
    starts issuing proxy commands. The client is stashed on
    ``transport._dummy_client`` to keep it alive — without that
    reference Python would garbage-collect the embedded PicoManager
    threads and the proxy would see ``is_available=False``.
    """
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        from .testing import DummyPandaClient

        transport = Transport(host=host, port=dummy_port)
        transport.reset()
        transport._dummy_client = DummyPandaClient(transport=transport)
        return transport
    return Transport(host=host, port=real_port)


def require_pico(proxy, *, hint_script: str = "pico_preflight.py") -> None:
    """Exit with a clear message if ``proxy``'s heartbeat is missing.

    Manual scripts go through :class:`picohost.proxy.PicoProxy`, which
    silently returns ``None`` from :meth:`send_command` when the
    targeted pico's heartbeat is absent. That's the right behavior for
    long-running observing loops, but a manual operator running a
    bring-up script needs to know immediately that the pico-manager
    service isn't reachable.
    """
    if proxy.is_available:
        return
    sys.stderr.write(
        f"ERROR: pico {proxy.name!r} is not registered with PicoManager.\n"
        f"  - Is pico-manager.service running on the panda?\n"
        f"  - Was the pico flashed? Run scripts/{hint_script} to verify.\n"
    )
    raise SystemExit(2)
