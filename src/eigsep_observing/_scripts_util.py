"""Shared helpers for the standalone manual test scripts under ``scripts/``.

Three responsibilities, all deliberately small:

- :func:`build_transport` mirrors the ``--dummy`` bootstrap used by
  the multi-pico manual scripts (``motor_manual.py`` / ``motor_scan.py``
  / ``potmon_manual.py`` / ...) so every per-app manual script can
  share one well-tested transport path.
- :func:`build_transport_bare` is the equivalent for bring-up scripts
  that build their *own* minimal producer surface and explicitly do
  not want a ``DummyPandaClient`` masquerading on the transport in
  dummy mode (see ``scripts/CLAUDE.md`` for the contract).
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

    Bring-up scripts that build their own minimal producer surface
    (e.g. ``vna_manual.py`` / ``record_vna.py``) want
    :func:`build_transport_bare` instead — the auto-attached
    ``DummyPandaClient`` here violates the no-PandaClient-masquerade
    rule documented in ``scripts/CLAUDE.md`` and competes with the
    script's own dummy-pico bootstrap.
    """
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        from .testing import DummyPandaClient

        transport = Transport(host=host, port=dummy_port)
        transport.reset()
        transport._dummy_client = DummyPandaClient(transport=transport)
        return transport
    return Transport(host=host, port=real_port)


def build_transport_bare(
    dummy: bool,
    *,
    host: str = "localhost",
    real_port: int = 6379,
    dummy_port: int = 6380,
) -> Transport:
    """Return a :class:`Transport` with no client attached.

    The bring-up-script variant of :func:`build_transport`: same port
    convention, same fakeredis reset on dummy, but explicitly does
    **not** instantiate a ``DummyPandaClient`` on the transport.

    Bring-up scripts (``vna_manual.py``, ``record_vna.py``, etc.) build
    their own minimal producer surface (see ``scripts/CLAUDE.md``) and
    spin up only the dummy picos they actually need. Attaching a
    ``DummyPandaClient`` from this helper would (a) start a heartbeat
    thread that collides with whatever the script is meant to test and
    (b) double-register dummy picos if the script also calls
    ``start_dummy_pico_manager`` directly.
    """
    if dummy:
        logger.warning("Running in DUMMY mode, no hardware will be used.")
        transport = Transport(host=host, port=dummy_port)
        transport.reset()
        return transport
    return Transport(host=host, port=real_port)


def add_redis_args(
    parser,
    *,
    default_host: str = "localhost",
    default_port: int = 6379,
) -> None:
    """Add the standard ``--redis-host`` / ``--redis-port`` flags.

    Single source of truth for how every bring-up script under
    ``scripts/`` exposes the Redis location, so an operator running any
    of them from a remote machine uses the same flag names everywhere.
    Pair with ``build_transport(args.dummy, host=args.redis_host,
    real_port=args.redis_port)`` (or ``build_transport_bare``).

    ``default_host`` is a parameter rather than hardcoded ``localhost``
    because a few scripts (``record_metadata.py``, ``pico_preflight.py``)
    are normally run from the ground computer *against* the panda and so
    default to the rig IP; they get the same flag names without
    regressing their no-arg behavior.
    """
    parser.add_argument(
        "--redis-host",
        default=default_host,
        help=f"Redis host (default: {default_host}). Set to the panda's "
        "IP to run from another computer on the rig network.",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=default_port,
        help=f"Redis port (default: {default_port}). Ignored in --dummy "
        "mode, which always targets the local fakeredis on 6380.",
    )


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
