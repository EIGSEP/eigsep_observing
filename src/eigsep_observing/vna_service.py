"""On-demand control of ``cmtvna.service`` (panda-side).

The CMT R60 driver binary runs as a systemd service in socket-server
mode and busy-loops at ~300% CPU whenever it is up, so the field stack
keeps it stopped and starts it only around an S11 measurement window.
This module is the thin start/stop/readiness layer; the session
lifecycle lives in :meth:`eigsep_observing.client.PandaClient.vna_session`
and :func:`eigsep_observing.vna.build_vna_subsystem`.

Panda-only: the caller runs on the same host as the service (``vna_ip``
is ``127.0.0.1``). ``start``/``stop`` shell out to ``systemctl`` and fall
back to ``sudo -n`` — the field image ships a sudoers drop-in granting
the ``eigsep`` user passwordless start/stop of exactly this unit (mirrors
the flash-picos ``systemctl``-then-``sudo -n`` fallback).
"""

import logging
import subprocess
import time

import pyvisa

UNIT = "cmtvna.service"

logger = logging.getLogger(__name__)


def _systemctl(action):
    """Run ``systemctl <action> --no-ask-password cmtvna.service``.

    Try plain ``systemctl`` first (works as root / when already
    permitted), then ``sudo -n`` (passwordless via the image sudoers
    drop-in). Raise ``RuntimeError`` with both captured outputs if both
    fail.
    """
    base = ["systemctl", action, "--no-ask-password", UNIT]
    first = subprocess.run(base, capture_output=True, text=True)
    if first.returncode == 0:
        return
    second = subprocess.run(
        ["sudo", "-n", *base], capture_output=True, text=True
    )
    if second.returncode == 0:
        return
    raise RuntimeError(
        f"{action} {UNIT} failed: "
        f"{(first.stderr or first.stdout).strip()!r} then "
        f"{(second.stderr or second.stdout).strip()!r}"
    )


def start():
    """Start ``cmtvna.service``. Idempotent (systemctl no-ops if active)."""
    logger.info("Starting %s", UNIT)
    _systemctl("start")


def stop():
    """Stop ``cmtvna.service`` to release the CPU."""
    logger.info("Stopping %s", UNIT)
    _systemctl("stop")


def wait_ready(ip, port, *, timeout=30.0, poll_interval=0.5):
    """Block until the cmtvna socket answers ``*IDN?``; raise on timeout.

    The socket server accepts TCP before the instrument is ready, so we
    probe at the SCPI level. Cold start is a consistent ~5.5s on the
    panda; the 30s default is ~5x headroom.

    Returns the trimmed ``*IDN?`` response. Raises ``TimeoutError`` if
    the instrument never answers within ``timeout`` seconds.
    """
    rm = pyvisa.ResourceManager("@py")
    addr = f"TCPIP::{ip}::{port}::SOCKET"
    deadline = time.monotonic() + timeout
    last_exc = None
    while time.monotonic() < deadline:
        res = None
        try:
            res = rm.open_resource(addr)
            res.read_termination = "\n"
            res.timeout = 2000
            idn = res.query("*IDN?\n")
            logger.info("%s ready: %s", UNIT, idn.strip())
            return idn.strip()
        except Exception as exc:  # pyvisa raises many error types
            last_exc = exc
            time.sleep(poll_interval)
        finally:
            if res is not None:
                try:
                    res.close()
                except Exception:
                    pass
    raise TimeoutError(
        f"cmtvna not ready on {ip}:{port} after {timeout}s "
        f"(last error: {last_exc})"
    )
