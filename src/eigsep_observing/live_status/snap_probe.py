"""SNAP FPGA TCP reachability probe.

Tests whether a CASPER service (katcp on port 7147 by default) is
accepting TCP connections at ``snap_ip``. The probe does not speak
katcp; a successful handshake is sufficient to prove the SNAP
firmware is up at the network layer.

Decoupled from the rest of the live-status stack — pure function,
no Redis dependency, no shared state. Called from
``LiveStatusAggregator._snap_tick`` only when ``corr_last_unix``
is stale (the corr stream is already a stronger liveness signal
when it's flowing).
"""

from __future__ import annotations

import logging
import socket
from typing import Optional

logger = logging.getLogger(__name__)


KATCP_PORT = 7147


def probe_snap_fpga(
    snap_ip: Optional[str],
    *,
    port: int = KATCP_PORT,
    timeout: float = 1.0,
) -> bool:
    """Return True iff a TCP connection to ``snap_ip:port`` opens.

    Parameters
    ----------
    snap_ip
        Target host (typically ``cfg['snap_ip']``). ``None`` or an
        empty string returns ``False`` without attempting a connect —
        callers may pass an unresolved value.
    port
        TCP port to probe. Defaults to the CASPER katcp port (7147).
    timeout
        Per-connect timeout in seconds. ~1 s is a reasonable upper
        bound on a loopback or directly-connected SNAP and keeps
        the aggregator's drain tick responsive.

    Returns
    -------
    bool
        ``True`` if the connect succeeded, ``False`` otherwise
        (refused, timeout, DNS failure, OS error).
    """
    if not snap_ip:
        return False
    try:
        with socket.create_connection((snap_ip, port), timeout=timeout):
            return True
    except OSError as exc:
        logger.debug("snap probe to %s:%d failed: %s", snap_ip, port, exc)
        return False
