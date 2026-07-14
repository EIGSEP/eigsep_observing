"""Standby/resume toggle for the RFI-quiet-capable picos.

The IMU (BNO085 held in reset) and lidar (GRF-250 laser off) picos accept
a universal standby/resume command (picohost >= 4.6). This module is the
single seam both the ``standby_manual`` bring-up tool and
``PandaClient.apply_standby_defaults`` route through, so the command name
and error handling live in one place.

Standby is in-RAM only on the pico — a reboot comes back sensing.
"""

#: Canonical PicoManager device names that accept standby/resume.
STANDBY_DEVICES = ("imu_el", "imu_az", "lidar")


def set_standby(proxy, on):
    """Toggle standby on a pico via its ``PicoProxy``.

    Sends ``"standby"`` when *on* is True, ``"resume"`` otherwise.
    Returns a short status string for operator/log feedback:
    ``"ok"``, ``"unavailable"`` (device heartbeat missing — the proxy
    returned ``None``), or ``"err <Type>: <msg>"`` on a command
    ``TimeoutError``/``RuntimeError``. Never raises — callers loop over
    devices and one down pico must not abort the others.
    """
    action = "standby" if on else "resume"
    try:
        result = proxy.send_command(action)
    except (TimeoutError, RuntimeError) as exc:
        return f"err {type(exc).__name__}: {exc}"
    if result is None:
        return "unavailable"
    return "ok"
