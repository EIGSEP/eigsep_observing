"""
Client-side tempctrl (Peltier) orchestrator.

Wraps a :class:`picohost.proxy.PicoProxy` (``tempctrl``) and a
:class:`eigsep_redis.MetadataSnapshotReader` so :class:`PandaClient`
can push setpoints/clamps/enable flags to the LNA and LOAD Peltier
channels and read back the most recent status without reaching inside
the :class:`picohost.manager.PicoManager` process. Mirrors the role of
:class:`eigsep_observing.motor_scanner.MotorScanner` for the motor pico.

Unlike motor scans, tempctrl commands are atomic — there is no
multi-step orchestration and no stall/timeout concept on the panda
side. The firmware runs its own closed-loop hysteresis control; the
Python side just publishes fresh setpoints periodically and watches
the metadata snapshot for health.
"""

import logging

from eigsep_redis import MetadataSnapshotReader
from picohost.proxy import PicoProxy

logger = logging.getLogger(__name__)


class TempCtrlClient:
    """Push LNA/LOAD Peltier settings through ``PicoManager`` via Redis.

    Parameters
    ----------
    transport : eigsep_redis.Transport
        Shared transport; used to build the proxy and metadata reader.
    settings : dict or None
        Validated yaml settings dict, shaped as::

            {
                "watchdog_timeout_ms": int,
                "LNA": {
                    "enable": bool,
                    "target_C": float,
                    "hysteresis_C": float,
                    "clamp": float,
                },
                "LOAD": {... same keys as LNA ...},
            }

        ``None`` or ``{}`` means "do not push anything on
        ``apply_settings`` beyond what's explicitly passed as an
        override." The yaml schema is kept readable (``target_C``,
        ``hysteresis_C``) and translated to firmware field names
        (``LNA_temp_target``, ``LNA_hysteresis``, ...) inside
        :meth:`apply_settings`.
    source : str
        Identifier stamped on proxy command stream entries.
    """

    _CHANNELS = ("LNA", "LOAD")

    def __init__(self, transport, *, settings=None, source="panda_client"):
        self.transport = transport
        self._proxy = PicoProxy("tempctrl", transport, source=source)
        self._reader = MetadataSnapshotReader(transport)
        self.settings = dict(settings) if settings else {}
        self.logger = logger

    @property
    def is_available(self):
        return self._proxy.is_available

    def get_status(self):
        """Latest tempctrl metadata snapshot, or ``None`` if absent."""
        try:
            return self._reader.get("tempctrl")
        except KeyError:
            return None

    def set_watchdog_timeout(self, timeout_ms):
        self._proxy.send_command(
            "set_watchdog_timeout", timeout_ms=int(timeout_ms)
        )

    def set_clamp(self, *, LNA=None, LOAD=None):
        kwargs = {}
        if LNA is not None:
            kwargs["LNA"] = float(LNA)
        if LOAD is not None:
            kwargs["LOAD"] = float(LOAD)
        if kwargs:
            self._proxy.send_command("set_clamp", **kwargs)

    def set_temperature(
        self, *, T_LNA=None, LNA_hyst=None, T_LOAD=None, LOAD_hyst=None
    ):
        """Push setpoints. Hysteresis piggybacks on the set_temperature
        command to match the :class:`picohost.base.PicoPeltier` signature.
        """
        kwargs = {}
        if T_LNA is not None:
            kwargs["T_LNA"] = float(T_LNA)
            if LNA_hyst is not None:
                kwargs["LNA_hyst"] = float(LNA_hyst)
        if T_LOAD is not None:
            kwargs["T_LOAD"] = float(T_LOAD)
            if LOAD_hyst is not None:
                kwargs["LOAD_hyst"] = float(LOAD_hyst)
        if kwargs:
            self._proxy.send_command("set_temperature", **kwargs)

    def set_enable(self, *, LNA=None, LOAD=None):
        """Arm/disarm per-channel peltier drive.

        Only sends the command if at least one channel is specified, so
        partial-application callers don't flip the untouched channel.
        ``PicoPeltier.set_enable`` defaults missing kwargs to ``True``
        firmware-side, so we pass both explicitly to avoid surprise
        arming.
        """
        if LNA is None and LOAD is None:
            return
        current = self.settings
        lna_enable = (
            bool(LNA)
            if LNA is not None
            else bool(current.get("LNA", {}).get("enable", False))
        )
        load_enable = (
            bool(LOAD)
            if LOAD is not None
            else bool(current.get("LOAD", {}).get("enable", False))
        )
        self._proxy.send_command(
            "set_enable", LNA=lna_enable, LOAD=load_enable
        )

    def apply_settings(self):
        """Push the full config to the pico in safe order.

        Order:

        1. ``set_watchdog_timeout`` first so any subsequent
           delay-between-commands cannot trip a zero-timeout default.
        2. ``set_clamp`` — establish the duty-cycle ceiling before
           anything is armed.
        3. ``set_temperature`` — publish the target (and hysteresis)
           while still disarmed (or at prior arm state).
        4. ``set_enable`` — arm last, so by the time the channel turns
           on the clamp and setpoint are already in place.

        Idempotent: calling repeatedly with unchanged settings is a
        no-op on the hardware side (firmware replaces current values
        with identical ones). Missing sections are skipped — e.g.
        omitting ``watchdog_timeout_ms`` leaves whatever the firmware
        currently has.

        Raises
        ------
        RuntimeError, TimeoutError
            From the underlying :class:`PicoProxy` on command delivery
            failure. Caller decides whether to log, retry, or surface.
        """
        s = self.settings
        if not s:
            return
        watchdog = s.get("watchdog_timeout_ms")
        if watchdog is not None:
            self.set_watchdog_timeout(watchdog)
        lna = s.get("LNA", {})
        load = s.get("LOAD", {})
        self.set_clamp(
            LNA=lna.get("clamp"),
            LOAD=load.get("clamp"),
        )
        self.set_temperature(
            T_LNA=lna.get("target_C"),
            LNA_hyst=lna.get("hysteresis_C"),
            T_LOAD=load.get("target_C"),
            LOAD_hyst=load.get("hysteresis_C"),
        )
        self.set_enable(
            LNA=lna.get("enable"),
            LOAD=load.get("enable"),
        )
