"""
Client-side tempctrl (LOAD heater) orchestrator.

Wraps a :class:`picohost.proxy.PicoProxy` (``tempctrl``) and a
:class:`eigsep_redis.MetadataSnapshotReader` so :class:`PandaClient`
can push setpoints/enable flags to the LOAD channel (a low-side FET
heater under on/off hysteresis control) and read back the most recent
status without reaching inside the :class:`picohost.manager.PicoManager`
process. Mirrors the role of :class:`eigsep_observing.motor_client.MotorClient`
for the motor pico.
"""

import logging

from eigsep_redis import MetadataSnapshotReader
from picohost.proxy import PicoProxy

logger = logging.getLogger(__name__)


class TempCtrlClient:
    """Push LOAD heater settings through ``PicoManager`` via Redis.

    Parameters
    ----------
    transport : eigsep_redis.Transport
        Shared transport; used to build the proxy and metadata reader.
    settings : dict or None
        Validated yaml settings dict, shaped as::

            {
                "watchdog_timeout_ms": int,
                "LOAD": {
                    "installed": bool,  # optional; firmware default True
                    "enable": bool,
                    "target_C": float,
                    "hysteresis_C": float,
                },
            }

        ``None`` or ``{}`` means "do not push anything on
        ``apply_settings`` beyond what's explicitly passed as an
        override." The yaml schema is kept readable (``target_C``,
        ``hysteresis_C``) and translated to firmware field names
        (``LOAD_temp_target``, ``LOAD_hysteresis``) inside
        :meth:`apply_settings`.
        ``installed: false`` descopes the channel: firmware never
        samples its thermistor or drives it, its Redis stream stops
        publishing entirely, and :meth:`get_status` stops reading it.
        Must be paired with ``enable: false`` (an absent module cannot
        be armed — rejected at construction).
    source : str
        Identifier stamped on proxy command stream entries.
    """

    def __init__(self, transport, *, settings=None, source="panda_client"):
        self.transport = transport
        self._proxy = PicoProxy("tempctrl", transport, source=source)
        self._reader = MetadataSnapshotReader(transport)
        self.settings = self._coerce_settings(settings)
        self.logger = logger

    @staticmethod
    def _coerce_settings(raw):
        """Validate yaml settings and pre-coerce each field to the
        firmware-ready type, so :meth:`apply_settings` cannot raise
        :class:`TypeError` / :class:`ValueError` mid-loop on a bad
        config.

        ``None`` → ``{}`` (nothing to push). Missing top-level
        sections (``watchdog_timeout_ms``, ``LOAD``) are skipped,
        matching :meth:`apply_settings`' "keep whatever firmware had"
        behavior.

        Raises
        ------
        ValueError
            Settings is not a dict, the ``LOAD`` section is not a
            dict, ``enable`` is not a real bool, or a numeric field is
            not int/float-coercible. Raised at construction so the
            caller (:meth:`PandaClient.init_tempctrl`) can disable
            tempctrl with a single WARNING instead of unwinding the
            loop thread on the first apply. A YAML string like
            ``"false"`` parses as truthy under ``bool(...)`` — so
            ``enable`` must be a real ``bool``, not merely truthy.
        """
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise ValueError(
                f"tempctrl settings must be a dict, got {type(raw).__name__}"
            )
        out = {}
        if "watchdog_timeout_ms" in raw:
            val = raw["watchdog_timeout_ms"]
            try:
                out["watchdog_timeout_ms"] = int(val)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"watchdog_timeout_ms: {val!r} not int-coercible ({exc})"
                ) from exc
        if "LOAD" in raw:
            section = raw["LOAD"]
            if not isinstance(section, dict):
                raise ValueError(
                    "tempctrl[LOAD] must be a dict, got "
                    f"{type(section).__name__}"
                )
            coerced = {}
            for fname in ("target_C", "hysteresis_C"):
                if fname in section:
                    val = section[fname]
                    try:
                        coerced[fname] = float(val)
                    except (TypeError, ValueError) as exc:
                        raise ValueError(
                            f"tempctrl[LOAD].{fname}: {val!r} not "
                            f"float-coercible ({exc})"
                        ) from exc
            for bname in ("installed", "enable"):
                if bname in section:
                    val = section[bname]
                    if not isinstance(val, bool):
                        raise ValueError(
                            f"tempctrl[LOAD].{bname}: {val!r} must be a "
                            f"bool, got {type(val).__name__}"
                        )
                    coerced[bname] = val
            if (
                coerced.get("installed") is False
                and coerced.get("enable") is True
            ):
                raise ValueError(
                    "tempctrl[LOAD]: installed: false with enable: true "
                    "— an absent module cannot be armed; set enable: "
                    "false or mark the channel installed"
                )
            out["LOAD"] = coerced
        return out

    @property
    def is_available(self):
        return self._proxy.is_available

    def get_status(self):
        """Latest tempctrl metadata snapshot, or ``None`` if absent.

        The picohost producer publishes the ``tempctrl_load`` stream
        with flat per-channel fields plus a duplicated copy of the
        device-wide watchdog state. This method republishes it under
        the flat ``LOAD_*`` shape used internally by
        ``_tempctrl_health_check`` so callers don't have to know about
        the underlying stream name.

        A channel whose settings say ``installed: false`` is never
        read: its stream stopped publishing at the producer, but a
        leftover hash entry (lab bring-up, pre-descope deployment, the
        reboot burst before pico-manager replays the flags) would
        otherwise feed stale data into the merged status and trigger
        the snapshot reader's staleness warning on every poll.
        """
        if self.settings.get("LOAD", {}).get("installed") is False:
            return None
        try:
            load = self._reader.get("tempctrl_load")
        except KeyError:
            load = None
        if not load:
            return None
        merged = {}
        for k, v in load.items():
            if k in ("sensor_name", "app_id"):
                continue
            if k in ("watchdog_tripped", "watchdog_timeout_ms"):
                merged[k] = v
            else:
                merged[f"LOAD_{k}"] = v
        return merged or None

    def set_watchdog_timeout(self, timeout_ms):
        self._proxy.send_command(
            "set_watchdog_timeout", timeout_ms=int(timeout_ms)
        )

    def set_installed(self, *, LOAD=None):
        """Mark the LOAD module's hardware present/absent.

        Mirrors :meth:`picohost.base.PicoTempCtrl.set_installed`.
        ``False`` descopes the channel: firmware never samples its
        thermistor (no ADC mux switch to a dead divider) or drives it,
        and the redis fan-out suppresses its stream entirely — clean
        absence downstream. Distinct from :meth:`set_enable` (drive
        intent for present hardware); not a trip ack. Firmware caches
        the setting for replay on reconnect.
        """
        if LOAD is not None:
            self._proxy.send_command("set_installed", LOAD=bool(LOAD))

    def set_temperature(self, *, T_LOAD=None, LOAD_hyst=None):
        """Push the setpoint. Hysteresis piggybacks on the
        set_temperature command to match the
        :class:`picohost.base.PicoTempCtrl` signature.
        """
        kwargs = {}
        if T_LOAD is not None:
            kwargs["T_LOAD"] = float(T_LOAD)
            if LOAD_hyst is not None:
                kwargs["LOAD_hyst"] = float(LOAD_hyst)
        if kwargs:
            self._proxy.send_command("set_temperature", **kwargs)

    def set_enable(self, *, LOAD=None):
        """Arm/disarm the LOAD heater drive.

        Only sends the command if ``LOAD`` is specified. ``PicoTempCtrl
        .set_enable`` defaults its kwarg to ``True`` firmware-side, so
        we pass it explicitly to avoid surprise arming.
        """
        if LOAD is None:
            return
        self._proxy.send_command("set_enable", LOAD=bool(LOAD))

    def apply_settings(self):
        """Push the full config to the pico in safe order.

        Order matches ``PicoTempCtrl``'s reconnect replay (watchdog →
        installed → temperature → enable):

        1. ``set_watchdog_timeout`` first so any subsequent
           delay-between-commands cannot trip a zero-timeout default.
        2. ``set_installed`` — gate a descoped channel (no sampling,
           no drive, no stream) before any drive-producing config
           arrives; firmware reboots to installed=true by default.
        3. ``set_temperature`` — publish the target (and hysteresis)
           while still disarmed (or at prior arm state).
        4. ``set_enable`` — arm last, so by the time the channel turns
           on the setpoint is already in place.

        Idempotent: calling repeatedly with unchanged settings is a
        no-op on the hardware side (firmware replaces current values
        with identical ones). Missing sections are skipped — e.g.
        omitting ``watchdog_timeout_ms`` leaves whatever the firmware
        currently has. Missing ``installed`` leaves the firmware
        default (True) in place. An uninstalled channel's setpoint
        still pushes — harmless while descoped (no sampling, no drive)
        and pre-staged for the moment the module is re-installed;
        arming it is what's forbidden (``_coerce_settings`` rejects
        ``installed: false`` with ``enable: true``).

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
        load = s.get("LOAD", {})
        self.set_installed(LOAD=load.get("installed"))
        self.set_temperature(
            T_LOAD=load.get("target_C"),
            LOAD_hyst=load.get("hysteresis_C"),
        )
        self.set_enable(LOAD=load.get("enable"))
