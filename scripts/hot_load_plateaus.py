"""Step the hot-load heater through a timed plateau program.

Bench companion to ``obs_config_switch_bench.yaml`` (receiver
calibration protocol § 5). Runs in a second terminal *alongside*
``eigsep-panda``: the switch and VNA loops keep cycling while this
script only moves the tempctrl LOAD setpoint/enable through
:class:`TempCtrlClient`. Sequence per pass::

    ambient (heater off) -> plateau 1 -> ... -> plateau N -> cool-down

Every action and a once-a-minute ``T_now`` readout go to stdout and to
a UTC log file (the protocol's written log). On exit — normal, Ctrl+C,
or error — the heater is disarmed.

Coexistence: this script does not claim ``run_tag`` (it is
``RUN_TAG_EXEMPT``). The setpoint in force is recorded per integration
in the ``tempctrl_load`` stream (``T_target``, ``enabled``,
``hysteresis``), so corr files stay self-describing without it.
``tempctrl_loop`` pushes the yaml ``tempctrl_settings`` once at
``eigsep-panda`` startup and never again, so the two do not fight —
but restarting ``eigsep-panda`` re-pushes the yaml (heater off) and
resets the program; restart this script too if that happens.

Firmware watchdog: the tempctrl pico boots with a 30 s communication
watchdog that latches ``watchdog_tripped`` (gating the heater) when no
command arrives in time; only a ``LOAD_enable=true`` rising edge clears
it. The script re-sends its current command every ``KEEPALIVE_S`` (a
heater-off ``set_enable(LOAD=False)`` or the plateau's
``set_temperature``, neither of which changes state), so the watchdog
cannot trip while it runs, whatever timeout is configured. A watchdog
trip latched *before* the script started (e.g. after a pico reboot) is
a comms gap, not a hardware fault: it is tolerated while the heater is
meant to be off and cleared by the first plateau's enable.

Hardware trips are never acked: a sensor/stall/runaway trip or a
sensor error, at any time, or a watchdog trip while heating, makes the
script disarm the heater and exit nonzero for the operator to
investigate. The first plateau's enable is only sent after checking
that no hardware trip is latched, so it can only clear a watchdog trip.

Example::

    python scripts/hot_load_plateaus.py --targets 47 67 87 \\
        --ambient-min 60 --hold-min 60 --cooldown-min 90 --passes 2
"""

import argparse
import logging
import sys
import time
from collections import deque
from datetime import datetime, timezone

from picohost.proxy import PicoProxy

from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport,
    require_pico,
)
from eigsep_observing.tempctrl_client import TempCtrlClient

logger = logging.getLogger("hot_load_plateaus")

# The YSI 44909 thermistor is rated -55..+90 C continuous (firmware's
# LOAD_MAX_SAFE_TEMP_C trip is 110 C); refuse targets that would hold
# it above its rating. The heater never exceeds the target.
MAX_TARGET_C = 88.0
_TRIPS = ("LOAD_sensor_tripped", "LOAD_stall_tripped", "LOAD_runaway_tripped")
# Keepalive cadence; well under the firmware's 30 s default watchdog.
KEEPALIVE_S = 10.0


class HeaterFault(RuntimeError):
    """A trip or sensor error gated the heater; stop the program."""


def _parse_args():
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--targets",
        type=float,
        nargs="+",
        default=[47.0, 67.0, 87.0],
        help="Plateau setpoints in deg C, in order.",
    )
    p.add_argument("--hysteresis", type=float, default=0.5)
    p.add_argument(
        "--ambient-min",
        type=float,
        default=60.0,
        help="Heater-off ambient block before the plateaus (>= 45).",
    )
    p.add_argument(
        "--hold-min",
        type=float,
        default=60.0,
        help="Time on each plateau, including the ramp.",
    )
    p.add_argument(
        "--cooldown-min",
        type=float,
        default=90.0,
        help="Heater-off cool-down after the last plateau.",
    )
    p.add_argument("--passes", type=int, default=1)
    p.add_argument(
        "--readout-s",
        type=float,
        default=60.0,
        help="Seconds between logged T_now readouts.",
    )
    p.add_argument(
        "--log-file",
        default=None,
        help="UTC action log (default hot_load_plateaus_<UTC>.log).",
    )
    p.add_argument("--dummy", action="store_true")
    add_redis_args(p)
    args = p.parse_args()
    bad = [t for t in args.targets if t > MAX_TARGET_C]
    if bad:
        p.error(
            f"targets {bad} exceed {MAX_TARGET_C} C "
            "(YSI 44909 rated to 90 C)"
        )
    return args


def _setup_logging(path):
    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    fmt.converter = time.gmtime
    for handler in (logging.StreamHandler(), logging.FileHandler(path)):
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def _check_faults(status, heating):
    """Raise :class:`HeaterFault` on a hardware trip, a sensor error,
    or (only while ``heating``) a watchdog trip. Returns whether a
    watchdog trip is latched."""
    faults = [flag for flag in _TRIPS if status.get(flag)]
    if status.get("LOAD_status") == "error":
        faults.append("LOAD_status=error")
    watchdog = bool(status.get("watchdog_tripped"))
    if watchdog and heating:
        faults.append("watchdog_tripped")
    if faults:
        raise HeaterFault(", ".join(faults))
    return watchdog


def _log_readout(status, history):
    t_now = status.get("LOAD_T_now")
    if t_now is not None:
        history.append(t_now)
    p2p = max(history) - min(history) if len(history) > 1 else None
    logger.info(
        "T_now=%s C target=%s enabled=%s active=%s p2p(10 min)=%s C%s",
        f"{t_now:.2f}" if t_now is not None else "--",
        status.get("LOAD_T_target"),
        status.get("LOAD_enabled"),
        status.get("LOAD_active"),
        f"{p2p:.2f}" if p2p is not None else "--",
        " [watchdog trip latched]" if status.get("watchdog_tripped") else "",
    )


def _hold(tc, minutes, readout_s, keepalive, heating):
    """Wait ``minutes``: send ``keepalive()`` and check faults every
    ``KEEPALIVE_S``, log a readout every ``readout_s``."""
    history = deque(maxlen=max(2, int(600 / readout_s)))
    end = time.monotonic() + minutes * 60.0
    next_log = time.monotonic()
    while True:
        keepalive()
        status = tc.get_status() or {}
        _check_faults(status, heating)
        now = time.monotonic()
        if now >= next_log:
            _log_readout(status, history)
            next_log = now + readout_s
        remaining = end - now
        if remaining <= 0:
            return
        time.sleep(min(KEEPALIVE_S, remaining))


def _heater_off(tc, label):
    tc.set_enable(LOAD=False)
    logger.info("ACTION %s: heater OFF", label)


def _plateau(tc, target, hysteresis, label):
    # Setpoint before enable, matching TempCtrlClient.apply_settings's
    # safe order. The enable rising edge is also the firmware trip ack,
    # so refuse to send it over a latched hardware trip: it may only
    # clear a (benign) watchdog trip.
    watchdog = _check_faults(tc.get_status() or {}, heating=False)
    tc.set_temperature(T_LOAD=target, LOAD_hyst=hysteresis)
    tc.set_enable(LOAD=True)
    if watchdog:
        logger.info("Watchdog trip cleared by the heater enable.")
    logger.info(
        "ACTION %s: heater ON, target %.2f C (%.2f K), hysteresis %.2f C",
        label,
        target,
        target + 273.15,
        hysteresis,
    )


def main():
    args = _parse_args()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    _setup_logging(args.log_file or f"hot_load_plateaus_{stamp}.log")

    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    require_pico(PicoProxy("tempctrl", transport, source="hot_load_plateaus"))
    tc = TempCtrlClient(transport, source="hot_load_plateaus")
    status = tc.get_status() or {}
    logger.info(
        "Firmware watchdog timeout %s ms (0 = disabled); keepalive every "
        "%.0f s.",
        status.get("watchdog_timeout_ms"),
        KEEPALIVE_S,
    )
    if status.get("watchdog_tripped"):
        logger.warning(
            "Watchdog trip already latched (no command reached the pico "
            "within its timeout, e.g. after a reboot). Tolerated while "
            "the heater is off; the first plateau's enable clears it."
        )
    logger.info(
        "Program: %d pass(es) of ambient %.0f min -> %s C x %.0f min -> "
        "cool-down %.0f min",
        args.passes,
        args.ambient_min,
        args.targets,
        args.hold_min,
        args.cooldown_min,
    )
    def off_keepalive():
        tc.set_enable(LOAD=False)  # no rising edge: never acks a trip

    rc = 0
    try:
        for n in range(1, args.passes + 1):
            _heater_off(tc, f"pass {n} ambient")
            _hold(
                tc, args.ambient_min, args.readout_s, off_keepalive, False
            )
            for i, target in enumerate(args.targets, start=1):
                _plateau(tc, target, args.hysteresis, f"pass {n} plateau {i}")

                def on_keepalive(target=target):
                    tc.set_temperature(
                        T_LOAD=target, LOAD_hyst=args.hysteresis
                    )

                _hold(tc, args.hold_min, args.readout_s, on_keepalive, True)
            _heater_off(tc, f"pass {n} cool-down")
            _hold(
                tc, args.cooldown_min, args.readout_s, off_keepalive, False
            )
        logger.info("Program complete.")
    except KeyboardInterrupt:
        logger.info("Interrupted by operator.")
    except HeaterFault as exc:
        logger.error(
            "Heater fault (%s): stopping the program, NOT acking the trip. "
            "Investigate the thermistor contact / heater, then restart.",
            exc,
        )
        rc = 1
    finally:
        try:
            _heater_off(tc, "exit")
        except Exception:
            logger.exception("Failed to disarm heater on exit!")
            return 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
