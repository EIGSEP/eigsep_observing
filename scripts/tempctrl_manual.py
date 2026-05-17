"""Interactive tempctrl (peltier) bring-up tool.

Curses UI showing live per-channel readouts for the two tempctrl
streams (``tempctrl_lna`` and ``tempctrl_load``) and single-key
commands that exercise every panda-side setter on
:class:`picohost.base.PicoPeltier`. Operator confirms that the cold
side temperature actually moves when the setpoint changes, that the
firmware watchdog trips when the panda goes silent, and that the
clamp limits drive saturation as expected.

Controls:
  l / L    enable LNA on / off
  o / O    enable LOAD on / off
  + / -    LNA setpoint +/- 0.5 deg C
  ] / [    LOAD setpoint +/- 0.5 deg C
  c        cycle clamp through (0.1, 0.3, 0.5, 1.0) on both channels
  w        push a 5 s watchdog (should trip if not refreshed)
  r        re-enable both channels at their last setpoint
  q        quit

Every command goes through :class:`picohost.proxy.PicoProxy` so
behavior mirrors the production tempctrl_loop path. Setpoints and
clamp values are tracked client-side so the +/- keys can bump them
without round-tripping the firmware to read back the current value.
"""

from argparse import ArgumentParser
import curses
import logging

from eigsep_redis import MetadataSnapshotReader
from picohost.proxy import PicoProxy

from eigsep_observing._scripts_util import build_transport, require_pico
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

CLAMPS = (0.1, 0.3, 0.5, 1.0)
SETPOINT_STEP_C = 0.5
WATCHDOG_PROBE_MS = 5000


class _State:
    """Operator-facing state the script tracks locally.

    Firmware is the source of truth for ``T_now`` / ``drive_level`` /
    ``watchdog_tripped`` (read from snapshot). The local copies of
    setpoints, enable flags, and the clamp index are only used so the
    +/- keys can bump them — they're seeded from the snapshot on
    startup if available, and re-pushed on every change so a missed
    command can't leave the firmware and the UI disagreeing.
    """

    def __init__(self, lna_setpoint, load_setpoint, lna_enabled, load_enabled):
        self.lna_setpoint = lna_setpoint
        self.load_setpoint = load_setpoint
        self.lna_enabled = lna_enabled
        self.load_enabled = load_enabled
        self.clamp_idx = 2  # default 0.5
        self.last_message = ""


def _snap(snapshot, name):
    return snapshot.get().get(name)


def _seed_state(snapshot):
    """Build a starting :class:`_State` from whatever the firmware reports.

    Defaults if a field is missing or the channel hasn't published yet
    are deliberately conservative: 20 deg C setpoint, channels off, so
    the operator has to opt into actually driving the peltier.
    """
    lna = _snap(snapshot, "tempctrl_lna") or {}
    load = _snap(snapshot, "tempctrl_load") or {}
    return _State(
        lna_setpoint=float(lna.get("T_target") or 20.0),
        load_setpoint=float(load.get("T_target") or 20.0),
        lna_enabled=bool(lna.get("enabled") or False),
        load_enabled=bool(load.get("enabled") or False),
    )


def _send(proxy, action, **kwargs):
    """Invoke a tempctrl command and return a short status string.

    Returns "ok", a "skipped: ..." reason, or an error summary. Used as
    the curses footer message so the operator sees per-keypress
    feedback without having to scrape the log.
    """
    try:
        result = proxy.send_command(action, **kwargs)
    except (TimeoutError, RuntimeError) as exc:
        return f"err {action}: {type(exc).__name__}: {exc}"
    if result is None:
        return f"skipped {action}: tempctrl unavailable"
    return f"ok {action} {kwargs}"


def _push_enables(proxy, state):
    state.last_message = _send(
        proxy,
        "set_enable",
        LNA=state.lna_enabled,
        LOAD=state.load_enabled,
    )


def _push_temperatures(proxy, state):
    state.last_message = _send(
        proxy,
        "set_temperature",
        T_LNA=state.lna_setpoint,
        T_LOAD=state.load_setpoint,
    )


def _push_clamp(proxy, state):
    value = CLAMPS[state.clamp_idx]
    state.last_message = _send(proxy, "set_clamp", LNA=value, LOAD=value)


def _fmt(value, fmt):
    if not isinstance(value, (int, float)):
        return "    --"
    return format(value, fmt)


def _render(screen, snapshot, state):
    lna = _snap(snapshot, "tempctrl_lna") or {}
    load = _snap(snapshot, "tempctrl_load") or {}
    screen.clear()
    screen.addstr(0, 0, "=== tempctrl manual ===")
    screen.addstr(
        1, 0, "channel  T_now    T_target  drive   clamp   enabled  status"
    )
    screen.addstr(
        2,
        0,
        "LNA      "
        f"{_fmt(lna.get('T_now'), '6.2f')}  "
        f"{_fmt(lna.get('T_target'), '6.2f')}    "
        f"{_fmt(lna.get('drive_level'), '6.2f')}  "
        f"{_fmt(lna.get('clamp'), '6.2f')}  "
        f"{str(lna.get('enabled')):>7}  {lna.get('status')!r}",
    )
    screen.addstr(
        3,
        0,
        "LOAD     "
        f"{_fmt(load.get('T_now'), '6.2f')}  "
        f"{_fmt(load.get('T_target'), '6.2f')}    "
        f"{_fmt(load.get('drive_level'), '6.2f')}  "
        f"{_fmt(load.get('clamp'), '6.2f')}  "
        f"{str(load.get('enabled')):>7}  {load.get('status')!r}",
    )
    # watchdog_tripped is duplicated across both streams (see
    # _PELTIER_SCHEMA) — read from either; LNA is fine.
    screen.addstr(
        5, 0, f"watchdog_tripped: {bool(lna.get('watchdog_tripped'))}"
    )
    screen.addstr(
        6,
        0,
        f"client setpoints: LNA={state.lna_setpoint:.2f} "
        f"LOAD={state.load_setpoint:.2f}  "
        f"clamp={CLAMPS[state.clamp_idx]:.2f}",
    )
    screen.addstr(8, 0, "l/L enable LNA on/off       o/O enable LOAD on/off")
    screen.addstr(9, 0, "+/- LNA setpoint  ][ LOAD setpoint  c cycle clamp")
    screen.addstr(10, 0, "w push 5s watchdog   r re-enable both   q quit")
    if state.last_message:
        screen.addstr(12, 0, f"> {state.last_message}"[: curses.COLS - 1])
    screen.refresh()


def _handle_key(ch, proxy, state):
    if ch in (ord("q"), 27):  # q or ESC
        return False
    if ch == ord("l"):
        state.lna_enabled = True
        _push_enables(proxy, state)
    elif ch == ord("L"):
        state.lna_enabled = False
        _push_enables(proxy, state)
    elif ch == ord("o"):
        state.load_enabled = True
        _push_enables(proxy, state)
    elif ch == ord("O"):
        state.load_enabled = False
        _push_enables(proxy, state)
    elif ch in (ord("+"), ord("=")):
        state.lna_setpoint += SETPOINT_STEP_C
        _push_temperatures(proxy, state)
    elif ch == ord("-"):
        state.lna_setpoint -= SETPOINT_STEP_C
        _push_temperatures(proxy, state)
    elif ch == ord("]"):
        state.load_setpoint += SETPOINT_STEP_C
        _push_temperatures(proxy, state)
    elif ch == ord("["):
        state.load_setpoint -= SETPOINT_STEP_C
        _push_temperatures(proxy, state)
    elif ch == ord("c"):
        state.clamp_idx = (state.clamp_idx + 1) % len(CLAMPS)
        _push_clamp(proxy, state)
    elif ch == ord("w"):
        state.last_message = _send(
            proxy, "set_watchdog_timeout", timeout_ms=WATCHDOG_PROBE_MS
        )
    elif ch == ord("r"):
        state.lna_enabled = True
        state.load_enabled = True
        _push_enables(proxy, state)
        _push_temperatures(proxy, state)
    return True


def _curses_main(screen, transport, args):
    curses.noecho()
    screen.timeout(int(args.interval * 1000))
    proxy = PicoProxy("tempctrl", transport, source="tempctrl_manual")
    require_pico(proxy)
    snapshot = MetadataSnapshotReader(transport)
    state = _seed_state(snapshot)
    while True:
        _render(screen, snapshot, state)
        ch = screen.getch()
        if ch == -1:
            # timeout — re-render so the readout stays live
            continue
        if not _handle_key(ch, proxy, state):
            return


def _parse_args():
    parser = ArgumentParser(
        description="Interactive tempctrl bring-up: drive setpoints and "
        "exercise the firmware watchdog."
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Refresh interval in seconds (default: 0.5).",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(args.dummy)
    curses.wrapper(_curses_main, transport, args)


if __name__ == "__main__":
    main()
