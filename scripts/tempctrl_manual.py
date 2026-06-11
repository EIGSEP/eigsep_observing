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
  n / N    LNA cooling (negative drive) allow / forbid
  m / M    LOAD cooling (negative drive) allow / forbid
  + / -    LNA setpoint +/- 0.5 deg C
  ] / [    LOAD setpoint +/- 0.5 deg C
  c / C    clamp one step up / down through (0.1, 0.2, 0.3, 0.5, 1.0)
           on both channels (no wraparound, so the clamp can be lowered
           without passing through the higher values first)
  g / G    LNA Kp +/- 0.05
  h / H    LOAD Kp +/- 0.05
  i / I    LNA Ki +/- 0.005
  k / K    LOAD Ki +/- 0.005
  z / Z    reset LNA / LOAD PI integrator
  w        push a 5 s watchdog (should trip if not refreshed)
  r        re-enable both channels at their last setpoint
  p        write a temperature-vs-time PNG of the session so far
  q        quit

Every loop tick records the firmware ``T_now`` / ``T_target`` /
``drive_level`` for both channels into an in-memory history. Pressing
``p`` renders that history to ``tempctrl_<timestamp>.png`` in the
current directory — one row per channel, ``T_now`` and ``T_target`` on
the left axis and ``drive_level`` on a twin right axis. The plot uses
the Agg backend so it works headless / over SSH to the panda; the
written path is reported in the footer rather than printed (curses owns
the screen). ``p`` may be pressed repeatedly; each press writes a fresh
timestamped file.

Every command goes through :class:`picohost.proxy.PicoProxy` so
behavior mirrors the production tempctrl_loop path. Setpoints and
clamp values are tracked client-side so the +/- keys can bump them
without round-tripping the firmware to read back the current value.

Trip clearing (picohost >= 3.4.0 / pico-firmware e0724e0): ``enabled``
is host intent only — firmware never mutates it. Drive engages iff
``enabled && !int_disabled && !stall_tripped && !watchdog_tripped``
(shown as the ``armed`` column in the readout). The sticky trips
``stall_tripped`` and ``watchdog_tripped`` are cleared by an explicit
``*_enable=true`` rising edge from the host. From this UI that means
``l`` (LNA on), ``o`` (LOAD on), or ``r`` (re-enable both) double as
the operator's trip-clear ack — bare keepalives refresh the watchdog
timer but no longer clear the trip flag.
"""

from argparse import ArgumentParser
import curses
import logging
from pathlib import Path
import time

import matplotlib

matplotlib.use("Agg")  # headless: render to PNG without a display/over SSH
import matplotlib.pyplot as plt  # noqa: E402  (must follow use("Agg"))

from eigsep_redis import MetadataSnapshotReader
from picohost.proxy import PicoProxy

from eigsep_observing import run_tag
from eigsep_observing._scripts_util import build_transport, require_pico
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)

CLAMPS = (0.1, 0.2, 0.3, 0.5, 1.0)
SETPOINT_STEP_C = 0.5
KP_STEP = 0.05
KI_STEP = 0.005  # smaller — integral accumulates over many ticks
DEFAULT_KP = 0.2  # firmware default, matches TempCtrlEmulator
DEFAULT_KI = 0.0  # firmware default — opt-in via this script or yaml
WATCHDOG_PROBE_MS = 5000
# picohost STATUS_CADENCE_MS = 200; poll at the same cadence so we
# wake on the next publish without busy-spinning.
PICO_PUBLISH_INTERVAL_S = 0.2
# Headroom over the 200 ms cadence: a healthy pico publishes within
# one tick; 5 s of slack absorbs a slow PicoManager restart without
# masking a stuck producer.
SEED_TIMEOUT_S = 5.0

# Streams plotted by the `p` hotkey, in render order (one row each).
PLOT_CHANNELS = ("tempctrl_lna", "tempctrl_load")
# Firmware fields buffered every loop tick for the history plot.
PLOT_FIELDS = ("T_now", "T_target", "drive_level")


class _State:
    """Operator-facing state the script tracks locally.

    Firmware is the source of truth for ``T_now`` / ``drive_level`` /
    ``watchdog_tripped`` (read from snapshot). The local copies of
    setpoints, enable flags, gains, and the clamp index are only used
    so the bump keys can step them — they're seeded from the snapshot
    on startup if available, and re-pushed on every change so a missed
    command can't leave the firmware and the UI disagreeing.
    """

    def __init__(
        self,
        lna_setpoint,
        load_setpoint,
        lna_enabled,
        load_enabled,
        lna_Kp,
        lna_Ki,
        load_Kp,
        load_Ki,
        lna_cooling_enabled,
        load_cooling_enabled,
    ):
        self.lna_setpoint = lna_setpoint
        self.load_setpoint = load_setpoint
        self.lna_enabled = lna_enabled
        self.load_enabled = load_enabled
        self.lna_Kp = lna_Kp
        self.lna_Ki = lna_Ki
        self.load_Kp = load_Kp
        self.load_Ki = load_Ki
        # Asymmetric-clamp safety setting per channel: False forbids
        # negative (cooling) drive, clamping it to [0, +clamp]. Seeded
        # from the firmware-published value (default True) so the UI
        # never disagrees with what the firmware is enforcing.
        self.lna_cooling_enabled = lna_cooling_enabled
        self.load_cooling_enabled = load_cooling_enabled
        self.clamp_idx = CLAMPS.index(0.2)  # firmware default clamp
        self.last_message = ""


class _History:
    """Append-only buffer of firmware readings for the `p` plot.

    One sample per loop tick: the elapsed seconds since the buffer was
    created, plus each :data:`PLOT_CHANNELS` channel's
    :data:`PLOT_FIELDS` values. A field that is missing or non-numeric
    in the snapshot is stored as ``float("nan")`` so a sensor dropout
    becomes a gap in the line rather than a spurious zero or a crash.

    Memory is unbounded by design — a multi-hour bring-up at the ~5 Hz
    refresh is still only ~100k floats — so there is no ring buffer.
    """

    def __init__(self):
        self.t = []
        # {channel: {field: [values]}}
        self.values = {
            ch: {field: [] for field in PLOT_FIELDS} for ch in PLOT_CHANNELS
        }

    def record(self, snapshot, *, now):
        """Append one sample read from ``snapshot`` at monotonic ``now``.

        ``now`` is passed in (rather than read here) so the loop's
        single ``time.monotonic()`` call is reused and the elapsed-time
        axis is consistent with the render cadence.
        """
        if not self.t:
            self._t0 = now
        self.t.append(now - self._t0)
        snap = snapshot.get()
        for ch in PLOT_CHANNELS:
            data = snap.get(ch) or {}
            for field in PLOT_FIELDS:
                v = data.get(field)
                ok = isinstance(v, (int, float)) and not isinstance(v, bool)
                self.values[ch][field].append(float(v) if ok else float("nan"))

    def __len__(self):
        return len(self.t)


def _plot_history(history, *, outdir=".", timestamp=None):
    """Render ``history`` to ``tempctrl_<timestamp>.png`` under ``outdir``.

    One row per channel: ``T_now`` (solid) and ``T_target`` (dashed) on
    the left axis, ``drive_level`` on a twin right axis. Returns the
    written path, or ``None`` if there is nothing to plot yet (so the
    caller can report "no data" instead of writing an empty figure).

    ``timestamp`` is injectable for tests; production passes ``None`` and
    gets a wall-clock ``%Y%m%d_%H%M%S`` stamp so repeated presses don't
    clobber each other.
    """
    if len(history) == 0:
        return None
    if timestamp is None:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
    path = Path(outdir) / f"tempctrl_{timestamp}.png"

    fig, axes = plt.subplots(
        len(PLOT_CHANNELS), 1, sharex=True, figsize=(10, 7)
    )
    t = history.t
    for ax, ch in zip(axes, PLOT_CHANNELS):
        vals = history.values[ch]
        ax.plot(t, vals["T_now"], color="C0", label="T_now")
        ax.plot(
            t, vals["T_target"], color="C1", linestyle="--", label="T_target"
        )
        ax.set_ylabel("temperature (deg C)")
        ax.set_title(ch)
        ax.grid(True, alpha=0.3)

        drive_ax = ax.twinx()
        drive_ax.plot(
            t, vals["drive_level"], color="C3", alpha=0.7, label="drive_level"
        )
        drive_ax.set_ylabel("drive_level")

        lines = ax.get_lines() + drive_ax.get_lines()
        ax.legend(lines, [ln.get_label() for ln in lines], loc="best")

    axes[-1].set_xlabel("elapsed time (s)")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def _snap(snapshot, name):
    return snapshot.get().get(name)


def _seed_state(
    snapshot,
    *,
    timeout_s=SEED_TIMEOUT_S,
    poll_interval_s=PICO_PUBLISH_INTERVAL_S,
):
    """Block until firmware has published ``T_target`` on both
    ``tempctrl_lna`` and ``tempctrl_load``, then build a starting
    :class:`_State` from those values.

    No hardcoded setpoint fallback — the pico's own ``T_target``
    (firmware default 30 deg C until reconfigured) is the single
    source of truth, so the UI can never disagree with what the
    firmware is actually driving. ``enabled`` likewise comes from the
    pico; missing ``Kp`` / ``Ki`` fall back to the firmware-side
    defaults (``DEFAULT_KP`` / ``DEFAULT_KI``).

    Raises
    ------
    SystemExit
        ``T_target`` did not appear on one or both streams within
        ``timeout_s``. ``require_pico`` passed first, so the proxy
        heartbeat is live — that means the pico is registered but the
        firmware tempctrl publisher hasn't pushed a status frame yet.
        Usually a misflashed pico or a stuck producer thread.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        lna = _snap(snapshot, "tempctrl_lna") or {}
        load = _snap(snapshot, "tempctrl_load") or {}
        if (
            lna.get("T_target") is not None
            and load.get("T_target") is not None
        ):
            break
        if time.monotonic() >= deadline:
            missing = [
                name
                for name, d in (
                    ("tempctrl_lna", lna),
                    ("tempctrl_load", load),
                )
                if d.get("T_target") is None
            ]
            raise SystemExit(
                f"ERROR: tempctrl pico is registered but did not publish "
                f"T_target on {missing} within {timeout_s:.1f}s. "
                f"Check pico-manager logs."
            )
        time.sleep(poll_interval_s)

    def _f(d, k, default):
        v = d.get(k)
        return (
            float(v)
            if isinstance(v, (int, float)) and not isinstance(v, bool)
            else default
        )

    def _b(d, k, default):
        v = d.get(k)
        return v if isinstance(v, bool) else default

    return _State(
        lna_setpoint=float(lna["T_target"]),
        load_setpoint=float(load["T_target"]),
        lna_enabled=bool(lna.get("enabled") or False),
        load_enabled=bool(load.get("enabled") or False),
        lna_Kp=_f(lna, "Kp", DEFAULT_KP),
        lna_Ki=_f(lna, "Ki", DEFAULT_KI),
        load_Kp=_f(load, "Kp", DEFAULT_KP),
        load_Ki=_f(load, "Ki", DEFAULT_KI),
        # Firmware default is True (cooling permitted); a missing field
        # means the firmware predates the setting, so default True too.
        lna_cooling_enabled=_b(lna, "cooling_enabled", True),
        load_cooling_enabled=_b(load, "cooling_enabled", True),
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


def _push_cooling(proxy, state):
    """Push the per-channel cooling-enable (asymmetric-clamp) flag.

    Both channels are pushed together to match the enable/gain pushes —
    one round-trip per keypress, and the readout shows the
    firmware-reported value back so the operator confirms it took. With
    ``cooling_enabled=False`` the firmware forbids negative drive, the
    guard against a Peltier that heats (rather than cools) when it can't
    dissipate its hot-side load.
    """
    state.last_message = _send(
        proxy,
        "set_cooling_enabled",
        LNA=state.lna_cooling_enabled,
        LOAD=state.load_cooling_enabled,
    )


def _push_gains(proxy, state):
    """Push all four gains. Mirrors `apply_settings`' partial-kwarg
    pattern: the LNA/LOAD knobs are independent, but bundling the push
    means one round-trip per keypress and matches what the operator
    sees in the readout.
    """
    state.last_message = _send(
        proxy,
        "set_gains",
        LNA_Kp=state.lna_Kp,
        LNA_Ki=state.lna_Ki,
        LOAD_Kp=state.load_Kp,
        LOAD_Ki=state.load_Ki,
    )


def _fmt(value, fmt):
    if not isinstance(value, (int, float)):
        return "    --"
    return format(value, fmt)


def _armed(channel):
    """Derive whether firmware drive is engaged for ``channel``.

    Mirrors the firmware gate (picohost >= 3.4.0): drive engages iff
    ``enabled && !int_disabled && !stall_tripped && !watchdog_tripped``.
    Since ``enabled`` is now host intent only (firmware never clears
    it on trip), this derived flag is what the operator actually wants
    to read off the panel to confirm the channel is driving.
    """
    if not channel:
        return None
    return bool(
        channel.get("enabled")
        and not channel.get("int_disabled")
        and not channel.get("stall_tripped")
        and not channel.get("watchdog_tripped")
    )


def _render(screen, snapshot, state):
    lna = _snap(snapshot, "tempctrl_lna") or {}
    load = _snap(snapshot, "tempctrl_load") or {}
    screen.clear()
    screen.addstr(0, 0, "=== tempctrl manual ===")
    screen.addstr(
        1,
        0,
        "channel  T_now    T_target  drive   clamp   cooling  "
        "enabled  armed  status",
    )
    screen.addstr(
        2,
        0,
        "LNA      "
        f"{_fmt(lna.get('T_now'), '6.2f')}  "
        f"{_fmt(lna.get('T_target'), '6.2f')}    "
        f"{_fmt(lna.get('drive_level'), '6.2f')}  "
        f"{_fmt(lna.get('clamp'), '6.2f')}  "
        f"{str(lna.get('cooling_enabled')):>7}  "
        f"{str(lna.get('enabled')):>7}  "
        f"{str(_armed(lna)):>5}  {lna.get('status')!r}",
    )
    screen.addstr(
        3,
        0,
        "LOAD     "
        f"{_fmt(load.get('T_now'), '6.2f')}  "
        f"{_fmt(load.get('T_target'), '6.2f')}    "
        f"{_fmt(load.get('drive_level'), '6.2f')}  "
        f"{_fmt(load.get('clamp'), '6.2f')}  "
        f"{str(load.get('cooling_enabled')):>7}  "
        f"{str(load.get('enabled')):>7}  "
        f"{str(_armed(load)):>5}  {load.get('status')!r}",
    )
    # PI controller readout — Kp/Ki are config-set (last-write-wins
    # cached in PicoPeltier._last_gains), `integral` is the firmware
    # accumulator. `stall_tripped` is per-channel and sticky; clear it
    # with an `l on` / `o on` (or `r`) rising-edge ack.
    screen.addstr(5, 0, "channel   Kp     Ki      integral  stall_tripped")
    screen.addstr(
        6,
        0,
        "LNA      "
        f"{_fmt(lna.get('Kp'), '6.3f')}  "
        f"{_fmt(lna.get('Ki'), '6.3f')}  "
        f"{_fmt(lna.get('integral'), '8.3f')}  "
        f"{str(lna.get('stall_tripped')):>13}",
    )
    screen.addstr(
        7,
        0,
        "LOAD     "
        f"{_fmt(load.get('Kp'), '6.3f')}  "
        f"{_fmt(load.get('Ki'), '6.3f')}  "
        f"{_fmt(load.get('integral'), '8.3f')}  "
        f"{str(load.get('stall_tripped')):>13}",
    )
    # watchdog_tripped is duplicated across both streams (see
    # _PELTIER_SCHEMA) — read from either; LNA is fine.
    screen.addstr(
        9, 0, f"watchdog_tripped: {bool(lna.get('watchdog_tripped'))}"
    )
    screen.addstr(
        10,
        0,
        f"client setpoints: LNA={state.lna_setpoint:.2f} "
        f"LOAD={state.load_setpoint:.2f}  "
        f"clamp={CLAMPS[state.clamp_idx]:.2f}",
    )
    screen.addstr(
        11,
        0,
        f"client gains: LNA(Kp={state.lna_Kp:.3f}, Ki={state.lna_Ki:.3f})  "
        f"LOAD(Kp={state.load_Kp:.3f}, Ki={state.load_Ki:.3f})",
    )
    screen.addstr(13, 0, "l/L enable LNA on/off       o/O enable LOAD on/off")
    screen.addstr(14, 0, "n/N LNA cooling on/off      m/M LOAD cooling on/off")
    screen.addstr(15, 0, "+/- LNA setpoint  ][ LOAD setpoint  c/C clamp up/down")
    screen.addstr(16, 0, "g/G LNA Kp  h/H LOAD Kp  i/I LNA Ki  k/K LOAD Ki")
    screen.addstr(17, 0, "z/Z reset LNA/LOAD integral")
    screen.addstr(
        18, 0, "w push 5s watchdog   r re-enable both   p plot PNG   q quit"
    )
    if state.last_message:
        screen.addstr(20, 0, f"> {state.last_message}"[: curses.COLS - 1])
    screen.refresh()


def _handle_key(ch, proxy, state, history=None, outdir="."):
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
    elif ch == ord("n"):
        state.lna_cooling_enabled = True
        _push_cooling(proxy, state)
    elif ch == ord("N"):
        state.lna_cooling_enabled = False
        _push_cooling(proxy, state)
    elif ch == ord("m"):
        state.load_cooling_enabled = True
        _push_cooling(proxy, state)
    elif ch == ord("M"):
        state.load_cooling_enabled = False
        _push_cooling(proxy, state)
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
        state.clamp_idx = min(state.clamp_idx + 1, len(CLAMPS) - 1)
        _push_clamp(proxy, state)
    elif ch == ord("C"):
        state.clamp_idx = max(state.clamp_idx - 1, 0)
        _push_clamp(proxy, state)
    elif ch == ord("g"):
        state.lna_Kp = max(0.0, state.lna_Kp + KP_STEP)
        _push_gains(proxy, state)
    elif ch == ord("G"):
        state.lna_Kp = max(0.0, state.lna_Kp - KP_STEP)
        _push_gains(proxy, state)
    elif ch == ord("h"):
        state.load_Kp = max(0.0, state.load_Kp + KP_STEP)
        _push_gains(proxy, state)
    elif ch == ord("H"):
        state.load_Kp = max(0.0, state.load_Kp - KP_STEP)
        _push_gains(proxy, state)
    elif ch == ord("i"):
        state.lna_Ki = max(0.0, state.lna_Ki + KI_STEP)
        _push_gains(proxy, state)
    elif ch == ord("I"):
        state.lna_Ki = max(0.0, state.lna_Ki - KI_STEP)
        _push_gains(proxy, state)
    elif ch == ord("k"):
        state.load_Ki = max(0.0, state.load_Ki + KI_STEP)
        _push_gains(proxy, state)
    elif ch == ord("K"):
        state.load_Ki = max(0.0, state.load_Ki - KI_STEP)
        _push_gains(proxy, state)
    elif ch == ord("z"):
        state.last_message = _send(proxy, "reset_integral", LNA=True)
    elif ch == ord("Z"):
        state.last_message = _send(proxy, "reset_integral", LOAD=True)
    elif ch == ord("w"):
        state.last_message = _send(
            proxy, "set_watchdog_timeout", timeout_ms=WATCHDOG_PROBE_MS
        )
    elif ch == ord("r"):
        state.lna_enabled = True
        state.load_enabled = True
        _push_enables(proxy, state)
        _push_temperatures(proxy, state)
    elif ch == ord("p"):
        path = _plot_history(history, outdir=outdir) if history else None
        state.last_message = f"wrote {path}" if path else "no data to plot yet"
    return True


def _curses_main(screen, transport, args):
    curses.noecho()
    screen.timeout(int(args.interval * 1000))
    proxy = PicoProxy("tempctrl", transport, source="tempctrl_manual")
    require_pico(proxy)
    snapshot = MetadataSnapshotReader(transport)
    state = _seed_state(snapshot)
    history = _History()
    while True:
        # Record every tick (including timeouts) so the `p` plot is a
        # dense trace independent of how often the operator types.
        history.record(snapshot, now=time.monotonic())
        _render(screen, snapshot, state)
        ch = screen.getch()
        if ch == -1:
            # timeout — re-render so the readout stays live
            continue
        if not _handle_key(ch, proxy, state, history=history):
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
    with run_tag.session(transport, "tempctrl_manual"):
        curses.wrapper(_curses_main, transport, args)


if __name__ == "__main__":
    main()
