"""Interactive tempctrl (LOAD heater) bring-up tool.

Curses UI showing a live readout for the ``tempctrl_load`` stream (the
sole tempctrl channel — the LNA/Peltier channel and its PI control were
removed) and single-key commands that exercise every panda-side setter
on :class:`picohost.base.PicoTempCtrl`. Operator confirms that the
temperature actually moves toward the setpoint and that the on/off
hysteresis control cycles the FET heater as expected.

Controls:
  o / O    enable LOAD on / off
  ] / [    LOAD setpoint +/- 0.5 deg C
  } / {    LOAD hysteresis +/- 0.1 deg C
  u / U    LOAD installed yes / no (hardware descope, see below)
  r        re-enable at the last setpoint/hysteresis
  p        write a temperature-vs-time PNG of the session so far
  q        quit

Every loop tick records the firmware ``T_now`` / ``T_target`` /
``drive_level`` into an in-memory history. Pressing ``p`` renders that
history to ``tempctrl_<timestamp>.png`` in the current directory —
``T_now`` and ``T_target`` on the left axis and ``drive_level`` on a
twin right axis. The plot uses the Agg backend so it works headless /
over SSH to the panda; the written path is reported in the footer
rather than printed (curses owns the screen). ``p`` may be pressed
repeatedly; each press writes a fresh timestamped file.

Every command goes through :class:`picohost.proxy.PicoProxy` so
behavior mirrors the production tempctrl loop path. Setpoint and
hysteresis values are tracked client-side so the bump keys can step
them without round-tripping the firmware to read back the current
value.

Trip clearing (picohost >= 3.4.0): ``enabled`` is host intent only —
firmware never mutates it. Drive engages iff ``enabled &&
!sensor_tripped && !stall_tripped && !runaway_tripped &&
!watchdog_tripped`` (shown as the ``armed`` column in the readout).
All sticky trips are cleared by an explicit ``LOAD_enable=true`` rising
edge from the host. From this UI that means ``o`` (LOAD on) or ``r``
(re-enable) double as the operator's trip-clear ack — bare keepalives
refresh the watchdog timer but no longer clear the trip flags.

Since the tempctrl status redesign, ``status`` reports data validity
only: the channel can read ``armed=False`` with ``status='update'``,
meaning the sensor data is fine but a sticky trip is gating drive —
check the ``trips`` column for which one. ``T_now`` reads ``--``
(null) exactly when the current sample is untrustworthy; ``voltage``
stays live then (approx 3.3 V says open thermistor, approx 0 V short).

Hardware descope (``installed`` flag): a channel marked not installed
is never sampled or driven and publishes no Redis stream — its readout
row shows all ``--``. On startup, a stream that stays silent through
the seed window is taken as a descoped channel; the UI still comes up,
seeded from firmware defaults. ``u`` re-installs the channel; expect
its stream to start publishing within a tick. ``U`` descopes it again.
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
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport,
    require_pico,
)
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)

SETPOINT_STEP_C = 0.5
HYSTERESIS_STEP_C = 0.1
HYSTERESIS_MIN_C = 0.05  # floor so the bump key can't zero out hysteresis
DEFAULT_T_TARGET_C = 30.0  # firmware default T_target (load_heater_init)
DEFAULT_HYSTERESIS_C = 0.5  # picohost PicoTempCtrl.set_temperature default
STREAM = "tempctrl_load"
# picohost STATUS_CADENCE_MS = 200; poll at the same cadence so we
# wake on the next publish without busy-spinning.
PICO_PUBLISH_INTERVAL_S = 0.2
# Headroom over the 200 ms cadence: a healthy pico publishes within
# one tick; 5 s of slack absorbs a slow PicoManager restart without
# masking a stuck producer.
SEED_TIMEOUT_S = 5.0

# Firmware fields buffered every loop tick for the history plot.
PLOT_FIELDS = ("T_now", "T_target", "drive_level")


class _State:
    """Operator-facing state the script tracks locally.

    Firmware is the source of truth for ``T_now`` / ``drive_level`` /
    ``watchdog_tripped`` (read from snapshot). The local copies of the
    setpoint, hysteresis, enable flag, and installed flag are only
    used so the bump keys can step them — they're seeded from the
    snapshot on startup if available, and re-pushed on every change so
    a missed command can't leave the firmware and the UI disagreeing.
    """

    def __init__(
        self,
        setpoint,
        hysteresis,
        enabled,
        installed=True,
    ):
        self.setpoint = setpoint
        self.hysteresis = hysteresis
        self.enabled = enabled
        # Hardware-descope flag (see module docstring): an uninstalled
        # channel publishes no stream, so it's seeded from stream
        # presence at startup and toggled by u/U.
        self.installed = installed
        self.last_message = ""


class _History:
    """Append-only buffer of firmware readings for the `p` plot.

    One sample per loop tick: the elapsed seconds since the buffer was
    created, plus each :data:`PLOT_FIELDS` value. A field that is
    missing or non-numeric in the snapshot is stored as
    ``float("nan")`` so a sensor dropout becomes a gap in the line
    rather than a spurious zero or a crash.

    Memory is unbounded by design — a multi-hour bring-up at the ~5 Hz
    refresh is still only ~tens of thousands of floats — so there is
    no ring buffer.
    """

    def __init__(self):
        self.t = []
        self.values = {field: [] for field in PLOT_FIELDS}

    def record(self, snapshot, *, now):
        """Append one sample read from ``snapshot`` at monotonic ``now``.

        ``now`` is passed in (rather than read here) so the loop's
        single ``time.monotonic()`` call is reused and the elapsed-time
        axis is consistent with the render cadence.
        """
        if not self.t:
            self._t0 = now
        self.t.append(now - self._t0)
        data = snapshot.get().get(STREAM) or {}
        for field in PLOT_FIELDS:
            v = data.get(field)
            ok = isinstance(v, (int, float)) and not isinstance(v, bool)
            self.values[field].append(float(v) if ok else float("nan"))

    def __len__(self):
        return len(self.t)


def _plot_history(history, *, outdir=".", timestamp=None):
    """Render ``history`` to ``tempctrl_<timestamp>.png`` under ``outdir``.

    ``T_now`` (solid) and ``T_target`` (dashed) on the left axis,
    ``drive_level`` on a twin right axis. Returns the written path, or
    ``None`` if there is nothing to plot yet (so the caller can report
    "no data" instead of writing an empty figure).

    ``timestamp`` is injectable for tests; production passes ``None`` and
    gets a wall-clock ``%Y%m%d_%H%M%S`` stamp so repeated presses don't
    clobber each other.
    """
    if len(history) == 0:
        return None
    if timestamp is None:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
    path = Path(outdir) / f"tempctrl_{timestamp}.png"

    fig, ax = plt.subplots(figsize=(10, 4))
    t = history.t
    vals = history.values
    ax.plot(t, vals["T_now"], color="C0", label="T_now")
    ax.plot(t, vals["T_target"], color="C1", linestyle="--", label="T_target")
    ax.set_ylabel("temperature (deg C)")
    ax.set_title(STREAM)
    ax.grid(True, alpha=0.3)

    drive_ax = ax.twinx()
    drive_ax.plot(
        t, vals["drive_level"], color="C3", alpha=0.7, label="drive_level"
    )
    drive_ax.set_ylabel("drive_level")

    lines = ax.get_lines() + drive_ax.get_lines()
    ax.legend(lines, [ln.get_label() for ln in lines], loc="best")
    ax.set_xlabel("elapsed time (s)")

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
    """Block until firmware has published ``T_target`` on
    ``tempctrl_load`` (or the seed window closes), then build a
    starting :class:`_State`.

    No hardcoded setpoint fallback for a live channel — the pico's own
    ``T_target`` (firmware default 30 deg C until reconfigured) is the
    single source of truth, so the UI can never disagree with what the
    firmware is actually driving. ``enabled`` likewise comes from the
    pico; a missing ``hysteresis`` falls back to
    :data:`DEFAULT_HYSTERESIS_C`.

    A stream still silent when the window closes is taken as a
    descoped channel (firmware ``installed=false`` publishes nothing)
    and marked not-installed, seeded from firmware defaults so a later
    re-install (``u``) starts from sane values. This does not raise:
    starting the UI against a deliberately descoped channel (to
    inspect it, or to re-install it with ``u``) is a supported flow,
    not an error. ``require_pico`` already confirmed the device
    heartbeat is live before this is called, so a silent stream here
    most likely reflects ``installed: false`` rather than a stuck
    producer — if it's the latter (a misflashed pico or a hung
    publisher thread), the readout row will keep showing all ``--``
    even after ``u``, which is the operator's cue to check
    pico-manager logs.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        load = _snap(snapshot, STREAM) or {}
        live = load.get("T_target") is not None
        if live:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_interval_s)

    def _f(d, k, default):
        v = d.get(k)
        return (
            float(v)
            if isinstance(v, (int, float)) and not isinstance(v, bool)
            else default
        )

    return _State(
        setpoint=(float(load["T_target"]) if live else DEFAULT_T_TARGET_C),
        hysteresis=_f(load, "hysteresis", DEFAULT_HYSTERESIS_C),
        enabled=bool(load.get("enabled") or False),
        installed=live,
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


def _push_enable(proxy, state):
    state.last_message = _send(proxy, "set_enable", LOAD=state.enabled)


def _push_temperature(proxy, state):
    state.last_message = _send(
        proxy,
        "set_temperature",
        T_LOAD=state.setpoint,
        LOAD_hyst=state.hysteresis,
    )


def _push_installed(proxy, state):
    """Push the ``installed`` (hardware-descope) flag.

    Re-installing (``u``) tells the firmware to resume sampling the
    channel; its stream starts publishing within a tick. Descoping
    (``U``) stops sampling, forces drive off, and the stream
    disappears; sticky trip latches survive (clear them with the
    enable ack as usual).
    """
    state.last_message = _send(proxy, "set_installed", LOAD=state.installed)


def _fmt(value, fmt):
    if not isinstance(value, (int, float)):
        return "    --"
    return format(value, fmt)


_TRIP_FLAGS = ("sensor_tripped", "stall_tripped", "runaway_tripped")


def _armed(channel):
    """Derive whether firmware drive is engaged.

    Mirrors the firmware gate (load_heater_drive_allowed): drive
    engages iff ``enabled && !sensor_tripped && !stall_tripped &&
    !runaway_tripped && !watchdog_tripped``. Since ``enabled`` is host
    intent only (firmware never clears it on trip), this derived flag
    is what the operator actually wants to read off the panel to
    confirm the channel is driving.
    """
    if not channel:
        return None
    return bool(
        channel.get("enabled")
        and not any(channel.get(flag) for flag in _TRIP_FLAGS)
        and not channel.get("watchdog_tripped")
    )


def _trips(channel):
    """Compact list of active sticky trips, or ``-`` when clear.

    The interesting read is ``armed=False`` with ``status='update'``:
    the data is healthy but a latch is gating drive — this column says
    which one (sensor = rate-guard garbage burst, stall = drive moved
    nothing, runaway = temperature moved against the drive: check the
    wiring before re-enabling).
    """
    if not channel:
        return "--"
    active = [
        flag.removesuffix("_tripped")
        for flag in _TRIP_FLAGS
        if channel.get(flag)
    ]
    return ",".join(active) if active else "-"


def _render(screen, snapshot, state):
    load = _snap(snapshot, STREAM) or {}
    screen.clear()
    screen.addstr(0, 0, "=== tempctrl manual (LOAD) ===")
    screen.addstr(
        1,
        0,
        "channel  T_now    T_target  drive   hyst    enabled  armed  status",
    )
    screen.addstr(
        2,
        0,
        "LOAD     "
        f"{_fmt(load.get('T_now'), '6.2f')}  "
        f"{_fmt(load.get('T_target'), '6.2f')}    "
        f"{_fmt(load.get('drive_level'), '6.2f')}  "
        f"{_fmt(load.get('hysteresis'), '6.2f')}  "
        f"{str(load.get('enabled')):>7}  "
        f"{str(_armed(load)):>5}  {load.get('status')!r}",
    )
    screen.addstr(4, 0, f"trips: {_trips(load)}")
    screen.addstr(
        5, 0, f"watchdog_tripped: {bool(load.get('watchdog_tripped'))}"
    )
    screen.addstr(
        6,
        0,
        f"client setpoint: {state.setpoint:.2f}  "
        f"hysteresis: {state.hysteresis:.2f}",
    )
    # An uninstalled channel publishes no stream, so its readout row
    # above shows all `--`; this line says whether that's a descope
    # (installed=False) or a fault.
    screen.addstr(7, 0, f"client installed: {state.installed}")
    screen.addstr(9, 0, "o/O enable LOAD on/off      ][ LOAD setpoint +/-")
    screen.addstr(10, 0, "}{ LOAD hysteresis +/-      u/U LOAD installed")
    screen.addstr(11, 0, "r re-enable   p plot PNG   q quit")
    if state.last_message:
        screen.addstr(13, 0, f"> {state.last_message}"[: curses.COLS - 1])
    screen.refresh()


def _handle_key(ch, proxy, state, history=None, outdir="."):
    if ch in (ord("q"), 27):  # q or ESC
        return False
    if ch == ord("o"):
        state.enabled = True
        _push_enable(proxy, state)
    elif ch == ord("O"):
        state.enabled = False
        _push_enable(proxy, state)
    elif ch == ord("]"):
        state.setpoint += SETPOINT_STEP_C
        _push_temperature(proxy, state)
    elif ch == ord("["):
        state.setpoint -= SETPOINT_STEP_C
        _push_temperature(proxy, state)
    elif ch == ord("}"):
        state.hysteresis += HYSTERESIS_STEP_C
        _push_temperature(proxy, state)
    elif ch == ord("{"):
        state.hysteresis = max(
            HYSTERESIS_MIN_C, state.hysteresis - HYSTERESIS_STEP_C
        )
        _push_temperature(proxy, state)
    elif ch == ord("u"):
        state.installed = True
        _push_installed(proxy, state)
    elif ch == ord("U"):
        state.installed = False
        _push_installed(proxy, state)
    elif ch == ord("r"):
        state.enabled = True
        _push_enable(proxy, state)
        _push_temperature(proxy, state)
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
        description="Interactive tempctrl bring-up: drive the LOAD "
        "heater's setpoint and hysteresis."
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient",
    )
    add_redis_args(parser)
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Refresh interval in seconds (default: 0.5).",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    with run_tag.session(transport, "tempctrl_manual"):
        curses.wrapper(_curses_main, transport, args)


if __name__ == "__main__":
    main()
