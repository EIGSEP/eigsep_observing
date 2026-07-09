"""Field zero: verify the pot tracks, set the operational az/el zero, and
re-pin the pot calibration intercept with the box in its hanging pose.

Self-contained active driver (see scripts/CLAUDE.md): claims run_tag,
talks to hardware only via PicoProxy, requires pico-manager. Performs
pot-slip pre-check, interactive jog-to-zero, motor-origin reset, and
pot intercept re-pin.
"""

import curses
import logging
from argparse import ArgumentParser

from picohost.buses import PotCalStore
from picohost.proxy import PicoProxy

from eigsep_redis import MetadataSnapshotReader

from eigsep_observing import (
    MotorClient,
    MotorLimitError,
    MotorZeroer,
    run_tag,
)
from eigsep_observing.home_ref import publish_home_ref
from eigsep_observing._scripts_util import (
    add_redis_args,
    build_transport,
    require_pico,
)
from eigsep_observing.utils import configure_eig_logger

configure_eig_logger(level=logging.INFO, console=False)
logger = logging.getLogger(__name__)

_POT_DOWN_MSG = "potmon not publishing pot_az_voltage; cannot run slip check"


def slip_verdict(expected_dv, measured_dv, *, warn=0.05, fail=0.10):
    """Classify how well the pot tracked a known motor move.

    expected_dv / measured_dv are pot voltage swings (V) for the same
    commanded move. Only a shortfall (pot under-traveled) indicates
    slip: "warn" at ``warn``, "fail" at ``fail`` fractional shortfall.
    An overshoot of at least ``warn`` returns "overshoot" — the pot
    swung MORE than the stored slope predicts, which a slipping
    coupling cannot do; it means the stored slope is stale, and it
    must never block zeroing (the zero is slope-independent: home_ref
    stores raw v0 and the re-pin keeps the slope). A non-positive
    expected swing is unusable -> "fail".
    """
    if expected_dv <= 0:
        return "fail"
    frac_short = (expected_dv - measured_dv) / expected_dv
    if frac_short >= fail:
        return "fail"
    if frac_short >= warn:
        return "warn"
    if frac_short <= -warn:
        return "overshoot"
    return "ok"


def _prompt_override(expected_dv, measured_dv):
    """Ask the operator whether to zero despite a failed slip check.

    Field reality: a slipping/odd pot coupling often cannot be fixed
    on the rig, and the zero itself is slope-independent, so the
    operator gets the final say — with the numbers in hand, before
    the probe move has to be repeated. Returns True only on an
    explicit y/yes; EOF (non-interactive stdin) is No.
    """
    print(
        f"POT SLIP DETECTED: measured {measured_dv:.3f} V vs "
        f"{expected_dv:.3f} V expected for the probe move."
    )
    try:
        answer = input("Proceed with zeroing anyway? [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in ("y", "yes")


def rezero_pot(transport, pot_proxy, v0):
    """Re-pin the pot intercept at home voltage v0, keeping the stored slope.

    angle = m*V + b, with b chosen so angle(v0) = 0  ->  b = -m*v0.
    Persists (BGSAVE) and pushes the new cal to the live pico.
    Returns (m, b).
    """
    store = PotCalStore(transport)
    cal = store.get()
    if not cal or "pot_az" not in cal:
        raise RuntimeError(
            "No stored pot calibration; run "
            "calibrate-pot --mode azimuth first."
        )
    m = float(cal["pot_az"][0])
    b = -m * float(v0)
    cal["pot_az"] = [m, b]
    cal.setdefault("metadata", {})["mode"] = "field_zero_rezero"
    store.upload(cal)
    transport.r.bgsave()
    try:
        pot_proxy.send_command("set_calibration", pot_az_params=[m, b])
    except (TimeoutError, RuntimeError):
        logger.warning(
            "Live push of pot cal failed; cal is stored in Redis and "
            "will apply on the next PicoManager restart."
        )
    return m, b


def _pot_voltage(snapshot):
    snap = snapshot.get().get("potmon") or {}
    return snap.get("pot_az_voltage")


def run_slip_check(motor_client, snapshot, slope_m, move_deg=30.0):
    """Probe pot tracking with a there-and-back motor move.

    Reads pot V, jogs +move_deg, reads pot V, jogs -move_deg (back to
    start). expected_dv = move_deg / |slope_m| (angle = m*V + b =>
    dV = dAngle / m). Returns (verdict, expected_dv, measured_dv).
    A probe jog denied by the travel guard / sensor fence raises
    SystemExit with recovery instructions (the rig may be left
    off-position mid-probe).
    """
    v_before = _pot_voltage(snapshot)
    if v_before is None:
        raise SystemExit(_POT_DOWN_MSG)
    try:
        motor_client.jog_az(move_deg)
        v_after = _pot_voltage(snapshot)
        motor_client.jog_az(-move_deg)
    except MotorLimitError as exc:
        raise SystemExit(
            f"Slip-check probe move denied by travel limit: {exc} "
            "The rig may be left off-position; jog back with "
            "motor_manual, or re-run with --override-limits "
            "(recovery) or --no-slip-check (skip the probe)."
        ) from exc
    if v_after is None:
        raise SystemExit(_POT_DOWN_MSG)
    expected_dv = abs(move_deg) / abs(slope_m)
    measured_dv = abs(v_after - v_before)
    return slip_verdict(expected_dv, measured_dv), expected_dv, measured_dv


def _write_home_ref(transport, snapshot, v0):
    """Write the home reference K/V after a confirmed zero.

    Records the pot voltage at the zeroed position (slope-independent
    az reference) and the current IMU elevation (None when uncalibrated).
    """
    el = (snapshot.get().get("imu_el") or {}).get("el_deg")
    publish_home_ref(transport, pot_az_voltage_v0=v0, imu_el_deg_home=el)


def _render(screen, zeroer, snapshot, deg):
    az, el, connected = zeroer.status_text()
    pot = snapshot.get().get("potmon") or {}
    imu = snapshot.get().get("imu_az") or {}
    screen.clear()
    screen.addstr(0, 0, "=== Field Zero ===")
    screen.addstr(2, 0, f"Jog step: {deg:.1f} deg")
    if connected:
        screen.addstr(3, 0, f"AZ: {az}   EL: {el}")
    else:
        screen.addstr(3, 0, "MOTOR DISCONNECTED (waiting)")
    screen.addstr(
        4,
        0,
        f"pot: {pot.get('pot_az_angle')} deg ({pot.get('pot_az_voltage')} V)",
    )
    screen.addstr(
        5,
        0,
        f"imu_az: yaw={imu.get('yaw')} az={imu.get('az_deg')} "
        f"el={imu.get('el_deg')}",
    )
    screen.addstr(
        7,
        0,
        "u/d EL | l/r AZ | +/- step | Enter=zero(confirm) | q=quit",
    )
    if zeroer.pending_zero:
        screen.addstr(
            9, 0, ">>> ZERO HERE? 'y' confirm, any other key cancels <<<"
        )
    if zeroer.notice:
        width = screen.getmaxyx()[1]
        screen.addstr(11, 0, f"! {zeroer.notice}"[: width - 1])
    screen.refresh()


def _curses_main(screen, zeroer, snapshot, pot_proxy, transport, deg):
    curses.noecho()
    screen.timeout(100)
    try:
        while True:
            _render(screen, zeroer, snapshot, deg)
            deg, should_exit, zeroed = zeroer.handle_key(screen.getch(), deg)
            if should_exit:
                if zeroed:
                    v0 = _pot_voltage(snapshot)
                    if v0 is None:
                        logger.warning(
                            "Motor origin reset, but pot re-pin SKIPPED: "
                            "potmon not publishing pot_az_voltage. Re-pin "
                            "once potmon is back (calibrate-pot --mode "
                            "rezero)."
                        )
                    else:
                        m, b = rezero_pot(transport, pot_proxy, v0)
                        _write_home_ref(transport, snapshot, v0)
                        logger.info(
                            "Zeroed: motor origin reset; pot re-pinned "
                            "m=%.3f b=%.3f at v0=%.4f",
                            m,
                            b,
                            v0,
                        )
                break
    finally:
        zeroer.cancel_home()
        zeroer.halt()


def _parse_args():
    p = ArgumentParser(
        description="Field zero: verify pot, set az/el zero, re-pin cal"
    )
    p.add_argument("--dummy", action="store_true")
    add_redis_args(p)
    p.add_argument(
        "--move-deg",
        type=float,
        default=30.0,
        help="Slip-check probe move in degrees (default: 30)",
    )
    p.add_argument(
        "--deg",
        type=float,
        default=1.0,
        help="Initial jog step size in degrees (default: 1.0)",
    )
    p.add_argument(
        "--no-slip-check",
        action="store_true",
        help="Skip the pot-slip pre-check (step 1).",
    )
    p.add_argument(
        "--override-limits",
        action="store_true",
        help=(
            "Disable travel limits for this session "
            "(recovery from out-of-window)."
        ),
    )
    return p.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    motor_proxy = PicoProxy("motor", transport, source="field_zero")
    pot_proxy = PicoProxy("potmon", transport, source="field_zero")
    require_pico(motor_proxy)
    require_pico(pot_proxy)
    snapshot = MetadataSnapshotReader(transport)

    cal = PotCalStore(transport).get()
    if not cal or "pot_az" not in cal:
        raise SystemExit(
            "No pot calibration; run calibrate-pot --mode azimuth first."
        )
    slope_m = float(cal["pot_az"][0])

    with run_tag.session(transport, "field_zero"):
        if args.override_limits:
            logger.warning(
                "Travel limits DISABLED for this session "
                "(--override-limits) — recovery mode."
            )
        mc = MotorClient(
            transport,
            source="field_zero",
            enforce_limits=not args.override_limits,
        )
        if not args.no_slip_check:
            verdict, exp, meas = run_slip_check(
                mc, snapshot, slope_m, args.move_deg
            )
            msg = (
                f"Pot slip check: {verdict} "
                f"(expected {exp:.4f} V, measured {meas:.4f} V)"
            )
            print(msg)
            logger.info(msg)
            if verdict == "fail":
                if not _prompt_override(exp, meas):
                    raise SystemExit(
                        f"POT SLIP DETECTED ({meas:.3f} V vs "
                        f"{exp:.3f} V expected). Fix the pot coupling "
                        "before zeroing."
                    )
                logger.warning(
                    "OPERATOR OVERRIDE: zeroing despite failed pot "
                    "slip check (measured %.4f V vs %.4f V expected).",
                    meas,
                    exp,
                )
            elif verdict == "warn":
                warn_msg = (
                    "Pot tracking marginal; proceeding but inspect "
                    "the coupling."
                )
                print(warn_msg)
                logger.warning(warn_msg)
            elif verdict == "overshoot":
                over_msg = (
                    "Pot swing LARGER than expected — not slip; the "
                    "stored slope is likely stale. Proceeding; re-run "
                    "calibrate-pot --mode azimuth when convenient."
                )
                print(over_msg)
                logger.warning(over_msg)
        zeroer = MotorZeroer(
            transport,
            source="field_zero",
            enforce_limits=not args.override_limits,
        )
        zeroer.set_delay()
        zeroer.halt()
        curses.wrapper(
            _curses_main, zeroer, snapshot, pot_proxy, transport, args.deg
        )


if __name__ == "__main__":
    main()
