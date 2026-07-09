"""Field zero: verify the pot tracks, then home-and-zero at the
cal-defined home (pot 0°, IMU-level el).

Self-contained active driver (see scripts/CLAUDE.md): claims run_tag,
talks to hardware only via PicoProxy, requires pico-manager. Performs
pot-slip pre-check and interactive jog (recovery/inspection); the
confirm action drives the closed-loop homer onto the pot calibration's
zero-angle voltage and IMU-level elevation, resetting the step counters
there. The pot calibration is the single az-home authority and is never
modified here — a deliberate intercept re-pin lives in
``calibrate-pot --mode rezero``.
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
    coupling cannot do; it means the stored slope is stale. That warns
    without blocking (a stale slope skews the cal-derived home target,
    so re-run calibrate-pot when convenient, but the pot still tracks
    and homing still converges on *a* well-defined point). A
    non-positive expected swing is unusable -> "fail".
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
    """Ask the operator whether to proceed despite a failed slip check.

    Field reality: a slipping/odd pot coupling often cannot be fixed
    on the rig, and the operator gets the final say — with the numbers
    in hand, before the probe move has to be repeated. A slipping pot
    makes the pot-referenced home unreliable, so the override is a
    deliberate, logged degraded mode. Returns True only on an explicit
    y/yes; EOF (non-interactive stdin) is No.
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
        f"imu_az: yaw={imu.get('yaw')} el={imu.get('el_deg')}",
    )
    screen.addstr(
        7,
        0,
        "u/d EL | l/r AZ | +/- step | Enter=home&zero(confirm) | q=quit",
    )
    if zeroer.pending_zero:
        screen.addstr(
            9,
            0,
            ">>> HOME & ZERO? drives to pot 0 deg / IMU level; "
            "'y' confirm, any other key cancels <<<",
        )
    elif zeroer.is_homing:
        screen.addstr(9, 0, ">>> HOMING to cal zero... any key cancels <<<")
    if zeroer.notice:
        width = screen.getmaxyx()[1]
        screen.addstr(11, 0, f"! {zeroer.notice}"[: width - 1])
    screen.refresh()


def _curses_main(screen, zeroer, snapshot, deg):
    """Pump keystrokes until quit or a converged home-and-zero.

    Returns the :class:`~eigsep_observing.motor_homer.HomeResult` of a
    converged confirm-committed home, or ``None`` when the operator
    quits without one. An unconverged / cancelled / refused home does
    NOT exit — the operator keeps jogging (the reason is on the notice
    line) and can re-confirm.
    """
    curses.noecho()
    screen.timeout(100)
    home_committed = False
    try:
        while True:
            _render(screen, zeroer, snapshot, deg)
            deg, should_exit, committed = zeroer.handle_key(
                screen.getch(), deg
            )
            if committed:
                home_committed = True
            if home_committed and not zeroer.is_homing:
                result = zeroer.last_home_result
                if result is not None and result.converged:
                    logger.info("Home & zero complete: %s", result)
                    return result
                home_committed = False
            if should_exit:
                return None
    finally:
        zeroer.cancel_home()
        zeroer.halt()


def _parse_args():
    p = ArgumentParser(
        description="Field zero: verify pot, home-and-zero at cal home"
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
            motor_client=mc,
            confirm_starts_home=True,
        )
        zeroer.set_delay()
        zeroer.halt()
        result = curses.wrapper(_curses_main, zeroer, snapshot, args.deg)
        if result is not None:
            msg = (
                f"Home & zero complete in {result.iterations} "
                f"iteration(s): residual az={result.residual_az_deg} "
                f"el={result.residual_el_deg} deg; step counters "
                f"reset={result.reset_count}."
            )
        else:
            msg = "Exited without a converged home & zero."
        print(msg)
        logger.info(msg)


if __name__ == "__main__":
    main()
