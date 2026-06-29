"""Field zero: verify the pot tracks, set the operational az/el zero, and
re-pin the pot calibration intercept with the box in its hanging pose.

Self-contained active driver (see scripts/CLAUDE.md): claims run_tag,
talks to hardware only via PicoProxy, requires pico-manager. Later tasks
add the interactive zeroing and the pot re-pin; this task is the pure
slip-verdict helper.
"""

from picohost.buses import PotCalStore


def slip_verdict(expected_dv, measured_dv, *, warn=0.05, fail=0.10):
    """Classify how well the pot tracked a known motor move.

    expected_dv / measured_dv are pot voltage swings (V) for the same
    commanded move. Returns "ok" / "warn" / "fail" on the fractional
    shortfall. A non-positive expected swing is unusable -> "fail".
    """
    if expected_dv <= 0:
        return "fail"
    frac = abs(measured_dv - expected_dv) / abs(expected_dv)
    if frac >= fail:
        return "fail"
    if frac >= warn:
        return "warn"
    return "ok"


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
    pot_proxy.send_command("set_calibration", pot_az_params=[m, b])
    return m, b
