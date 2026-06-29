"""Field zero: verify the pot tracks, set the operational az/el zero, and
re-pin the pot calibration intercept with the box in its hanging pose.

Self-contained active driver (see scripts/CLAUDE.md): claims run_tag,
talks to hardware only via PicoProxy, requires pico-manager. Later tasks
add the interactive zeroing and the pot re-pin; this task is the pure
slip-verdict helper.
"""


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
