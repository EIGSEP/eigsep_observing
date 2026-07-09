"""Closed-loop return-to-home for the motor.

Sibling of MotorZeroer. Home is defined by the pot calibration, not by a
recorded pose: az home is the voltage where the calibrated pot reads 0°
(``v_home = -b/m`` from ``PotCalStore``), el home is IMU-level (0°). home()
runs a coarse-approach → settle → measure → damped-corrective-jog loop
through MotorClient (inheriting the travel-limit guard) until the pot
voltage and IMU elevation are within tolerance of those targets, then
re-zeros the step counter. Az feedback is raw pot voltage; el feedback is
the redundant imu_el-signed / imu_az-|θ| estimate. Because the targets are
derived live from the cal, a recalibration (``calibrate-pot``, including
``--mode rezero``) moves home for every consumer on the next home() call —
there is no intermediate home-reference K/V to refresh.
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional

from eigsep_redis import MetadataSnapshotReader
from picohost.buses import PotCalStore

from .el_sensor import read_el_estimate
from .motor_client import MotorClient, MotorLimitError, validate_axes
from .motor_limits import read_motor_limits

logger = logging.getLogger(__name__)

_AZ_GAIN_FALLBACK_DEG_PER_VOLT = 90.0
# Matches the pico metadata producer cadence (~200 ms) so consecutive
# integrated-read samples are distinct snapshot frames, not rereads.
_POT_SAMPLE_INTERVAL_S = 0.2


@dataclass
class HomeResult:
    converged: bool
    iterations: int
    residual_az_deg: Optional[float]
    residual_el_deg: Optional[float]
    degraded: bool
    reset_count: bool


class _AzDivergenceGuard:
    """Halt an az move that is driving the pot away from home.

    Instances are the zero-arg ``guard`` callables polled by
    :meth:`MotorClient._wait_for_stop` at fence cadence during a
    move. The guard tracks the closest approach ``|v - v_home|``
    seen so far; once the current distance exceeds that minimum by
    more than ``diverge_deg`` (converted through the cal slope
    magnitude) it raises :class:`MotorLimitError`, which halts the
    motor mid-flight. This catches a wrong step counter (post-reboot)
    or a wrong-signed cal long before the ±limit fence would, and the
    min-so-far form also stops a move that reaches home and keeps
    going. Missing pot samples are skipped — the guard only acts on
    live readings, matching the sensor fence's convention (so it is
    inert exactly when the pot fence is inert).

    Parameters
    ----------
    read_pot : callable
        Zero-arg callable returning the current pot voltage or
        ``None`` when unavailable.
    v_home : float
        Az home target voltage.
    slope_deg_per_volt : float
        Signed cal slope; only its magnitude is used here.
    diverge_deg : float
        Allowed growth of ``|v - v_home|`` past the closest approach,
        in degrees, before the move is halted.
    """

    def __init__(self, read_pot, v_home, slope_deg_per_volt, diverge_deg):
        self._read_pot = read_pot
        self._v_home = v_home
        self._deg_per_volt = abs(slope_deg_per_volt)
        self._diverge_deg = diverge_deg
        self._min_dist_deg = None

    def __call__(self):
        v = self._read_pot()
        if v is None:
            return
        dist_deg = abs(v - self._v_home) * self._deg_per_volt
        if self._min_dist_deg is None or dist_deg < self._min_dist_deg:
            self._min_dist_deg = dist_deg
            return
        if dist_deg - self._min_dist_deg > self._diverge_deg:
            raise MotorLimitError(
                f"az move diverging from pot home: |pot - home| grew "
                f"to {dist_deg:.1f} deg from a closest approach of "
                f"{self._min_dist_deg:.1f} deg "
                f"(> {self._diverge_deg:.1f} deg allowance); halting."
            )


class MotorHomer:
    """Drive the motor to the cal-defined home (pot 0°, IMU-level el).

    Parameters
    ----------
    transport : eigsep_redis.Transport
        Shared transport; used to build the snapshot reader and
        PotCalStore when not supplied.
    motor_client : MotorClient or None
        Pre-built client.  When ``None`` a default is constructed.
    snapshot : MetadataSnapshotReader or None
        Pre-built reader.  When ``None`` a default is constructed.
    tol_az_deg : float
        Azimuth convergence tolerance in degrees (default 3.0).
    tol_el_deg : float
        Elevation convergence tolerance in degrees (default 2.0).
    settle_s : float
        Seconds to wait after a jog before re-reading sensors
        (default 10.0).
    damping : float
        Fraction of the residual applied per corrective jog
        (el loop only; default 0.5).
    max_iters : int
        Maximum correction iterations before giving up
        (el loop only; default 6).
    az_integrate_s : float
        Seconds of pot samples averaged per az reading (default 2.0,
        ~10 samples at the 200 ms producer cadence) to beat down pot
        noise before deciding on the single corrective jog.
    az_diverge_deg : float
        Allowed growth of the pot's distance from home past its
        closest approach during an az move before the divergence
        guard halts the motor (default 20.0 — above the known ~7°
        1/rev pot nonlinearity; hardware tuning expected).
    az_step0_fallback : bool
        When the potmon is not publishing, ``True`` parks az at step
        0 open-loop (position unverified, ``degraded``); ``False``
        (default) skips az motion entirely — with the pot dead the
        pot-voltage fence is inert too, so a blind move is the
        dangerous case and must be opted into.
    az_gain_deg_per_volt : float or None
        Signed override for the az pot slope (deg/V); the sign sets
        the corrective-jog direction.  When ``None`` the slope
        is read from ``PotCalStore``, with a fallback
        of ``_AZ_GAIN_FALLBACK_DEG_PER_VOLT`` (90.0) when the store is
        empty or unreachable.
    reset_count : bool
        Whether to re-zero the step counter upon convergence (default True).
    enforce_limits : bool
        Passed to the internally built ``MotorClient``; ignored when an
        external ``motor_client`` is supplied (that client's own
        ``enforce_limits`` governs).
    source : str
        Identifier stamped on proxy command stream entries.
    """

    def __init__(
        self,
        transport,
        *,
        motor_client=None,
        snapshot=None,
        tol_az_deg=3.0,
        tol_el_deg=2.0,
        settle_s=10.0,
        damping=0.5,
        max_iters=6,
        az_integrate_s=2.0,
        az_diverge_deg=20.0,
        az_step0_fallback=False,
        az_gain_deg_per_volt=None,
        reset_count=True,
        enforce_limits=True,
        source="motor_homer",
    ):
        self.transport = transport
        if motor_client is None:
            motor_client = MotorClient(
                transport, source=source, enforce_limits=enforce_limits
            )
        self.motor_client = motor_client
        self.snapshot = snapshot or MetadataSnapshotReader(transport)
        self.tol_az_deg = tol_az_deg
        self.tol_el_deg = tol_el_deg
        self.settle_s = settle_s
        self.damping = damping
        self.max_iters = max_iters
        self.az_integrate_s = az_integrate_s
        self.az_diverge_deg = az_diverge_deg
        self.az_step0_fallback = az_step0_fallback
        self.az_gain_deg_per_volt = az_gain_deg_per_volt
        self.reset_count = reset_count
        self.logger = logger

    # ------------------------------------------------------------------
    # Pure helpers (also called by Task C5's home() loop)
    # ------------------------------------------------------------------

    def _pot_cal(self):
        """``(m, b)`` from ``PotCalStore``, or ``None`` when absent.

        A cal with a zero/missing slope is treated as absent — it can
        derive neither a home voltage nor a gain.
        """
        try:
            cal = PotCalStore(self.transport).get() or {}
        except Exception as exc:
            self.logger.warning("PotCalStore unavailable: %s", exc)
            return None
        pair = cal.get("pot_az")
        if not pair or not pair[0]:
            return None
        return float(pair[0]), float(pair[1])

    def az_home_voltage(self):
        """Pot voltage at the cal's zero angle — the az home target.

        ``angle = m*V + b = 0`` → ``v_home = -b/m``. Raises
        ``RuntimeError`` when no pot calibration is stored; home is
        defined by the cal, so there is no fallback target.
        """
        cal = self._pot_cal()
        if cal is None:
            raise RuntimeError(
                "No pot calibration; run calibrate-pot --mode azimuth first."
            )
        m, b = cal
        return -b / m

    def _check_home_in_window(self, v_home):
        """Refuse a home target outside the rig's pot fence, if one is set.

        A cal whose zero-angle voltage lies outside ``pot_az_v_limits``
        is inconsistent with the rig limits — homing toward it would
        drive into the sensor fence, so fail loudly before moving.
        """
        try:
            limits = read_motor_limits(self.transport) or {}
        except Exception:
            return
        window = limits.get("pot_az_v_limits")
        if not window:
            return
        lo, hi = window
        if not (lo <= v_home <= hi):
            raise RuntimeError(
                f"cal-derived az home voltage {v_home:.3f} V is outside "
                f"the pot limit window [{lo:.3f}, {hi:.3f}]; the pot "
                "calibration is inconsistent with the rig limits — "
                "re-run calibrate-pot."
            )

    def _az_slope(self):
        """Signed slope (deg/V) of the az potentiometer cal.

        Priority: constructor override -> PotCalStore slope ->
        ``_AZ_GAIN_FALLBACK_DEG_PER_VOLT`` (+90.0). The sign is
        load-bearing: the single corrective jog has no trial-and-error
        sign detection, so direction comes entirely from the cal's
        sign convention (cal angle is fit against motor az degrees,
        so ``m * dV`` is already a motor-frame jog).
        """
        if self.az_gain_deg_per_volt is not None:
            return float(self.az_gain_deg_per_volt)
        cal = self._pot_cal()
        if cal is not None:
            return cal[0]
        self.logger.warning(
            "No pot calibration for az slope; using fallback %.1f deg/V",
            _AZ_GAIN_FALLBACK_DEG_PER_VOLT,
        )
        return _AZ_GAIN_FALLBACK_DEG_PER_VOLT

    def _az_residual_deg(self, v_home, pot_v):
        """Signed degrees to jog so the pot returns to its home voltage.

        ``m * (v_home - pot_v)``, where ``m`` is the signed cal slope
        — positive residual means jog positive az. Returns ``None``
        when no pot reading is available.

        Parameters
        ----------
        v_home : float
            Az home target voltage (from ``az_home_voltage``).
        pot_v : float or None
            Current pot voltage reading.
        """
        if pot_v is None:
            return None
        dv = v_home - pot_v
        return dv * self._az_slope()

    def _el_residual(self, el_est):
        """Elevation residual in degrees and whether it is magnitude-only.

        Returns ``(residual_deg, magnitude_only)``. El home is the
        constant IMU-level pose (0°), so the residual is simply the
        negated current elevation.

        When the primary IMU (``imu_el``, signed) is available the
        residual is signed: ``0 - current_el``.  When only the failover
        IMU (``imu_az``, magnitude-only |θ|) is available the residual
        is the magnitude itself: ``|current| - 0``.

        Parameters
        ----------
        el_est : ElEstimate
            Current elevation estimate from ``read_el_estimate``.
        """
        if el_est.el_deg is None:
            return None, False
        if el_est.magnitude_only:
            return el_est.el_deg, True
        return -el_est.el_deg, False

    def _within_tol(self, res_az, res_el):
        """True when both residuals are within their configured tolerances.

        A ``None`` residual (sensor absent) is treated as "within
        tolerance" so a missing sensor does not block convergence on the
        axis that is present.

        Parameters
        ----------
        res_az : float or None
            Azimuth residual in degrees (from ``_az_residual_deg``).
        res_el : float or None
            Elevation residual in degrees (from ``_el_residual``).
        """
        ok_az = res_az is None or abs(res_az) <= self.tol_az_deg
        ok_el = res_el is None or abs(res_el) <= self.tol_el_deg
        return ok_az and ok_el

    # ------------------------------------------------------------------
    # Task C5: sensor read, settle, and converge loop
    # ------------------------------------------------------------------

    def _read_pot_once(self):
        """Current ``pot_az_voltage`` from the snapshot, or ``None``
        when absent or on a reader error. Single sample — the guard's
        per-poll read; see :meth:`_read_pot_integrated` for the
        noise-averaged version."""
        try:
            return (self.snapshot.get("potmon") or {}).get("pot_az_voltage")
        except Exception:
            return None

    def _read_pot_integrated(self, stop_event=None):
        """Mean pot voltage over ``az_integrate_s`` seconds of samples.

        Samples :meth:`_read_pot_once` at the producer cadence
        (``_POT_SAMPLE_INTERVAL_S``); dropped samples (``None``) are
        skipped. Returns ``None`` when no sample arrived at all. A
        set ``stop_event`` ends the window early with whatever was
        collected.
        """
        n = max(1, int(round(self.az_integrate_s / _POT_SAMPLE_INTERVAL_S)))
        samples = []
        for i in range(n):
            v = self._read_pot_once()
            if v is not None:
                samples.append(float(v))
            if i + 1 < n:
                if stop_event is not None:
                    if stop_event.wait(_POT_SAMPLE_INTERVAL_S):
                        break
                else:
                    time.sleep(_POT_SAMPLE_INTERVAL_S)
        if not samples:
            return None
        return sum(samples) / len(samples)

    def _read_el(self):
        """Current :class:`~.el_sensor.ElEstimate` from the IMU
        streams (``el_deg=None`` when both are absent)."""
        return read_el_estimate(self.snapshot, logger=self.logger)

    def _read_sensors(self):
        """(pot_v, el_est) — legacy combined read; removal pending the
        per-axis home() split."""
        return self._read_pot_once(), self._read_el()

    def _settle(self, stop_event):
        """Wait ``settle_s`` seconds; uses ``stop_event.wait`` when present."""
        if stop_event is not None:
            stop_event.wait(self.settle_s)
        elif self.settle_s:
            time.sleep(self.settle_s)

    def home(self, stop_event=None, axes=("az", "el")):
        """Drive the motor to the cal-defined home (pot 0°, IMU-level el).

        Parameters
        ----------
        stop_event : threading.Event or None
            When set, the loop exits at the next interruptible point.
        axes : tuple of str
            Axes to home, a non-empty subset of ``("az", "el")``
            (default both). An axis not requested is never moved, its
            residual in the result is ``None``, and its step counter is
            preserved on the convergence re-zero. An el-only home needs
            no pot calibration — the cal requirement is purely an az
            concern.

        Returns
        -------
        HomeResult
            ``converged`` is ``True`` when every requested axis'
            residual is within tolerance; ``degraded`` is ``True`` when
            the requested axes' sensors were unavailable and open-loop
            fall-back was used.

        Raises
        ------
        RuntimeError
            When az is requested and no pot calibration is stored, or
            when the cal's zero-angle voltage falls outside the
            configured pot limit window (broken cal — refuse before
            moving).
        ValueError
            If ``axes`` is empty or names an unknown axis.
        """
        axes = validate_axes(axes)
        do_az = "az" in axes
        do_el = "el" in axes
        v_home = None
        if do_az:
            v_home = self.az_home_voltage()
            self._check_home_in_window(v_home)

        pot_v, el_est = self._read_sensors()
        has_feedback = (do_az and pot_v is not None) or (
            do_el and el_est.el_deg is not None
        )
        if not has_feedback:
            self.logger.warning(
                "Homing sensors unavailable; falling back to open-loop "
                "home() — position will not be verified."
            )
            self.motor_client.home(stop_event=stop_event, axes=axes)
            return HomeResult(
                False,
                0,
                float("nan") if do_az else None,
                float("nan") if do_el else None,
                degraded=True,
                reset_count=False,
            )

        # coarse approach
        self.motor_client.home(stop_event=stop_event, axes=axes)
        az_sign, el_sign = 1.0, 1.0
        last_az = last_el = None
        res_az = float("nan") if do_az else None
        res_el = float("nan") if do_el else None
        converged = False
        i = 0
        for i in range(1, self.max_iters + 1):
            if stop_event is not None and stop_event.is_set():
                break
            self._settle(stop_event)
            pot_v, el_est = self._read_sensors()
            res_az = self._az_residual_deg(v_home, pot_v) if do_az else None
            res_el = self._el_residual(el_est)[0] if do_el else None
            # An unrequested axis' residual is None by construction, so
            # this still means "every requested axis lost feedback".
            if res_az is None and res_el is None:
                self.logger.warning(
                    "Homing lost all sensor feedback mid-loop; aborting "
                    "without re-zero (position unverified)."
                )
                return HomeResult(
                    False,
                    i,
                    res_az,
                    res_el,
                    degraded=True,
                    reset_count=False,
                )
            if self._within_tol(res_az, res_el):
                converged = True
                break
            # az corrective jog with sign auto-detect
            if res_az is not None and abs(res_az) > self.tol_az_deg:
                if last_az is not None and abs(res_az) > last_az:
                    az_sign = -az_sign
                last_az = abs(res_az)
                self.motor_client.jog_az(
                    az_sign * self.damping * res_az,
                    stop_event=stop_event,
                )
            # el corrective jog with sign auto-detect (needed in the
            # magnitude-only failover; harmless when signed)
            if res_el is not None and abs(res_el) > self.tol_el_deg:
                if last_el is not None and abs(res_el) > last_el:
                    el_sign = -el_sign
                last_el = abs(res_el)
                self.motor_client.jog_el(
                    el_sign * self.damping * res_el,
                    stop_event=stop_event,
                )

        did_reset = False
        if converged and self.reset_count:
            # None preserves the unrequested axis' counter (firmware
            # omits a None axis from the reset command).
            self.motor_client.reset_step_position(
                az_step=0 if do_az else None,
                el_step=0 if do_el else None,
            )
            did_reset = True
        if not converged:
            self.logger.warning(
                "Homing did not converge in %d iterations "
                "(residual az=%.1f el=%.1f deg).",
                self.max_iters,
                res_az if res_az is not None else float("nan"),
                res_el if res_el is not None else float("nan"),
            )
        return HomeResult(
            converged,
            i,
            res_az,
            res_el,
            degraded=False,
            reset_count=did_reset,
        )
