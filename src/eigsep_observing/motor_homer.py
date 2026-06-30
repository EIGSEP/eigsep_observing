"""Closed-loop return-to-known-home for the motor.

Sibling of MotorZeroer. Reads the home reference (home_ref K/V), then runs
a coarse-approach → settle → measure → damped-corrective-jog loop through
MotorClient (inheriting the travel-limit guard) until the pot voltage and
IMU elevation are back within tolerance of home, then re-zeros the step
counter. Az feedback is raw pot voltage (slope-independent); el feedback is
the redundant imu_el-signed / imu_az-|θ| estimate.
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional

from eigsep_redis import MetadataSnapshotReader
from picohost.buses import PotCalStore

from .el_sensor import read_el_estimate
from .home_ref import read_home_ref
from .motor_client import MotorClient

logger = logging.getLogger(__name__)

_AZ_GAIN_FALLBACK_DEG_PER_VOLT = 90.0


@dataclass
class HomeResult:
    converged: bool
    iterations: int
    residual_az_deg: Optional[float]
    residual_el_deg: Optional[float]
    degraded: bool
    reset_count: bool


class MotorHomer:
    """Drive the motor back to its recorded home pose.

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
        Fraction of the residual applied per corrective jog (default 0.5).
    max_iters : int
        Maximum correction iterations before giving up (default 6).
    az_gain_deg_per_volt : float or None
        Override for the az pot gain (deg/V).  When ``None`` the gain
        is read from ``PotCalStore`` (``abs(slope)``), with a fallback
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
        self.az_gain_deg_per_volt = az_gain_deg_per_volt
        self.reset_count = reset_count
        self.logger = logger

    # ------------------------------------------------------------------
    # Pure helpers (also called by Task C5's home() loop)
    # ------------------------------------------------------------------

    def _az_gain(self):
        """Deg/volt magnitude for the az potentiometer.

        Priority: constructor override → PotCalStore abs(slope) →
        ``_AZ_GAIN_FALLBACK_DEG_PER_VOLT`` (90.0).
        """
        if self.az_gain_deg_per_volt is not None:
            return abs(self.az_gain_deg_per_volt)
        try:
            cal = PotCalStore(self.transport).get() or {}
            slope = (cal.get("pot_az") or [None])[0]
            if slope:
                return abs(float(slope))
        except Exception as exc:
            self.logger.warning(
                "PotCalStore unavailable; using az gain fallback "
                "%.1f deg/V: %s",
                _AZ_GAIN_FALLBACK_DEG_PER_VOLT,
                exc,
            )
        return _AZ_GAIN_FALLBACK_DEG_PER_VOLT

    def _az_residual_deg(self, ref, pot_v):
        """Degrees to jog so the pot returns to its home voltage.

        The sign convention: positive residual → need to jog positive az.
        Returns ``None`` when no pot reading is available.

        Parameters
        ----------
        ref : dict
            Home reference dict from ``read_home_ref``; must contain
            ``pot_az_voltage_v0``.
        pot_v : float or None
            Current pot voltage reading.
        """
        if pot_v is None:
            return None
        dv = ref["pot_az_voltage_v0"] - pot_v
        return dv * self._az_gain()

    def _el_residual(self, ref, el_est):
        """Elevation residual in degrees and whether it is magnitude-only.

        Returns ``(residual_deg, magnitude_only)``.

        When the primary IMU (``imu_el``, signed) is available the
        residual is signed: ``home_el - current_el``.  When only the
        failover IMU (``imu_az``, magnitude-only |θ|) is available the
        residual is a magnitude comparison: ``|current| - |home|``.

        Parameters
        ----------
        ref : dict
            Home reference dict from ``read_home_ref``; must contain
            ``imu_el_deg_home``.
        el_est : ElEstimate
            Current elevation estimate from ``read_el_estimate``.
        """
        if el_est.el_deg is None:
            return None, False
        home = ref.get("imu_el_deg_home") or 0.0
        if el_est.magnitude_only:
            return el_est.el_deg - abs(home), True
        return home - el_est.el_deg, False

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

    def _read_sensors(self):
        """Return ``(pot_v, el_est)`` from the snapshot reader.

        ``pot_v`` is the current az pot voltage (``None`` if absent or
        on exception).  ``el_est`` is an :class:`~.el_sensor.ElEstimate`
        from the two IMU streams (``el_deg=None`` when both are absent).
        """
        try:
            pot_v = (self.snapshot.get("potmon") or {}).get("pot_az_voltage")
        except Exception:
            pot_v = None
        el_est = read_el_estimate(self.snapshot, logger=self.logger)
        return pot_v, el_est

    def _settle(self, stop_event):
        """Wait ``settle_s`` seconds; uses ``stop_event.wait`` when present."""
        if stop_event is not None:
            stop_event.wait(self.settle_s)
        elif self.settle_s:
            time.sleep(self.settle_s)

    def home(self, stop_event=None):
        """Drive the motor back to its recorded home pose.

        Parameters
        ----------
        stop_event : threading.Event or None
            When set, the loop exits at the next interruptible point.

        Returns
        -------
        HomeResult
            ``converged`` is ``True`` when both residuals are within
            tolerance; ``degraded`` is ``True`` when sensors were
            unavailable and open-loop fall-back was used.

        Raises
        ------
        RuntimeError
            When no home reference is stored in Redis.
        """
        ref = read_home_ref(self.transport)
        if ref is None:
            raise RuntimeError(
                "No home reference in Redis; run field_zero to set home first."
            )

        pot_v, el_est = self._read_sensors()
        if pot_v is None and el_est.el_deg is None:
            self.logger.warning(
                "Homing sensors unavailable; falling back to open-loop "
                "home() — position will not be verified."
            )
            self.motor_client.home(stop_event=stop_event)
            return HomeResult(
                False,
                0,
                float("nan"),
                float("nan"),
                degraded=True,
                reset_count=False,
            )

        self.motor_client.home(stop_event=stop_event)  # coarse approach
        az_sign, el_sign = 1.0, 1.0
        last_az = last_el = None
        res_az = res_el = float("nan")
        converged = False
        i = 0
        for i in range(1, self.max_iters + 1):
            if stop_event is not None and stop_event.is_set():
                break
            self._settle(stop_event)
            pot_v, el_est = self._read_sensors()
            res_az = self._az_residual_deg(ref, pot_v)
            res_el, _ = self._el_residual(ref, el_est)
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
            self.motor_client.reset_step_position(az_step=0, el_step=0)
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
