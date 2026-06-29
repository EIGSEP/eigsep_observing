"""Closed-loop return-to-known-home for the motor.

Sibling of MotorZeroer. Reads the home reference (home_ref K/V), then runs
a coarse-approach → settle → measure → damped-corrective-jog loop through
MotorClient (inheriting the travel-limit guard) until the pot voltage and
IMU elevation are back within tolerance of home, then re-zeros the step
counter. Az feedback is raw pot voltage (slope-independent); el feedback is
the redundant imu_el-signed / imu_az-|θ| estimate.
"""

import logging
from dataclasses import dataclass

from eigsep_redis import MetadataSnapshotReader
from picohost.buses import PotCalStore

from .el_sensor import read_el_estimate  # noqa: F401 (used by home() in C5)
from .home_ref import read_home_ref  # noqa: F401 (used by home() in C5)
from .motor_client import MotorClient

logger = logging.getLogger(__name__)

_AZ_GAIN_FALLBACK_DEG_PER_VOLT = 90.0


@dataclass
class HomeResult:
    converged: bool
    iterations: int
    residual_az_deg: float
    residual_el_deg: float
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
        source="motor_homer",
    ):
        self.transport = transport
        if motor_client is None:
            motor_client = MotorClient(transport, source=source)
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
