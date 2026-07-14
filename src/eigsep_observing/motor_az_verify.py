"""Closed-loop azimuth slip detection and correction against the pot.

After a commanded az move, the firmware step counter may report the
target while the antenna under-travelled (a slip: the driver failed to
energize). The potentiometer's calibrated ``pot_az_angle`` is the
absolute azimuth reference, fit against motor-frame degrees, so
``residual = target_deg - pot_az_angle`` is the pointing error. This
module bounds-corrects that error and gives up loudly on a stuck axis.

Kept deliberately self-contained: it reuses no private symbol from
``motor_homer`` (the homer stays untouched), only ``MotorLimitError``
from ``motor_client`` — the exception type ``_wait_for_stop`` already
catches to halt a jog mid-flight.
"""

import logging
import time
from dataclasses import dataclass

import redis.exceptions

from .motor_client import MotorLimitError

logger = logging.getLogger(__name__)

# Pico metadata producer cadence (~200 ms); one integrated read samples
# distinct snapshot frames rather than rereading a single value.
_SAMPLE_INTERVAL_S = 0.2


class _AzAngleDivergenceGuard:
    """Halt an az jog that drives ``pot_az_angle`` away from the target.

    Native to angle space (degrees in, degrees out) — the same
    closest-approach logic as ``motor_homer._AzDivergenceGuard`` without
    the volts->deg scale. Polled by ``MotorClient._wait_for_stop``
    during a jog; a raised ``MotorLimitError`` halts the motor mid-move.

    Parameters
    ----------
    read_angle : callable
        Zero-arg callable returning the current ``pot_az_angle`` in
        degrees, or ``None`` when unavailable.
    target_deg : float
        The az target the jog is converging toward.
    diverge_deg : float
        Allowed growth of ``|angle - target|`` past its closest
        approach before the move is halted.
    """

    def __init__(self, read_angle, target_deg, diverge_deg):
        self._read_angle = read_angle
        self._target_deg = target_deg
        self._diverge_deg = diverge_deg
        self._min_dist = None

    def __call__(self):
        a = self._read_angle()
        if a is None:
            return
        dist = abs(a - self._target_deg)
        if self._min_dist is None or dist < self._min_dist:
            self._min_dist = dist
            return
        if dist - self._min_dist > self._diverge_deg:
            raise MotorLimitError(
                f"az jog diverging from target {self._target_deg:.1f} "
                f"deg: |pot - target| grew to {dist:.1f} deg from a "
                f"closest approach of {self._min_dist:.1f} deg "
                f"(> {self._diverge_deg:.1f} deg allowance); halting."
            )


@dataclass
class VerifyResult:
    """Outcome of one ``AzPotVerifier.verify`` call.

    converged : pot within ``tol_az_deg`` of the target (before or after
        corrective jogs).
    iters : corrective jogs performed (0 when already within tol).
    residual_deg : final ``|target - pot_az_angle|`` in degrees; ``nan``
        when degraded.
    degraded : the pot reference was absent / uncalibrated / near a rail,
        so verify was skipped and the move left open-loop (unverified).
    """

    converged: bool
    iters: int
    residual_deg: float
    degraded: bool


class AzPotVerifier:
    """Bounded pot-referenced correction of az slip.

    After a commanded az move, read the calibrated ``pot_az_angle`` and,
    while it is more than ``tol_az_deg`` from the target, jog by the
    signed residual (guarded) up to ``max_iters`` times. Converges in a
    single jog on a healthy axis (plant gain ~1); on a stuck axis the
    pot never advances and the iteration cap surfaces the slip.

    Parameters
    ----------
    motor_client : object with ``jog_az(delta_deg, *, guard=None)``.
    reader : object with ``.get("potmon") -> dict|None``
        (``eigsep_redis.MetadataSnapshotReader`` in production).
    tol_az_deg : deadband; must exceed the pot noise + nonlinearity
        floor (~3 deg) so noise can't trigger a spurious reverse jog.
    max_iters : correction cap (stuck-actuator alarm).
    settle_s : pause after each jog before re-reading (clears the pot's
        ~0.7 s lag while moving).
    integrate_s : seconds of pot samples averaged per read (beats the
        ~2 deg pot noise).
    diverge_deg : divergence-guard allowance in degrees.
    """

    def __init__(
        self,
        motor_client,
        reader,
        *,
        tol_az_deg=3.0,
        max_iters=3,
        settle_s=1.5,
        integrate_s=1.0,
        diverge_deg=20.0,
    ):
        self.motor_client = motor_client
        self.reader = reader
        self.tol_az_deg = tol_az_deg
        self.max_iters = max_iters
        self.settle_s = settle_s
        self.integrate_s = integrate_s
        self.diverge_deg = diverge_deg
        self.logger = logger

    def _read_pot_angle_once(self):
        """Current ``pot_az_angle`` (deg), or ``None`` when the reference
        is unavailable or at risk: potmon absent / connection error,
        uncalibrated (``pot_az_angle`` is ``None``), or near an ADC rail
        (``pot_az_near_rail``). Missing == inert, matching the sensor
        fence's convention."""
        try:
            snap = self.reader.get("potmon") or {}
        except (KeyError, redis.exceptions.ConnectionError):
            return None
        if snap.get("pot_az_near_rail"):
            return None
        ang = snap.get("pot_az_angle")
        return None if ang is None else float(ang)

    def _read_pot_angle_integrated(self):
        """Mean ``pot_az_angle`` over ``integrate_s`` of samples, or
        ``None`` when no sample arrived. Dropped samples are skipped."""
        n = max(1, int(round(self.integrate_s / _SAMPLE_INTERVAL_S)))
        samples = []
        for i in range(n):
            a = self._read_pot_angle_once()
            if a is not None:
                samples.append(a)
            if i + 1 < n and _SAMPLE_INTERVAL_S:
                time.sleep(_SAMPLE_INTERVAL_S)
        if not samples:
            return None
        return sum(samples) / len(samples)

    def verify(self, target_deg):
        """Bounded-correct az to ``target_deg`` against the pot.

        Returns a :class:`VerifyResult`. Never raises for a slip — the
        caller decides how loudly to surface a non-converged result.
        """
        ang = self._read_pot_angle_integrated()
        if ang is None:
            return VerifyResult(False, 0, float("nan"), True)
        residual = target_deg - ang
        if abs(residual) <= self.tol_az_deg:
            return VerifyResult(True, 0, abs(residual), False)
        guard = _AzAngleDivergenceGuard(
            self._read_pot_angle_once, target_deg, self.diverge_deg
        )
        for i in range(1, self.max_iters + 1):
            self.logger.info(
                "az verify: residual %.1f deg from target %.1f; "
                "corrective jog %d/%d",
                residual,
                target_deg,
                i,
                self.max_iters,
            )
            self.motor_client.jog_az(residual, guard=guard)
            if self.settle_s:
                time.sleep(self.settle_s)
            ang = self._read_pot_angle_integrated()
            if ang is None:
                return VerifyResult(False, i, float("nan"), True)
            residual = target_deg - ang
            if abs(residual) <= self.tol_az_deg:
                return VerifyResult(True, i, abs(residual), False)
        return VerifyResult(False, self.max_iters, abs(residual), False)
