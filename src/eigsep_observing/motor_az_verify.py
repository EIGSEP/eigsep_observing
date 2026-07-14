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
