"""Coordinate RF switching with motor motion via a shared lock.

The panda runs two independent command paths today: ``switch_loop`` /
``vna_loop`` serialize against each other through ``switch_lock``, while
``motor_loop`` deliberately stays out of that mutual exclusion. If
motor stepping pulses turn out to bleed RF interference into VNA or
correlator data, we want to be able to opt into "no switching while
motors move" without rewriting either subsystem.

This module provides the coordinator that mediates that decision. One
``RLock`` underlies both flows so:

* ``switch_section`` always acquires (replaces ``with switch_lock``).
* ``motion_section`` acquires only when ``serialize`` is True.
* The same thread can re-enter from a wrapped context — required by
  the no-switch-observation script, which hosts a ``MotorScanner.scan``
  inside an outer ``switch_session``.

Default-off: with ``serialize=False`` the motion side is a true no-op
and ``motor_loop`` behaves byte-for-byte as it does today.
"""

import logging
from contextlib import contextmanager


class MotionSwitchCoordinator:
    """Mediate switch ↔ motion mutual exclusion via a shared lock.

    Parameters
    ----------
    lock : threading.RLock
        The lock that serializes both classes of access. Must be an
        ``RLock`` (not a plain ``Lock``) so the no-switch-observation
        script can hold an outer ``switch_section`` while inner per-move
        ``motion_section`` calls re-acquire from the same thread.
    serialize : bool
        If True, ``motion_section`` acquires ``lock``; if False, it is
        a no-op. ``switch_section`` always acquires.
    logger : logging.Logger or None
        Optional logger for debug-level acquire/release breadcrumbs on
        the motion path; useful when correlating interference with
        specific move phases.
    """

    def __init__(self, lock, *, serialize, logger=None):
        self._lock = lock
        self._serialize = bool(serialize)
        self._logger = logger or logging.getLogger(__name__)

    @property
    def serialize(self):
        return self._serialize

    @serialize.setter
    def serialize(self, value):
        """Allow standalone scripts to flip the flag at runtime.

        ``vna_position_sweep`` and ``no_switch_observation`` force
        ``serialize=True`` after constructing the panda client so
        characterization runs always serialize, regardless of the
        deployed yaml flag.
        """
        self._serialize = bool(value)

    @contextmanager
    def switch_section(self):
        """Acquire the lock for an RF-switch operation."""
        with self._lock:
            yield

    @contextmanager
    def motion_section(self, *, label=""):
        """Acquire the lock for a motor-motion operation, if enabled.

        When ``serialize`` is False this is a true no-op: the calling
        thread does not touch the lock. When True, the calling thread
        acquires the same ``RLock`` used by ``switch_section``, so a
        switch-side caller cannot interleave a state change with the
        move.
        """
        if not self._serialize:
            yield
            return
        with self._lock:
            if label:
                self._logger.debug("motion_section acquired (label=%s)", label)
            try:
                yield
            finally:
                if label:
                    self._logger.debug(
                        "motion_section released (label=%s)", label
                    )
