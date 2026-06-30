"""
Client-side motor orchestrator.

Wraps a :class:`picohost.proxy.PicoProxy` (``motor``) and a
:class:`eigsep_redis.MetadataSnapshotReader` to drive the motor pico
from outside the :class:`picohost.manager.PicoManager` process. The
manager has a single shared ``cmd_loop`` dispatch thread; running a
full ``scan`` as one server-side action would stall command routing
for every other pico, so ``MotorClient`` issues one movement at a
time and polls the metadata snapshot for completion client-side.
"""

import logging
import threading
import time

import numpy as np
import redis.exceptions

from eigsep_redis import MetadataSnapshotReader
from picohost.proxy import PicoProxy

from .el_sensor import read_el_estimate
from .motor_cal import cal_motor
from .motor_limits import read_motor_limits
from .motion_switch import MotionSwitchCoordinator

_UNSET = object()

logger = logging.getLogger(__name__)


class MotorLimitError(ValueError):
    """Raised when a commanded move or a live sensor reading would take an
    axis outside its configured safe-travel window."""


class MotorClient:
    """Drive the motor pico through ``PicoManager`` via Redis.

    Parameters
    ----------
    transport : eigsep_redis.Transport
        Shared transport; used to build the proxy and metadata reader.
    az_up_delay_us, az_dn_delay_us, el_up_delay_us, el_dn_delay_us : int
        Per-axis step-delay defaults applied by :meth:`set_delay`.
    poll_interval_s : float
        Metadata poll cadence while waiting for a move to complete.
    stall_timeout_s : float
        Seconds without position progress before
        :meth:`_wait_for_stop` raises :class:`TimeoutError`.
    start_timeout_s : float
        Seconds to wait for a just-issued move to register in the
        metadata snapshot (target acknowledged or motion observed)
        before :meth:`_send_and_wait` treats it as a no-op and proceeds.
        Must exceed the producer status cadence (~200 ms) so a real move
        is never mistaken for a no-op — this start phase is what keeps a
        move reliably one-axis-at-a-time.
    az_limits_deg : tuple of (float, float) or _UNSET
        Inclusive ``(min, max)`` azimuth travel limits in degrees.
        Precedence: explicit kwarg > MotorLimitStore K/V > ``(-180.0,
        180.0)``. Commands that would violate these bounds raise
        :class:`MotorLimitError`.
    el_limits_deg : tuple of (float, float) or _UNSET
        Inclusive ``(min, max)`` elevation travel limits in degrees.
        Precedence: explicit kwarg > MotorLimitStore K/V > ``(-180.0,
        180.0)``. Commands that would violate these bounds raise
        :class:`MotorLimitError`.
    pot_az_v_limits : tuple of (float, float) or None or _UNSET
        Inclusive ``(min, max)`` safe voltage range for the azimuth
        potentiometer. When ``None`` no potentiometer limit is enforced.
        Precedence: explicit kwarg > MotorLimitStore K/V > ``None``.
        A live reading outside this window raises
        :class:`MotorLimitError`.
    imu_el_limits_deg : tuple of (float, float) or None or _UNSET
        Inclusive ``(min, max)`` safe elevation range derived from the
        IMU. When ``None`` no IMU elevation limit is enforced.
        Precedence: explicit kwarg > MotorLimitStore K/V > ``None``.
        A live reading outside this window raises
        :class:`MotorLimitError`.
    enforce_limits : bool
        When ``False`` both the commanded-target guard
        (:meth:`_check_target_limit`) and the live-sensor fence
        (:meth:`_check_sensor_fence`) are bypassed entirely. Intended
        for bring-up scripts that need unconstrained motion (e.g.
        field-zero calibration). Default ``True``.
    source : str
        Identifier stamped on proxy command stream entries.
    coord : MotionSwitchCoordinator or None
        Optional coordinator. When ``None``, the client builds an
        internal coordinator with ``serialize=False`` so standalone use
        (e.g. ``scripts/motor_scan.py``) is unchanged.
        :class:`PandaClient` passes its own coordinator so the panda's
        ``serialize_motion_and_switching`` flag flows through.
    """

    def __init__(
        self,
        transport,
        *,
        az_up_delay_us=2400,
        az_dn_delay_us=300,
        el_up_delay_us=2400,
        el_dn_delay_us=600,
        poll_interval_s=0.1,
        stall_timeout_s=30.0,
        start_timeout_s=1.0,
        az_limits_deg=_UNSET,
        el_limits_deg=_UNSET,
        pot_az_v_limits=_UNSET,
        imu_el_limits_deg=_UNSET,
        enforce_limits=True,
        source="motor_client",
        coord=None,
    ):
        self.transport = transport
        self._proxy = PicoProxy("motor", transport, source=source)
        self._reader = MetadataSnapshotReader(transport)
        self._delay_kwargs = {
            "az_up_delay_us": az_up_delay_us,
            "az_dn_delay_us": az_dn_delay_us,
            "el_up_delay_us": el_up_delay_us,
            "el_dn_delay_us": el_dn_delay_us,
        }
        self.poll_interval_s = poll_interval_s
        self.stall_timeout_s = stall_timeout_s
        self.start_timeout_s = start_timeout_s
        stored = self._load_stored_limits(transport)
        self.az_limits_deg = self._resolve_limit(
            az_limits_deg, stored.get("az_limits_deg"), (-180.0, 180.0)
        )
        self.el_limits_deg = self._resolve_limit(
            el_limits_deg, stored.get("el_limits_deg"), (-180.0, 180.0)
        )
        self.pot_az_v_limits = self._resolve_limit(
            pot_az_v_limits, stored.get("pot_az_v_limits"), None
        )
        self.imu_el_limits_deg = self._resolve_limit(
            imu_el_limits_deg, stored.get("imu_el_limits_deg"), None
        )
        self.enforce_limits = enforce_limits
        self._cal = cal_motor()
        self.logger = logger
        if coord is None:
            coord = MotionSwitchCoordinator(
                threading.RLock(), serialize=False, logger=self.logger
            )
        self._coord = coord

    @staticmethod
    def _resolve_limit(explicit, stored, default):
        """Precedence: explicit kwarg > stored K/V value > hardcoded default.

        A stored ``None`` (fence explicitly disabled in the K/V) means
        'no window', so it is treated the same as absent → default.
        """
        if explicit is not _UNSET:
            return explicit
        if stored is not None:
            return stored
        return default

    @staticmethod
    def _load_stored_limits(transport):
        """MotorLimitStore values as a dict, or {} when unset / unreachable
        / malformed. Never blocks MotorClient construction — any non-dict
        payload (corrupt/hand-edited key) degrades to the hardcoded safe
        defaults."""
        stored = read_motor_limits(transport)
        return stored if isinstance(stored, dict) else {}

    @property
    def is_available(self):
        return self._proxy.is_available

    @property
    def coord(self):
        return self._coord

    def set_delay(self, **overrides):
        """Push the current delay config to firmware, optionally overriding fields."""
        self._delay_kwargs.update(overrides)
        self._proxy.send_command("set_delay", **self._delay_kwargs)

    def halt(self):
        """Best-effort hard-stop. Logs and swallows proxy errors so a
        halt issued on a failing path (``finally`` blocks,
        ``KeyboardInterrupt`` handlers) never masks the real exception.
        """
        try:
            self._proxy.send_command("halt")
        except (RuntimeError, TimeoutError) as exc:
            self.logger.warning("halt skipped: %s", exc)

    def reset_step_position(self, az_step=0, el_step=0):
        """Define the current physical pose as the given step counts
        (default origin). No motion — intentionally bypasses the travel
        guard; used by MotorHomer to re-zero the count at converged home."""
        self._proxy.send_command(
            "reset_step_position", az_step=az_step, el_step=el_step
        )

    def _motor_status(self):
        try:
            return self._reader.get("motor")
        except KeyError:
            return None

    @staticmethod
    def _is_moving(status):
        """True if either axis has not reached its target in *status*.

        The negation of the at-rest condition. Mirrors
        :meth:`picohost.motor.PicoMotor.is_moving`, but reads a Redis
        metadata snapshot dict rather than an in-process ``last_status``.
        """
        return status.get("az_pos") != status.get(
            "az_target_pos"
        ) or status.get("el_pos") != status.get("el_target_pos")

    def _wait_for_start(self, axis, before_target, stop_event=None):
        """Block until a just-issued move registers, bounded by
        :attr:`start_timeout_s`.

        After ``send_command`` returns, the metadata snapshot can still
        show the pre-command at-rest state (``pos == target``) for up to
        one producer status cadence. Returning from the move's wait on
        that stale frame is the home double-move bug — the next axis
        command fires while this one is still moving. So before handing
        off to :meth:`_wait_for_stop`, wait until the snapshot reflects
        the command, via either:

        * **acknowledgement** — ``{axis}_target_pos`` changed from
          ``before_target`` (captured before the send). This fires even
          for moves that complete faster than one status tick, so a
          quick jog is not penalised by ``start_timeout_s``.
        * **motion** — either axis is observed off-target.

        A move whose target never changes and never moves is a genuine
        no-op (e.g. homing an axis already at step 0); the
        ``start_timeout_s`` bound returns instead of hanging.
        ``axis``/``before_target`` may be ``None`` (unknown action or no
        prior status) — the motion check carries it in that case.
        """
        deadline = time.monotonic() + self.start_timeout_s
        while time.monotonic() < deadline:
            if stop_event is not None and stop_event.is_set():
                return
            status = self._motor_status()
            if status is not None:
                acknowledged = (
                    axis is not None
                    and before_target is not None
                    and status.get(f"{axis}_target_pos") != before_target
                )
                if acknowledged or self._is_moving(status):
                    return
            time.sleep(self.poll_interval_s)

    def _wait_for_stop(self, timeout=None, stop_event=None, axis=None):
        """Block until the motor's position equals its target on both axes.

        Mirrors the progress-reset stall detection in
        ``picohost.motor.PicoMotor.wait_for_stop``: the timer resets
        whenever ``(az_pos, el_pos)`` changes, so a slow move never
        trips the stall guard.

        If ``stop_event`` is supplied and set, the wait halts the motor
        and returns mid-move — cooperative cancellation for callers that
        drive ``home`` from a background thread (``motor_manual.py``).
        ``scan`` does not pass an event here (it cancels between moves),
        so its semantics are unchanged.

        If ``axis`` is supplied, the sensor fence is polled on every
        iteration. A breach calls :meth:`halt` and re-raises
        :class:`MotorLimitError` immediately, aborting the move
        mid-flight.
        """
        timeout = self.stall_timeout_s if timeout is None else timeout
        t = time.monotonic()
        last_pos = None
        while True:
            if stop_event is not None and stop_event.is_set():
                self.halt()
                return
            if axis is not None:
                try:
                    self._check_sensor_fence(axis)
                except MotorLimitError:
                    self.halt()
                    raise
            status = self._motor_status()
            if status is None:
                if time.monotonic() - t >= timeout:
                    raise TimeoutError(
                        f"No motor metadata within {timeout:.1f}s"
                    )
                time.sleep(self.poll_interval_s)
                continue
            if not self._is_moving(status):
                return
            pos = (status.get("az_pos"), status.get("el_pos"))
            if pos != last_pos:
                last_pos = pos
                t = time.monotonic()
            elif time.monotonic() - t >= timeout:
                raise TimeoutError(
                    f"Motor stalled for {timeout:.1f}s without progress"
                )
            time.sleep(self.poll_interval_s)

    def _await_initial_status(self, timeout=5.0):
        """Block until the manager's reader thread has published at
        least one motor status to Redis.

        The firmware-side ``wait_for_start`` handshake inside every
        move helper calls ``is_moving`` → ``_require_status``, which
        raises :class:`RuntimeError` if ``last_status`` is still empty.
        A scan that fires immediately after ``PicoManager`` boot can
        race the reader thread; this method gives the manager a bounded
        grace window to pump the first status packet before we start
        issuing commands.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._motor_status() is not None:
                return
            time.sleep(0.05)
        raise TimeoutError(
            f"No motor status within {timeout:.1f}s — "
            "is PicoManager running and the motor pico registered?"
        )

    def _send_and_wait(
        self, action, *, label, timeout=None, stop_event=None, **kwargs
    ):
        """Send a single move command and block until the motor stops.

        The whole send-then-wait window is wrapped in
        ``coord.motion_section`` so per-move serialization (when
        enabled) is enforced at the lowest level — every public mover
        on this class composes from this helper, so neither callers
        nor subclasses have to remember to take the lock.

        The wait is two phases: :meth:`_wait_for_start` blocks until the
        command is reflected in the metadata snapshot, then
        :meth:`_wait_for_stop` blocks until motion ends. The start phase
        is what makes a move reliably one-axis-at-a-time: without it a
        stale at-rest snapshot read immediately after the send would let
        the next axis command fire mid-move (the home double-move bug).
        ``stop_event`` is forwarded to both for cooperative cancellation.
        """
        axis = (
            "az"
            if action.startswith("az_")
            else "el"
            if action.startswith("el_")
            else None
        )
        with self._coord.motion_section(label=label):
            before_target = self._axis_target(axis)
            self._check_target_limit(axis, action, before_target, kwargs)
            self._check_sensor_fence(axis)
            self._proxy.send_command(action, **kwargs)
            self._wait_for_start(axis, before_target, stop_event=stop_event)
            if stop_event is not None and stop_event.is_set():
                self.halt()
                return
            self._wait_for_stop(
                timeout=timeout, stop_event=stop_event, axis=axis
            )

    def _axis_target(self, axis):
        """Current ``{axis}_target_pos`` from the snapshot, or ``None``.

        Captured before a send so :meth:`_wait_for_start` can detect the
        firmware acknowledging the new target.
        """
        if axis is None:
            return None
        status = self._motor_status()
        if status is None:
            return None
        return status.get(f"{axis}_target_pos")

    def _resulting_deg(self, action, before_target_steps, kwargs):
        """Absolute resulting axis position in degrees for a move action,
        or None for actions without a positional target (e.g. ``halt``).

        Handles the three move shapes: absolute degrees
        (``*_target_deg``), absolute steps (``*_target_steps``), and
        relative degrees (``*_move_deg``, added to the current target).
        """
        if action.endswith("_target_deg"):
            return float(kwargs["target_deg"])
        if action.endswith("_target_steps"):
            return self._cal.steps_to_deg(int(kwargs["target_steps"]))
        if action.endswith("_move_deg"):
            base = (
                0 if before_target_steps is None else int(before_target_steps)
            )
            return self._cal.steps_to_deg(
                base + self._cal.deg_to_steps(float(kwargs["delta_deg"]))
            )
        return None

    def _check_target_limit(self, axis, action, before_target_steps, kwargs):
        """Raise MotorLimitError if the resulting absolute position for
        *axis* falls outside its configured window. No-op for axis-less
        actions or actions with no positional target."""
        if not self.enforce_limits:
            return
        if axis is None:
            return
        deg = self._resulting_deg(action, before_target_steps, kwargs)
        if deg is None:
            return
        lo, hi = self.az_limits_deg if axis == "az" else self.el_limits_deg
        if not (lo <= deg <= hi):
            raise MotorLimitError(
                f"{axis} move to {deg:.1f} deg outside safe window "
                f"[{lo:.1f}, {hi:.1f}]; refusing to send {action}."
            )

    def _read_fence_sensors(self):
        """(pot_az_voltage, imu_el_deg) from the snapshot; each None if
        unavailable. ConnectionError (panda down) yields (None, None) so a
        sensor-less move is allowed rather than crashing — the commanded
        guard still applies.

        The elevation reading comes from :func:`.el_sensor.read_el_estimate`:
        ``imu_el`` (signed) is the primary; ``imu_az`` (|θ|) is the
        magnitude-only failover when ``imu_el`` is absent.  Both absent or a
        connection error yields ``None``.  The fence treats ``el`` as a
        magnitude vs a symmetric window, so the magnitude-only failover is
        handled correctly without sign."""
        try:
            pot_v = (self._reader.get("potmon") or {}).get("pot_az_voltage")
        except (KeyError, redis.exceptions.ConnectionError):
            pot_v = None
        # IMU cross-check warning suppressed on the high-frequency fence
        # path (~10 Hz inside _wait_for_stop); the homer's settle-cadence
        # read (MotorHomer._read_sensors) and the live-status dashboard
        # surface IMU disagreement at a sane rate.
        el = read_el_estimate(self._reader, logger=None).el_deg
        return pot_v, el

    def _check_sensor_fence(self, axis):
        """Raise MotorLimitError if a present sensor reading shows the
        relevant axis already outside its configured raw-sensor window.
        No-op for an unset window or a missing reading."""
        if not self.enforce_limits:
            return
        pot_v, el = self._read_fence_sensors()
        if axis in (None, "az") and self.pot_az_v_limits and pot_v is not None:
            lo, hi = self.pot_az_v_limits
            if not (lo <= pot_v <= hi):
                raise MotorLimitError(
                    f"pot_az_voltage {pot_v:.3f} V outside safe window "
                    f"[{lo:.3f}, {hi:.3f}]; refusing az move."
                )
        if axis in (None, "el") and self.imu_el_limits_deg and el is not None:
            lo, hi = self.imu_el_limits_deg
            if not (lo <= el <= hi):
                raise MotorLimitError(
                    f"imu el {el:.1f} deg outside safe window "
                    f"[{lo:.1f}, {hi:.1f}]; refusing el move."
                )

    def move_to(
        self,
        *,
        az_deg=None,
        el_deg=None,
        axis_order=("az", "el"),
        timeout=None,
    ):
        """Move to an absolute ``(az, el)`` position in degrees.

        Either or both axes may be supplied. Axes move sequentially in
        ``axis_order`` (default az then el — matches the mechanical
        safety constraint that only one motor moves at a time, see
        :meth:`home`). Each per-axis send is wrapped in a fresh
        ``motion_section`` so the lock releases between axes when
        serialization is enabled.

        Parameters
        ----------
        az_deg, el_deg : float or None
            Target position in degrees. ``None`` skips that axis.
        axis_order : tuple of str
            Order in which the supplied axes are driven. Entries that
            don't correspond to a supplied target are skipped silently
            (so passing only ``az_deg`` does just the az move).
        timeout : float or None
            Per-move stall timeout override.
        """
        moves = []
        for axis in axis_order:
            if axis == "az" and az_deg is not None:
                moves.append(("az_target_deg", float(az_deg), "move_to az"))
            elif axis == "el" and el_deg is not None:
                moves.append(("el_target_deg", float(el_deg), "move_to el"))
        if not moves:
            return
        self._await_initial_status()
        for action, target_deg, label in moves:
            self._send_and_wait(
                action,
                label=label,
                timeout=timeout,
                target_deg=target_deg,
            )

    def home(self, stop_event=None):
        """Drive both axes to step position 0, one at a time.

        Only one motor moves at once: az homes first, then el. Running
        both simultaneously is a mechanical-safety hazard on the rig
        and matches the historical ``picohost`` script behavior.

        ``stop_event`` (optional) enables cooperative cancellation: a
        set event halts the in-flight axis and skips the next one, so a
        background home (``motor_manual.py``) can be aborted mid-move.
        """
        self._await_initial_status()
        self._send_and_wait(
            "az_target_steps",
            label="home az",
            target_steps=0,
            stop_event=stop_event,
        )
        if stop_event is not None and stop_event.is_set():
            return
        self._send_and_wait(
            "el_target_steps",
            label="home el",
            target_steps=0,
            stop_event=stop_event,
        )

    def jog_az(self, delta_deg, *, stop_event=None):
        """Jog az by ``delta_deg`` degrees, blocking until the move stops.

        Relative to the current az target. Routes through
        :meth:`_send_and_wait`, so the move is serialized start->stop the
        same way absolute moves are: a cross-axis jog issued after this
        returns cannot overlap it. Backs the interactive jogger in
        :class:`~eigsep_observing.motor_zeroer.MotorZeroer` — blocking is
        what enforces one-motor-at-a-time there.
        """
        self._jog("az_move_deg", delta_deg, stop_event=stop_event)

    def jog_el(self, delta_deg, *, stop_event=None):
        """Jog el by ``delta_deg`` degrees, blocking until the move stops.

        See :meth:`jog_az`.
        """
        self._jog("el_move_deg", delta_deg, stop_event=stop_event)

    def _jog(self, action, delta_deg, *, stop_event=None):
        self._send_and_wait(
            action,
            label=action,
            delta_deg=float(delta_deg),
            stop_event=stop_event,
        )

    def scan(
        self,
        az_range_deg=None,
        el_range_deg=None,
        el_first=False,
        repeat_count=None,
        pause_s=None,
        sleep_between=None,
        stop_event=None,
    ):
        """Run the beam-scan grid.

        Homes both axes to ``(0, 0)`` before the first pass and after
        normal completion. Use an earlier ``reset_step_position``
        command (e.g. via ``motor_manual.py``) to define where home
        is. Mirrors the serpentine traversal of
        ``picohost.motor.PicoMotor.scan``: axis2 reverses direction at
        every step of axis1, and axis1 reverses at every repeat.

        Parameters
        ----------
        az_range_deg, el_range_deg : array_like
            Grid values in degrees. Defaults to
            ``np.arange(-180.0, 180.0, 5)`` on each axis, matching the
            firmware-side scan default.
        el_first : bool
            If True, azimuth is the outer loop; otherwise elevation is.
        repeat_count : int or None
            Number of full-grid passes. ``None`` means "run until
            stopped" (Ctrl-C or ``stop_event``).
        pause_s : float or None
            Seconds to pause at each grid point. ``None`` means continuous
            axis2 sweep (endpoints only) rather than a per-point grid.
        sleep_between : float or None
            Seconds to sleep between passes when ``repeat_count`` is set.
        stop_event : threading.Event or None
            Cooperative cancellation signal checked between moves.

        Raises
        ------
        TimeoutError
            If any individual move fails to make progress within
            ``stall_timeout_s``.
        """
        if az_range_deg is None:
            az_range_deg = np.arange(-180.0, 180.0, 5)
        if el_range_deg is None:
            el_range_deg = np.arange(-180.0, 180.0, 5)

        self.home()

        if el_first:
            mv_axis1_action = "az_target_deg"
            mv_axis2_action = "el_target_deg"
            axis1_label = "scan az (outer)"
            axis2_label = "scan el (inner)"
            axis1_rng = np.asarray(az_range_deg).copy()
            axis2_rng = np.asarray(el_range_deg).copy()
        else:
            mv_axis1_action = "el_target_deg"
            mv_axis2_action = "az_target_deg"
            axis1_label = "scan el (outer)"
            axis2_label = "scan az (inner)"
            axis1_rng = np.asarray(el_range_deg).copy()
            axis2_rng = np.asarray(az_range_deg).copy()

        def _cancelled():
            return stop_event is not None and stop_event.is_set()

        completed = False
        i = 0
        try:
            while True:
                if _cancelled():
                    break
                if repeat_count is not None and i >= repeat_count:
                    completed = True
                    break
                for val1 in axis1_rng:
                    if _cancelled():
                        break
                    self.logger.info("MOVE AXIS 1 TO %s", val1)
                    self._send_and_wait(
                        mv_axis1_action,
                        label=axis1_label,
                        target_deg=float(val1),
                    )
                    if _cancelled():
                        break
                    if pause_s is None:
                        self.logger.info(
                            "MOVE AXIS 2 FROM %s TO %s",
                            axis2_rng[0],
                            axis2_rng[-1],
                        )
                        self._send_and_wait(
                            mv_axis2_action,
                            label=axis2_label,
                            target_deg=float(axis2_rng[0]),
                        )
                        if _cancelled():
                            break
                        self._send_and_wait(
                            mv_axis2_action,
                            label=axis2_label,
                            target_deg=float(axis2_rng[-1]),
                        )
                    else:
                        for val2 in axis2_rng:
                            if _cancelled():
                                break
                            self._send_and_wait(
                                mv_axis2_action,
                                label=axis2_label,
                                target_deg=float(val2),
                            )
                            if stop_event is not None:
                                if stop_event.wait(pause_s):
                                    break
                            else:
                                time.sleep(pause_s)
                    axis2_rng = axis2_rng[::-1]
                axis1_rng = axis1_rng[::-1]
                i += 1
                if sleep_between is not None:
                    self.logger.info("Sleeping for %ss", sleep_between)
                    if stop_event is not None:
                        if stop_event.wait(sleep_between):
                            break
                    else:
                        time.sleep(sleep_between)
        finally:
            self.halt()

        if completed:
            self.home()
