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

from eigsep_redis import MetadataSnapshotReader
from picohost.proxy import PicoProxy

from .motion_switch import MotionSwitchCoordinator

logger = logging.getLogger(__name__)


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
    source : str
        Identifier stamped on proxy command stream entries.
    coord : MotionSwitchCoordinator or None
        Optional coordinator. When ``None``, the client builds an
        internal coordinator with ``serialize=False`` so standalone use
        (e.g. ``scripts/motor_control.py``) is unchanged.
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
        self.logger = logger
        if coord is None:
            coord = MotionSwitchCoordinator(
                threading.RLock(), serialize=False, logger=self.logger
            )
        self._coord = coord

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

    def _motor_status(self):
        try:
            return self._reader.get("motor")
        except KeyError:
            return None

    def _wait_for_stop(self, timeout=None, stop_event=None):
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
        """
        timeout = self.stall_timeout_s if timeout is None else timeout
        t = time.monotonic()
        last_pos = None
        while True:
            if stop_event is not None and stop_event.is_set():
                self.halt()
                return
            status = self._motor_status()
            if status is None:
                if time.monotonic() - t >= timeout:
                    raise TimeoutError(
                        f"No motor metadata within {timeout:.1f}s"
                    )
                time.sleep(self.poll_interval_s)
                continue
            az_pos = status.get("az_pos")
            el_pos = status.get("el_pos")
            az_target = status.get("az_target_pos")
            el_target = status.get("el_target_pos")
            if az_pos == az_target and el_pos == el_target:
                return
            pos = (az_pos, el_pos)
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
        nor subclasses have to remember to take the lock. ``stop_event``
        is forwarded to :meth:`_wait_for_stop` for cooperative
        cancellation.
        """
        with self._coord.motion_section(label=label):
            self._proxy.send_command(action, **kwargs)
            self._wait_for_stop(timeout=timeout, stop_event=stop_event)

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
