"""
Client-side motor zeroing helper.

Backs the interactive ``motor_manual.py`` script. All state-machine
logic lives here so the curses frame reduces to keystroke pumping +
rendering, and the same logic is reachable by unit tests without a
terminal.
"""

import inspect
import logging
import time

from eigsep_redis import MetadataSnapshotReader
from picohost.motor import PicoMotor
from picohost.proxy import PicoProxy

logger = logging.getLogger(__name__)

# Linux-only: curses delivers Enter as "\n" on the terminals we target
# (Ubuntu + Raspberry Pi). Other platforms may send KEY_ENTER / "\r".
_KEY_ENTER = ord("\n")
_REQUIRE_STATUS_RETRY_S = 0.5


def _default_cal_motor():
    """A serial-less :class:`PicoMotor` carrying only the calibration
    constants, used purely to convert step counts to axis degrees for
    display.

    ``PicoManager`` constructs the real motor pico with ``PicoMotor``'s
    constructor defaults and never overrides ``step_angle_deg`` /
    ``gear_teeth`` / ``microstep`` (see ``picohost.manager``), so reusing
    those defaults here makes the displayed degrees match the mover's
    own ``deg_to_steps`` exactly instead of duplicating the gear math.
    Pulling the values from the constructor signature keeps them in
    lockstep with picohost. The ``__new__`` bypass (no serial I/O)
    mirrors ``contract_tests.test_producer_contracts``.
    """
    sig = inspect.signature(PicoMotor.__init__)
    cal = PicoMotor.__new__(PicoMotor)
    for attr in ("step_angle_deg", "gear_teeth", "microstep"):
        setattr(cal, attr, sig.parameters[attr].default)
    return cal


_CAL_MOTOR = _default_cal_motor()


def _format_pos(raw):
    """Render a raw motor step count as ``"<steps> (<deg> deg)"``.

    Non-numeric values (sentinels like ``"?"`` for a missing key) are
    returned verbatim so a partial status dict never crashes the UI.
    """
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return str(raw)
    steps = int(round(raw))
    return f"{steps} ({_CAL_MOTOR.steps_to_deg(steps):.1f} deg)"


class MotorZeroer:
    """Drive motor jog/zero commands through ``PicoManager`` via Redis.

    Parameters mirror :class:`eigsep_observing.motor_client.MotorClient`
    for consistency — the same delay values apply to manual jogging.
    """

    def __init__(
        self,
        transport,
        *,
        az_up_delay_us=2400,
        az_dn_delay_us=300,
        el_up_delay_us=2400,
        el_dn_delay_us=600,
        source="motor_zeroer",
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
        self.logger = logger
        # Two-step zero guard: Enter arms this, a deliberate 'y' commits.
        self._pending_zero = False

    @property
    def is_available(self):
        return self._proxy.is_available

    @property
    def pending_zero(self):
        """True while a zero confirmation is armed — Enter has been
        pressed and we are awaiting a deliberate 'y'. The curses layer
        reads this to render the confirmation prompt.
        """
        return self._pending_zero

    def set_delay(self, **overrides):
        self._delay_kwargs.update(overrides)
        self._proxy.send_command("set_delay", **self._delay_kwargs)

    def halt(self):
        try:
            self._proxy.send_command("halt")
        except (RuntimeError, TimeoutError) as exc:
            self.logger.warning("halt skipped: %s", exc)

    def _jog(self, action, delta_deg):
        """Invoke a delta-degree move, retrying once if the firmware
        complains about missing status (can happen immediately after a
        reconnect — the reader thread hasn't populated ``last_status``
        yet).
        """
        try:
            self._proxy.send_command(action, delta_deg=float(delta_deg))
        except RuntimeError as exc:
            if "No status" not in str(exc):
                raise
            time.sleep(_REQUIRE_STATUS_RETRY_S)
            self._proxy.send_command(action, delta_deg=float(delta_deg))

    def jog_az(self, delta_deg):
        self._jog("az_move_deg", delta_deg)

    def jog_el(self, delta_deg):
        self._jog("el_move_deg", delta_deg)

    def zero(self):
        """Halt, then set both step counters to 0. After this call the
        current physical position is the scan origin.
        """
        self._proxy.send_command("halt")
        self._proxy.send_command("reset_step_position", az_step=0, el_step=0)

    def status_text(self):
        """Return ``(az_str, el_str, connected)`` for the UI.

        ``connected`` is the heartbeat bool from the proxy — it
        reflects manager liveness and is returned unchanged regardless
        of whether the motor has published metadata yet. When no
        metadata is present (fresh boot, reconnect racing the first
        status packet), the position fields are literal ``"WAITING"``
        / ``"---"`` rather than a fabricated dict.
        """
        connected = self.is_available
        try:
            status = self._reader.get("motor")
        except KeyError:
            return "WAITING", "---", connected
        az_pos = status.get("az_pos", "?")
        el_pos = status.get("el_pos", "?")
        return _format_pos(az_pos), _format_pos(el_pos), connected

    def handle_key(self, ch, deg_state):
        """Advance the zeroing state machine in response to one keystroke.

        Parameters
        ----------
        ch : int
            Keycode from ``curses.getch()`` (``-1`` on no-input means
            the caller should poll again; this method treats it as a
            no-op).
        deg_state : float
            Current jog-step size in degrees.

        Returns
        -------
        (new_deg, should_exit, zeroed) : tuple
            ``new_deg`` — possibly-adjusted jog step size.
            ``should_exit`` — True when the caller should break the
            input loop.
            ``zeroed`` — True if this keystroke committed a successful
            zeroing. Callers use it to choose the exit message.

        Zeroing is two-step: Enter *arms* a confirmation
        (``pending_zero`` becomes True) but does not zero. While armed,
        a deliberate ``y``/``Y`` commits the zero (and exits); any other
        key cancels back to jogging. This guards an accidental Enter
        from redefining scan home.
        """
        if ch == -1:
            # No keypress this tick — leave any armed prompt intact so a
            # ~100ms idle getch() doesn't silently cancel it.
            return deg_state, False, False
        if self._pending_zero:
            return self._confirm_zero(ch, deg_state)
        if ch == _KEY_ENTER:
            if not self.is_available:
                return deg_state, False, False
            # Arm the confirmation; the real zero happens in
            # _confirm_zero once the operator presses 'y'.
            self._pending_zero = True
            return deg_state, False, False
        if not (0 <= ch < 256):
            return deg_state, False, False
        key = chr(ch).lower()
        if key == "q":
            return deg_state, True, False
        if key == "+":
            # Snap back to integer ladder when climbing out of the
            # 0.1 fine-jog floor; otherwise "- - + +" leaves a 0.1
            # offset baked in forever (1.1, 2.1, ...).
            if deg_state < 1.0:
                return 1.0, False, False
            return deg_state + 1, False, False
        if key == "-":
            return max(0.1, deg_state - 1), False, False
        if key in ("u", "d", "l", "r"):
            if not self.is_available:
                return deg_state, False, False
            try:
                if key == "u":
                    self.jog_el(deg_state)
                elif key == "d":
                    self.jog_el(-deg_state)
                elif key == "l":
                    self.jog_az(deg_state)
                else:
                    self.jog_az(-deg_state)
            except (RuntimeError, TimeoutError) as exc:
                self.logger.warning("jog %s failed: %s", key, exc)
        return deg_state, False, False

    def _confirm_zero(self, ch, deg_state):
        """Resolve an armed zero prompt for one keystroke.

        ``y``/``Y`` commits the zero and signals exit; any other key
        cancels back to jogging. The prompt clears either way (callers
        keep it armed only across ``-1`` no-input ticks, handled in
        :meth:`handle_key`). A cancelling key is swallowed here — it is
        not re-interpreted as a jog or step-size change.
        """
        key = chr(ch).lower() if 0 <= ch < 256 else None
        self._pending_zero = False
        if key != "y":
            return deg_state, False, False
        if not self.is_available:
            return deg_state, False, False
        try:
            self.zero()
        except (RuntimeError, TimeoutError) as exc:
            self.logger.warning("zero failed: %s", exc)
            return deg_state, False, False
        return deg_state, True, True
