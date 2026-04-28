"""Tests for ``MotionSwitchCoordinator`` mutual exclusion."""

import threading
import time

from eigsep_observing import MotionSwitchCoordinator


def _coord(serialize):
    return MotionSwitchCoordinator(threading.RLock(), serialize=serialize)


def test_serialize_off_motion_section_is_noop():
    """With ``serialize=False`` the motion section must not touch the
    lock — preserves today's ``motor_loop`` byte-for-byte behavior.

    A switch thread holds ``switch_section`` (which always acquires);
    a motion thread under serialize=False must run unblocked.
    """
    coord = _coord(serialize=False)

    switch_holding = threading.Event()
    motion_done = threading.Event()
    release_switch = threading.Event()

    def switch_holder():
        with coord.switch_section():
            switch_holding.set()
            release_switch.wait(timeout=2.0)

    def motion_runner():
        with coord.motion_section(label="test"):
            motion_done.set()

    s = threading.Thread(target=switch_holder, daemon=True)
    s.start()
    assert switch_holding.wait(timeout=1.0)

    m = threading.Thread(target=motion_runner, daemon=True)
    m.start()
    assert motion_done.wait(timeout=1.0), (
        "motion_section must not block when serialize=False"
    )

    release_switch.set()
    s.join(timeout=1.0)
    m.join(timeout=1.0)


def test_serialize_on_motion_blocks_switch():
    """With ``serialize=True`` a motion-side hold must block a switch-
    side acquire from another thread until the motion section exits.
    """
    coord = _coord(serialize=True)

    motion_holding = threading.Event()
    release_motion = threading.Event()
    switch_acquired = threading.Event()

    def motion_holder():
        with coord.motion_section(label="test"):
            motion_holding.set()
            release_motion.wait(timeout=2.0)

    def switch_acquirer():
        with coord.switch_section():
            switch_acquired.set()

    m = threading.Thread(target=motion_holder, daemon=True)
    m.start()
    assert motion_holding.wait(timeout=1.0)

    s = threading.Thread(target=switch_acquirer, daemon=True)
    s.start()
    assert not switch_acquired.wait(timeout=0.2), (
        "switch_section acquired while motion_section held the lock"
    )

    release_motion.set()
    assert switch_acquired.wait(timeout=1.0), (
        "switch_section did not acquire after motion released"
    )
    m.join(timeout=1.0)
    s.join(timeout=1.0)


def test_motion_section_releases_between_calls():
    """Two sequential motion_sections must let a switch_section
    interpose between them — this is the per-move granularity that
    keeps a multi-minute scan from blocking VNA entirely.
    """
    coord = _coord(serialize=True)

    seen = []

    def motion_block(label):
        with coord.motion_section(label=label):
            seen.append(f"motion:{label}")
            time.sleep(0.05)

    def switch_block():
        with coord.switch_section():
            seen.append("switch")

    motion_block("first")
    s = threading.Thread(target=switch_block, daemon=True)
    s.start()
    s.join(timeout=1.0)
    motion_block("second")

    assert seen == ["motion:first", "switch", "motion:second"]


def test_switch_session_can_nest_motion_with_rlock():
    """The no-switch-observation script holds an outer
    ``switch_session`` (acquires switch_section on the underlying
    RLock) while inner per-move ``motion_section`` calls re-acquire
    the same lock from the same thread. With a plain Lock this would
    deadlock; the RLock change in PandaClient is load-bearing.
    """
    coord = _coord(serialize=True)

    seen = []
    with coord.switch_section():
        seen.append("outer-switch")
        with coord.motion_section(label="nested"):
            seen.append("inner-motion")
        seen.append("outer-switch-resume")

    assert seen == ["outer-switch", "inner-motion", "outer-switch-resume"]


def test_serialize_setter_flips_at_runtime():
    """Standalone scripts force ``serialize=True`` after constructing
    the panda client; the setter must propagate to motion_section.
    """
    coord = _coord(serialize=False)
    assert coord.serialize is False
    coord.serialize = True
    assert coord.serialize is True

    motion_holding = threading.Event()
    release_motion = threading.Event()
    switch_acquired = threading.Event()

    def motion_holder():
        with coord.motion_section(label="post-flip"):
            motion_holding.set()
            release_motion.wait(timeout=2.0)

    def switch_acquirer():
        with coord.switch_section():
            switch_acquired.set()

    m = threading.Thread(target=motion_holder, daemon=True)
    m.start()
    assert motion_holding.wait(timeout=1.0)
    s = threading.Thread(target=switch_acquirer, daemon=True)
    s.start()
    assert not switch_acquired.wait(timeout=0.2)
    release_motion.set()
    assert switch_acquired.wait(timeout=1.0)
    m.join(timeout=1.0)
    s.join(timeout=1.0)


def test_motion_section_propagates_exception():
    """An exception raised inside a held motion section must release
    the lock — otherwise a transient TimeoutError in
    ``MotorScanner._wait_for_stop`` would wedge the next switch.
    """
    coord = _coord(serialize=True)

    class _Boom(Exception):
        pass

    try:
        with coord.motion_section(label="boom"):
            raise _Boom()
    except _Boom:
        pass

    # Lock must be released — a fresh switch_section in another thread
    # acquires immediately.
    acquired = threading.Event()

    def acquirer():
        with coord.switch_section():
            acquired.set()

    t = threading.Thread(target=acquirer, daemon=True)
    t.start()
    assert acquired.wait(timeout=1.0)
    t.join(timeout=1.0)
