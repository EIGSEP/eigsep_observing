"""Interactive RF switch bring-up tool.

Pick a switch state from a numbered menu (or ``c`` to cycle through
them all with a pause between each). Every command goes through
PicoManager via :class:`picohost.proxy.PicoProxy`, the same path the
observing loops use — so a switch that works here will work in
production. After each switch, the script reads ``sw_state_name`` back
from the metadata snapshot and prints it; cross-check against the
live-status dashboard tile.

Run alongside ``scripts/live_status.py`` so you can see the rfswitch
tile flip in the browser as you exercise each state.
"""

from argparse import ArgumentParser
import logging
import time

from eigsep_redis import MetadataSnapshotReader
from picohost.base import PicoRFSwitch
from picohost.proxy import PicoProxy

from eigsep_observing._scripts_util import build_transport, require_pico
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

# Canonical state list — sourced from the firmware-side class so a
# new state added in firmware shows up here without a code change.
STATES = list(PicoRFSwitch.path_str)


def _print_menu():
    print()
    print("=== RF switch states ===")
    for i, state in enumerate(STATES):
        print(f"  [{i:>2}] {state}")
    print("  [ c] cycle through all states (2 s dwell)")
    print("  [ q] quit")


def _read_current_state(snapshot):
    try:
        snap = snapshot.get("rfswitch")
    except KeyError:
        return None
    return snap.get("sw_state_name") if snap else None


def _switch(proxy, state):
    """Issue the switch command and return True on success.

    ``send_command`` returns ``None`` when the pico's heartbeat is
    missing, raises ``TimeoutError`` if PicoManager doesn't respond
    within the proxy timeout, and ``RuntimeError`` if the firmware
    reports a failure. All three cases are operator-actionable here —
    print a one-line summary and let the caller decide whether to keep
    going.
    """
    try:
        result = proxy.send_command("switch", state=state)
    except (TimeoutError, RuntimeError) as exc:
        print(f"  !! switch to {state} failed: {type(exc).__name__}: {exc}")
        return False
    if result is None:
        print(f"  !! switch to {state} failed: rfswitch unavailable")
        return False
    return True


def _do_switch(proxy, snapshot, state, *, settle_s=0.4):
    print(f"-> switching to {state}")
    if not _switch(proxy, state):
        return
    # Give PicoManager a tick to publish the new sw_state_name.
    time.sleep(settle_s)
    seen = _read_current_state(snapshot)
    if seen == state:
        print(f"   metadata confirms sw_state_name={seen}")
    else:
        print(f"   metadata shows sw_state_name={seen!r} (expected {state})")


def _cycle(proxy, snapshot, dwell_s):
    for state in STATES:
        _do_switch(proxy, snapshot, state)
        time.sleep(dwell_s)


def _repl(proxy, snapshot, cycle_dwell_s):
    while True:
        _print_menu()
        current = _read_current_state(snapshot)
        print(f"Current sw_state_name: {current!r}")
        try:
            choice = input("Select> ").strip().lower()
        except EOFError:
            print()
            return
        if not choice:
            continue
        if choice == "q":
            return
        if choice == "c":
            _cycle(proxy, snapshot, cycle_dwell_s)
            continue
        try:
            idx = int(choice)
        except ValueError:
            print(f"  ?? unrecognized input: {choice!r}")
            continue
        if not 0 <= idx < len(STATES):
            print(f"  ?? index {idx} out of range")
            continue
        _do_switch(proxy, snapshot, STATES[idx])


def _parse_args():
    parser = ArgumentParser(
        description="Interactive RF switch bring-up: drive each state by hand."
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient",
    )
    parser.add_argument(
        "--cycle-dwell",
        type=float,
        default=2.0,
        help="Seconds to dwell at each state in cycle (c) mode.",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(args.dummy)
    proxy = PicoProxy("rfswitch", transport, source="rfswitch_manual")
    require_pico(proxy)
    snapshot = MetadataSnapshotReader(transport)
    try:
        _repl(proxy, snapshot, args.cycle_dwell)
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
