"""Interactive standby/resume bring-up tool for the RFI-quiet picos.

Toggle standby (BNO085 held in reset / GRF-250 laser off) on imu_el,
imu_az, and lidar from one REPL. Every command routes through
PicoManager via picohost.proxy.PicoProxy — the same path the observing
loops use — and the new state is read back from the metadata snapshot
(standby / status / laser_firing) and printed for confirmation.

run_tag-exempt by design: standby changes no physical *observing* state
(no switch/motor/VNA) and is an RFI-mitigation toggle that must coexist
with the autonomous driver, so this tool can be run *during* a
panda_observe session (commands serialize at the pico level). It
therefore does NOT claim run_tag.session — see scripts/CLAUDE.md
("coexisting commands").

Standby is in-RAM only on the pico: a device reboot comes back sensing
(IMU) / laser-on (lidar).
"""

from argparse import ArgumentParser
import logging

from eigsep_redis import MetadataSnapshotReader
from picohost.proxy import PicoProxy

from eigsep_observing._scripts_util import add_redis_args, build_transport
from eigsep_observing.standby import STANDBY_DEVICES, set_standby
from eigsep_observing.utils import configure_eig_logger


configure_eig_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


def _read_standby_state(snapshot, device):
    """Latest {standby, status, laser_firing} for ``device``, or {}."""
    snap = snapshot.get().get(device)
    if not snap:
        return {}
    return {
        "standby": snap.get("standby"),
        "status": snap.get("status"),
        "laser_firing": snap.get("laser_firing"),
    }


def _do_toggle(proxy, snapshot, device, on):
    """Send standby/resume, read the state back, return a summary line."""
    result = set_standby(proxy, on)
    state = _read_standby_state(snapshot, device)
    verb = "standby" if on else "resume"
    return (
        f"{device} {verb}: {result} | "
        f"standby={state.get('standby')!r} "
        f"status={state.get('status')!r} "
        f"laser_firing={state.get('laser_firing')!r}"
    )


def _print_menu():
    print()
    print("=== standby manual ===")
    for i, d in enumerate(STANDBY_DEVICES):
        print(f"  [{i}] {d}: '{i}s' standby / '{i}r' resume")
    print("  [as] standby ALL   [ar] resume ALL   [q] quit")


def _print_states(snapshot):
    for d in STANDBY_DEVICES:
        st = _read_standby_state(snapshot, d)
        print(
            f"  {d}: standby={st.get('standby')!r} "
            f"status={st.get('status')!r} "
            f"laser_firing={st.get('laser_firing')!r}"
        )


def _repl(proxies, snapshot):
    while True:
        _print_menu()
        _print_states(snapshot)
        try:
            choice = input("Select> ").strip().lower()
        except EOFError:
            print()
            return
        if not choice:
            continue
        if choice == "q":
            return
        if choice in ("as", "ar"):
            on = choice == "as"
            for d in STANDBY_DEVICES:
                print("-> " + _do_toggle(proxies[d], snapshot, d, on))
            continue
        if (
            len(choice) == 2
            and choice[0].isdigit()
            and choice[1] in ("s", "r")
        ):
            idx = int(choice[0])
            if 0 <= idx < len(STANDBY_DEVICES):
                d = STANDBY_DEVICES[idx]
                print(
                    "-> "
                    + _do_toggle(proxies[d], snapshot, d, choice[1] == "s")
                )
                continue
        print(f"  ?? unrecognized input: {choice!r}")


def _parse_args():
    parser = ArgumentParser(
        description="Interactive standby/resume for imu_el/imu_az/lidar."
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help="Run against a fakeredis-backed DummyPandaClient",
    )
    add_redis_args(parser)
    return parser.parse_args()


def main():
    args = _parse_args()
    transport = build_transport(
        args.dummy, host=args.redis_host, real_port=args.redis_port
    )
    # No run_tag.session — coexisting command, see module docstring.
    proxies = {
        d: PicoProxy(d, transport, source="standby_manual")
        for d in STANDBY_DEVICES
    }
    snapshot = MetadataSnapshotReader(transport)
    try:
        _repl(proxies, snapshot)
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
