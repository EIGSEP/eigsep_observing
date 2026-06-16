import json
import time

from eigsep_redis import MetadataSnapshotReader, Transport


def main():
    transport = Transport("10.10.10.11")
    snapshot = MetadataSnapshotReader(transport)
    # Passive readout: no run_tag.session, by design. Snapshot-only
    # (MetadataSnapshotReader), no commands or files, so it changes no
    # physical state and must coexist with the active driver it watches.
    # See imu_manual.py / scripts/CLAUDE.md for the active-vs-passive rule.
    while True:
        try:
            m = snapshot.get()
            print(json.dumps(m, indent=2, sort_keys=False))
            time.sleep(1.0)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
