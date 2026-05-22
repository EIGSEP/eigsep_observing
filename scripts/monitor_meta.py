import json
import time

from eigsep_redis import MetadataSnapshotReader, Transport

from eigsep_observing import run_tag


def main():
    transport = Transport("10.10.10.11")
    snapshot = MetadataSnapshotReader(transport)
    with run_tag.session(transport, "monitor_meta"):
        while True:
            try:
                m = snapshot.get()
                print(json.dumps(m, indent=2, sort_keys=False))
                time.sleep(1.0)
            except KeyboardInterrupt:
                break


if __name__ == "__main__":
    main()
