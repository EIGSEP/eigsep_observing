import json
import time

from eigsep_redis import MetadataSnapshotReader, Transport

transport = Transport("10.10.10.11")
snapshot = MetadataSnapshotReader(transport)

while True:
    try:
        # m = snapshot.get()
        m = snapshot.get(keys=["lidar", "lidar_ts"])
        # m = snapshot.get(keys=["imu_el", "imu_az", "lidar"])
        print(json.dumps(m, indent=2, sort_keys=False))
        time.sleep(1.0)
    except KeyboardInterrupt:
        break
