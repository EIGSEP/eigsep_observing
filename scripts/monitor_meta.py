import json
import time
from eigsep_observing import EigsepObsRedis

r = EigsepObsRedis("10.10.10.11")

while True:
    try:
        # m = r.metadata_snapshot.get()
        m = r.metadata_snapshot.get(keys=["lidar", "lidar_ts"])
        # m = r.metadata_snapshot.get(keys=["imu_el", "imu_az", "lidar"])
        print(json.dumps(m, indent=2, sort_keys=False))
        time.sleep(1.0)
    except KeyboardInterrupt:
        break
