import json
import time
from eigsep_observing import EigsepRedis

r = EigsepRedis("10.10.10.11")

while True:
    try:
        #m = r.get_live_metadata()
        m = r.get_live_metadata(keys=["lidar", "lidar_ts"])
        #m = r.get_live_metadata(keys=["imu_panda", "imu_antenna", "lidar"])
        print(json.dumps(m, indent=2, sort_keys=False))
        time.sleep(1.)
    except KeyboardInterrupt:
        break
