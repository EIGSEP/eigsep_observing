# Field-debug notebooks

Quick-run notebooks for diagnosing sensors in the field. Each one loads a single
HDF5 file and produces a fast visual — point it at a file, "Run All", read the
plot. Designed to work on **either**:

- a recorder metadata file (`metadata_*.h5`, raw per-sample streams), **or**
- a regular correlation file (`*.h5`, where the per-integration metadata ships
  inside the file's `metadata` group).

Set the `FILE` variable at the top of a notebook (or leave it blank to auto-pick
the newest `*.h5` nearby) and run all cells.

| notebook | sensors |
|---|---|
| `az_sensors.ipynb` | azimuth: motor steps, potentiometer, IMU (imu_az) |

More notebooks (other sensors: tempctrl, lidar, el-axis, …) go here as they're
written. The deeper one-off analysis lives elsewhere (e.g.
`../motor_pot_imu/`); this folder is for fast field triage.
