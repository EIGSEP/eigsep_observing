# Live Status Feature Requests

Planned features for the live status app that monitors the system via Redis.

## ADC RMS Monitoring

- Periodically take ADC snapshots from the SNAP and compute RMS per input
- Push RMS values to Redis as metadata
- Flag if RMS falls outside the 10-20 ADC counts target range
- Lightweight check: one snapshot + `np.std` every few minutes
- Requires SNAP-side code to capture and publish (e.g. in the observing loop)

## ADC Clipping Detection

- Flag if any ADC samples hit -128 or 127 (saturation)
- Track clipping fraction per input — even occasional clipping corrupts correlator data
- Can be computed alongside the RMS check at no extra cost

## Accumulation Counter Health

- Monitor `corr_acc_cnt` — should increment steadily
- Flag if it stalls (FPGA hang) or jumps (missed integrations)
- Compare expected vs actual cadence based on `corr_acc_len`

## Pico Sensor Connectivity

- Flag if any Pico device (IMU, thermometers, lidar) stops reporting
- Track time since last update per sensor — stale data indicates a disconnect
- Surface `eigsep_redis.metadata` WARNING logs ("key … is stale") — `MetadataSnapshotReader.get` already compares each key's `{key}_ts` against `max_age_s` (default 30 s) and warns on stale reads; the app should either subscribe to that logger or replicate the `_ts` vs. now check directly against the metadata hash

## RF Switch State

- Display current switch position and time since last switch
- Flag if switch hasn't cycled on the expected schedule (from obs_config.yaml)

## Temperature Trends

- Track thermometer readings over time
- Flag if temperature drifts outside acceptable range or changes rapidly
- Important for gain stability of the analog signal chain

## Data Flow Health

- Monitor Redis stream sizes — are correlator and sensor data streams growing?
- Flag if any stream stops receiving new entries
- Track data write rate to disk (if applicable)
