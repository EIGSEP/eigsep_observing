# eigsep_observing.live_status

The field live-status dashboard: aggregates instrument health signals and
serves them as a Flask app for monitoring an active deployment.

## Layout

| Module | Purpose |
|---|---|
| `aggregator.py` | `LiveStatusAggregator`/`StateSnapshot` — background aggregator that polls signal sources and holds the current state. |
| `app.py` | `create_app` — the Flask app serving the dashboard (`static/`, `templates/`). |
| `signals.py` | `Signal`/`SIGNAL_REGISTRY` — the registry of monitored signals and their default thresholds/enabled set. |
| `thresholds.py` | `Thresholds` — the threshold classifier backing the signal registry's health calls. |
| `calibration.py` | First-order Y-factor calibration computed live for the dashboard — the same formalism `abscal/` deliberately reuses so the field dashboard and offline calibration pipeline can't silently disagree. |
| `orientation.py` | Per-sensor az/el consensus for the pointing panel. |
| `snap_probe.py` | SNAP FPGA TCP reachability probe. |

## Recent changes

- 2026-09-15 (`software-engineer`): added this file (README-convention
  retrofit, fleet-wide consolidation pass).
