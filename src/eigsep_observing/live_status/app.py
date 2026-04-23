"""Flask app for the live-status dashboard.

Thin JSON API over the in-memory :class:`LiveStatusAggregator`.
Responses follow a ``{ok, data, warnings}`` envelope so the front-end
can render warnings alongside values without a special-case code path.

The app is intentionally local-only: bind to ``127.0.0.1`` in the
entry-point script. Auth, TLS, and non-loopback hosting are out of
scope for v1 — remote viewing is an SSH-tunnel problem, not a
Flask-Login problem.
"""

from __future__ import annotations

import time
from typing import Any, Optional

import numpy as np
from flask import Flask, Response, jsonify, render_template
from plotly.offline import get_plotlyjs

from .aggregator import LiveStatusAggregator, StateSnapshot
from .signals import enabled_signals
from .thresholds import Thresholds


def _envelope(data: Any, warnings: Optional[list] = None) -> dict:
    return {"ok": True, "data": data, "warnings": warnings or []}


def _corr_payload(state: StateSnapshot) -> dict:
    pairs_out: dict[str, dict] = {}
    freqs = state.corr_freqs
    if state.corr_pairs and freqs is not None:
        for pair, arr in state.corr_pairs.items():
            if arr is None or arr.size == 0:
                continue
            # Aggregator stores the output of reshape_data(avg_even_odd=True):
            #   autos   → shape (ntimes=1, nchan) int32
            #   crosses → shape (ntimes=1, nchan, 2) int32
            if arr.ndim == 3 and arr.shape[-1] == 2:
                real = arr[0, :, 0]
                imag = arr[0, :, 1]
                complex_vals = real.astype(np.float64) + 1j * imag.astype(
                    np.float64
                )
                mag = np.abs(complex_vals)
                phase = np.angle(complex_vals)
                pairs_out[pair] = {
                    "mag": mag.tolist(),
                    "phase": phase.tolist(),
                }
            else:
                row = arr[0] if arr.ndim == 2 else arr
                pairs_out[pair] = {
                    "mag": np.asarray(row, dtype=np.float64).tolist(),
                    "phase": None,
                }
    return {
        "acc_cnt": state.corr_acc_cnt,
        "acc_cadence_s": state.corr_cadence_s,
        "freq_mhz": ((freqs * 1e-6).tolist() if freqs is not None else None),
        "pairs": pairs_out,
    }


def _metadata_payload(
    state: StateSnapshot, thresholds: Thresholds, now: float
) -> dict:
    """Return per-sensor ``{value, ts_unix, age_s, status, classify}``.

    ``classify`` is the tile color tag. It uses the panda ``_ts``
    stamps that :class:`MetadataSnapshotReader` returns alongside each
    sensor's value; a stale _ts is rendered as ``"stale"`` regardless
    of the value-based band.
    """
    out: dict[str, dict] = {}
    snapshot = state.metadata_snapshot or {}
    for sensor, value in snapshot.items():
        if sensor.endswith("_ts"):
            continue
        ts_unix = snapshot.get(f"{sensor}_ts")
        try:
            ts_unix = float(ts_unix) if ts_unix is not None else None
        except (TypeError, ValueError):
            ts_unix = None
        age_s = (now - ts_unix) if ts_unix is not None else None
        status = value.get("status") if isinstance(value, dict) else None
        classify_by_sig: dict[str, str] = {}
        # For every registered signal whose "domain" matches this
        # sensor, run the classifier on the matching field.
        for sig_name, sig in enabled_signals(thresholds.obs_cfg).items():
            domain, _, field_ = sig_name.partition(".")
            if domain != sensor:
                continue
            if isinstance(value, dict):
                field_value = value.get(field_)
            else:
                field_value = value if field_ == "" else None
            classify_by_sig[sig_name] = thresholds.classify(
                sig_name, field_value, age_s=age_s
            )
        out[sensor] = {
            "value": value,
            "ts_unix": ts_unix,
            "age_s": age_s,
            "status": status,
            "classify": classify_by_sig,
        }
    return out


def _adc_payload(state: StateSnapshot) -> dict:
    adc_stats = state.adc_stats_latest or {}
    per_input = []
    for n in range(6):
        for c in range(2):
            rms = adc_stats.get(f"input{n}_core{c}_rms")
            mean = adc_stats.get(f"input{n}_core{c}_mean")
            power = adc_stats.get(f"input{n}_core{c}_power")
            per_input.append(
                {
                    "input": n,
                    "core": c,
                    "rms": rms,
                    "mean": mean,
                    "power": power,
                    "clip_frac": state.adc_clip_fraction.get(str(n)),
                }
            )
    return {
        "per_input": per_input,
        "stats_status": adc_stats.get("status"),
        "stats_last_unix": state.adc_stats_last_unix,
        "snapshot_last_unix": state.adc_snapshot_last_unix,
        "clip_fraction": dict(state.adc_clip_fraction),
    }


def _rfswitch_payload(state: StateSnapshot, obs_cfg: dict) -> dict:
    latest = state.metadata_latest.get("rfswitch") or {}
    name = latest.get("sw_state_name")
    entered_unix = state.rfswitch_state_entered_unix
    schedule = obs_cfg.get("switch_schedule", {}) or {}
    time_in_state_s = None
    if entered_unix is not None:
        time_in_state_s = max(0.0, time.time() - entered_unix)
    expected_dwell = schedule.get(name) if name else None
    on_schedule = True
    next_expected_change_s = None
    if expected_dwell is not None and time_in_state_s is not None:
        next_expected_change_s = expected_dwell - time_in_state_s
        # If we're past the expected dwell by more than 10%, flag as off-schedule.
        if time_in_state_s > expected_dwell * 1.1:
            on_schedule = False
    return {
        "state": name,
        "time_in_state_s": time_in_state_s,
        "schedule": schedule,
        "next_expected_change_s": next_expected_change_s,
        "on_schedule": on_schedule,
    }


def _file_payload(state: StateSnapshot, thresholds: Thresholds) -> dict:
    fh = dict(state.file_heartbeat or {})
    age = fh.get("seconds_since_write")
    fh["classify"] = thresholds.classify("file.seconds_since_write", age)
    return fh


def _status_payload(state: StateSnapshot) -> list:
    return list(state.status_log)


def _corr_observing_timeout_s(state: StateSnapshot) -> float:
    """Infer a reasonable observing-idle timeout from corr cadence.

    The corr loop advances ``corr_last_unix`` once per integration, so
    a fixed 2 s threshold misreports "not observing" whenever
    ``integration_time`` is configured above ~1 s. Prefer the measured
    cadence, fall back to the header's ``integration_time``, and
    finally to a 2 s floor. Scale by 2.5× so one slipped integration
    doesn't flip the indicator.
    """
    cadence: Optional[float] = None
    if state.corr_cadence_s is not None and state.corr_cadence_s > 0:
        cadence = float(state.corr_cadence_s)
    elif state.corr_header is not None:
        int_time = state.corr_header.get("integration_time")
        if int_time is not None:
            try:
                int_time_f = float(int_time)
            except (TypeError, ValueError):
                int_time_f = None
            if int_time_f and int_time_f > 0:
                cadence = int_time_f
    if cadence is None:
        return 2.0
    return max(2.0, cadence * 2.5)


def _health_payload(state: StateSnapshot, now: float) -> dict:
    panda_hb_age = None
    if state.panda_heartbeat_last_check_unix is not None:
        panda_hb_age = max(0.0, now - state.panda_heartbeat_last_check_unix)
    observing_inferred = False
    if state.corr_last_unix is not None:
        timeout_s = _corr_observing_timeout_s(state)
        observing_inferred = (now - state.corr_last_unix) < timeout_s
    return {
        "snap_connected": state.snap_connected,
        "panda_connected": state.panda_connected,
        "panda_heartbeat": state.panda_heartbeat,
        "panda_heartbeat_age_s": panda_hb_age,
        "observing_inferred": observing_inferred,
        "snap_last_tick_unix": state.snap_last_tick_unix,
        "panda_last_tick_unix": state.panda_last_tick_unix,
        "snap_error": state.snap_error,
        "panda_error": state.panda_error,
    }


def _config_payload(obs_cfg: dict, thresholds: Thresholds) -> dict:
    return {
        "switch_schedule": obs_cfg.get("switch_schedule", {}) or {},
        "tempctrl_settings": obs_cfg.get("tempctrl_settings", {}) or {},
        "corr_save_dir": obs_cfg.get("corr_save_dir"),
        "corr_ntimes": obs_cfg.get("corr_ntimes"),
        "use_tempctrl": obs_cfg.get("use_tempctrl", False),
        "use_switches": obs_cfg.get("use_switches", False),
        "use_vna": obs_cfg.get("use_vna", False),
        "thresholds": thresholds.as_dict(),
    }


def create_app(aggregator: LiveStatusAggregator) -> Flask:
    """Build the Flask app for one aggregator instance.

    The aggregator owns the drain threads and the snapshot lock; the
    Flask app is just a read-only projection layer.
    """
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )

    @app.route("/")
    def index():
        return render_template(
            "index.html",
            obs_cfg=aggregator.obs_cfg,
        )

    @app.route("/plotly.min.js")
    def plotly_js():
        return Response(get_plotlyjs(), mimetype="application/javascript")

    @app.route("/api/health")
    def api_health():
        state = aggregator.snapshot()
        return jsonify(_envelope(_health_payload(state, time.time())))

    @app.route("/api/corr")
    def api_corr():
        state = aggregator.snapshot()
        return jsonify(_envelope(_corr_payload(state)))

    @app.route("/api/metadata")
    def api_metadata():
        state = aggregator.snapshot()
        return jsonify(
            _envelope(
                _metadata_payload(state, aggregator.thresholds, time.time())
            )
        )

    @app.route("/api/adc")
    def api_adc():
        state = aggregator.snapshot()
        return jsonify(_envelope(_adc_payload(state)))

    @app.route("/api/rfswitch")
    def api_rfswitch():
        state = aggregator.snapshot()
        return jsonify(_envelope(_rfswitch_payload(state, aggregator.obs_cfg)))

    @app.route("/api/file")
    def api_file():
        state = aggregator.snapshot()
        return jsonify(_envelope(_file_payload(state, aggregator.thresholds)))

    @app.route("/api/status")
    def api_status():
        state = aggregator.snapshot()
        return jsonify(_envelope(_status_payload(state)))

    @app.route("/api/config")
    def api_config():
        return jsonify(
            _envelope(
                _config_payload(aggregator.obs_cfg, aggregator.thresholds)
            )
        )

    return app
