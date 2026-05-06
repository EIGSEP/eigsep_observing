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

import math
import time
from typing import Any, Optional

import numpy as np
from flask import Flask, Response, jsonify, render_template, request
from plotly.offline import get_plotlyjs

from .aggregator import LiveStatusAggregator, StateSnapshot
from .calibration import (
    apply_calibration_auto,
    apply_calibration_cross_mag,
    compute_gain_trx,
)
from .signals import enabled_signals
from .thresholds import Thresholds


CELSIUS_TO_KELVIN = 273.15
T_REF_K = 290.0  # IEEE ENR reference temperature


def _envelope(data: Any, warnings: Optional[list] = None) -> dict:
    return {"ok": True, "data": data, "warnings": warnings or []}


def _input_to_ant(wiring: Optional[dict]) -> dict[str, str]:
    """Invert ``wiring["ants"]`` to ``{snap_input_str: antenna_name}``.

    Permissive: missing ``snap.input`` fields are skipped, an absent or
    empty wiring returns an empty map. Lab/test setups that publish no
    wiring (or wire to test loads) end up with ``{}``, and the
    front-end falls back to raw input numbers.
    """
    if not wiring:
        return {}
    out: dict[str, str] = {}
    for ant, spec in (wiring.get("ants") or {}).items():
        snap = (spec or {}).get("snap") or {}
        inp = snap.get("input")
        if inp is None:
            continue
        out[str(inp)] = ant
    return out


def _pair_label(pair: str, input_to_ant: dict[str, str]) -> Optional[str]:
    """Antenna-friendly label for a corr pair, or ``None`` if unmappable.

    Crosses use the ``"{a} / {b}"`` convention. A pair with any input
    not in the map yields ``None`` so the front-end can fall back to
    the raw key rather than rendering a half-mapped label.
    """
    if len(pair) == 1:
        return input_to_ant.get(pair)
    if len(pair) == 2:
        a = input_to_ant.get(pair[0])
        b = input_to_ant.get(pair[1])
        if a is None or b is None:
            return None
        return f"{a} / {b}"
    return None


def _solve_calibration(
    state: StateSnapshot, obs_cfg: dict, now: float
) -> tuple[Optional[dict], dict]:
    """Build per-channel ``(gain, t_rx)`` for the dashboard cal toggle.

    Returns ``(coeffs, meta)`` — ``coeffs`` is ``None`` when the cal
    can't run (missing cache, stale cache, missing T_LOAD, missing
    config); the ``meta`` dict is always returned and includes ``stale``
    plus a short ``reason`` so the frontend can render a warning.
    """
    cal_cfg = obs_cfg.get("calibration") or {}
    raw_max_age_s = cal_cfg.get("max_onoff_age_s", 300.0)

    raw_enr_db = cal_cfg.get("noise_diode_enr_db")
    try:
        enr_db = float(raw_enr_db) if raw_enr_db is not None else None
    except (TypeError, ValueError):
        enr_db = None
    if enr_db is not None and not math.isfinite(enr_db):
        enr_db = None

    t_enr_k = T_REF_K * 10.0 ** (enr_db / 10.0) if enr_db is not None else None

    meta: dict = {
        "stale": True,
        "reason": None,
        "t_load_k": None,
        "noise_diode_enr_db": enr_db,
        "t_enr_k": t_enr_k,
        "last_rfnoff_age_s": None,
        "last_rfnon_age_s": None,
        "max_onoff_age_s": raw_max_age_s,
        "gain_median": None,
    }

    if enr_db is None:
        meta["reason"] = "noise_diode_enr_db missing or non-numeric"
        return None, meta

    try:
        max_age_s = float(raw_max_age_s)
    except (TypeError, ValueError):
        meta["reason"] = "max_onoff_age_s invalid"
        return None, meta
    if not np.isfinite(max_age_s) or max_age_s <= 0:
        meta["reason"] = "max_onoff_age_s missing or non-positive"
        return None, meta
    meta["max_onoff_age_s"] = float(max_age_s)

    rfnoff = state.last_rfnoff_pairs
    rfnon = state.last_rfnon_pairs
    if rfnoff is None or rfnon is None:
        meta["reason"] = "no on/off pair cached yet"
        return None, meta

    if state.last_rfnoff_unix is not None:
        meta["last_rfnoff_age_s"] = max(0.0, now - state.last_rfnoff_unix)
    if state.last_rfnon_unix is not None:
        meta["last_rfnon_age_s"] = max(0.0, now - state.last_rfnon_unix)
    for age in (meta["last_rfnoff_age_s"], meta["last_rfnon_age_s"]):
        if age is None or age > max_age_s:
            meta["reason"] = "on/off cache older than max_onoff_age_s"
            return None, meta

    tempctrl = state.metadata_snapshot.get("tempctrl")
    load_t_now = (
        tempctrl.get("LOAD_T_now") if isinstance(tempctrl, dict) else None
    )
    try:
        load_t_now_f = float(load_t_now) if load_t_now is not None else None
    except (TypeError, ValueError):
        load_t_now_f = None
    if load_t_now_f is None:
        meta["reason"] = "tempctrl.LOAD_T_now missing or non-numeric"
        return None, meta
    t_load_k = load_t_now_f + CELSIUS_TO_KELVIN
    meta["t_load_k"] = t_load_k

    coeffs: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for pair, off_arr in rfnoff.items():
        on_arr = rfnon.get(pair)
        if on_arr is None or off_arr.shape != on_arr.shape:
            continue
        # Autos are ``(1, NCHAN)`` int32 power; gain is solved per-channel
        # against the auto power. Cross pairs (``(1, NCHAN, 2)``) reuse
        # the same per-channel gain at apply time.
        if off_arr.ndim == 2:
            p_off = off_arr[0].astype(np.float64)
            p_on = on_arr[0].astype(np.float64)
            try:
                gain, t_rx = compute_gain_trx(p_on, p_off, t_load_k, t_enr_k)
            except ValueError:
                continue
            coeffs[pair] = (gain, t_rx)

    if not coeffs:
        meta["reason"] = "no auto pair available to solve gain"
        return None, meta

    medians = [
        float(np.nanmedian(g))
        for g, _ in coeffs.values()
        if np.any(np.isfinite(g))
    ]
    if not medians:
        meta["reason"] = "computed gain contains no finite values"
        return None, meta

    meta["gain_median"] = float(np.median(medians))
    meta["stale"] = False
    meta["reason"] = None
    return coeffs, meta


def _pick_pair_coeffs(
    pair: str, coeffs: dict[str, tuple[np.ndarray, np.ndarray]]
) -> Optional[tuple[np.ndarray, np.ndarray]]:
    """Map a corr pair to the gain/T_rx solved on its underlying inputs.

    Auto: ``coeffs[pair]`` directly. Cross ``"ab"``: prefer the geometric
    mean of inputs ``a`` and ``b``'s gains (single-receiver assumption);
    fall back to either input's gain if only one is solved.
    """
    if pair in coeffs:
        return coeffs[pair]
    if len(pair) == 2:
        a = coeffs.get(pair[0])
        b = coeffs.get(pair[1])
        if a is not None and b is not None:
            gain = np.sqrt(a[0] * b[0])
            return gain, a[1]
        if a is not None:
            return a
        if b is not None:
            return b
    return None


def _corr_payload(
    state: StateSnapshot,
    *,
    calibrated: bool = False,
    obs_cfg: Optional[dict] = None,
) -> dict:
    pairs_out: dict[str, dict] = {}
    freqs = state.corr_freqs
    input_to_ant = _input_to_ant((state.corr_header or {}).get("wiring"))
    coeffs: Optional[dict] = None
    cal_meta: Optional[dict] = None
    if calibrated:
        coeffs, cal_meta = _solve_calibration(
            state, obs_cfg or {}, time.time()
        )
        if coeffs is None:
            calibrated = False  # fall back to raw, keep cal_meta for the UI
    if state.corr_pairs and freqs is not None:
        for pair, arr in state.corr_pairs.items():
            if arr is None or arr.size == 0:
                continue
            label = _pair_label(pair, input_to_ant)
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
                if calibrated and coeffs is not None:
                    pair_coeffs = _pick_pair_coeffs(pair, coeffs)
                    if pair_coeffs is not None:
                        gain, _ = pair_coeffs
                        mag = apply_calibration_cross_mag(mag, gain)
                pairs_out[pair] = {
                    "mag": mag.tolist(),
                    "phase": phase.tolist(),
                    "label": label,
                }
            else:
                row = arr[0] if arr.ndim == 2 else arr
                row = np.asarray(row, dtype=np.float64)
                if calibrated and coeffs is not None:
                    pair_coeffs = _pick_pair_coeffs(pair, coeffs)
                    if pair_coeffs is not None:
                        gain, t_rx = pair_coeffs
                        row = apply_calibration_auto(row, gain, t_rx)
                pairs_out[pair] = {
                    "mag": row.tolist(),
                    "phase": None,
                    "label": label,
                }
    payload = {
        "acc_cnt": state.corr_acc_cnt,
        "acc_cadence_s": state.corr_cadence_s,
        "freq_mhz": ((freqs * 1e-6).tolist() if freqs is not None else None),
        "pairs": pairs_out,
    }
    if cal_meta is not None:
        payload["calibration_meta"] = cal_meta
    return payload


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
    # Prefer the corr header's wiring (canonical, written on every
    # state-changing FPGA call); fall back to the ADC sidecar so
    # standalone ADC snapshots still carry labels even when corr
    # publishing is paused.
    sidecar_wiring = (state.adc_snapshot_sidecar or {}).get("wiring")
    header_wiring = (state.corr_header or {}).get("wiring")
    input_to_ant = _input_to_ant(header_wiring or sidecar_wiring)
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
                    "label": input_to_ant.get(str(n)),
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
    reinit = dict(state.snap_reinit or {})
    # Recompute the age against ``now`` so the dashboard's "Reinits"
    # tile shows fresh seconds-since values between drain ticks.
    last_unix = reinit.get("last_reinit_unix")
    reinit["seconds_since_reinit"] = (
        max(0.0, now - last_unix) if last_unix is not None else None
    )
    # ``run_age_s`` is derived against ``now`` (not the drain-tick
    # time) so the dashboard tile counts up between panda drains, the
    # same pattern the reinit and file tiles use.
    run_age_s = None
    if state.run_started_at_unix is not None:
        run_age_s = max(0.0, now - state.run_started_at_unix)
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
        "snap_reinit": reinit,
        "run_tag": state.run_tag,
        "run_started_at_unix": state.run_started_at_unix,
        "run_age_s": run_age_s,
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
        calibrated = request.args.get("calibrated") == "1"
        return jsonify(
            _envelope(
                _corr_payload(
                    state, calibrated=calibrated, obs_cfg=aggregator.obs_cfg
                )
            )
        )

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
