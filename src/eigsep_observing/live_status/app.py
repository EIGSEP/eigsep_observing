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

import inspect
import logging
import math
import time
from typing import Any, Optional

import numpy as np
from flask import Flask, Response, jsonify, render_template, request
from picohost.motor import PicoMotor
from plotly.offline import get_plotlyjs

from .aggregator import (
    LiveStatusAggregator,
    StateSnapshot,
    corr_observing_timeout_s,
)
from .calibration import (
    apply_calibration_auto,
    apply_calibration_cross_mag,
    compute_gain_trx,
)
from ..io import pair_label
from ..vna_calibration import VnaCache, calibrate_s11
from .orientation import compute_orientation
from .signals import enabled_signals
from .thresholds import Thresholds


logger = logging.getLogger(__name__)


CELSIUS_TO_KELVIN = 273.15
T_REF_K = 290.0  # IEEE ENR reference temperature


def _default_ori_motor():
    """Serial-less :class:`~picohost.motor.PicoMotor` for steps→degrees.

    Mirrors :func:`eigsep_observing.motor_zeroer._default_cal_motor`:
    ``PicoMotor.__new__`` bypass (no serial I/O) + constructor-default
    geometry attributes so ``steps_to_deg`` matches the firmware's own
    ``deg_to_steps`` exactly without duplicating the gear constants here.
    """
    sig = inspect.signature(PicoMotor.__init__)
    motor = PicoMotor.__new__(PicoMotor)
    for attr in ("step_angle_deg", "gear_teeth", "microstep"):
        setattr(motor, attr, sig.parameters[attr].default)
    return motor


_ORI_MOTOR = _default_ori_motor()


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


def _header_input_to_ant(header: Optional[dict]) -> dict[str, str]:
    """Effective ``{input_str: antenna}`` map for a corr header.

    Prefers the producer-written ``input_to_ant`` (mux-aware); falls
    back to inverting the physical ``wiring`` for headers that predate
    the field (mux was effectively off then).
    """
    header = header or {}
    mapping = header.get("input_to_ant")
    if mapping:
        return {str(k): v for k, v in mapping.items()}
    return _input_to_ant(header.get("wiring"))


def _solve_calibration(
    state: StateSnapshot, obs_cfg: dict, now: float
) -> tuple[Optional[dict], dict]:
    """Build per-channel ``(gain, t_rx)`` for the dashboard cal toggle.

    Coefficients are solved from the most recent ``RFNON``/``RFAMB``
    pair — ``RFAMB`` (ambient load) is the cold reference, ``RFNON``
    (noise diode on) the hot reference. ``RFNOFF`` stays cached and its
    age is retained in ``meta`` for offline cross-check / API
    consumers, but it no longer feeds the solve.

    Returns ``(coeffs, meta)`` — ``coeffs`` is ``None`` when the cal
    can't run (missing cache, missing T_LOAD, missing config); the
    ``meta`` dict is always returned and exposes the cached pair ages
    so the frontend can render a "cal is N seconds old" indicator. We
    intentionally do not gate on cache age: the ``RFANT`` dwell is an
    hour, so any fixed threshold either rejects nearly every antenna
    integration or is so loose it adds nothing. The "switch has stopped
    cycling" failure mode is already covered by ``on_schedule`` in the
    rfswitch payload.
    """
    cal_cfg = obs_cfg.get("calibration") or {}

    raw_enr_db = cal_cfg.get("noise_diode_enr_db")
    try:
        enr_db = float(raw_enr_db) if raw_enr_db is not None else None
    except (TypeError, ValueError):
        logger.error(
            "Live-status cal: noise_diode_enr_db=%r is not coercible to "
            "float; calibration disabled until obs_config is fixed.",
            raw_enr_db,
        )
        enr_db = None
    if enr_db is not None and not math.isfinite(enr_db):
        logger.error(
            "Live-status cal: noise_diode_enr_db=%r is not finite; "
            "calibration disabled until obs_config is fixed.",
            raw_enr_db,
        )
        enr_db = None

    t_enr_k = T_REF_K * 10.0 ** (enr_db / 10.0) if enr_db is not None else None

    # Which metadata stream carries the calibration load's temperature.
    # Default is the LOAD channel's stream; the knob exists for the
    # hot-swap contingency where the LOAD module rides the LNA connector
    # and publishes as tempctrl_lna (see OPERATIONS.md "Tempctrl channel
    # descope and hot-swap").
    t_load_stream = cal_cfg.get("t_load_stream") or "tempctrl_load"

    meta: dict = {
        "stale": True,
        "reason": None,
        "t_load_k": None,
        "t_load_stream": t_load_stream,
        "noise_diode_enr_db": enr_db,
        "t_enr_k": t_enr_k,
        "last_rfnoff_age_s": None,
        "last_rfnon_age_s": None,
        "last_rfamb_age_s": None,
        "gain_median": None,
    }

    if enr_db is None:
        meta["reason"] = "noise_diode_enr_db missing or non-numeric"
        return None, meta

    rfamb = state.last_rfamb_pairs
    rfnon = state.last_rfnon_pairs
    if rfamb is None or rfnon is None:
        meta["reason"] = "no on/amb pair cached yet"
        return None, meta

    if state.last_rfnoff_unix is not None:
        meta["last_rfnoff_age_s"] = max(0.0, now - state.last_rfnoff_unix)
    if state.last_rfnon_unix is not None:
        meta["last_rfnon_age_s"] = max(0.0, now - state.last_rfnon_unix)
    if state.last_rfamb_unix is not None:
        meta["last_rfamb_age_s"] = max(0.0, now - state.last_rfamb_unix)

    load_entry = state.metadata_snapshot.get(t_load_stream)
    load_t_now = (
        load_entry.get("T_now") if isinstance(load_entry, dict) else None
    )
    try:
        load_t_now_f = float(load_t_now) if load_t_now is not None else None
    except (TypeError, ValueError):
        logger.error(
            "Live-status cal: %s.T_now=%r is not coercible to "
            "float; producer/schema contract violation (T_now must be "
            "float per SENSOR_SCHEMAS).",
            t_load_stream,
            load_t_now,
        )
        load_t_now_f = None
    if load_t_now_f is None:
        meta["reason"] = f"{t_load_stream}.T_now missing or non-numeric"
        return None, meta
    t_load_k = load_t_now_f + CELSIUS_TO_KELVIN
    meta["t_load_k"] = t_load_k

    coeffs: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for pair, amb_arr in rfamb.items():
        on_arr = rfnon.get(pair)
        if on_arr is None or amb_arr.shape != on_arr.shape:
            continue
        # Autos are ``(1, NCHAN)`` int32 power; gain is solved per-channel
        # against the auto power. Cross pairs (``(1, NCHAN, 2)``) reuse
        # the same per-channel gain at apply time.
        if amb_arr.ndim == 2:
            p_amb = amb_arr[0].astype(np.float64)
            p_on = on_arr[0].astype(np.float64)
            try:
                gain, t_rx = compute_gain_trx(p_on, p_amb, t_load_k, t_enr_k)
            except ValueError as exc:
                # Outer guards force `t_enr_k` finite and positive, so this
                # path is only reachable via a logic regression — log loudly.
                logger.error(
                    "Live-status cal: compute_gain_trx failed for pair %r "
                    "(t_load_k=%r, t_enr_k=%r): %s",
                    pair,
                    t_load_k,
                    t_enr_k,
                    exc,
                )
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
    input_to_ant = _header_input_to_ant(state.corr_header)
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
            label = pair_label(pair, input_to_ant)
            # Aggregator stores the output of reshape_data:
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
    if state.corr_linear_min is not None and state.corr_linear_max is not None:
        # NaN (degenerate-fit channels, e.g. above the LPF cutoff)
        # must become JSON null: stdlib json emits bare ``NaN`` tokens
        # that JS ``JSON.parse`` rejects, and Plotly renders null as a
        # line gap — exactly the wanted look for masked channels. The
        # bounds are raw counts, so the front end draws them on the
        # raw (uncalibrated) view only.
        payload["linear_min"] = _nan_to_none(state.corr_linear_min)
        payload["linear_max"] = _nan_to_none(state.corr_linear_max)
    if cal_meta is not None:
        payload["calibration_meta"] = cal_meta
    return payload


def _nan_to_none(arr: np.ndarray) -> list:
    """Return ``arr`` as a JSON-safe list with NaN mapped to None."""
    return [None if math.isnan(v) else v for v in arr.tolist()]


def _metadata_payload(state: StateSnapshot, thresholds: Thresholds) -> dict:
    """Return per-sensor ``{value, ts_unix, age_s, status, classify}``.

    ``classify`` is the tile color tag. It uses the panda ``_ts``
    stamps that :class:`MetadataSnapshotReader` returns alongside each
    sensor's value; a stale _ts is rendered as ``"stale"`` regardless
    of the value-based band.

    ``age_s`` is the producer's age *at the moment the drain read the
    snapshot* (``metadata_snapshot_read_unix - {key}_ts``), not at the
    moment of this request — computing against the request wallclock
    folded the aggregator's cache staleness (up to a full tick period)
    into every sensor's age, making healthy 200 ms picos read as ~1 s
    old. A dead sensor still ages: successful drains keep advancing
    the read stamp while its ``_ts`` freezes. A dead panda *bus*
    freezes both (last-known values); that state is surfaced by
    ``panda_connected``, not here. Clamped at zero because ``_ts`` is
    panda-clock and the read stamp is ground-clock.
    """
    out: dict[str, dict] = {}
    snapshot = state.metadata_snapshot or {}
    read_unix = state.metadata_snapshot_read_unix
    for sensor, value in snapshot.items():
        if sensor.endswith("_ts"):
            continue
        ts_unix = snapshot.get(f"{sensor}_ts")
        try:
            ts_unix = float(ts_unix) if ts_unix is not None else None
        except (TypeError, ValueError):
            ts_unix = None
        if ts_unix is not None and read_unix is not None:
            age_s = max(0.0, read_unix - ts_unix)
        else:
            age_s = None
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
    ori = compute_orientation(out, _ORI_MOTOR.steps_to_deg)
    out["orientation"] = {
        "value": ori,
        "ts_unix": None,
        "age_s": None,
        "status": None,
        "classify": {
            "orientation.az_spread_deg": thresholds.classify(
                "orientation.az_spread_deg", ori["az"].get("spread")
            ),
            "orientation.el_spread_deg": thresholds.classify(
                "orientation.el_spread_deg", ori["el"].get("spread")
            ),
        },
    }
    return out


def _adc_payload(state: StateSnapshot) -> dict:
    adc_stats = state.adc_stats_latest or {}
    # Prefer the corr header's effective input->antenna map (mux-aware,
    # written on every state-changing FPGA call); fall back to the ADC
    # sidecar wiring so standalone snapshots still carry labels when corr
    # publishing is paused.
    header = state.corr_header or {}
    if header.get("input_to_ant"):
        input_to_ant = _header_input_to_ant(header)
    else:
        sidecar_wiring = (state.adc_snapshot_sidecar or {}).get("wiring")
        input_to_ant = _input_to_ant(header.get("wiring") or sidecar_wiring)
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


def _rfswitch_payload(state: StateSnapshot) -> dict:
    """Project the current RF-switch state for the dashboard.

    ``state`` (the current sw_state_name from the pico) is reported
    unconditionally — it's a fact the pico publishes whether or not
    panda_observe is running. ``time_in_state_s`` and
    ``next_expected_change_s`` are gated on whether we actually know
    them: dwell requires an observed transition (so we know when this
    state was entered, not just when we first saw it), and the
    countdown additionally requires both a schedule published to Redis
    and a live panda heartbeat. Any missing input returns ``None``,
    which the dashboard renders as N/A rather than a misleading
    projection of an idle schedule.
    """
    latest = state.metadata_latest.get("rfswitch") or {}
    name = latest.get("sw_state_name")
    entered_unix = state.rfswitch_state_entered_unix
    schedule = (state.panda_config_latest or {}).get(
        "switch_schedule", {}
    ) or {}

    time_in_state_s = None
    if entered_unix is not None:
        time_in_state_s = max(0.0, time.time() - entered_unix)

    expected_dwell = schedule.get(name) if name else None
    next_expected_change_s = None
    on_schedule: Optional[bool] = None
    if (
        expected_dwell is not None
        and time_in_state_s is not None
        and state.panda_heartbeat
    ):
        next_expected_change_s = expected_dwell - time_in_state_s
        on_schedule = time_in_state_s <= expected_dwell * 1.1

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


# A VNA measurement that's older than this is flagged ``stale`` in the
# /api/vna response. The producer cadence is ~1/hour; 1.5× gives one
# missed sweep of slack before the dashboard turns the bar red.
_VNA_STALE_AGE_S = 5400.0


def _vna_payload(state: StateSnapshot, mode: str, now: float) -> dict:
    """Calibrated VNA pane payload for ``mode in {"ant", "rec", "sp1"}``.

    ``sp1`` (Spare-1 open-cable trace) carries an extra ``"phase_deg"``
    field (unwrapped phase in degrees) alongside the standard
    magnitude data — see the mode-specific block below.

    Calibration is computed lazily here so the drain thread never pays
    the calkit cost when the pane isn't visible. A failure of the cal
    math (NaN OSL, unequal shapes, cmt_vna raising) logs at ERROR and
    surfaces ``available=false`` to the front-end — corr / metadata
    panes stay unaffected.
    """
    caches: dict[str, Optional[VnaCache]] = {
        "ant": state.last_vna_ant,
        "rec": state.last_vna_rec,
        "sp1": state.last_vna_sp1,
    }
    if mode not in caches:
        return {
            "available": False,
            "mode": mode,
            "reason": f"unknown mode {mode!r}",
        }
    cache = caches[mode]
    if cache is None:
        return {
            "available": False,
            "mode": mode,
            "reason": "no measurement received yet",
        }
    age_s = max(0.0, now - cache.received_unix)
    try:
        cal = calibrate_s11(
            cache.raw_s11, cache.cal_o, cache.cal_s, cache.cal_l
        )
    except (ValueError, np.linalg.LinAlgError) as exc:
        logger.error(
            "Live-status VNA cal failed for mode=%r (received_unix=%s): "
            "%s. Producer-side payload likely violated the cmt_vna "
            "calibration contract; check cal:VNAO/VNAS/VNAL shapes "
            "and finiteness.",
            mode,
            cache.received_unix,
            exc,
        )
        return {
            "available": False,
            "mode": mode,
            "reason": "calibration_failed",
            "age_s": age_s,
        }
    mag = np.abs(cal)
    # Replace zeros with NaN so log10 returns NaN (a hole in the plot)
    # rather than -inf (which Plotly renders as a spike to the bottom
    # of the axis).
    with np.errstate(divide="ignore", invalid="ignore"):
        mag_safe = np.where(mag > 0, mag, np.nan)
        s11_db = 20.0 * np.log10(mag_safe)
    # Drop non-finite values to None so JSON encodes them as null —
    # plotly draws a gap rather than crashing on NaN/Inf.
    s11_db_list = [float(v) if np.isfinite(v) else None for v in s11_db]
    freqs_mhz = (cache.freqs * 1e-6).tolist()
    payload = {
        "available": True,
        "mode": mode,
        "freqs_mhz": freqs_mhz,
        "s11_db": s11_db_list,
        "age_s": age_s,
        "stale": age_s > _VNA_STALE_AGE_S,
        "metadata_snapshot_unix": cache.metadata_snapshot_unix,
    }
    if mode == "sp1":
        # An open cable's phase vs. frequency is ~linear (slope = the
        # cable's round-trip delay); unwrapping makes slope drift or
        # ripple visible instead of sawtoothing at +-180 deg.
        #
        # Unwrap only the finite channels: np.unwrap cumsums phase
        # diffs, so a single NaN would poison every later channel.
        # Unwrapping across a NaN gap can miss a wrap inside the gap —
        # acceptable for this display-only pane.
        ang = np.angle(cal)
        phase = np.full(ang.shape, np.nan)
        finite = np.isfinite(ang)
        phase[finite] = np.degrees(np.unwrap(ang[finite]))
        payload["phase_deg"] = [
            float(v) if np.isfinite(v) else None for v in phase
        ]
    return payload


def _host_health_payload(
    entry: dict, signal: str, now: float, thresholds: Thresholds
) -> dict:
    """Project one pi's host_health K/V with a band classification.

    ``seconds_since_publish`` is recomputed against ``now`` (not the
    drain-tick value) so the tile ages between drains — the same
    pattern the reinit and run tiles use. The recomputed age also
    feeds ``classify``, so a dead publisher degrades to ``"stale"``
    instead of painting the last temperature green forever.
    """
    out = dict(entry or {})
    pub = out.get("published_unix")
    age = max(0.0, now - pub) if pub is not None else None
    out["seconds_since_publish"] = age
    out["classify"] = thresholds.classify(signal, out.get("temp_c"), age_s=age)
    return out


def _health_payload(
    state: StateSnapshot, now: float, thresholds: Thresholds
) -> dict:
    corr_health = dict(state.corr_health or {})
    panda_hb_age = None
    if state.panda_heartbeat_last_check_unix is not None:
        panda_hb_age = max(0.0, now - state.panda_heartbeat_last_check_unix)
    observing_inferred = False
    if state.corr_last_unix is not None:
        timeout_s = corr_observing_timeout_s(state)
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
    # Tier the SNAP FPGA tile: corr stream beats probe, probe beats
    # silence. ``observing_inferred`` and the probe both flag SNAP
    # liveness; this layer picks the strongest available evidence so
    # the operator sees "live" during normal observing and only sees
    # the active-probe result when the observe loop isn't running.
    if observing_inferred:
        snap_fpga_state = "live"
    elif state.snap_fpga_reachable is True:
        snap_fpga_state = "reachable"
    elif state.snap_fpga_reachable is False:
        snap_fpga_state = "unreachable"
    else:
        snap_fpga_state = "unknown"
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
        "snap_fpga_state": snap_fpga_state,
        "snap_fpga_last_probe_unix": state.snap_fpga_last_probe_unix,
        "run_tag": state.run_tag,
        "run_started_at_unix": state.run_started_at_unix,
        "run_age_s": run_age_s,
        # Corr-loop health (dashboard-only K/V; see the corr_health
        # module): cumulative dropped integrations and the latest
        # readout wall-time, surfaced on the corr-loop tile so the
        # operator can watch drops accumulate and see how much of the
        # integration window the readout consumes.
        "corr_dropped_integrations": corr_health.get("dropped_integrations"),
        "corr_readout_time_ms": corr_health.get("readout_time_ms"),
        "corr_health_published_unix": corr_health.get("published_unix"),
        # Raspberry Pi host vitals (dashboard-only K/V; see the
        # host_health module): each pi's eigsep-host-health service
        # publishes CPU temperature to its local Redis.
        "host_backend": _host_health_payload(
            state.host_health_backend, "host_backend.temp_c", now, thresholds
        ),
        "host_panda": _host_health_payload(
            state.host_health_panda, "host_panda.temp_c", now, thresholds
        ),
    }


def _config_payload(
    state: StateSnapshot, obs_cfg: dict, thresholds: Thresholds
) -> dict:
    """Surface configuration to the dashboard.

    ``switch_schedule`` and ``config_upload_unix`` come from the
    panda-side ``ConfigStore`` (Redis), so the panel shows what panda
    last uploaded. They persist after panda exits — operators can read
    ``config_upload_unix`` to see how stale the published config is.
    The other fields (``tempctrl_settings``, ``corr_*``, ``use_*``)
    still come from the on-disk ``obs_config.yaml`` the dashboard was
    started with; they drive ``Thresholds`` rendering decisions, not
    runtime claims about what panda is doing.
    """
    panda_cfg = state.panda_config_latest or {}
    return {
        "switch_schedule": panda_cfg.get("switch_schedule", {}) or {},
        "config_upload_unix": state.panda_config_upload_unix,
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
        return jsonify(
            _envelope(
                _health_payload(state, time.time(), aggregator.thresholds)
            )
        )

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
            _envelope(_metadata_payload(state, aggregator.thresholds))
        )

    @app.route("/api/adc")
    def api_adc():
        state = aggregator.snapshot()
        return jsonify(_envelope(_adc_payload(state)))

    @app.route("/api/rfswitch")
    def api_rfswitch():
        state = aggregator.snapshot()
        return jsonify(_envelope(_rfswitch_payload(state)))

    @app.route("/api/file")
    def api_file():
        state = aggregator.snapshot()
        return jsonify(_envelope(_file_payload(state, aggregator.thresholds)))

    @app.route("/api/status")
    def api_status():
        state = aggregator.snapshot()
        return jsonify(_envelope(_status_payload(state)))

    @app.route("/api/vna")
    def api_vna():
        state = aggregator.snapshot()
        mode = request.args.get("mode", "ant")
        return jsonify(_envelope(_vna_payload(state, mode, time.time())))

    @app.route("/api/config")
    def api_config():
        state = aggregator.snapshot()
        return jsonify(
            _envelope(
                _config_payload(
                    state, aggregator.obs_cfg, aggregator.thresholds
                )
            )
        )

    return app
