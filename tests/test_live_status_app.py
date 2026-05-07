"""Tests for the live-status Flask app.

Uses Flask's test_client against a real :class:`LiveStatusAggregator`
bound to :class:`DummyTransport` instances, but does not start the
drain threads — the tick methods are called directly so assertions
are deterministic.
"""

from __future__ import annotations

import math
import time

import numpy as np
import pytest
from eigsep_redis import (
    HeartbeatWriter,
    MetadataWriter,
    StatusWriter,
)
from eigsep_redis.testing import DummyTransport

from eigsep_observing.adc import AdcSnapshotWriter
from eigsep_observing.corr import CorrConfigStore, CorrWriter
from eigsep_observing.live_status import (
    LiveStatusAggregator,
    create_app,
)
from eigsep_observing.live_status.app import _solve_calibration


NCHAN = 1024
DTYPE = ">i4"


OBS_CFG = {
    "use_tempctrl": True,
    "corr_ntimes": 240,
    "corr_save_dir": None,
    "tempctrl_settings": {
        "LNA": {"target_C": 25.0, "hysteresis_C": 0.5, "clamp": 0.6},
        "LOAD": {"target_C": 25.0, "hysteresis_C": 0.5, "clamp": 0.6},
    },
    "switch_schedule": {"RFANT": 3600, "RFNOFF": 60, "RFNON": 60},
    "use_switches": True,
    "calibration": {
        "noise_diode_enr_db": 10.0 * math.log10(1500.0 / 290.0),
    },
}


CORR_CONFIG = {
    "sample_rate": 500.0,
    "nchan": NCHAN,
    "pairs": ["0", "02"],
    "corr_acc_len": 0x10000000,
    "acc_bins": 2,
    "dtype": DTYPE,
}


CORR_HEADER = {
    "sync_time": 1000.0,
    "integration_time": 0.27,
    "wiring": {"ants": {}},
}


def _rewind(transport, names):
    for n in names:
        transport._set_last_read_id(n, "0")


def _auto_bytes(value=100):
    # ``np.full`` keeps the big-endian dtype that ``np.ones * scalar``
    # would silently upcast to native int32 — production SNAP output is
    # big-endian, so the fixture must round-trip through ``np.frombuffer``
    # at the right byte order.
    return np.full((NCHAN, 2), value, dtype=np.dtype(DTYPE)).tobytes()


def _cross_bytes(value=5):
    return np.full((NCHAN, 2, 2), value, dtype=np.dtype(DTYPE)).tobytes()


@pytest.fixture
def agg_primed():
    """Aggregator with state populated by one SNAP tick + one panda tick.

    Producers publish corr + adc_stats + heartbeat + rfswitch + tempctrl
    + lidar; the test then ticks the aggregator once per bus so the
    Flask handlers have something to project.
    """
    snap = DummyTransport()
    panda = DummyTransport()
    CorrConfigStore(snap).upload(CORR_CONFIG)
    CorrConfigStore(snap).upload_header(CORR_HEADER)

    # Corr row.
    CorrWriter(snap).add(
        {"0": _auto_bytes(), "02": _cross_bytes()},
        cnt=100,
        sync_time=CORR_HEADER["sync_time"],
        dtype=DTYPE,
    )

    # ADC snapshot (2 ant, 2 cores, 200 samples), 5% clipping on input 0.
    data = np.zeros((2, 2, 200), dtype=np.int8)
    data[0, 0, :10] = 127
    data[0, 1, :10] = -128
    AdcSnapshotWriter(snap).add(
        data,
        unix_ts=time.time(),
        sync_time=CORR_HEADER["sync_time"],
        corr_acc_cnt=100,
        wiring={"ants": {}},
    )

    # adc_stats.
    MetadataWriter(snap).add(
        "adc_stats",
        {
            "sensor_name": "adc_stats",
            "status": "update",
            **{
                f"input{n}_core{c}_{stat}": 15.0
                for n in range(6)
                for c in range(2)
                for stat in ("mean", "power", "rms")
            },
        },
    )

    # Panda-side.
    panda_md = MetadataWriter(panda)
    panda_md.add(
        "lidar",
        {
            "sensor_name": "lidar",
            "app_id": 5,
            "status": "update",
            "distance_m": 1.4,
        },
    )
    panda_md.add(
        "rfswitch",
        {
            "sensor_name": "rfswitch",
            "app_id": 7,
            "status": "update",
            "sw_state": 1,
            "sw_state_name": "RFANT",
        },
    )
    panda_md.add(
        "tempctrl",
        {
            "sensor_name": "tempctrl",
            "app_id": 4,
            "watchdog_tripped": False,
            "watchdog_timeout_ms": 30000,
            "LNA_status": "update",
            "LNA_T_now": 25.1,
            "LNA_timestamp": time.time(),
            "LNA_T_target": 25.0,
            "LNA_drive_level": 0.25,
            "LNA_enabled": True,
            "LNA_active": True,
            "LNA_int_disabled": False,
            "LNA_hysteresis": 0.5,
            "LNA_clamp": 0.6,
            "LOAD_status": "update",
            "LOAD_T_now": 25.0,
            "LOAD_timestamp": time.time(),
            "LOAD_T_target": 25.0,
            "LOAD_drive_level": 0.3,
            "LOAD_enabled": True,
            "LOAD_active": True,
            "LOAD_int_disabled": False,
            "LOAD_hysteresis": 0.5,
            "LOAD_clamp": 0.6,
        },
    )

    StatusWriter(panda).send("observation started", level=20)
    HeartbeatWriter(panda, name="client").set(ex=60, alive=True)

    _rewind(
        snap,
        [
            "stream:corr",
            "stream:adc_snapshot",
            "stream:adc_stats",
        ],
    )
    _rewind(
        panda,
        [
            "stream:lidar",
            "stream:rfswitch",
            "stream:tempctrl",
            "stream:status",
        ],
    )

    agg = LiveStatusAggregator(
        transport_snap=snap,
        transport_panda=panda,
        obs_cfg=OBS_CFG,
        read_timeout_s=0.05,
    )
    agg._snap_tick()
    agg._panda_tick()
    yield agg
    agg.stop(timeout=1.0)


@pytest.fixture
def client(agg_primed):
    app = create_app(agg_primed)
    app.config.update(TESTING=True)
    return app.test_client()


# ---------------------------------------------------------------------


def test_health_route(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    data = body["data"]
    assert data["snap_connected"] is True
    assert data["panda_connected"] is True
    assert data["panda_heartbeat"] is True
    assert data["observing_inferred"] is True
    # snap_reinit always present (empty-sentinel before any publish).
    assert "snap_reinit" in data
    assert data["snap_reinit"]["count"] is None


def test_health_route_surfaces_run_tag(agg_primed):
    """When a panda script has published its tag, /api/health must
    expose it (with the start time and a derived seconds-since-start
    computed against ``now`` so the tile counts up between drains)."""
    from eigsep_observing.run_tag import publish as publish_run_tag

    started = time.time() - 5.0
    publish_run_tag(agg_primed.transport_panda, "panda_observe", started)
    agg_primed._panda_tick()

    app = create_app(agg_primed)
    app.config.update(TESTING=True)
    client = app.test_client()
    body = client.get("/api/health").get_json()
    data = body["data"]
    assert data["run_tag"] == "panda_observe"
    assert data["run_started_at_unix"] == pytest.approx(started)
    assert data["run_age_s"] is not None
    assert data["run_age_s"] >= 5.0


def test_health_route_run_tag_absent_returns_none(client):
    """No publish: /api/health carries explicit nulls so the dashboard
    renders an 'idle' tile rather than a stale tag."""
    body = client.get("/api/health").get_json()
    data = body["data"]
    assert data["run_tag"] is None
    assert data["run_started_at_unix"] is None
    assert data["run_age_s"] is None


def test_health_route_surfaces_snap_reinit_count(agg_primed):
    """When eigsep-fpga-init has bumped the counter, the /api/health
    payload must surface the count and a fresh seconds_since_reinit
    derived against ``now`` (not the drain-tick time)."""
    from eigsep_observing.snap_reinit import publish as publish_reinit

    publish_reinit(agg_primed.transport_snap)
    publish_reinit(agg_primed.transport_snap)
    agg_primed._snap_tick()

    app = create_app(agg_primed)
    app.config.update(TESTING=True)
    client = app.test_client()
    body = client.get("/api/health").get_json()
    data = body["data"]
    assert data["snap_reinit"]["count"] == 2
    assert data["snap_reinit"]["last_reinit_unix"] is not None
    assert data["snap_reinit"]["seconds_since_reinit"] is not None


def test_corr_route_has_pairs_and_freqs(client):
    body = client.get("/api/corr").get_json()
    data = body["data"]
    assert data["acc_cnt"] == 100
    assert "0" in data["pairs"]
    assert "02" in data["pairs"]
    # Auto pair has mag but no phase.
    assert data["pairs"]["0"]["mag"] is not None
    assert data["pairs"]["0"]["phase"] is None
    # Cross pair has both.
    assert data["pairs"]["02"]["mag"] is not None
    assert data["pairs"]["02"]["phase"] is not None
    # No wiring in CORR_HEADER fixture → label field present but null.
    assert data["pairs"]["0"]["label"] is None
    assert data["pairs"]["02"]["label"] is None
    # Frequency axis in MHz.
    assert data["freq_mhz"] is not None
    assert len(data["freq_mhz"]) == NCHAN


def test_corr_and_adc_routes_use_wiring_labels_when_published():
    """When the corr header carries a wiring manifest, the API should
    expose ``label`` on each pair (autos as the antenna name, crosses
    as ``"{a} / {b}"``) and on each ADC per-input entry. Lab/test setups
    that publish no wiring fall back to ``label=None`` — covered by the
    base ``test_corr_route_has_pairs_and_freqs`` above.
    """
    snap = DummyTransport()
    panda = DummyTransport()
    CorrConfigStore(snap).upload(CORR_CONFIG)
    CorrConfigStore(snap).upload_header(
        {
            **CORR_HEADER,
            "wiring": {
                "ants": {
                    "ant0": {"snap": {"input": 0}},
                    "ant2": {"snap": {"input": 2}},
                }
            },
        }
    )
    CorrWriter(snap).add(
        {"0": _auto_bytes(), "02": _cross_bytes()},
        cnt=100,
        sync_time=CORR_HEADER["sync_time"],
        dtype=DTYPE,
    )
    MetadataWriter(snap).add(
        "adc_stats",
        {
            "sensor_name": "adc_stats",
            "status": "update",
            **{
                f"input{n}_core{c}_{stat}": 15.0
                for n in range(6)
                for c in range(2)
                for stat in ("mean", "power", "rms")
            },
        },
    )
    _rewind(snap, ["stream:corr", "stream:adc_stats"])

    agg = LiveStatusAggregator(
        transport_snap=snap,
        transport_panda=panda,
        obs_cfg=OBS_CFG,
        read_timeout_s=0.05,
    )
    try:
        agg._snap_tick()
        client = create_app(agg).test_client()

        corr = client.get("/api/corr").get_json()["data"]
        assert corr["pairs"]["0"]["label"] == "ant0"
        assert corr["pairs"]["02"]["label"] == "ant0 / ant2"

        adc = client.get("/api/adc").get_json()["data"]
        per_input = {
            entry["input"]: entry["label"] for entry in adc["per_input"]
        }
        assert per_input[0] == "ant0"
        assert per_input[2] == "ant2"
        # Inputs without a wiring entry stay None (lab/test scenario).
        assert per_input[1] is None
        assert per_input[3] is None
    finally:
        agg.stop(timeout=1.0)


def test_metadata_route_includes_classify(client):
    body = client.get("/api/metadata").get_json()
    data = body["data"]
    assert "lidar" in data
    assert "tempctrl" in data
    tc = data["tempctrl"]
    # tempctrl.LNA_T_now = 25.1 is inside healthy (24.0, 26.0).
    assert tc["classify"]["tempctrl.LNA_T_now"] == "ok"


def test_adc_route_lists_per_input_with_clip_frac(client):
    body = client.get("/api/adc").get_json()
    data = body["data"]
    assert len(data["per_input"]) == 12  # 6 inputs x 2 cores
    entries = {(e["input"], e["core"]): e for e in data["per_input"]}
    # Input 0 got 20/400 = 5% clipping across both cores combined.
    assert entries[(0, 0)]["clip_frac"] == pytest.approx(0.05)
    # adc_stats RMS values made it through.
    assert entries[(0, 0)]["rms"] == pytest.approx(15.0)


def test_rfswitch_route(client):
    body = client.get("/api/rfswitch").get_json()
    data = body["data"]
    assert data["state"] == "RFANT"
    assert data["schedule"] == OBS_CFG["switch_schedule"]
    assert data["time_in_state_s"] is not None
    assert data["on_schedule"] is True  # just published, well within 3600s


def test_rfswitch_time_in_state_tracks_transitions_not_push_cadence(
    agg_primed,
):
    """Dwell timer must reflect state transitions, not producer pushes.

    Producers push rfswitch metadata every ~200 ms regardless of whether
    the switch actually changed. Prior to this test we used
    ``metadata_last_stream_unix["rfswitch"]`` as the dwell origin, which
    bumps on every push, so ``time_in_state_s`` capped at ~200 ms and
    ``on_schedule = False`` was unreachable. The fix tracks the
    transition timestamp separately in
    ``StateSnapshot.rfswitch_state_entered_unix``.
    """
    agg = agg_primed
    panda = agg.transport_panda
    panda_md = MetadataWriter(panda)
    app = create_app(agg)
    app.config.update(TESTING=True)
    client = app.test_client()

    # Fixture already published RFANT once; capture the entry time.
    entered_after_prime = agg.state.rfswitch_state_entered_unix
    assert entered_after_prime is not None

    # Push RFANT again (same state) and tick. The entry timestamp must
    # not advance — same state, still in the same dwell window.
    panda_md.add(
        "rfswitch",
        {
            "sensor_name": "rfswitch",
            "app_id": 7,
            "status": "update",
            "sw_state": 1,
            "sw_state_name": "RFANT",
        },
    )
    agg._panda_tick()
    assert agg.state.rfswitch_state_entered_unix == entered_after_prime

    # Push a different state (RFNOFF) and tick. The entry timestamp
    # must advance to the tick time, and the payload must reflect the
    # new state with a freshly-reset dwell window.
    panda_md.add(
        "rfswitch",
        {
            "sensor_name": "rfswitch",
            "app_id": 7,
            "status": "update",
            "sw_state": 2,
            "sw_state_name": "RFNOFF",
        },
    )
    agg._panda_tick()
    assert agg.state.rfswitch_state_entered_unix > entered_after_prime

    data = client.get("/api/rfswitch").get_json()["data"]
    assert data["state"] == "RFNOFF"
    # Dwell starts fresh; must be well below the 60 s RFNOFF schedule.
    assert 0.0 <= data["time_in_state_s"] < 5.0
    assert data["on_schedule"] is True


def test_rfswitch_on_schedule_flips_false_when_dwell_exceeded(
    agg_primed,
):
    """on_schedule flips False when time_in_state_s exceeds dwell * 1.1.

    Regression test for the push-cadence dwell bug: the old timestamp
    source reset every ~200 ms, making this branch unreachable.
    """
    agg = agg_primed
    app = create_app(agg)
    app.config.update(TESTING=True)
    client = app.test_client()

    # Backdate the transition timestamp by 120 s — past 1.1x the
    # RFANT dwell schedule entry of 3600 s? No, 3600 * 1.1 = 3960.
    # Use RFNOFF instead (60 s schedule, 1.1x = 66 s) so 120 s trips.
    agg.state.metadata_latest["rfswitch"] = {"sw_state_name": "RFNOFF"}
    agg.state.rfswitch_state_entered_unix = time.time() - 120.0

    data = client.get("/api/rfswitch").get_json()["data"]
    assert data["state"] == "RFNOFF"
    assert data["time_in_state_s"] > 66.0
    assert data["on_schedule"] is False
    assert data["next_expected_change_s"] < 0


def test_file_route(client):
    body = client.get("/api/file").get_json()
    data = body["data"]
    # corr_save_dir is None in OBS_CFG → no file found.
    assert data["newest_h5_path"] is None
    # File-heartbeat tile should classify as unknown (age None).
    assert data["classify"] == "unknown"


def test_status_route(client):
    body = client.get("/api/status").get_json()
    msgs = [e["msg"] for e in body["data"]]
    assert "observation started" in msgs


def test_config_route_exposes_thresholds_with_provenance(client):
    body = client.get("/api/config").get_json()
    data = body["data"]
    assert data["use_tempctrl"] is True
    thresh = data["thresholds"]
    # adc.rms is YAML-override per bundled live_status_thresholds.yaml.
    assert thresh["adc.rms"]["source"] == "yaml_override"
    # tempctrl.LNA_T_now is derived from obs_config.
    assert thresh["tempctrl.LNA_T_now"]["source"] == "derived"


def test_envelope_shape(client):
    """All /api/* routes use {ok, data, warnings}."""
    for path in (
        "/api/health",
        "/api/corr",
        "/api/metadata",
        "/api/adc",
        "/api/rfswitch",
        "/api/file",
        "/api/status",
        "/api/config",
    ):
        body = client.get(path).get_json()
        assert set(body.keys()) == {"ok", "data", "warnings"}, path
        assert body["ok"] is True


# ---------------------------------------------------------------------
# /api/corr?calibrated=1 — first-order Y-factor cal toggle
# ---------------------------------------------------------------------


def _seed_onoff_cache(
    agg, *, p_off_value: int = 100, p_on_value: int = 250
) -> None:
    """Inject a fresh on/off pair into the aggregator state.

    Uses the post-``reshape_data`` shape (``(1, NCHAN)`` int32 autos,
    ``(1, NCHAN, 2)`` int32 crosses) so the live-status calibration
    sees exactly what the SNAP drain would produce.
    """
    auto_off = np.full((1, NCHAN), p_off_value, dtype=np.int32)
    auto_on = np.full((1, NCHAN), p_on_value, dtype=np.int32)
    cross_off = np.full((1, NCHAN, 2), p_off_value // 20, dtype=np.int32)
    cross_on = np.full((1, NCHAN, 2), p_on_value // 20, dtype=np.int32)
    now = time.time()
    with agg._lock:
        agg.state.last_rfnoff_pairs = {"0": auto_off, "02": cross_off}
        agg.state.last_rfnoff_unix = now
        agg.state.last_rfnoff_acc_cnt = 90
        agg.state.last_rfnon_pairs = {"0": auto_on, "02": cross_on}
        agg.state.last_rfnon_unix = now
        agg.state.last_rfnon_acc_cnt = 95


def test_corr_route_default_returns_raw_with_no_calibration_meta(client):
    """Without ``?calibrated=1`` the response is unchanged from the
    pre-feature shape: raw int32 magnitude, no cal block."""
    body = client.get("/api/corr").get_json()
    data = body["data"]
    # Auto pair "0" was published with raw value 100 per channel.
    assert data["pairs"]["0"]["mag"][0] == pytest.approx(100.0)
    # No cal block on the raw path — keeps the wire identical to the
    # pre-feature contract for the default toggle-off case.
    assert data.get("calibration_meta") is None


def test_corr_route_calibrated_returns_t_load_for_p_ant_equals_p_off(
    agg_primed,
):
    """End-to-end: with a fresh on/off cache and a known T_LOAD, an
    RFANT integration whose power happens to equal ``P_off`` calibrates
    out to ``T_LOAD`` (in Kelvin). This is the operator-visible sanity
    check baked into the cal path.
    """
    _seed_onoff_cache(agg_primed, p_off_value=100, p_on_value=250)
    app = create_app(agg_primed)
    app.config.update(TESTING=True)
    client_ = app.test_client()

    body = client_.get("/api/corr?calibrated=1").get_json()
    data = body["data"]
    # tempctrl LOAD_T_now is 25.0 C → 298.15 K. P_ant=P_off=100 in the
    # fixture; T_in collapses to T_LOAD in Kelvin.
    expected_k = 25.0 + 273.15
    assert data["pairs"]["0"]["mag"][0] == pytest.approx(expected_k, rel=1e-6)
    meta = data["calibration_meta"]
    assert meta["stale"] is False
    assert meta["t_load_k"] == pytest.approx(expected_k, rel=1e-6)
    assert meta["t_enr_k"] == pytest.approx(1500.0, rel=1e-9)
    assert meta["noise_diode_enr_db"] == pytest.approx(
        10.0 * math.log10(1500.0 / 290.0), rel=1e-12
    )
    assert meta["last_rfnoff_age_s"] is not None
    assert meta["last_rfnon_age_s"] is not None
    # Gain summary present and finite.
    assert meta["gain_median"] == pytest.approx(0.1, rel=1e-6)


def test_corr_route_calibrated_with_no_cache_returns_raw_and_stale_true(
    client,
):
    """With ``?calibrated=1`` but no on/off cache populated, the route
    must fall back to raw and flag the cal block as stale so the
    dashboard renders a warning rather than a hole."""
    body = client.get("/api/corr?calibrated=1").get_json()
    data = body["data"]
    # Raw values pass through unchanged.
    assert data["pairs"]["0"]["mag"][0] == pytest.approx(100.0)
    meta = data["calibration_meta"]
    assert meta["stale"] is True
    assert meta["reason"]


def test_corr_route_calibrated_with_aged_cache_still_calibrates_and_exposes_age(
    agg_primed,
):
    """An on/off cache older than the previous 300 s threshold is no
    longer treated as stale: the ``RFANT`` dwell is an hour, so any
    fixed threshold either rejects nearly every antenna integration or
    is so loose it adds nothing. The "switch has stopped cycling"
    failure mode is covered separately by ``on_schedule`` on the
    rfswitch tile. Cache age is exposed in meta so the dashboard can
    render a "cal is N seconds old" indicator."""
    _seed_onoff_cache(agg_primed, p_off_value=100, p_on_value=250)
    # Backdate the cache well past the old 300 s window.
    with agg_primed._lock:
        agg_primed.state.last_rfnoff_unix -= 1800.0
        agg_primed.state.last_rfnon_unix -= 1800.0

    app = create_app(agg_primed)
    app.config.update(TESTING=True)
    client_ = app.test_client()
    body = client_.get("/api/corr?calibrated=1").get_json()
    data = body["data"]
    expected_k = 25.0 + 273.15
    assert data["pairs"]["0"]["mag"][0] == pytest.approx(expected_k, rel=1e-6)
    meta = data["calibration_meta"]
    assert meta["stale"] is False
    assert meta["last_rfnoff_age_s"] >= 1800.0
    assert meta["last_rfnon_age_s"] >= 1800.0


def test_corr_route_calibrated_without_t_load_returns_raw_and_stale_true(
    agg_primed,
):
    """If LOAD_T_now is missing from the snapshot (sensor offline,
    pico booted but tempctrl never reported), the cal can't proceed.
    Fall back to raw and keep the dashboard painting."""
    _seed_onoff_cache(agg_primed)
    # Drop tempctrl from the snapshot to simulate a missing producer.
    with agg_primed._lock:
        agg_primed.state.metadata_snapshot.pop("tempctrl", None)

    app = create_app(agg_primed)
    app.config.update(TESTING=True)
    client_ = app.test_client()
    body = client_.get("/api/corr?calibrated=1").get_json()
    data = body["data"]
    assert data["pairs"]["0"]["mag"][0] == pytest.approx(100.0)
    assert data["calibration_meta"]["stale"] is True


def test_corr_route_calibrated_scales_cross_magnitudes_by_gain(
    agg_primed,
):
    """Cross-correlation magnitudes are scaled by 1/G; phase is left
    untouched (the JSON's separate field). The dashboard renders
    crosses in the same K-equivalent units as the autos when the
    toggle is on."""
    _seed_onoff_cache(agg_primed, p_off_value=100, p_on_value=250)
    app = create_app(agg_primed)
    app.config.update(TESTING=True)
    client_ = app.test_client()

    raw = client_.get("/api/corr").get_json()["data"]
    cal = client_.get("/api/corr?calibrated=1").get_json()["data"]
    raw_mag0 = raw["pairs"]["02"]["mag"][0]
    cal_mag0 = cal["pairs"]["02"]["mag"][0]
    # Gain solved above is 0.1 → cal mag = raw mag / 0.1.
    assert cal_mag0 == pytest.approx(raw_mag0 / 0.1, rel=1e-6)
    # Phase array is preserved on calibrated path.
    assert cal["pairs"]["02"]["phase"] == raw["pairs"]["02"]["phase"]


def test_solve_calibration_bails_on_missing_enr_db(agg_primed):
    """Cal block without noise_diode_enr_db disables cal with a clear reason."""
    _seed_onoff_cache(agg_primed)
    obs_cfg = {"calibration": {}}
    coeffs, meta = _solve_calibration(
        agg_primed.state, obs_cfg, now=time.time()
    )
    assert coeffs is None
    assert "noise_diode_enr_db" in meta["reason"]
    assert "missing or non-numeric" in meta["reason"]


@pytest.mark.parametrize("bad_value", ["oops", [1, 2], {"x": 1}])
def test_solve_calibration_bails_on_non_numeric_enr_db(bad_value, agg_primed):
    _seed_onoff_cache(agg_primed)
    obs_cfg = {"calibration": {"noise_diode_enr_db": bad_value}}
    coeffs, meta = _solve_calibration(
        agg_primed.state, obs_cfg, now=time.time()
    )
    assert coeffs is None
    assert "noise_diode_enr_db" in meta["reason"]


@pytest.mark.parametrize(
    "bad_value", [float("nan"), float("inf"), float("-inf")]
)
def test_solve_calibration_bails_on_non_finite_enr_db(bad_value, agg_primed):
    _seed_onoff_cache(agg_primed)
    obs_cfg = {"calibration": {"noise_diode_enr_db": bad_value}}
    coeffs, meta = _solve_calibration(
        agg_primed.state, obs_cfg, now=time.time()
    )
    assert coeffs is None
    assert "noise_diode_enr_db" in meta["reason"]


def test_solve_calibration_meta_exposes_both_db_and_kelvin(agg_primed):
    """Meta carries both configured dB and derived K."""
    _seed_onoff_cache(agg_primed)
    enr_db = 6.5
    obs_cfg = {"calibration": {"noise_diode_enr_db": enr_db}}
    coeffs, meta = _solve_calibration(
        agg_primed.state, obs_cfg, now=time.time()
    )
    assert coeffs is not None
    assert meta["noise_diode_enr_db"] == pytest.approx(enr_db, rel=1e-12)
    expected_k = 290.0 * 10.0 ** (enr_db / 10.0)
    assert meta["t_enr_k"] == pytest.approx(expected_k, rel=1e-9)


def test_solve_calibration_logs_error_on_non_numeric_enr_db(
    agg_primed, caplog
):
    """Non-coercible noise_diode_enr_db is a config contract violation;
    CLAUDE.md requires it surface at ERROR, not just in meta.reason."""
    _seed_onoff_cache(agg_primed)
    obs_cfg = {"calibration": {"noise_diode_enr_db": "not-a-number"}}
    with caplog.at_level("ERROR", logger="eigsep_observing.live_status.app"):
        _solve_calibration(agg_primed.state, obs_cfg, now=time.time())
    assert any(
        "noise_diode_enr_db" in r.message and r.levelname == "ERROR"
        for r in caplog.records
    )


def test_solve_calibration_logs_error_on_non_finite_enr_db(agg_primed, caplog):
    """NaN/inf ENR is not a usable config value — log loudly."""
    _seed_onoff_cache(agg_primed)
    obs_cfg = {"calibration": {"noise_diode_enr_db": float("nan")}}
    with caplog.at_level("ERROR", logger="eigsep_observing.live_status.app"):
        _solve_calibration(agg_primed.state, obs_cfg, now=time.time())
    assert any(
        "noise_diode_enr_db" in r.message
        and "not finite" in r.message
        and r.levelname == "ERROR"
        for r in caplog.records
    )


def test_solve_calibration_logs_error_on_non_numeric_load_t_now(
    agg_primed, caplog
):
    """A non-numeric tempctrl.LOAD_T_now is a producer/schema contract
    violation; CLAUDE.md requires ERROR-level logging on top of the
    dashboard-level reason."""
    _seed_onoff_cache(agg_primed)
    with agg_primed._lock:
        agg_primed.state.metadata_snapshot["tempctrl"] = {
            "LOAD_T_now": "warm-ish",
        }
    obs_cfg = {"calibration": {"noise_diode_enr_db": 6.5}}
    with caplog.at_level("ERROR", logger="eigsep_observing.live_status.app"):
        _solve_calibration(agg_primed.state, obs_cfg, now=time.time())
    assert any(
        "LOAD_T_now" in r.message and r.levelname == "ERROR"
        for r in caplog.records
    )


def test_solve_calibration_logs_error_on_compute_gain_trx_failure(
    agg_primed, caplog, monkeypatch
):
    """The ``compute_gain_trx`` ValueError arm is unreachable in normal
    operation (the outer guard forces ``t_enr_k`` finite/positive), so
    this guards a regression of those guards. Force the path with a
    monkeypatched solver."""
    from eigsep_observing.live_status import app as app_mod

    def boom(*_a, **_kw):
        raise ValueError("synthetic regression")

    monkeypatch.setattr(app_mod, "compute_gain_trx", boom)
    _seed_onoff_cache(agg_primed)
    obs_cfg = {"calibration": {"noise_diode_enr_db": 6.5}}
    with caplog.at_level("ERROR", logger="eigsep_observing.live_status.app"):
        _solve_calibration(agg_primed.state, obs_cfg, now=time.time())
    assert any(
        "compute_gain_trx failed" in r.message and r.levelname == "ERROR"
        for r in caplog.records
    )


def test_index_renders_with_aggregator_cfg(client):
    r = client.get("/")
    assert r.status_code == 200
    assert b"EIGSEP live status" in r.data


def test_plotly_js_served_from_pypi_package(client):
    r = client.get("/plotly.min.js")
    assert r.status_code == 200
    assert r.mimetype == "application/javascript"
    # plotly.offline.get_plotlyjs() returns the minified bundle with a
    # leading banner comment — a crude but stable integrity check.
    assert b"plotly.js" in r.data[:200]
    assert len(r.data) > 1_000_000
