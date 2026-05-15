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
    ConfigStore,
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
from eigsep_observing.vna import VnaWriter


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
    """Aggregator with state populated by SNAP + panda ticks.

    Producers publish corr + adc_stats + heartbeat + rfswitch + tempctrl
    + lidar + obs_config; the panda side also drives one observed
    rfswitch transition (RFNOFF → RFANT) during the prime so
    ``rfswitch_state_entered_unix`` is latched and the dashboard's
    dwell-time / on-schedule projections have a real entry timestamp
    to work from. The first metadata arrival never latches
    ``entered_unix`` on its own (see aggregator.py), so a single push
    would leave it ``None``.
    """
    snap = DummyTransport()
    panda = DummyTransport()
    CorrConfigStore(snap).upload(CORR_CONFIG)
    CorrConfigStore(snap).upload_header(CORR_HEADER)
    # Panda-side obs_config: drives ``_rfswitch_payload``'s schedule
    # (the on-disk obs_cfg only seeds Thresholds now).
    ConfigStore(panda).upload(OBS_CFG)

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
    # Seed a prior rfswitch state — required so the next push lands as
    # an observed transition (entered_unix latches only on a real
    # prev->new change, never on first arrival).
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
    now = time.time()
    panda_md.add(
        "tempctrl_lna",
        {
            "sensor_name": "tempctrl_lna",
            "status": "update",
            "app_id": 4,
            "watchdog_tripped": False,
            "watchdog_timeout_ms": 30000,
            "T_now": 25.1,
            "timestamp": now,
            "T_target": 25.0,
            "drive_level": 0.25,
            "enabled": True,
            "active": True,
            "int_disabled": False,
            "hysteresis": 0.5,
            "clamp": 0.6,
        },
    )
    panda_md.add(
        "tempctrl_load",
        {
            "sensor_name": "tempctrl_load",
            "status": "update",
            "app_id": 4,
            "watchdog_tripped": False,
            "watchdog_timeout_ms": 30000,
            "T_now": 25.0,
            "timestamp": now,
            "T_target": 25.0,
            "drive_level": 0.3,
            "enabled": True,
            "active": True,
            "int_disabled": False,
            "hysteresis": 0.5,
            "clamp": 0.6,
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
            "stream:tempctrl_lna",
            "stream:tempctrl_load",
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
    # Now drive the observed RFNOFF → RFANT transition so entered_unix
    # latches and the dashboard's dwell-window projections work.
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
    assert "tempctrl_lna" in data
    assert "tempctrl_load" in data
    lna = data["tempctrl_lna"]
    # tempctrl_lna.T_now = 25.1 is inside healthy (24.0, 26.0).
    assert lna["classify"]["tempctrl_lna.T_now"] == "ok"


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

    # Fixture observed a RFNOFF -> RFANT transition during prime, so
    # entered_unix is latched to the RFANT tick time.
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


def test_rfswitch_payload_no_countdown_without_observed_transition():
    """No latch on first metadata arrival — dashboard shows N/A.

    A fresh aggregator that has only seen the switch parked (one
    arrival, no transition) does not know when the switch actually
    entered its current state. ``time_in_state_s`` and
    ``next_expected_change_s`` must both be ``None`` so the dashboard
    renders N/A rather than a misleading "just entered" countdown.
    """
    snap = DummyTransport()
    panda = DummyTransport()
    CorrConfigStore(snap).upload(CORR_CONFIG)
    ConfigStore(panda).upload(OBS_CFG)

    panda_md = MetadataWriter(panda)
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
    HeartbeatWriter(panda, name="client").set(ex=60, alive=True)
    _rewind(panda, ["stream:rfswitch", "stream:status"])

    agg = LiveStatusAggregator(
        transport_snap=snap,
        transport_panda=panda,
        obs_cfg=OBS_CFG,
        read_timeout_s=0.05,
    )
    try:
        agg._panda_tick()
        assert agg.state.rfswitch_state_entered_unix is None

        app = create_app(agg)
        app.config.update(TESTING=True)
        data = app.test_client().get("/api/rfswitch").get_json()["data"]
        assert data["state"] == "RFANT"
        assert data["time_in_state_s"] is None
        assert data["next_expected_change_s"] is None
        assert data["on_schedule"] is None
    finally:
        agg.stop(timeout=1.0)


def test_rfswitch_payload_no_countdown_without_schedule_in_redis():
    """No config in Redis -> no schedule -> no countdown.

    Mimics the "panda_observe never ran on this Redis" case: the pico
    publishes rfswitch state but no panda script has uploaded an
    obs_config. The dashboard must show current state but N/A for the
    countdown — the schedule from the live-status app's on-disk
    obs_config.yaml is no longer consulted for runtime claims.
    """
    snap = DummyTransport()
    panda = DummyTransport()
    CorrConfigStore(snap).upload(CORR_CONFIG)
    # NO ConfigStore(panda).upload — Redis "config" key is empty.

    panda_md = MetadataWriter(panda)
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
    HeartbeatWriter(panda, name="client").set(ex=60, alive=True)
    _rewind(panda, ["stream:rfswitch", "stream:status"])

    agg = LiveStatusAggregator(
        transport_snap=snap,
        transport_panda=panda,
        obs_cfg=OBS_CFG,
        read_timeout_s=0.05,
    )
    try:
        # First tick: drain seeds metadata_latest["rfswitch"] = RFNOFF,
        # prev was None → no latch (correct: first arrival).
        agg._panda_tick()
        # Now publish a real transition (RFNOFF → RFANT).
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
        assert agg.state.panda_config_latest is None
        assert agg.state.rfswitch_state_entered_unix is not None

        app = create_app(agg)
        app.config.update(TESTING=True)
        data = app.test_client().get("/api/rfswitch").get_json()["data"]
        assert data["state"] == "RFANT"
        assert data["schedule"] == {}
        assert data["time_in_state_s"] is not None
        assert data["next_expected_change_s"] is None
        assert data["on_schedule"] is None
    finally:
        agg.stop(timeout=1.0)


def test_rfswitch_payload_no_countdown_when_heartbeat_dead(agg_primed):
    """Schedule + transition observed, but panda heartbeat dead → N/A.

    Models 'panda_observe was running, observed at least one switch,
    then died'. The schedule remains in Redis, entered_unix remains
    latched, but the dashboard must stop projecting a countdown
    because no scheduler is actually driving the next transition.
    """
    agg = agg_primed
    # Force heartbeat dead despite the fixture's prior tick reading it
    # alive. Use the lock to mirror the way drain threads update state.
    with agg._lock:
        agg.state.panda_heartbeat = False

    app = create_app(agg)
    app.config.update(TESTING=True)
    data = app.test_client().get("/api/rfswitch").get_json()["data"]
    assert data["state"] == "RFANT"
    # entered_unix is still latched from the prime, so we still know
    # how long the switch has been in state — that's a fact.
    assert data["time_in_state_s"] is not None
    # But the countdown is gated on heartbeat liveness.
    assert data["next_expected_change_s"] is None
    assert data["on_schedule"] is None


def test_config_route_surfaces_redis_schedule_and_upload_time(client):
    """``_config_payload.switch_schedule`` comes from Redis ConfigStore
    (uploaded by panda), and ``config_upload_unix`` carries the
    ``upload_time`` ``Transport.upload_dict`` stamps on every upload.
    """
    body = client.get("/api/config").get_json()
    data = body["data"]
    assert data["switch_schedule"] == OBS_CFG["switch_schedule"]
    assert data["config_upload_unix"] is not None
    # upload_time must be a Unix timestamp near now.
    assert abs(data["config_upload_unix"] - time.time()) < 60.0


def test_config_route_schedule_empty_when_no_config_in_redis():
    """No panda config uploaded → ``switch_schedule`` is ``{}`` and
    ``config_upload_unix`` is ``None``. The disk-loaded obs_cfg's
    ``switch_schedule`` is no longer consulted.
    """
    snap = DummyTransport()
    panda = DummyTransport()
    CorrConfigStore(snap).upload(CORR_CONFIG)
    # No ConfigStore upload.
    _rewind(panda, ["stream:status"])

    agg = LiveStatusAggregator(
        transport_snap=snap,
        transport_panda=panda,
        obs_cfg=OBS_CFG,
        read_timeout_s=0.05,
    )
    try:
        agg._panda_tick()
        assert agg.state.panda_config_latest is None
        app = create_app(agg)
        app.config.update(TESTING=True)
        data = app.test_client().get("/api/config").get_json()["data"]
        assert data["switch_schedule"] == {}
        assert data["config_upload_unix"] is None
        # But disk-backed fields still come through (Thresholds-driving
        # rendering decisions, not runtime claims).
        assert data["use_tempctrl"] is True
    finally:
        agg.stop(timeout=1.0)


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
    # tempctrl_lna.T_now is derived from obs_config.
    assert thresh["tempctrl_lna.T_now"]["source"] == "derived"


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
    # tempctrl_load T_now is 25.0 C → 298.15 K. P_ant=P_off=100 in the
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
    """If T_now is missing from the snapshot (sensor offline, pico
    booted but tempctrl_load never reported), the cal can't proceed.
    Fall back to raw and keep the dashboard painting."""
    _seed_onoff_cache(agg_primed)
    # Drop tempctrl_load from the snapshot to simulate a missing producer.
    with agg_primed._lock:
        agg_primed.state.metadata_snapshot.pop("tempctrl_load", None)

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
    """A non-numeric tempctrl_load.T_now is a producer/schema contract
    violation; CLAUDE.md requires ERROR-level logging on top of the
    dashboard-level reason."""
    _seed_onoff_cache(agg_primed)
    with agg_primed._lock:
        agg_primed.state.metadata_snapshot["tempctrl_load"] = {
            "T_now": "warm-ish",
        }
    obs_cfg = {"calibration": {"noise_diode_enr_db": 6.5}}
    with caplog.at_level("ERROR", logger="eigsep_observing.live_status.app"):
        _solve_calibration(agg_primed.state, obs_cfg, now=time.time())
    assert any(
        "T_now" in r.message and r.levelname == "ERROR" for r in caplog.records
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


# ---- VNA pane -------------------------------------------------------


# Smaller than the production sweep (npoints=1000 in
# config/dummy_config.yaml) to keep these route tests fast — the
# calibration math is shape-agnostic and tested independently in
# test_live_status_vna_calibration.py.
_VNA_NFREQ = 32


def _publish_vna(
    transport,
    mode,
    *,
    raw_s11=None,
    cal_o=None,
    cal_s=None,
    cal_l=None,
    metadata_snapshot_unix=None,
):
    """Publish one synthetic VNA entry to the given transport.

    Defaults model a no-error VNA: ideal OSL standards (+1, -1, 0) and
    a constant raw S11 of 0.3+0j across the band. With those, the
    calibrated output equals the input — which is what the route's
    s11_db assertion exploits.
    """
    if raw_s11 is None:
        raw_s11 = np.full(_VNA_NFREQ, 0.3 + 0.0j, dtype=complex)
    if cal_o is None:
        cal_o = np.ones(_VNA_NFREQ, dtype=complex)
    if cal_s is None:
        cal_s = -np.ones(_VNA_NFREQ, dtype=complex)
    if cal_l is None:
        cal_l = np.zeros(_VNA_NFREQ, dtype=complex)
    if metadata_snapshot_unix is None:
        metadata_snapshot_unix = time.time()

    data = {
        mode: raw_s11,
        "cal:VNAO": cal_o,
        "cal:VNAS": cal_s,
        "cal:VNAL": cal_l,
    }
    header = {
        "mode": mode,
        "freqs": np.linspace(50e6, 250e6, _VNA_NFREQ).tolist(),
        "metadata_snapshot_unix": metadata_snapshot_unix,
    }
    VnaWriter(transport).add(data, header=header)


def test_vna_route_returns_unavailable_before_first_measurement(client):
    """No VNA writes yet: the pane should render an explicit unavailable
    payload, not a 500 or a stale trace."""
    body = client.get("/api/vna?mode=ant").get_json()
    assert body["ok"] is True
    assert body["data"]["available"] is False
    assert body["data"]["mode"] == "ant"


def test_vna_route_ant_calibrated_with_ideal_osl(agg_primed):
    """Publish an ant payload with ideal OSL standards; the route must
    return calibrated |S11| in dB, computed from the cached entry.

    With the ideal-OSL standards baked into the helper, calibrated
    output equals the raw DUT, so a flat 0.3 raw S11 must come back as
    a flat 20*log10(0.3) ≈ -10.46 dB trace.
    """
    panda = agg_primed.transport_panda
    _publish_vna(panda, "ant")
    _rewind(panda, ["stream:vna"])
    agg_primed._vna_tick()

    body = client_for(agg_primed).get("/api/vna?mode=ant").get_json()
    data = body["data"]
    assert data["available"] is True
    assert data["mode"] == "ant"
    assert len(data["s11_db"]) == _VNA_NFREQ
    assert len(data["freqs_mhz"]) == _VNA_NFREQ
    expected_db = 20.0 * math.log10(0.3)
    for v in data["s11_db"]:
        assert v == pytest.approx(expected_db, rel=1e-9)
    # Frequency axis converted Hz → MHz.
    assert data["freqs_mhz"][0] == pytest.approx(50.0)
    assert data["freqs_mhz"][-1] == pytest.approx(250.0)
    # Just-published, so not stale.
    assert data["stale"] is False
    assert data["age_s"] >= 0.0


def test_vna_route_rec_independent_of_ant(agg_primed):
    """ant and rec caches evict independently. Publishing one mode must
    not surface the other; publishing both leaves both queryable."""
    panda = agg_primed.transport_panda
    _publish_vna(
        panda,
        "rec",
        raw_s11=np.full(_VNA_NFREQ, 0.5 + 0.0j, dtype=complex),
    )
    _rewind(panda, ["stream:vna"])
    agg_primed._vna_tick()

    client = client_for(agg_primed)
    rec = client.get("/api/vna?mode=rec").get_json()["data"]
    assert rec["available"] is True
    assert rec["s11_db"][0] == pytest.approx(20.0 * math.log10(0.5), rel=1e-9)

    ant = client.get("/api/vna?mode=ant").get_json()["data"]
    assert ant["available"] is False  # ant never published


def test_vna_route_unknown_mode_returns_unavailable(client):
    """Mode is a query param; an unknown value must be a clean
    'available=false' response, not a 500 or a leaked default."""
    body = client.get("/api/vna?mode=bogus").get_json()
    assert body["ok"] is True
    assert body["data"]["available"] is False
    assert "unknown mode" in body["data"]["reason"]


def test_vna_payload_stale_flag_fires_past_threshold(agg_primed):
    """Drive _vna_payload directly with a synthetic now far in the
    future; the stale flag must flip past _VNA_STALE_AGE_S without
    mutating the underlying cache."""
    from eigsep_observing.live_status.app import _VNA_STALE_AGE_S, _vna_payload

    panda = agg_primed.transport_panda
    _publish_vna(panda, "ant")
    _rewind(panda, ["stream:vna"])
    agg_primed._vna_tick()

    state = agg_primed.snapshot()
    received = state.last_vna_ant.received_unix
    fresh = _vna_payload(state, "ant", now=received + 10.0)
    assert fresh["stale"] is False
    stale = _vna_payload(state, "ant", now=received + _VNA_STALE_AGE_S + 60.0)
    assert stale["stale"] is True
    assert stale["age_s"] >= _VNA_STALE_AGE_S


def test_vna_drain_drops_payload_with_unknown_mode(agg_primed, caplog):
    """The producer contract pins header['mode'] to 'ant' or 'rec'. An
    out-of-contract value must log at ERROR and leave the cache empty
    rather than poisoning either slot with a guessed assignment."""
    panda = agg_primed.transport_panda
    _publish_vna(
        panda,
        "ant",  # data uses the 'ant' DUT key
    )
    # Drain the well-formed entry first so it doesn't satisfy the test.
    _rewind(panda, ["stream:vna"])
    agg_primed._vna_tick()
    # Confirm the well-formed one landed.
    assert agg_primed.state.last_vna_ant is not None
    agg_primed.state.last_vna_ant = None  # reset for the violation test

    raw = np.full(_VNA_NFREQ, 0.3 + 0.0j, dtype=complex)
    bad_data = {
        "ant": raw,
        "cal:VNAO": np.ones(_VNA_NFREQ, dtype=complex),
        "cal:VNAS": -np.ones(_VNA_NFREQ, dtype=complex),
        "cal:VNAL": np.zeros(_VNA_NFREQ, dtype=complex),
    }
    bad_header = {
        "mode": "WAT",  # contract violation
        "freqs": np.linspace(50e6, 250e6, _VNA_NFREQ).tolist(),
        "metadata_snapshot_unix": time.time(),
    }
    VnaWriter(panda).add(bad_data, header=bad_header)

    with caplog.at_level(
        "ERROR", logger="eigsep_observing.live_status.aggregator"
    ):
        agg_primed._vna_tick()

    assert agg_primed.state.last_vna_ant is None
    assert agg_primed.state.last_vna_rec is None
    assert any(
        "unexpected mode" in r.message and r.levelname == "ERROR"
        for r in caplog.records
    )


def client_for(agg):
    """Helper: build a Flask test_client for a primed aggregator."""
    app = create_app(agg)
    app.config.update(TESTING=True)
    return app.test_client()


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
