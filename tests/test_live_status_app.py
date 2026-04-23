"""Tests for the live-status Flask app.

Uses Flask's test_client against a real :class:`LiveStatusAggregator`
bound to :class:`DummyTransport` instances, but does not start the
drain threads — the tick methods are called directly so assertions
are deterministic.
"""

from __future__ import annotations

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


def _auto_bytes():
    return (np.ones((NCHAN, 2), dtype=np.dtype(DTYPE)) * 100).tobytes()


def _cross_bytes():
    return (np.ones((NCHAN, 2, 2), dtype=np.dtype(DTYPE)) * 5).tobytes()


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
    # Frequency axis in MHz.
    assert data["freq_mhz"] is not None
    assert len(data["freq_mhz"]) == NCHAN


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
