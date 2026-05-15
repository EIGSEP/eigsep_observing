"""Tests for eigsep_observing.live_status.aggregator.LiveStatusAggregator.

Driven by direct writer publishes against ``DummyTransport``
(fakeredis-backed) rather than by spinning a full ``DummyEigsepFpga``
+ ``DummyPandaClient`` through their real threads: the drain surfaces
are what's under test, and keeping the producer side synchronous
means we can assert state deterministically without sleeps.
"""

from __future__ import annotations

import threading
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
    Thresholds,
)


NCHAN = 1024
DTYPE = ">i4"


OBS_CFG = {
    "use_tempctrl": True,
    "corr_ntimes": 240,
    "corr_save_dir": None,  # tests that need a dir set it per-test
    "tempctrl_settings": {
        "LNA": {"target_C": 25.0, "hysteresis_C": 0.5, "clamp": 0.6},
        "LOAD": {"target_C": 25.0, "hysteresis_C": 0.5, "clamp": 0.6},
    },
    "switch_schedule": {"RFANT": 3600, "RFNOFF": 60, "RFNON": 60},
}


CORR_CONFIG = {
    "sample_rate": 500.0,
    "nchan": NCHAN,
    "pairs": ["0", "1", "02"],
    "corr_acc_len": 0x10000000,
    "acc_bins": 2,
    "dtype": DTYPE,
}


CORR_HEADER = {
    "sync_time": 1000.0,
    "integration_time": 0.27,
    "wiring": {"ants": {}},
}


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def transports():
    snap = DummyTransport()
    panda = DummyTransport()
    yield snap, panda
    # DummyTransport/fakeredis releases on GC; no explicit teardown.


@pytest.fixture
def seeded(transports):
    """Transports pre-seeded with corr config + header."""
    snap, panda = transports
    store = CorrConfigStore(snap)
    store.upload(CORR_CONFIG)
    store.upload_header(CORR_HEADER)
    return snap, panda


def _rewind_streams(transport, names):
    """Test helper: rewind per-stream cursors so a reader built on
    ``transport`` sees entries published *before* its first read.

    In production the aggregator runs alongside a live producer and
    sees each integration as it arrives — ``last-generated-id`` as the
    default cursor is exactly that semantics. Tests publish first and
    read after, so we manually reset the cursor to the beginning.
    """
    for name in names:
        transport._set_last_read_id(name, "0")


@pytest.fixture
def agg(seeded):
    snap, panda = seeded
    a = LiveStatusAggregator(
        transport_snap=snap,
        transport_panda=panda,
        obs_cfg=OBS_CFG,
        snap_tick_s=0.01,
        panda_tick_s=0.01,
        read_timeout_s=0.05,
    )
    yield a
    a.stop(timeout=1.0)


def _make_corr_row(pairs=("0", "1", "02"), auto_value=100, cross_value=5):
    """Build a dict of ``{pair: bytes}`` matching the corr wire format.

    Autos: shape ``(nchan, acc_bins)`` int32 big-endian (raw, un-averaged).
    Cross "02": same shape but 2x length for real/imag interleave.

    Uses ``np.full(..., dtype=DTYPE)`` rather than ``np.ones(...) * value``
    because the latter upcasts to native int32 in numpy's broadcasting,
    losing the big-endian byte order — production SNAP output is
    big-endian, so the fixture must be too or ``np.frombuffer`` reads
    back byteswapped values.
    """
    out = {}
    for p in pairs:
        if len(p) == 1:
            arr = np.full((NCHAN, 2), auto_value, dtype=np.dtype(DTYPE))
        else:
            # Cross: real/imag interleaved; shape (nchan, 2, 2).
            arr = np.full((NCHAN, 2, 2), cross_value, dtype=np.dtype(DTYPE))
        out[p] = arr.tobytes()
    return out


# ---------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------


def test_aggregator_starts_and_stops_cleanly(agg):
    agg.start()
    # Give threads a moment to pass through at least one tick.
    time.sleep(0.1)
    agg.stop(timeout=1.0)
    # Threads should be joined after stop().
    assert agg._snap_thread is None
    assert agg._panda_thread is None


def test_aggregator_double_start_raises(agg):
    agg.start()
    try:
        with pytest.raises(RuntimeError):
            agg.start()
    finally:
        agg.stop()


def test_aggregator_rejects_zero_timeout(seeded):
    snap, panda = seeded
    with pytest.raises(ValueError):
        LiveStatusAggregator(
            transport_snap=snap,
            transport_panda=panda,
            obs_cfg=OBS_CFG,
            read_timeout_s=0,
        )


# ---------------------------------------------------------------------
# Role-surface guard (live-status side)
# ---------------------------------------------------------------------


def test_aggregator_holds_no_writer_attribute(agg):
    """LiveStatusAggregator is a pure consumer role: no writer of any
    kind should ever appear on the instance.

    Type-based check — iterating ``vars(agg).values()`` with
    ``isinstance`` catches a writer attached under any attribute name,
    not just the handful we happen to list.
    """
    from eigsep_observing.vna import VnaWriter

    forbidden_writer_types = (
        CorrWriter,
        VnaWriter,
        MetadataWriter,
        StatusWriter,
        HeartbeatWriter,
        AdcSnapshotWriter,
    )
    for attr_name, attr_value in vars(agg).items():
        assert not isinstance(attr_value, forbidden_writer_types), (
            "LiveStatusAggregator must not hold writer objects; found "
            f"{type(attr_value).__name__} on attribute {attr_name!r}"
        )


def test_aggregator_exposes_expected_surfaces(agg):
    expected = agg._role_surface_attrs()
    attrs = set(vars(agg))
    missing = expected - attrs
    assert not missing, f"expected surfaces missing: {missing}"


# ---------------------------------------------------------------------
# SNAP tick
# ---------------------------------------------------------------------


def test_snap_tick_populates_corr_and_config(agg, seeded):
    snap, _ = seeded
    writer = CorrWriter(snap)
    writer.add(
        _make_corr_row(),
        cnt=42,
        sync_time=CORR_HEADER["sync_time"],
        dtype=DTYPE,
    )
    _rewind_streams(snap, ["stream:corr"])

    agg._snap_tick()

    state = agg.snapshot()
    assert state.snap_connected is True
    assert state.corr_acc_cnt == 42
    assert state.corr_config["sample_rate"] == 500.0
    assert state.corr_header["integration_time"] == 0.27
    # Frequencies were cached (nchan=1024 → 1024 bins).
    assert state.corr_freqs is not None
    assert state.corr_freqs.shape == (NCHAN,)
    # Pair "0" is an auto: reshape_data(avg_even_odd=True) returns
    # shape (ntimes=1, nchan) int32.
    assert state.corr_pairs["0"].shape == (1, NCHAN)
    assert state.corr_pairs["0"].dtype == np.int32
    # Cross "02": shape (ntimes=1, nchan, 2) int32 (real/imag).
    assert state.corr_pairs["02"].shape == (1, NCHAN, 2)


def test_snap_tick_computes_cadence_from_acc_cnt_delta(agg, seeded):
    snap, _ = seeded
    writer = CorrWriter(snap)

    # First tick: seed acc_cnt=10.
    writer.add(_make_corr_row(), cnt=10, sync_time=1000.0, dtype=DTYPE)
    _rewind_streams(snap, ["stream:corr"])
    agg._snap_tick()

    # Force a known dt by spoofing the stored last_unix.
    with agg._lock:
        agg.state.corr_last_unix = time.time() - 0.5

    # Second tick: acc_cnt jumps by 2 in ~0.5 s → cadence ~0.25 s.
    writer.add(_make_corr_row(), cnt=12, sync_time=1000.0, dtype=DTYPE)
    agg._snap_tick()

    state = agg.snapshot()
    assert state.corr_acc_cnt == 12
    assert state.corr_cadence_s is not None
    assert 0.1 < state.corr_cadence_s < 1.0


def test_snap_tick_drains_corr_backlog_to_latest(agg, seeded):
    """Regression: a tick must consume the full backlog and surface the
    *newest* entry, not the next one in stream order.

    Before the drain-to-tail fix, ``CorrReader.read`` consumed one
    entry per tick at ``count=1``; with a 4 Hz producer and a 2 Hz
    drain (default ``snap_tick_s=0.5``), the position pointer fell
    behind by ~2 entries/sec, surfacing as ~10 s of plot lag after
    20 s of dashboard runtime. Seeding three entries here would have
    landed acc_cnt=10 in the state on the first tick, not acc_cnt=12.
    """
    snap, _ = seeded
    writer = CorrWriter(snap)
    for cnt in (10, 11, 12):
        writer.add(_make_corr_row(), cnt=cnt, sync_time=1000.0, dtype=DTYPE)
    _rewind_streams(snap, ["stream:corr"])

    agg._snap_tick()

    state = agg.snapshot()
    assert state.corr_acc_cnt == 12


def test_snap_tick_drains_adc_stats_stream(agg, seeded):
    snap, _ = seeded
    writer = MetadataWriter(snap)
    payload = {
        "sensor_name": "adc_stats",
        "status": "update",
        **{
            f"input{n}_core{c}_{stat}": 0.0
            for n in range(6)
            for c in range(2)
            for stat in ("mean", "power", "rms")
        },
    }
    # Writer .add handles stream registration internally.
    writer.add("adc_stats", payload)
    _rewind_streams(snap, ["stream:adc_stats"])

    agg._snap_tick()

    state = agg.snapshot()
    assert state.adc_stats_latest is not None
    assert state.adc_stats_latest["status"] == "update"
    assert state.adc_stats_last_unix is not None


def test_snap_tick_ingests_adc_snapshot_and_computes_clipping(agg, seeded):
    snap, _ = seeded
    writer = AdcSnapshotWriter(snap)
    # 2 antennas, 2 cores, 100 samples.
    data = np.zeros((2, 2, 100), dtype=np.int8)
    # Force antenna 0 to have 10/200 clipped samples (5%).
    data[0, 0, :5] = 127
    data[0, 1, :5] = -128
    writer.add(
        data,
        unix_ts=time.time(),
        sync_time=1000.0,
        corr_acc_cnt=7,
        wiring={"ants": {}},
    )
    _rewind_streams(snap, ["stream:adc_snapshot"])

    agg._snap_tick()

    state = agg.snapshot()
    assert state.adc_snapshot_data is not None
    assert state.adc_snapshot_data.shape == (2, 2, 100)
    # input 0 should have clip fraction 10/200 = 0.05; input 1 is 0.
    assert state.adc_clip_fraction["0"] == pytest.approx(0.05)
    assert state.adc_clip_fraction["1"] == 0.0


def test_snap_tick_no_corr_data_yet_does_not_block(agg):
    """A freshly-started aggregator with no corr publisher should
    complete its tick quickly (finite timeout) and record
    snap_connected=True thanks to get_header/config succeeding."""
    t0 = time.time()
    agg._snap_tick()
    dt = time.time() - t0
    # read_timeout_s=0.05, so the tick shouldn't take more than ~0.3 s
    # even with both corr and adc_snapshot reads timing out.
    assert dt < 1.0
    state = agg.snapshot()
    # Header + config were seeded; snap_connected should still flip True.
    assert state.snap_connected is True
    # Corr pairs stayed empty — no corr data was published.
    assert state.corr_acc_cnt is None


# ---------------------------------------------------------------------
# Panda tick
# ---------------------------------------------------------------------


def test_panda_tick_drains_metadata_streams(agg, seeded):
    _, panda = seeded
    writer = MetadataWriter(panda)
    writer.add(
        "imu_el",
        {
            "sensor_name": "imu_el",
            "app_id": 3,
            "status": "update",
            "yaw": 1.0,
            "pitch": 2.0,
            "roll": 3.0,
            "accel_x": 0.0,
            "accel_y": 0.0,
            "accel_z": 9.8,
        },
    )
    _rewind_streams(panda, ["stream:imu_el"])

    agg._panda_tick()

    state = agg.snapshot()
    assert "imu_el" in state.metadata_latest
    assert state.metadata_latest["imu_el"]["yaw"] == 1.0
    assert "imu_el" in state.metadata_last_stream_unix


def test_panda_tick_captures_snapshot_hash(agg, seeded):
    _, panda = seeded
    writer = MetadataWriter(panda)
    writer.add(
        "lidar",
        {
            "sensor_name": "lidar",
            "app_id": 5,
            "status": "update",
            "distance_m": 1.5,
        },
    )

    agg._panda_tick()

    state = agg.snapshot()
    assert "lidar" in state.metadata_snapshot
    assert state.metadata_snapshot["lidar"]["distance_m"] == 1.5
    # _ts bookkeeping key is included in the raw snapshot.
    assert "lidar_ts" in state.metadata_snapshot


def test_panda_tick_reads_status_log(agg, seeded):
    _, panda = seeded
    sw = StatusWriter(panda)
    sw.send("first event", level=20)
    sw.send("second event", level=30)
    _rewind_streams(panda, ["stream:status"])

    agg._panda_tick()

    state = agg.snapshot()
    messages = [entry["msg"] for entry in state.status_log]
    assert "first event" in messages
    assert "second event" in messages


def test_panda_tick_sees_heartbeat(agg, seeded):
    _, panda = seeded
    hb = HeartbeatWriter(panda, name="client")
    hb.set(ex=60, alive=True)

    agg._panda_tick()

    state = agg.snapshot()
    assert state.panda_heartbeat is True
    assert state.panda_heartbeat_last_check_unix is not None


def test_panda_tick_reads_run_tag_from_redis(agg, seeded):
    """The active panda script publishes its name to a panda-side Redis
    key on startup; the aggregator must surface it so the dashboard's
    Run tile shows what's currently driving the run."""
    from eigsep_observing.run_tag import publish as publish_run_tag

    _, panda = seeded
    publish_run_tag(panda, "panda_observe", started_unix=1_713_200_000.0)

    agg._panda_tick()

    state = agg.snapshot()
    assert state.run_tag == "panda_observe"
    assert state.run_started_at_unix == 1_713_200_000.0


def test_panda_tick_run_tag_absent_returns_none(agg):
    """Before any panda script publishes (or after one cleared on
    exit), the aggregator carries ``None`` so the dashboard renders an
    'idle' tile rather than a stale tag."""
    agg._panda_tick()

    state = agg.snapshot()
    assert state.run_tag is None
    assert state.run_started_at_unix is None


def test_snap_tick_reads_file_heartbeat_from_redis(agg, seeded):
    """The dashboard and the writer run on different hosts — the
    aggregator must get the last-write info from Redis, not from a
    filesystem probe of ``corr_save_dir`` (which it cannot see)."""
    from eigsep_observing.file_heartbeat import publish

    snap, _ = seeded
    publish(snap, "/tmp/corr_20260424_120000.h5", 1_713_200_000.0)

    agg._snap_tick()

    state = agg.snapshot()
    assert (
        state.file_heartbeat["newest_h5_path"]
        == "/tmp/corr_20260424_120000.h5"
    )
    assert state.file_heartbeat["mtime_unix"] == 1_713_200_000.0
    assert state.file_heartbeat["seconds_since_write"] is not None


def test_snap_tick_reads_snap_reinit_from_redis(agg, seeded):
    """eigsep-fpga-init publishes a reinit heartbeat on each
    supervised --reinit; the aggregator must surface it on the same
    SNAP-side transport so the dashboard can render the count."""
    from eigsep_observing.snap_reinit import publish as publish_reinit

    snap, _ = seeded
    publish_reinit(snap)
    publish_reinit(snap)

    agg._snap_tick()

    state = agg.snapshot()
    assert state.snap_reinit["count"] == 2
    assert state.snap_reinit["last_reinit_unix"] is not None
    assert state.snap_reinit["seconds_since_reinit"] is not None


def test_snap_tick_snap_reinit_absent_returns_empty(agg, seeded):
    """When eigsep-fpga-init has never published (e.g. fresh deploy
    or Redis was flushed), the aggregator carries the empty-sentinel
    dict so the dashboard renders an unknown tile, not an error."""
    agg._snap_tick()

    state = agg.snapshot()
    assert state.snap_reinit == {
        "count": None,
        "last_reinit_unix": None,
        "seconds_since_reinit": None,
    }


# ---------------------------------------------------------------------
# Thresholds recompute on re-sync
# ---------------------------------------------------------------------


def test_snap_tick_recomputes_thresholds_on_header_change(agg, seeded):
    snap, _ = seeded

    # First tick gets the seeded header (integration_time=0.27).
    agg._snap_tick()
    cadence_band_before = agg.thresholds.bands["corr.acc_cadence_s"]["healthy"]

    # Bump integration_time; aggregator should rebuild thresholds.
    store = CorrConfigStore(snap)
    store.upload_header({**CORR_HEADER, "integration_time": 0.5})
    agg._snap_tick()

    cadence_band_after = agg.thresholds.bands["corr.acc_cadence_s"]["healthy"]
    assert cadence_band_before != cadence_band_after
    assert cadence_band_after == pytest.approx([0.4, 0.6])


# ---------------------------------------------------------------------
# Swallow-exception policy
# ---------------------------------------------------------------------


def test_snap_tick_swallows_reader_exception(agg, monkeypatch, caplog):
    def _boom():
        raise RuntimeError("transient redis blip")

    monkeypatch.setattr(agg.corr_config, "get_header", _boom)
    # Tick should not raise; error is logged and state marches on.
    agg._snap_tick()
    state = agg.snapshot()
    # Other reads still succeed; snap_connected flips True because the
    # rest of the tick didn't fail.
    assert state.snap_connected is True


def test_thresholds_classify_uses_live_tempctrl_band(agg, seeded):
    """End-to-end: panda ticks that publish each tempctrl channel's
    stream flow into the aggregator state, and the thresholds classifier
    correctly reports 'ok' for the in-band value."""
    _, panda = seeded
    writer = MetadataWriter(panda)
    now = time.time()
    # Per-channel schema (abbreviated) — only the fields the classifier
    # will read. Schema is not enforced by MetadataWriter; the
    # producer-contract suite handles that separately.
    writer.add(
        "tempctrl_lna",
        {
            "sensor_name": "tempctrl_lna",
            "status": "update",
            "app_id": 4,
            "watchdog_tripped": False,
            "watchdog_timeout_ms": 30000,
            "T_now": 25.2,
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
    writer.add(
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
    _rewind_streams(panda, ["stream:tempctrl_lna", "stream:tempctrl_load"])
    agg._panda_tick()

    # Simulate the SNAP side having surfaced the header so derived
    # tempctrl bands exist.
    agg._snap_tick()

    state = agg.snapshot()
    latest = state.metadata_latest["tempctrl_lna"]
    assert (
        agg.thresholds.classify("tempctrl_lna.T_now", latest["T_now"]) == "ok"
    )
    # 40 C would be outside the derived danger band (25 +/- 10).
    assert agg.thresholds.classify("tempctrl_lna.T_now", 40.0) == "danger"


# ---------------------------------------------------------------------
# Shutdown under load: start() + stop() while producers are pushing
# ---------------------------------------------------------------------


def test_start_stop_under_continuous_publishing(agg, seeded):
    """End-to-end sanity: spin up the real drain loops, have a producer
    push corr data, then shut down cleanly within a bounded time.

    The panda-side metadata drain has a startup race where the cursor
    snaps to ``last-generated-id`` each tick until the first successful
    read saves one — that's covered deterministically in the unit-level
    panda tests above. This test is about lifecycle (start + tight
    shutdown), not panda coverage.
    """
    snap, _ = seeded
    corr_w = CorrWriter(snap)

    stop_producer = threading.Event()

    def _produce():
        cnt = 0
        while not stop_producer.is_set():
            corr_w.add(
                _make_corr_row(), cnt=cnt, sync_time=1000.0, dtype=DTYPE
            )
            cnt += 1
            time.sleep(0.02)

    prod = threading.Thread(target=_produce, daemon=True)
    prod.start()
    try:
        agg.start()
        time.sleep(0.3)
        state = agg.snapshot()
        # Drain threads should have populated corr data at least once.
        assert state.corr_acc_cnt is not None
    finally:
        stop_producer.set()
        prod.join(timeout=1.0)
        t0 = time.time()
        agg.stop(timeout=2.0)
        # Shutdown should complete within a few ticks.
        assert time.time() - t0 < 3.0


# ---------------------------------------------------------------------
# RFNOFF / RFNON spectrum cache (first-order calibration on the live
# dashboard reads from these — see live_status/calibration.py).
# ---------------------------------------------------------------------


def _publish_rfswitch(panda, state_name: str, sw_state: int = 1) -> None:
    MetadataWriter(panda).add(
        "rfswitch",
        {
            "sensor_name": "rfswitch",
            "app_id": 7,
            "status": "update",
            "sw_state": sw_state,
            "sw_state_name": state_name,
        },
    )


def test_rfnoff_spectrum_caches_when_dwell_past_transition_window(agg, seeded):
    """A corr tick that arrives while the switch has been in RFNOFF
    for longer than the transition window must populate
    ``state.last_rfnoff_pairs`` and the matching unix/acc_cnt fields."""
    snap, panda = seeded
    _publish_rfswitch(panda, "RFNOFF")
    _rewind_streams(panda, ["stream:rfswitch"])
    agg._panda_tick()
    # Backdate the transition timestamp so the dwell exceeds the
    # 0.5 s transition window without sleeping.
    with agg._lock:
        agg.state.rfswitch_state_entered_unix = time.time() - 5.0

    CorrWriter(snap).add(
        _make_corr_row(), cnt=200, sync_time=1000.0, dtype=DTYPE
    )
    _rewind_streams(snap, ["stream:corr"])

    agg._snap_tick()

    state = agg.snapshot()
    assert state.last_rfnoff_pairs is not None
    assert "0" in state.last_rfnoff_pairs
    assert state.last_rfnoff_acc_cnt == 200
    assert state.last_rfnoff_unix is not None
    # The RFNON cache is independent and stays empty until RFNON dwells.
    assert state.last_rfnon_pairs is None


def test_rfnon_spectrum_caches_independently_of_rfnoff(agg, seeded):
    snap, panda = seeded
    _publish_rfswitch(panda, "RFNON", sw_state=3)
    _rewind_streams(panda, ["stream:rfswitch"])
    agg._panda_tick()
    with agg._lock:
        agg.state.rfswitch_state_entered_unix = time.time() - 5.0

    CorrWriter(snap).add(
        _make_corr_row(), cnt=300, sync_time=1000.0, dtype=DTYPE
    )
    _rewind_streams(snap, ["stream:corr"])

    agg._snap_tick()

    state = agg.snapshot()
    assert state.last_rfnon_pairs is not None
    assert state.last_rfnon_acc_cnt == 300
    assert state.last_rfnoff_pairs is None  # RFNOFF was never observed


def test_corr_during_transition_window_is_not_cached(agg, seeded):
    """Spectra captured while the physical switch is mid-actuation
    (i.e. less than ``RFSWITCH_TRANSITION_WINDOW_S`` since the state
    name flipped) are contaminated and must be skipped — same policy
    as the on-disk file writer."""
    snap, panda = seeded
    _publish_rfswitch(panda, "RFNOFF")
    _rewind_streams(panda, ["stream:rfswitch"])
    agg._panda_tick()
    # Pretend the state just changed: dwell well below the 0.5 s window.
    with agg._lock:
        agg.state.rfswitch_state_entered_unix = time.time() - 0.05

    CorrWriter(snap).add(
        _make_corr_row(), cnt=400, sync_time=1000.0, dtype=DTYPE
    )
    _rewind_streams(snap, ["stream:corr"])
    agg._snap_tick()

    state = agg.snapshot()
    assert state.last_rfnoff_pairs is None


def test_corr_in_rfant_does_not_evict_cache(agg, seeded):
    """RFANT is the science state — the cache must hold the most recent
    on/off pair across many RFANT integrations, not get blown away
    every time we land on the antenna."""
    snap, panda = seeded
    # First: observe an RFNOFF integration that gets cached.
    _publish_rfswitch(panda, "RFNOFF")
    _rewind_streams(panda, ["stream:rfswitch"])
    agg._panda_tick()
    with agg._lock:
        agg.state.rfswitch_state_entered_unix = time.time() - 5.0
    CorrWriter(snap).add(
        _make_corr_row(), cnt=500, sync_time=1000.0, dtype=DTYPE
    )
    _rewind_streams(snap, ["stream:corr"])
    agg._snap_tick()
    cached_acc_cnt = agg.snapshot().last_rfnoff_acc_cnt
    assert cached_acc_cnt == 500

    # Then switch to RFANT and tick several times — the RFNOFF cache
    # must survive untouched.
    _publish_rfswitch(panda, "RFANT")
    agg._panda_tick()
    for cnt in (501, 502, 503):
        CorrWriter(snap).add(
            _make_corr_row(), cnt=cnt, sync_time=1000.0, dtype=DTYPE
        )
        agg._snap_tick()

    state = agg.snapshot()
    assert state.last_rfnoff_acc_cnt == 500
    assert state.last_rfnoff_pairs is not None


def test_unknown_state_does_not_cache(agg, seeded):
    """The file writer marks transition windows as ``"UNKNOWN"``; that
    sentinel must never end up tagged as either RFNOFF or RFNON in
    the cache."""
    snap, panda = seeded
    _publish_rfswitch(panda, "UNKNOWN")
    _rewind_streams(panda, ["stream:rfswitch"])
    agg._panda_tick()
    with agg._lock:
        agg.state.rfswitch_state_entered_unix = time.time() - 5.0

    CorrWriter(snap).add(
        _make_corr_row(), cnt=600, sync_time=1000.0, dtype=DTYPE
    )
    _rewind_streams(snap, ["stream:corr"])
    agg._snap_tick()

    state = agg.snapshot()
    assert state.last_rfnoff_pairs is None
    assert state.last_rfnon_pairs is None


def test_thresholds_from_constructor_override():
    snap, panda = DummyTransport(), DummyTransport()
    CorrConfigStore(snap).upload(CORR_CONFIG)
    custom = Thresholds(
        OBS_CFG,
        CORR_HEADER,
        yaml_overrides={
            "adc.rms": {"healthy": [1.0, 2.0], "danger": [0.0, 3.0]},
        },
    )
    agg = LiveStatusAggregator(
        transport_snap=snap,
        transport_panda=panda,
        obs_cfg=OBS_CFG,
        thresholds=custom,
    )
    try:
        assert agg.thresholds.bands["adc.rms"]["healthy"] == [1.0, 2.0]
    finally:
        agg.stop()
