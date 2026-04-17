from datetime import datetime, timedelta, timezone
import json
import logging
import numpy as np
import pytest
import time
from concurrent.futures import ThreadPoolExecutor

from cmt_vna.testing import DummyVNA
from picohost.testing import DummyPicoRFSwitch

from eigsep_observing import corr as corr_mod
from eigsep_observing.corr import CorrConfigStore, CorrReader, CorrWriter
from eigsep_observing.keys import CORR_STREAM
from eigsep_observing.testing.utils import compare_dicts, generate_data
from eigsep_observing.testing import DummyEigsepObsRedis
from eigsep_observing.vna import VnaReader, VnaWriter
from eigsep_redis import (
    ConfigStore,
    HeartbeatReader,
    HeartbeatWriter,
    MetadataSnapshotReader,
    MetadataStreamReader,
    MetadataWriter,
    StatusReader,
    StatusWriter,
)
from eigsep_redis.keys import METADATA_HASH
from eigsep_redis.testing import DummyEigsepRedis


@pytest.fixture
def server():
    return DummyEigsepRedis()


@pytest.fixture
def client(server):
    c = DummyEigsepRedis()
    # share the underlying fakeredis so both clients talk to the
    # same in-memory DB but keep independent last-read-id state
    c.transport.r = server.transport.r
    return c


@pytest.fixture
def obs_server():
    return DummyEigsepObsRedis()


@pytest.fixture
def obs_client(obs_server):
    c = DummyEigsepObsRedis()
    c.transport.r = obs_server.transport.r
    return c


def test_metadata(server, client):
    assert server.data_streams == {}  # initially empty
    today = datetime.now(timezone.utc).isoformat().split("T")[0]

    # Test live metadata functionality - this is the primary use case
    for acc_cnt in range(10):
        client.metadata.add("acc_cnt", acc_cnt)
        assert client.r.smembers("data_streams") == {b"stream:acc_cnt"}
        assert server.r.smembers("data_streams") == {b"stream:acc_cnt"}
        if acc_cnt == 0:  # data stream should be created on first call
            assert "stream:acc_cnt" in server.data_streams
        # live metadata should be updated
        assert server.metadata_snapshot.get(keys="acc_cnt") == acc_cnt
        assert server.metadata_snapshot.get(keys=["acc_cnt"]) == {
            "acc_cnt": acc_cnt
        }
        live = server.metadata_snapshot.get()
        # can't expect the exact timestamp - live metadata uses string keys
        assert "acc_cnt_ts" in live
        ts = live.pop("acc_cnt_ts")
        assert ts.startswith(today)
        compare_dicts(live, {"acc_cnt": acc_cnt})

    # Test stream reading behavior - with current API, reads start from $
    # which means only new messages after stream is established
    metadata = server.metadata_stream.drain(stream_keys="acc_cnt")
    assert metadata == {}  # No new messages since stream starts at $

    # Test multiple streams
    test_date = "2025-06-02T16:25:15.089640"
    client.metadata.add("update_date", test_date)
    live = server.metadata_snapshot.get()
    assert "acc_cnt_ts" in live
    assert "update_date_ts" in live
    assert "update_date" in live
    assert set(server.data_streams.keys()) == {
        "stream:acc_cnt",
        "stream:update_date",
    }

    # test typeerror on malformed keys
    with pytest.raises(TypeError):
        server.metadata_snapshot.get(keys=[1])

    # test reset
    server.reset()
    assert server.data_streams == {}


def _backdate_ts(server, key, seconds_ago):
    """Rewrite METADATA_HASH's ``{key}_ts`` to simulate sensor
    silence. Paired with MetadataWriter.add, which stamps current
    UTC isoformat; this replaces it with a past value so the
    snapshot reader's freshness check fires deterministically."""
    past = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    server.r.hset(
        METADATA_HASH,
        f"{key}_ts",
        json.dumps(past.isoformat()).encode("utf-8"),
    )


def test_metadata_snapshot_fresh_no_warning(server, client, caplog):
    client.metadata.add("acc_cnt", 1)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        server.metadata_snapshot.get()
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_snapshot_stale_warns_but_returns_value(
    server, client, caplog
):
    client.metadata.add("acc_cnt", 7)
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        val = server.metadata_snapshot.get("acc_cnt")
    # value still returned — staleness is informational
    assert val == 7
    stale = [r for r in caplog.records if "is stale" in r.message]
    assert len(stale) == 1
    assert "acc_cnt" in stale[0].message


def test_metadata_snapshot_stale_warns_on_full_get(server, client, caplog):
    client.metadata.add("acc_cnt", 1)
    client.metadata.add("temp", 25.5)
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        m = server.metadata_snapshot.get()
    assert m["acc_cnt"] == 1 and m["temp"] == 25.5
    messages = [r.message for r in caplog.records if "is stale" in r.message]
    assert any("acc_cnt" in msg for msg in messages)
    assert not any("temp" in msg for msg in messages)


def test_metadata_snapshot_missing_ts_silent(server, caplog):
    """Pre-timestamp entries (or direct hset bypasses) must not
    trigger false positives — freshness is simply unknown."""
    server.r.hset(METADATA_HASH, "legacy", json.dumps(42).encode("utf-8"))
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        val = server.metadata_snapshot.get("legacy")
    assert val == 42
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_snapshot_malformed_ts_silent(server, client, caplog):
    """MetadataWriter.add always writes a valid UTC isoformat _ts, so
    the fromisoformat ValueError branch is unreachable via the writer.
    Overwrite _ts directly via hset to simulate a non-compliant
    producer or manual redis intervention — the only way to exercise
    this boundary condition."""
    client.metadata.add("acc_cnt", 1)
    server.r.hset(
        METADATA_HASH,
        "acc_cnt_ts",
        json.dumps("not-a-timestamp").encode("utf-8"),
    )
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        server.metadata_snapshot.get()
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_snapshot_staleness_can_be_disabled(server, client, caplog):
    client.metadata.add("acc_cnt", 1)
    _backdate_ts(server, "acc_cnt", seconds_ago=3600)
    try:
        server.metadata_snapshot.max_age_s = float("inf")
        with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
            server.metadata_snapshot.get()
    finally:
        server.metadata_snapshot.max_age_s = MetadataSnapshotReader.max_age_s
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_snapshot_staleness_restricted_to_requested_keys(
    server, client, caplog
):
    client.metadata.add("acc_cnt", 1)
    client.metadata.add("temp", 25.5)
    _backdate_ts(server, "temp", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        server.metadata_snapshot.get("acc_cnt")
    # temp is stale but wasn't requested — must stay silent
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_stream_silent_fresh_no_warning(server, client, caplog):
    """A stream that returned no entries this drain but whose
    panda-side ``_ts`` is recent is just slow — no warning."""
    client.metadata.add("acc_cnt", 1)
    server.metadata_stream.drain()  # establish position past the seed
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        out = server.metadata_stream.drain()
    assert out == {}
    assert not any(
        "drained empty and is stale" in r.message for r in caplog.records
    )


def test_metadata_stream_silent_stale_warns(server, client, caplog):
    """A stream that returned no entries this drain AND whose
    ``_ts`` is older than ``max_age_s`` warns once."""
    client.metadata.add("acc_cnt", 7)
    server.metadata_stream.drain()  # advance past the seed
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        out = server.metadata_stream.drain()
    assert out == {}
    stale = [
        r for r in caplog.records if "drained empty and is stale" in r.message
    ]
    assert len(stale) == 1
    assert "stream:acc_cnt" in stale[0].message


def test_metadata_stream_with_entries_skips_check(server, client, caplog):
    """A stream that returned entries this drain is fresh by
    definition; even an old ``_ts`` (impossible in practice — the
    writer restamps ``_ts`` on every add — but cheap to assert)
    must not warn."""
    client.metadata.add("acc_cnt", 1)
    # Read from the start so the just-added entry is visible to drain
    # (default position is the last-generated-id, which xread excludes).
    server._set_last_read_id("stream:acc_cnt", "0-0")
    # Backdate _after_ the add so the stream entry is visible but
    # the hash _ts is artificially old.
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        out = server.metadata_stream.drain()
    assert out["stream:acc_cnt"] == [1]
    assert not any(
        "drained empty and is stale" in r.message for r in caplog.records
    )


def test_metadata_stream_stale_warning_throttled(server, client, caplog):
    """At the corr cadence (~4 Hz) a permanently dead sensor would
    spam the log; the per-stream throttle suppresses repeats inside
    ``warn_interval_s``."""
    client.metadata.add("acc_cnt", 1)
    server.metadata_stream.drain()
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        for _ in range(5):
            server.metadata_stream.drain()
    stale = [
        r for r in caplog.records if "drained empty and is stale" in r.message
    ]
    assert len(stale) == 1


def test_metadata_stream_stale_missing_ts_silent(server, client, caplog):
    """Direct ``hset`` bypasses or pre-timestamp entries leave
    ``_ts`` absent; freshness is unknown, so stay silent."""
    client.metadata.add("acc_cnt", 1)
    server.metadata_stream.drain()
    server.r.hdel(METADATA_HASH, "acc_cnt_ts")
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        server.metadata_stream.drain()
    assert not any(
        "drained empty and is stale" in r.message for r in caplog.records
    )


def test_metadata_stream_stale_can_be_disabled(server, client, caplog):
    client.metadata.add("acc_cnt", 1)
    server.metadata_stream.drain()
    _backdate_ts(server, "acc_cnt", seconds_ago=3600)
    try:
        server.metadata_stream.max_age_s = float("inf")
        with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
            server.metadata_stream.drain()
    finally:
        server.metadata_stream.max_age_s = MetadataStreamReader.max_age_s
    assert not any(
        "drained empty and is stale" in r.message for r in caplog.records
    )


def test_raw(server):
    # one integration from snap
    data = generate_data(ntimes=1, raw=True, reshape=False)
    data_back = {}
    for p, d in data.items():
        server.add_raw(f"data:{p}", d)
        data_back[p] = server.get_raw(f"data:{p}")
    compare_dicts(data, data_back)


def test_metadata_stream_drain_ignores_vna_stream(obs_server, obs_client):
    """MetadataStreamReader.drain() must skip stream:vna even when it's
    present on the same Redis.

    Regression guard: historically the streaming read defaulted to the
    full ``data_streams`` set, which includes ``stream:vna`` /
    ``stream:corr``. Those carry raw numpy payloads, not JSON, so the
    default path would raise on ``json.loads``. Metadata is now tracked
    in a separate ``metadata_streams`` set and the default path only
    touches those. This test mirrors the redis_panda scenario where
    both kinds of producers share one Redis instance.
    """
    # Producer-side: push sensor metadata and a VNA measurement to the
    # same Redis that the consumer will query.
    obs_client.metadata.add("acc_cnt", 7)
    obs_client.metadata.add("temp", 25.5)

    switch = DummyPicoRFSwitch(port="/dev/null", name="switch")
    vna = DummyVNA(switch_fn=switch.switch)
    vna.setup(fstart=1e6, fstop=250e6, npoints=10, ifbw=100, power_dBm=0)
    s11 = vna.measure_ant(measure_noise=True, measure_load=True)
    header = dict(vna.header)
    header["freqs"] = header["freqs"].tolist()
    header["mode"] = "ant"
    obs_client.vna.add(s11, header=header, metadata={"temp": 25.5})

    # Both stream families are registered in data_streams...
    assert b"stream:vna" in obs_server.r.smembers("data_streams")
    assert b"stream:acc_cnt" in obs_server.r.smembers("data_streams")
    # ...but only metadata streams are in metadata_streams.
    metadata_members = obs_server.r.smembers("metadata_streams")
    assert b"stream:vna" not in metadata_members
    assert b"stream:acc_cnt" in metadata_members
    assert b"stream:temp" in metadata_members

    # Reset read positions so the streaming drain picks up seeded entries.
    obs_server._set_last_read_id("stream:acc_cnt", "0-0")
    obs_server._set_last_read_id("stream:temp", "0-0")

    # Default drain() must not touch stream:vna (would raise on
    # json.loads of the numpy payload) and must return the metadata.
    out = obs_server.metadata_stream.drain()
    assert set(out.keys()) == {"stream:acc_cnt", "stream:temp"}
    assert out["stream:acc_cnt"] == [7]
    assert out["stream:temp"] == [25.5]

    # Explicit-keys path must also refuse non-metadata streams. If
    # the explicit path resolved against ``data_streams`` it would
    # find ``stream:vna`` and try to ``json.loads`` the numpy payload.
    out_explicit = obs_server.metadata_stream.drain(["stream:vna"])
    assert out_explicit == {}


def test_metadata_writer_has_no_cross_bus_methods():
    """Structural guard: the metadata writer surface must not expose any
    method or attribute that could be used to write a corr or VNA payload.

    This is the whole point of the writer/reader split — wrong-stream
    writes should be unrepresentable at the type level, not runtime-
    checked. If someone adds a method here in the future, this test
    fails and forces the split to be revisited.
    """
    # The only public writer method is add().
    public = {
        name for name in vars(MetadataWriter) if not name.startswith("_")
    }
    assert public == {"add", "maxlen"}, (
        f"MetadataWriter surface has grown: {public}"
    )
    # Negative guards — the old god-class methods must not reappear.
    for forbidden in (
        "add_corr_data",
        "add_vna_data",
        "send_status",
        "upload_config",
        "upload_corr_config",
        "upload_corr_header",
    ):
        assert not hasattr(MetadataWriter, forbidden), (
            f"MetadataWriter should not expose {forbidden!r}"
        )


def test_metadata_readers_have_no_cross_bus_methods():
    """Structural guard: metadata readers only read metadata, nothing else."""
    for cls, expected in (
        # ``max_age_s`` / ``warn_interval_s`` are tunables, not bus
        # methods — see the readers' docstrings.
        (MetadataSnapshotReader, {"get", "max_age_s"}),
        (
            MetadataStreamReader,
            {"drain", "streams", "max_age_s", "warn_interval_s"},
        ),
    ):
        public = {name for name in vars(cls) if not name.startswith("_")}
        assert public == expected, (
            f"{cls.__name__} surface has grown: {public}"
        )
        for forbidden in (
            "read_corr_data",
            "read_vna_data",
            "read_status",
            "get_corr_config",
            "get_corr_header",
        ):
            assert not hasattr(cls, forbidden), (
                f"{cls.__name__} should not expose {forbidden!r}"
            )


def test_metadata_writer_rejects_non_json_payload(server):
    """Contract: the writer is the JSON-serialization boundary.

    A payload that can't be JSON-encoded is a producer bug; the writer
    must surface it as ValueError rather than silently write a broken
    stream entry.
    """

    class Unserializable:
        pass

    with pytest.raises(ValueError):
        server.metadata.add("broken", Unserializable())


def test_metadata_writer_rejects_bad_keys(server):
    """Contract: keys are strings, non-empty, no ':' (Redis separator)."""
    with pytest.raises(TypeError):
        server.metadata.add(123, "value")
    with pytest.raises(ValueError):
        server.metadata.add("", "value")
    with pytest.raises(ValueError):
        server.metadata.add("   ", "value")
    with pytest.raises(ValueError):
        server.metadata.add("a:b", "value")


def test_bus_classes_have_no_cross_bus_methods():
    """Structural guard for every writer/reader class across all buses.

    Each class should expose only the surface for its own bus. The
    forbidden list is deliberately broad — it catches the original
    god-class methods (add_metadata, add_corr_data, read_corr_data,
    …) reappearing on any of these smaller classes.
    """
    cross_bus_methods = (
        "add_metadata",
        "get_live_metadata",
        "get_metadata",
        "add_corr_data",
        "read_corr_data",
        "add_vna_data",
        "read_vna_data",
        "upload_corr_config",
        "get_corr_config",
        "upload_corr_header",
        "get_corr_header",
        "send_status",
        "read_status",
        "upload_config",
        "get_config",
        "client_heartbeat_set",
        "client_heartbeat_check",
    )
    surfaces = {
        StatusWriter: {"send", "maxlen"},
        StatusReader: {"read", "stream"},
        HeartbeatWriter: {"set"},
        HeartbeatReader: {"check"},
        ConfigStore: {"upload", "get"},
        CorrWriter: {"add", "maxlen"},
        CorrReader: {"read", "seek"},
        CorrConfigStore: {
            "upload",
            "get",
            "upload_header",
            "get_header",
        },
        VnaWriter: {"add", "maxlen"},
        VnaReader: {"read"},
    }
    # CorrConfigStore legitimately owns upload/get/upload_header/
    # get_header — those are its surface, not cross-bus. Scope the
    # forbidden check to each class's non-own methods.
    for cls, expected in surfaces.items():
        public = {name for name in vars(cls) if not name.startswith("_")}
        assert public == expected, (
            f"{cls.__name__} surface has grown: {public}"
        )
        for forbidden in cross_bus_methods:
            if forbidden in expected:
                continue  # the class legitimately owns this name
            assert not hasattr(cls, forbidden), (
                f"{cls.__name__} should not expose {forbidden!r}"
            )


def test_add_metadata_shim_emits_deprecation_warning(server):
    """The picohost shim must be loud — it should disappear at the monorepo cutover."""
    with pytest.warns(DeprecationWarning, match="redis.metadata.add"):
        server.add_metadata("via_shim", 42)
    # The shim still writes correctly in the meantime.
    assert server.metadata_snapshot.get("via_shim") == 42


def test_int32_redis_round_trip(obs_server, obs_client):
    """Int32 data survives add_corr_data → read_corr_data bit-for-bit.

    Mirrors the production pattern: consumer (server) blocks on
    read_corr_data *before* the producer (client) writes, matching
    the EigObserver ↔ EigsepFpga interaction.
    """
    data = generate_data(ntimes=1, reshape=False)
    # Convert one time-step to bytes (the wire format)
    raw = {p: d[0].tobytes() for p, d in data.items()}
    dtype = ">i4"
    pairs = list(data.keys())
    # In production, the FPGA is already running when the observer
    # starts reading — stream:corr exists and has at least one entry.
    # Seed the stream so read_corr_data doesn't bail on the
    # "no stream" guard, mirroring the production startup order.
    # sync_time now rides on corr_header (tested separately in
    # test_fpga.py:test_synchronize) — this test only exercises the
    # raw corr payload round-trip.
    obs_client.corr.add(raw, cnt=0, sync_time=1713200000.0, dtype=dtype)
    # Consumer blocks first (like EigObserver), producer writes after
    # (like EigsepFpga) — same pattern as test_status.
    with ThreadPoolExecutor(max_workers=1) as executor:
        read_future = executor.submit(
            obs_server.corr_reader.read, pairs=pairs, unpack=True
        )
        time.sleep(0.1)  # let consumer block
        obs_client.corr.add(raw, cnt=42, sync_time=1713200000.0, dtype=dtype)
        acc_cnt, read_data = read_future.result(timeout=5.0)
    assert acc_cnt == 42
    for p in pairs:
        original = np.frombuffer(raw[p], dtype=dtype)
        read_back = read_data[p]
        assert read_back.dtype == np.dtype(dtype), (
            f"pair '{p}': expected {dtype}, got {read_back.dtype}"
        )
        np.testing.assert_array_equal(read_back, original)


def test_corr_reader_skips_producer_backlog(obs_server, obs_client):
    """Tier-2 guard: when the producer has pushed to stream:corr before
    the consumer ever reads, the consumer must skip that backlog. The
    blocking read returns the *next* entry pushed, not the oldest
    backlog entry.

    Pins the "only pull data after observer is on" guarantee implemented
    by ``Transport._streams_from_set`` falling back to
    ``XINFO last-generated-id`` on cache miss.
    """
    data = generate_data(ntimes=1, reshape=False)
    raw = {p: d[0].tobytes() for p, d in data.items()}
    pairs = list(data.keys())
    # Seed a backlog; cnt=1 would be returned first if tier 2 regressed.
    obs_client.corr.add(raw, cnt=1, sync_time=1713200000.0, dtype=">i4")
    obs_client.corr.add(raw, cnt=2, sync_time=1713200000.0, dtype=">i4")
    # Start the reader blocking first, then push. If tier 2 works the
    # read blocks past the backlog and returns the post-start entry.
    with ThreadPoolExecutor(max_workers=1) as executor:
        fut = executor.submit(obs_server.corr_reader.read, pairs=pairs)
        time.sleep(0.1)  # let reader block
        obs_client.corr.add(raw, cnt=99, sync_time=1713200000.0, dtype=">i4")
        acc_cnt, _ = fut.result(timeout=5.0)
    assert acc_cnt == 99


def test_corr_writer_drops_unsynced_zero(obs_client, caplog):
    """sync_time=0 means SNAP not synced; the integration must be
    dropped at the writer so downstream never sees untimestamped data.
    """
    corr_mod._last_unsynced_log[0] = 0.0  # reset throttle
    data = generate_data(ntimes=1, reshape=False)
    raw = {p: d[0].tobytes() for p, d in data.items()}
    with caplog.at_level(logging.ERROR, logger="eigsep_observing.corr"):
        obs_client.corr.add(raw, cnt=5, sync_time=0, dtype=">i4")
    assert obs_client.transport.r.xlen(CORR_STREAM) == 0
    assert any("SNAP not synchronized" in r.message for r in caplog.records)


def test_corr_writer_drops_unsynced_none(obs_client, caplog):
    """sync_time=None (missing key on the caller side) is treated the
    same as 0 — drop the integration."""
    corr_mod._last_unsynced_log[0] = 0.0
    data = generate_data(ntimes=1, reshape=False)
    raw = {p: d[0].tobytes() for p, d in data.items()}
    with caplog.at_level(logging.ERROR, logger="eigsep_observing.corr"):
        obs_client.corr.add(raw, cnt=5, sync_time=None, dtype=">i4")
    assert obs_client.transport.r.xlen(CORR_STREAM) == 0
    assert any("SNAP not synchronized" in r.message for r in caplog.records)


def test_corr_writer_unsynced_log_throttled(obs_client, caplog):
    """At ~4Hz corr cadence, a persistent pre-sync state must not
    drown the log file — the throttle collapses to one ERROR per
    window.
    """
    corr_mod._last_unsynced_log[0] = 0.0
    data = generate_data(ntimes=1, reshape=False)
    raw = {p: d[0].tobytes() for p, d in data.items()}
    with caplog.at_level(logging.ERROR, logger="eigsep_observing.corr"):
        for _ in range(10):
            obs_client.corr.add(raw, cnt=5, sync_time=0, dtype=">i4")
    records = [
        r for r in caplog.records if "SNAP not synchronized" in r.message
    ]
    assert len(records) == 1


def _seed_corr(obs_client, cnt):
    """Write one corr entry with the given acc_cnt."""
    data = generate_data(ntimes=1, reshape=False)
    raw = {p: d[0].tobytes() for p, d in data.items()}
    obs_client.corr.add(raw, cnt=cnt, sync_time=1713200000.0, dtype=">i4")
    return list(data.keys())


def test_corr_reader_warns_on_acc_cnt_gap(obs_server, obs_client, caplog):
    """A jump of >1 in ``acc_cnt`` between reads fires a WARNING so
    that a silently trimmed / dropped corr integration isn't missed
    online. Models the ``acc_cnt=1`` → ``acc_cnt=3`` gap the observer
    would see if one entry were trimmed under reader backpressure.
    """
    pairs = _seed_corr(obs_client, cnt=1)
    _seed_corr(obs_client, cnt=3)
    obs_server.corr_reader.seek("0-0")
    with caplog.at_level(logging.WARNING, logger="eigsep_observing.corr"):
        obs_server.corr_reader.read(pairs=pairs)
        obs_server.corr_reader.read(pairs=pairs)
    gap_records = [r for r in caplog.records if "Corr stream gap" in r.message]
    assert len(gap_records) == 1
    assert "1 -> 3" in gap_records[0].message
    assert "1 missed" in gap_records[0].message


def test_corr_reader_no_warn_on_monotonic(obs_server, obs_client, caplog):
    """Consecutive ``acc_cnt`` values (1, 2, 3) are the normal case and
    must not produce any gap warning."""
    pairs = _seed_corr(obs_client, cnt=1)
    _seed_corr(obs_client, cnt=2)
    _seed_corr(obs_client, cnt=3)
    obs_server.corr_reader.seek("0-0")
    with caplog.at_level(logging.WARNING, logger="eigsep_observing.corr"):
        for _ in range(3):
            obs_server.corr_reader.read(pairs=pairs)
    assert not [r for r in caplog.records if "Corr stream gap" in r.message]


def test_corr_reader_resets_on_resync(obs_server, obs_client, caplog):
    """A SNAP re-sync drives ``acc_cnt`` back to a small value. That
    backwards jump is a real event (handled at file level by the
    observer) and must not fire the gap warning. The tracker rebaselines
    on the post-resync value so a subsequent *forward* gap is still
    caught.
    """
    obs_server.corr_reader._last_gap_warn_monotonic = 0.0
    pairs = _seed_corr(obs_client, cnt=100)
    _seed_corr(obs_client, cnt=1)  # resync; acc_cnt resets
    _seed_corr(obs_client, cnt=3)  # gap from the new baseline
    obs_server.corr_reader.seek("0-0")
    with caplog.at_level(logging.WARNING, logger="eigsep_observing.corr"):
        obs_server.corr_reader.read(pairs=pairs)  # 100, baseline
        obs_server.corr_reader.read(pairs=pairs)  # 1, backwards: silent
        obs_server.corr_reader.read(pairs=pairs)  # 3, forward gap
    gap_records = [r for r in caplog.records if "Corr stream gap" in r.message]
    assert len(gap_records) == 1
    assert "1 -> 3" in gap_records[0].message


def test_corr_reader_seek_resets_gap_tracker(obs_server, obs_client):
    """``seek`` clears ``_prev_acc_cnt`` so offline replays (e.g. the
    linearity sweep) don't mistake a rewind for a gap.
    """
    pairs = _seed_corr(obs_client, cnt=1)
    obs_server.corr_reader.seek("0-0")
    obs_server.corr_reader.read(pairs=pairs)
    assert obs_server.corr_reader._prev_acc_cnt == 1
    obs_server.corr_reader.seek("0-0")
    assert obs_server.corr_reader._prev_acc_cnt is None


def test_corr_reader_gap_warn_throttled(obs_server, obs_client, caplog):
    """Persistent reader backpressure at the 4 Hz corr rate must not
    drown the log file — the throttle collapses repeated gaps to one
    WARNING per window.
    """
    obs_server.corr_reader._last_gap_warn_monotonic = 0.0
    pairs = _seed_corr(obs_client, cnt=1)
    _seed_corr(obs_client, cnt=3)
    _seed_corr(obs_client, cnt=5)
    _seed_corr(obs_client, cnt=7)
    obs_server.corr_reader.seek("0-0")
    with caplog.at_level(logging.WARNING, logger="eigsep_observing.corr"):
        for _ in range(4):
            obs_server.corr_reader.read(pairs=pairs)
    gap_records = [r for r in caplog.records if "Corr stream gap" in r.message]
    assert len(gap_records) == 1


def test_vna_reader_skips_producer_backlog(obs_server, obs_client):
    """Tier-2 guard for stream:vna — same rationale as the corr test."""
    old = {"ant": np.array([1 + 2j], dtype=np.complex128)}
    new = {"ant": np.array([9 + 9j], dtype=np.complex128)}
    obs_client.vna.add(old, metadata={"marker": "old-1"})
    obs_client.vna.add(old, metadata={"marker": "old-2"})
    with ThreadPoolExecutor(max_workers=1) as executor:
        fut = executor.submit(obs_server.vna_reader.read)
        time.sleep(0.1)  # let reader block
        obs_client.vna.add(new, metadata={"marker": "new"})
        _, _, metadata = fut.result(timeout=5.0)
    assert metadata["marker"] == "new"


def test_metadata_drain_skips_producer_backlog(server, client):
    """Tier-2 guard for metadata streams: a consumer whose cache is
    empty must see a producer-first backlog as "past" and return an
    empty drain, not replay the backlog.

    Narrower than the corr/vna tests because ``drain()`` is
    non-blocking — it returns immediately rather than waiting for the
    next entry. The guarantee under test is just "backlog skipped."
    """
    for i in range(5):
        client.metadata.add("acc_cnt", i)
    # Fresh reader (empty cache) must not drain the backlog.
    assert server.metadata_stream.drain() == {}


def test_is_alive(server, client):
    # Test heartbeat functionality with current API
    # initially, both should be empty
    assert server.heartbeat_reader.check() is False
    # set client alive (server checks client heartbeat)
    client.heartbeat.set(ex=1, alive=True)
    assert server.heartbeat_reader.check() is True
    time.sleep(1.1)  # wait for expiration
    assert server.heartbeat_reader.check() is False
    # turn on/off
    client.heartbeat.set(ex=100, alive=True)
    assert server.heartbeat_reader.check() is True
    client.heartbeat.set(ex=100, alive=False)  # turn off
    assert server.heartbeat_reader.check() is False
    # test reset
    client.heartbeat.set(ex=100, alive=True)
    assert server.heartbeat_reader.check() is True
    server.reset()
    assert server.heartbeat_reader.check() is False


def test_status(server, client):
    # initial state
    assert client.status_reader.stream == {"stream:status": "$"}

    # Test blocking reads using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Start reading in background thread (will block until message arrives)
        read_future = executor.submit(server.status_reader.read)

        # Give the read thread a moment to start
        time.sleep(0.1)

        # Send status message
        msg = "test"
        client.status.send(msg)

        # Get the result from the read
        level, status = read_future.result(timeout=2.0)
        assert status == msg
        assert level == 20  # logging.INFO

    # Send many statuses and read them
    messages = [f"status {i}" for i in range(5)]
    for msg in messages:
        client.status.send(msg)

    # Read all statuses
    for expected_msg in messages:
        level, status = server.status_reader.read()
        assert status == expected_msg
        assert level == 20

    # test specific statuses
    client.status.send("VNA_COMPLETE")
    level, status = server.status_reader.read()
    assert status == "VNA_COMPLETE"

    client.status.send("VNA_ERROR")
    level, status = server.status_reader.read()
    assert status == "VNA_ERROR"

    client.status.send("VNA_TIMEOUT")
    level, status = server.status_reader.read()
    assert status == "VNA_TIMEOUT"
