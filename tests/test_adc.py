"""Tests for the ADC diagnostic path: AdcSnapshot writer/reader
round-trip, adc_stats schema reduction through ``avg_metadata``, and
the snapshot-tick publish hooks in ``EigsepFpga``.

The FPGA-level tests patch ``Input.get_adc_snapshot`` (the one
FPGA-touching call) with scoped ``patch.object`` rather than replacing
``fpga.inp`` wholesale — the real ``Input`` block runs unchanged
against ``DummyFpga``, and only the sample values need to be
deterministic for the assertions below. Stats math is asserted against
an independent numpy reduction of the same frames, so an
interleave-indexing bug in the producer cannot hide behind a mirrored
fixture.
"""

import json
import logging
from threading import Event, Thread
from unittest.mock import patch

import numpy as np
import pytest

from eigsep_observing import io
from eigsep_observing.adc import AdcSnapshotReader, AdcSnapshotWriter
from eigsep_observing.corr_health import read as read_corr_health
from eigsep_observing.keys import ADC_SNAPSHOT_STREAM
from eigsep_observing.testing import DummyEigsepFpga
from eigsep_redis.keys import DATA_STREAMS_SET
from eigsep_redis.testing import DummyTransport


@pytest.fixture
def transport():
    return DummyTransport()


@pytest.fixture
def fpga():
    return DummyEigsepFpga(program=False)


def _seed_reader_cursor(transport):
    """Seed the reader's last-read-id to the stream beginning so the
    round-trip tests below can verify the serialization contract
    without needing threads. Mirrors the intent of
    ``CorrReader.seek("0-0")`` — consumer-first rewind — for a reader
    whose structural surface is just ``{"read"}``."""
    transport.set_last_read_id(ADC_SNAPSHOT_STREAM, "0")


def _make_frames(n_pairs=3, n_samples=2048):
    """Deterministic int8 frames shaped like ``_grab_adc_frames``
    output: ``(n_pairs, 2, n_samples)``.

    The value pattern has period coprime to 2, so the even/odd (core
    0/1) interleaves carry different statistics — identical cores
    would let a core-indexing bug in ``_publish_adc_stats`` pass
    unnoticed.
    """
    n = n_pairs * 2 * n_samples
    vals = (np.arange(n) * 7 + 11) % 251 - 125
    return vals.astype(np.int8).reshape(n_pairs, 2, n_samples)


class TestAdcSnapshotRoundTrip:
    def test_publish_and_read_roundtrip(self, transport):
        writer = AdcSnapshotWriter(transport)
        reader = AdcSnapshotReader(transport)
        _seed_reader_cursor(transport)
        data = (
            (np.arange(3 * 2 * 2048) % 127 - 63)
            .astype(np.int8)
            .reshape(3, 2, 2048)
        )
        writer.add(
            data,
            unix_ts=1700.5,
            sync_time=1600.0,
            corr_acc_cnt=42,
            wiring={"ants": {"a0": {"snap": {"input": 0}}}},
        )
        out, sidecar = reader.read(timeout=1)
        np.testing.assert_array_equal(out, data)
        assert out.dtype == np.int8
        assert sidecar["unix_ts"] == 1700.5
        assert sidecar["sync_time"] == 1600.0
        assert sidecar["corr_acc_cnt"] == 42
        assert sidecar["wiring"]["ants"]["a0"]["snap"]["input"] == 0

    def test_reader_returns_none_when_stream_absent(self, transport):
        reader = AdcSnapshotReader(transport)
        data, sidecar = reader.read(timeout=0)
        assert data is None
        assert sidecar is None

    def test_reader_timeout_when_stream_present_but_empty(self, transport):
        writer = AdcSnapshotWriter(transport)
        reader = AdcSnapshotReader(transport)
        _seed_reader_cursor(transport)
        data = np.zeros((1, 2, 4), dtype=np.int8)
        writer.add(data, unix_ts=0.0)
        reader.read(timeout=1)  # drain the one entry
        # ...then another read with no new data must time out.
        with pytest.raises(TimeoutError):
            reader.read(timeout=0.1)

    def test_writer_rejects_non_ndarray(self, transport):
        writer = AdcSnapshotWriter(transport)
        with pytest.raises(ValueError):
            writer.add([1, 2, 3], unix_ts=0.0)

    def test_writer_registers_in_data_streams_set(self, transport):
        writer = AdcSnapshotWriter(transport)
        data = np.zeros((1, 2, 4), dtype=np.int8)
        writer.add(data, unix_ts=0.0)
        assert transport.r.sismember(
            DATA_STREAMS_SET, ADC_SNAPSHOT_STREAM.encode()
        ) or transport.r.sismember(DATA_STREAMS_SET, ADC_SNAPSHOT_STREAM)


class TestAdcStatsSchemaReduction:
    """``_avg_sensor_values`` handles the adc_stats schema via the
    standard float→mean path. One regression test per the "fixtures
    must match production data" rule — shape, field names, and dtypes
    mirror what ``EigsepFpga._publish_adc_stats`` actually emits."""

    def _sample(self, rms_offset=0.0, status="update"):
        out = {"sensor_name": "adc_stats", "status": status}
        for i in range(12):
            n, c = i // 2, i % 2
            out[f"input{n}_core{c}_mean"] = 0.01 + 0.001 * i
            out[f"input{n}_core{c}_power"] = 10.0 + i + rms_offset
            out[f"input{n}_core{c}_rms"] = np.sqrt(10.0 + i + rms_offset)
        return out

    def test_reduction_averages_floats_and_collapses_status(self):
        schema = io.SENSOR_SCHEMAS["adc_stats"]
        samples = [self._sample(rms_offset=o) for o in (0.0, 2.0, 4.0)]
        avg = io._avg_sensor_values(samples, schema=schema)
        assert avg["status"] == "update"
        assert avg["sensor_name"] == "adc_stats"
        for i in range(12):
            n, c = i // 2, i % 2
            # power mean over (10+i, 12+i, 14+i) is 12+i
            assert avg[f"input{n}_core{c}_power"] == pytest.approx(12.0 + i)

    def test_reduction_marks_row_error_on_any_errored_sample(self):
        schema = io.SENSOR_SCHEMAS["adc_stats"]
        samples = [
            self._sample(status="update"),
            self._sample(status="error"),
        ]
        avg = io._avg_sensor_values(samples, schema=schema)
        assert avg["status"] == "error"
        # errored sample is filtered; the clean one carries the value.
        assert avg["input0_core0_power"] == pytest.approx(10.0)


class TestGrabAdcFrames:
    """``_grab_adc_frames`` is the single FPGA-touching call of the
    snapshot tick; both publishers consume its output."""

    def test_grab_stacks_all_antenna_pairs(self, fpga):
        pols = [
            (
                np.full(2048, 2 * i, dtype=np.int8),
                np.full(2048, 2 * i + 1, dtype=np.int8),
            )
            for i in range(3)
        ]
        with patch.object(
            fpga.inp, "get_adc_snapshot", side_effect=pols
        ) as snap:
            out = fpga._grab_adc_frames()
        assert snap.call_count == 3
        assert out.shape == (3, 2, 2048)
        assert out.dtype == np.int8
        # Row order is the snap-input order the corr file uses for
        # autos: pair p carries inputs 2p (x) and 2p+1 (y).
        for i in range(3):
            assert (out[i, 0] == 2 * i).all()
            assert (out[i, 1] == 2 * i + 1).all()

    def test_grab_failure_disables_both_publishers_one_warning(
        self, fpga, caplog
    ):
        caplog.set_level(logging.WARNING)
        with patch.object(
            fpga.inp,
            "get_adc_snapshot",
            side_effect=RuntimeError("bram gone"),
        ):
            # Must not raise — corr data is sacred.
            out = fpga._grab_adc_frames()
        assert out is None
        assert fpga._adc_snapshot_enabled is False
        assert fpga._adc_stats_enabled is False
        assert "bram gone" in caplog.text
        warnings = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING and "Disabling" in r.getMessage()
        ]
        assert len(warnings) == 1


class TestPublishAdcStats:
    """``_publish_adc_stats`` derives per-core stats from snapshot
    frames — no FPGA access of its own (the flashed bitstreams carry
    no ``input_rms_*`` registers; see the 2026-07-09 eigsep-backend
    diagnosis)."""

    def test_publish_writes_per_core_stats_from_frames(self, fpga):
        frames = _make_frames()
        fpga._publish_adc_stats(frames)
        raw = fpga.transport.r.hget("metadata", "adc_stats")
        assert raw is not None
        payload = json.loads(raw)
        assert payload["sensor_name"] == "adc_stats"
        assert payload["status"] == "update"
        # Independent reduction: snap input n is row n of the
        # flattened (pair, pol) axis; core c is the even/odd sample
        # interleave.
        flat = frames.reshape(-1, frames.shape[-1]).astype(np.float64)
        for n in range(6):
            for c in range(2):
                core = flat[n, c::2]
                power = np.mean(core**2)
                assert payload[f"input{n}_core{c}_mean"] == pytest.approx(
                    core.mean()
                )
                assert payload[f"input{n}_core{c}_power"] == pytest.approx(
                    power
                )
                assert payload[f"input{n}_core{c}_rms"] == pytest.approx(
                    np.sqrt(power)
                )

    def test_payload_validates_against_sensor_schema(self, fpga):
        frames = _make_frames()
        fpga._publish_adc_stats(frames)
        payload = json.loads(fpga.transport.r.hget("metadata", "adc_stats"))
        violations = io._validate_metadata(
            payload, io.SENSOR_SCHEMAS["adc_stats"]
        )
        assert violations == []

    def test_publish_failure_disables_publisher_with_warning(
        self, fpga, caplog
    ):
        caplog.set_level(logging.WARNING)
        with patch.object(
            fpga.adc_metadata_writer,
            "add",
            side_effect=RuntimeError("redis down"),
        ):
            # Must not raise — corr data is sacred.
            fpga._publish_adc_stats(_make_frames())
        assert fpga._adc_stats_enabled is False
        # The raw-snapshot publisher is independent; a stats-side
        # failure must not take it down.
        assert fpga._adc_snapshot_enabled is True
        assert "Disabling adc_stats publisher" in caplog.text
        assert "redis down" in caplog.text

    def test_publish_after_disable_is_no_op(self, fpga, caplog):
        caplog.set_level(logging.WARNING)
        with patch.object(
            fpga.adc_metadata_writer,
            "add",
            side_effect=RuntimeError("redis down"),
        ) as add:
            fpga._publish_adc_stats(_make_frames())  # fails, disables
            assert add.call_count == 1
            assert fpga._adc_stats_enabled is False
            # Subsequent calls must not touch the writer again.
            fpga._publish_adc_stats(_make_frames())
            fpga._publish_adc_stats(_make_frames())
            assert add.call_count == 1
        # Only one WARNING line for the whole run.
        warnings = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING and "adc_stats" in r.getMessage()
        ]
        assert len(warnings) == 1


class TestPublishAdcSnapshot:
    def test_publish_snapshot_writes_frame_with_sidecar(self, fpga):
        frames = _make_frames()
        fpga._publish_adc_snapshot(frames)
        reader = AdcSnapshotReader(fpga.transport)
        _seed_reader_cursor(fpga.transport)
        data, sidecar = reader.read(timeout=1)
        np.testing.assert_array_equal(data, frames)
        assert data.dtype == np.int8
        assert sidecar["wiring"]["ants"]  # non-empty
        assert "unix_ts" in sidecar

    def test_publish_snapshot_failure_disables_publisher_with_warning(
        self, fpga, caplog
    ):
        caplog.set_level(logging.WARNING)
        with patch.object(
            fpga.adc_snapshot_writer,
            "add",
            side_effect=RuntimeError("stream write refused"),
        ):
            fpga._publish_adc_snapshot(_make_frames())
        assert fpga._adc_snapshot_enabled is False
        # Stats are derived from the same frames but publish on their
        # own bus; a raw-side failure must not take them down.
        assert fpga._adc_stats_enabled is True
        assert "Disabling adc_snapshot publisher" in caplog.text
        assert "stream write refused" in caplog.text

    def test_publish_snapshot_after_disable_is_no_op(self, fpga, caplog):
        caplog.set_level(logging.WARNING)
        with patch.object(
            fpga.adc_snapshot_writer,
            "add",
            side_effect=RuntimeError("stream write refused"),
        ) as add:
            fpga._publish_adc_snapshot(_make_frames())  # fails, disables
            assert add.call_count == 1
            assert fpga._adc_snapshot_enabled is False
            fpga._publish_adc_snapshot(_make_frames())
            fpga._publish_adc_snapshot(_make_frames())
            assert add.call_count == 1
        warnings = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING
            and "adc_snapshot" in r.getMessage()
        ]
        assert len(warnings) == 1


class TestObserveSnapshotThread:
    """``observe()`` spawns or skips the snapshot thread based on
    ``adc_snapshot_period_s``. Mocks ``Thread`` so neither the
    producer nor the snapshot loop actually runs — the tests assert
    only on *how* ``observe()`` constructed the threads."""

    def _run_observe_with_mocked_threads(self, fpga):
        """Shared setup: patch ``Thread`` with a fake whose start()
        short-circuits the consumer loop by pushing the None
        sentinel and setting the event. Returns the list of
        (args, kwargs) each Thread was constructed with."""
        from unittest.mock import Mock

        calls = []

        def make_mock(*args, **kwargs):
            calls.append((args, kwargs))
            m = Mock()
            target = kwargs.get("target")
            if target is not None and target.__name__ == "_read_integrations":

                def fake_start():
                    fpga.queue.put(None)
                    fpga.event.set()

                m.start = fake_start
            else:
                m.start = lambda: None
            return m

        with (
            patch.object(fpga, "upload_config"),
            patch("eigsep_observing.fpga.Thread", side_effect=make_mock),
        ):
            fpga.observe(pairs=["0"])
        return calls

    def test_snapshot_thread_spawned_when_period_set(self, fpga):
        fpga.cfg["adc_snapshot_period_s"] = 0.5
        calls = self._run_observe_with_mocked_threads(fpga)
        targets = [kwargs.get("target") for _, kwargs in calls]
        assert fpga._publish_snapshots_loop in targets

    def test_snapshot_thread_skipped_when_period_unset(self, fpga, caplog):
        fpga.cfg["adc_snapshot_period_s"] = None
        caplog.set_level(logging.INFO)
        calls = self._run_observe_with_mocked_threads(fpga)
        targets = [kwargs.get("target") for _, kwargs in calls]
        assert fpga._publish_snapshots_loop not in targets
        # The skip message must name both casualties — adc_stats rides
        # the snapshot tick now.
        assert "adc_stats publishers disabled" in caplog.text

    def test_snapshot_thread_skipped_when_period_zero(self, fpga, caplog):
        fpga.cfg["adc_snapshot_period_s"] = 0
        caplog.set_level(logging.INFO)
        calls = self._run_observe_with_mocked_threads(fpga)
        targets = [kwargs.get("target") for _, kwargs in calls]
        assert fpga._publish_snapshots_loop not in targets


class TestPublishSnapshotsLoop:
    """One grab per tick feeds both publishers; the loop exits once
    both are latched off and skips ticks pre-sync."""

    def _run_loop_briefly(self, fpga, period=0.02, runtime=0.1):
        t = Thread(
            target=fpga._publish_snapshots_loop,
            args=(period,),
            daemon=True,
        )
        t.start()
        import time as _time

        _time.sleep(runtime)
        fpga.event.set()
        t.join(timeout=1)
        return t

    def test_one_grab_feeds_both_publishers(self, fpga):
        fpga.event = Event()
        fpga.is_synchronized = True
        frames = _make_frames()
        with (
            patch.object(
                fpga, "_grab_adc_frames", return_value=frames
            ) as grab,
            patch.object(fpga, "_publish_adc_snapshot") as snap,
            patch.object(fpga, "_publish_adc_stats") as stats,
        ):
            self._run_loop_briefly(fpga)
        assert grab.call_count >= 1
        assert snap.call_count == grab.call_count
        assert stats.call_count == grab.call_count
        snap.assert_called_with(frames)
        stats.assert_called_with(frames)

    def test_failed_grab_publishes_nothing(self, fpga):
        fpga.event = Event()
        fpga.is_synchronized = True
        with (
            patch.object(fpga, "_grab_adc_frames", return_value=None),
            patch.object(fpga, "_publish_adc_snapshot") as snap,
            patch.object(fpga, "_publish_adc_stats") as stats,
        ):
            self._run_loop_briefly(fpga)
        snap.assert_not_called()
        stats.assert_not_called()

    def test_loop_exits_on_event(self, fpga):
        fpga.event = Event()
        fpga.is_synchronized = False
        # Set the event before entering the loop; event.wait() returns
        # True immediately and the function returns.
        fpga.event.set()
        fpga._publish_snapshots_loop(0.01)  # returns cleanly

    def test_loop_exits_when_both_publishers_disabled(self, fpga):
        """Once both latches flip off the loop must return rather
        than keep grabbing frames nobody will publish."""
        fpga.event = Event()
        fpga.is_synchronized = True
        fpga._adc_snapshot_enabled = False
        fpga._adc_stats_enabled = False
        t = Thread(
            target=fpga._publish_snapshots_loop,
            args=(0.01,),
            daemon=True,
        )
        t.start()
        t.join(timeout=1)
        assert not t.is_alive()

    def test_loop_keeps_grabbing_when_only_snapshot_disabled(self, fpga):
        """adc_stats alone still justifies the FPGA grab."""
        fpga.event = Event()
        fpga.is_synchronized = True
        fpga._adc_snapshot_enabled = False
        frames = _make_frames()
        with (
            patch.object(
                fpga, "_grab_adc_frames", return_value=frames
            ) as grab,
            patch.object(fpga, "_publish_adc_stats") as stats,
        ):
            self._run_loop_briefly(fpga)
        assert grab.call_count >= 1
        assert stats.call_count == grab.call_count

    def test_loop_skips_grab_when_unsynced(self, fpga):
        fpga.event = Event()
        fpga.is_synchronized = False
        with patch.object(fpga, "_grab_adc_frames") as grab:
            self._run_loop_briefly(fpga)
        grab.assert_not_called()


class TestPublishCorrHealth:
    """``_publish_corr_health`` ships the in-memory counters as the
    ``corr_health`` Redis K/V (dashboard-only; deliberately not on the
    metadata bus — the file records drops as acc_cnt gaps)."""

    def test_publish_writes_expected_fields(self, fpga):
        fpga._dropped_integrations = 3
        fpga._last_readout_s = 0.05  # 50 ms
        fpga._publish_corr_health()
        out = read_corr_health(fpga.transport)
        assert out["dropped_integrations"] == 3
        assert out["readout_time_ms"] == pytest.approx(50.0)
        assert out["published_unix"] is not None

    def test_publish_with_no_readout_yet_emits_null(self, fpga):
        """Before the first read, ``_last_readout_s`` is None; the K/V
        ships null honestly so the dashboard omits the readout suffix
        rather than rendering a fake 0 ms."""
        assert fpga._last_readout_s is None
        fpga._publish_corr_health()
        out = read_corr_health(fpga.transport)
        assert out["readout_time_ms"] is None
        assert out["dropped_integrations"] == 0

    def test_publish_failure_disables_publisher_with_error(self, fpga, caplog):
        caplog.set_level(logging.ERROR)
        with patch.object(
            fpga.transport,
            "add_raw",
            side_effect=RuntimeError("redis down"),
        ):
            # Must not raise — corr data is sacred.
            fpga._publish_corr_health()
        assert fpga._corr_health_enabled is False
        assert "Disabling corr_health publisher" in caplog.text
        assert "redis down" in caplog.text

    def test_publish_after_disable_is_no_op(self, fpga):
        with patch.object(
            fpga.transport,
            "add_raw",
            side_effect=RuntimeError("redis down"),
        ) as add_raw:
            fpga._publish_corr_health()  # first call: fails, disables
            assert fpga._corr_health_enabled is False
            assert add_raw.call_count == 1
            fpga._publish_corr_health()
            fpga._publish_corr_health()
            assert add_raw.call_count == 1


class TestDiagnosticsLoop:
    """The diagnostics loop is corr_health-only: adc_stats moved to
    the snapshot tick, so this loop never touches the FPGA."""

    def _run_loop_briefly(self, fpga, period=0.02, runtime=0.1):
        t = Thread(
            target=fpga._publish_diagnostics_loop,
            args=(period,),
            daemon=True,
        )
        t.start()
        import time as _time

        _time.sleep(runtime)
        fpga.event.set()
        t.join(timeout=1)

    def test_loop_publishes_corr_health_only_when_synced(self, fpga):
        fpga.event = Event()
        fpga.is_synchronized = True
        with (
            patch.object(fpga, "_publish_corr_health") as corr_health,
            patch.object(fpga, "_publish_adc_stats") as adc_stats,
        ):
            self._run_loop_briefly(fpga)
        assert corr_health.call_count >= 1
        adc_stats.assert_not_called()

    def test_loop_skips_publish_when_unsynced(self, fpga):
        fpga.event = Event()
        fpga.is_synchronized = False
        with patch.object(fpga, "_publish_corr_health") as corr_health:
            self._run_loop_briefly(fpga)
        corr_health.assert_not_called()
