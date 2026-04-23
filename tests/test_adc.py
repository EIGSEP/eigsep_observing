"""Tests for the ADC diagnostic path: AdcSnapshot writer/reader
round-trip, adc_stats schema reduction through ``avg_metadata``, and
the per-integration / periodic publish hooks in ``EigsepFpga``.

The FPGA-level tests patch ``Input.get_stats`` / ``Input.get_adc_snapshot``
with scoped ``patch.object`` rather than replacing ``fpga.inp`` wholesale
— the real ``Input`` block runs unchanged against ``DummyFpga``, and
only the values returned by those two methods need to be deterministic
for the assertions below.
"""

import logging
from queue import Queue
from threading import Event
from unittest.mock import patch

import numpy as np
import pytest

from eigsep_observing import io
from eigsep_observing.adc import AdcSnapshotReader, AdcSnapshotWriter
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
    transport._set_last_read_id(ADC_SNAPSHOT_STREAM, "0")


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
        out = {"sensor_name": "adc", "status": status}
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
        assert avg["sensor_name"] == "adc"
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


class TestPublishAdcStats:
    def test_publish_writes_expected_schema_fields(self, fpga):
        means = np.linspace(-1.0, 1.0, 12)
        powers = np.linspace(0.5, 2.0, 12)
        rmss = np.sqrt(powers)
        with patch.object(
            fpga.inp, "get_stats", return_value=(means, powers, rmss)
        ):
            fpga._publish_adc_stats()
        # Read back via the metadata snapshot hash — the writer wrote
        # there as part of ``MetadataWriter.add``.
        import json

        raw = fpga.transport.r.hget("metadata", "adc_stats")
        assert raw is not None
        payload = json.loads(raw)
        assert payload["sensor_name"] == "adc"
        assert payload["status"] == "update"
        for i in range(12):
            n, c = i // 2, i % 2
            assert payload[f"input{n}_core{c}_mean"] == pytest.approx(means[i])
            assert payload[f"input{n}_core{c}_power"] == pytest.approx(
                powers[i]
            )
            assert payload[f"input{n}_core{c}_rms"] == pytest.approx(rmss[i])

    def test_publish_failure_is_logged_not_raised(self, fpga, caplog):
        caplog.set_level(logging.ERROR)
        with patch.object(
            fpga.inp, "get_stats", side_effect=RuntimeError("FPGA dead")
        ):
            # Must not raise — corr data is sacred.
            fpga._publish_adc_stats()
        assert "Failed to publish adc_stats" in caplog.text


class TestPublishAdcSnapshot:
    def test_publish_snapshot_writes_frame_with_sidecar(self, fpga):
        # Two inputs per "antenna" in the Input block; DummyFpga has
        # ``nstreams=12`` so 3 antennas × 2 pols per antenna.
        fake_pol = np.arange(2048, dtype=np.int8)
        with patch.object(
            fpga.inp, "get_adc_snapshot", return_value=(fake_pol, fake_pol)
        ):
            fpga._publish_adc_snapshot()
        reader = AdcSnapshotReader(fpga.transport)
        _seed_reader_cursor(fpga.transport)
        data, sidecar = reader.read(timeout=1)
        assert data.shape == (3, 2, 2048)
        assert data.dtype == np.int8
        assert sidecar["wiring"]["ants"]  # non-empty
        assert "unix_ts" in sidecar

    def test_publish_snapshot_failure_logged_not_raised(self, fpga, caplog):
        caplog.set_level(logging.ERROR)
        with patch.object(
            fpga.inp,
            "get_adc_snapshot",
            side_effect=RuntimeError("snapshot bram misaligned"),
        ):
            fpga._publish_adc_snapshot()
        assert "Failed to publish adc_snapshot" in caplog.text


class TestReadIntegrationsPublishesStats:
    def test_publishes_on_each_integration_when_synced(self, fpga):
        fpga.queue = Queue()
        fpga.event = Event()
        fpga.is_synchronized = True
        pairs = ["0"]

        calls = []

        def read_int(reg):
            calls.append(reg)
            n = len(calls)
            if n == 1:
                return 0
            if n == 2:
                return 1
            fpga.event.set()
            return 1

        publish_spy = []

        def spy_publish():
            publish_spy.append(1)

        with (
            patch.object(fpga.fpga, "read_int", side_effect=read_int),
            patch.object(fpga, "read_data", return_value={"0": b"\x00"}),
            patch.object(fpga, "_publish_adc_stats", side_effect=spy_publish),
        ):
            fpga._read_integrations(pairs, timeout=1)

        assert len(publish_spy) == 1

    def test_does_not_publish_when_unsynced(self, fpga):
        fpga.queue = Queue()
        fpga.event = Event()
        fpga.is_synchronized = False  # key precondition
        pairs = ["0"]

        calls = []

        def read_int(reg):
            calls.append(reg)
            n = len(calls)
            if n == 1:
                return 0
            if n == 2:
                return 1
            fpga.event.set()
            return 1

        publish_spy = []
        with (
            patch.object(fpga.fpga, "read_int", side_effect=read_int),
            patch.object(fpga, "read_data", return_value={"0": b"\x00"}),
            patch.object(
                fpga,
                "_publish_adc_stats",
                side_effect=lambda: publish_spy.append(1),
            ),
        ):
            fpga._read_integrations(pairs, timeout=1)
        assert publish_spy == []


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
        assert "ADC snapshot publisher disabled" in caplog.text

    def test_snapshot_thread_skipped_when_period_zero(self, fpga, caplog):
        fpga.cfg["adc_snapshot_period_s"] = 0
        caplog.set_level(logging.INFO)
        calls = self._run_observe_with_mocked_threads(fpga)
        targets = [kwargs.get("target") for _, kwargs in calls]
        assert fpga._publish_snapshots_loop not in targets


class TestPublishSnapshotsLoopShutdown:
    """``_publish_snapshots_loop`` must exit cleanly on ``event.set()``
    without publishing pre-sync garbage."""

    def test_loop_exits_on_event(self, fpga):
        fpga.event = Event()
        fpga.is_synchronized = False
        # Set the event before entering the loop; event.wait() returns
        # True immediately and the function returns.
        fpga.event.set()
        fpga._publish_snapshots_loop(0.01)  # returns cleanly

    def test_loop_skips_publish_when_unsynced(self, fpga):
        fpga.event = Event()
        fpga.is_synchronized = False
        calls = []
        with patch.object(
            fpga,
            "_publish_adc_snapshot",
            side_effect=lambda: calls.append(1),
        ):
            # Start the loop in a thread; it waits period, sees
            # unsynced, continues. Set event after a moment to exit.
            from threading import Thread as RealThread

            t = RealThread(
                target=fpga._publish_snapshots_loop,
                args=(0.02,),
                daemon=True,
            )
            t.start()
            import time as _time

            _time.sleep(0.1)  # let several loop iterations elapse
            fpga.event.set()
            t.join(timeout=1)
        assert calls == []
