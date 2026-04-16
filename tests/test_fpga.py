import logging
from contextlib import contextmanager
from queue import Queue
from threading import Event

import pytest
from unittest.mock import Mock, patch

from eigsep_observing import EigsepFpga
from eigsep_observing import corr as corr_mod
from eigsep_observing.keys import CORR_STREAM
from eigsep_observing.testing import DummyEigsepFpga, utils


@contextmanager
def _patch_observe_thread(fpga, items):
    """
    Patch ``eigsep_observing.fpga.Thread`` for tests of
    ``EigsepFpga.observe()``.

    Replaces the producer thread with a mock whose ``start()`` runs
    synchronously in the calling thread, pushes ``items`` into
    ``fpga.queue``, and sets ``fpga.event``. ``observe()``'s consumer
    loop then runs in the main thread against deterministic input.

    Parameters
    ----------
    fpga : DummyEigsepFpga
        The fpga fixture under test. ``fpga.queue`` and ``fpga.event``
        are created by ``observe()`` itself, so they are accessed
        lazily inside the fake ``start``.
    items : iterable
        Items to inject. Each is either a dict
        ``{"data": ..., "cnt": ...}`` (a normal integration) or
        ``None`` (the end-of-stream sentinel that the real producer
        pushes via ``end_observing``). The helper does NOT auto-append
        a sentinel — pass one explicitly if you want the consumer to
        log ``"End of queue, processing finished."``.

    Yields
    ------
    Mock
        The patched ``Thread`` class, so tests can assert on how
        ``observe()`` constructed it (e.g. ``args``, ``kwargs``).
    """
    with patch("eigsep_observing.fpga.Thread") as mock_thread_class:
        mock_thread = Mock()
        mock_thread_class.return_value = mock_thread

        def fake_start():
            for item in items:
                fpga.queue.put(item)
            fpga.event.set()

        mock_thread.start = fake_start
        yield mock_thread_class


@pytest.fixture
def fpga_instance():
    """
    DummyEigsepFpga instance for FPGA tests.

    Uses the real DummyEigsepObsRedis (fakeredis-backed) the dummy
    constructs in __init__. We do *not* mock fpga.fpga, fpga.logger,
    fpga.read_data, fpga.validate_config, or fpga.redis — the dummy
    provides working implementations of all of those, and clobbering
    them with bare Mocks just hides what's there. Tests that need to
    assert on outbound redis calls should use a per-test
    ``patch.object(fpga_instance.redis, "<method>",
    wraps=fpga_instance.redis.<method>)`` spy scoped to the specific
    method — see ``test_upload_config_with_validation_success`` for
    the canonical pattern. Tests that need to control a specific
    method (e.g. ``_read_integrations`` sequencing ``read_int`` return
    values) should use the same per-test ``patch.object`` shape.
    """
    return DummyEigsepFpga(program=False)


class TestEigsepFpga:
    """Test cases for EigsepFpga class."""

    @patch("eigsep_observing.fpga.EigsepObsRedis")
    def test_create_redis(self, mock_redis_class):
        """Test _create_redis static method."""
        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        redis = EigsepFpga._create_redis("localhost", 6379)

        mock_redis_class.assert_called_once_with(host="localhost", port=6379)
        assert redis == mock_redis_instance

    def test_upload_config_with_validation_success(
        self, fpga_instance, caplog
    ):
        """upload_config(validate=True) runs real validate_config and uploads."""
        caplog.set_level(logging.DEBUG)

        with patch.object(
            fpga_instance.redis.corr_config,
            "upload_config",
            wraps=fpga_instance.redis.corr_config.upload_config,
        ) as spy:
            fpga_instance.upload_config(validate=True)

        # Implicit success: if validate_config had raised, the upload
        # below would never have happened.
        spy.assert_called_once_with(fpga_instance.cfg, from_file=False)
        assert "Uploading configuration to Redis." in caplog.text

    def test_upload_config_without_validation(self, fpga_instance):
        """upload_config(validate=False) skips validation but still uploads."""
        with (
            patch.object(
                fpga_instance,
                "validate_config",
                wraps=fpga_instance.validate_config,
            ) as validate_spy,
            patch.object(
                fpga_instance.redis.corr_config,
                "upload_config",
                wraps=fpga_instance.redis.corr_config.upload_config,
            ) as upload_spy,
        ):
            fpga_instance.upload_config(validate=False)

        validate_spy.assert_not_called()
        upload_spy.assert_called_once_with(fpga_instance.cfg, from_file=False)

    def test_upload_config_validation_failure(self, fpga_instance, caplog):
        """upload_config raises and logs when validate_config fails."""
        caplog.set_level(logging.ERROR)

        with (
            patch.object(
                fpga_instance,
                "validate_config",
                side_effect=RuntimeError("Config invalid"),
            ),
            patch.object(
                fpga_instance.redis.corr_config,
                "upload_config",
                wraps=fpga_instance.redis.corr_config.upload_config,
            ) as upload_spy,
        ):
            with pytest.raises(
                RuntimeError, match="Configuration validation failed"
            ):
                fpga_instance.upload_config(validate=True)

        assert "Configuration validation failed: Config invalid" in caplog.text
        upload_spy.assert_not_called()

    def test_synchronize(self, fpga_instance):
        """synchronize sets sync_time on the corr header and uploads it."""
        # Spy on the real Sync block so we observe the high-level
        # sequence (set_delay / arm_sync / sw_sync) without stubbing
        # out the register writes — the wraps= keeps DummyFpga state
        # in sync with what real hardware would see. upload_corr_header
        # is wraps=, so the value actually lands in fakeredis and we
        # can read it back via the production get_corr_header path —
        # this is the writer↔reader contract guard for the corr header
        # round-trip (sync_time lives on the header, not metadata).
        sync = fpga_instance.sync
        with (
            patch.object(
                sync, "set_delay", wraps=sync.set_delay
            ) as spy_set_delay,
            patch.object(
                sync, "arm_sync", wraps=sync.arm_sync
            ) as spy_arm_sync,
            patch.object(sync, "sw_sync", wraps=sync.sw_sync) as spy_sw_sync,
            patch(
                "eigsep_observing.fpga.time.time",
                return_value=1234567890.0,
            ),
            patch.object(
                fpga_instance.redis.corr_config,
                "upload_header",
                wraps=fpga_instance.redis.corr_config.upload_header,
            ) as spy_upload_header,
        ):
            fpga_instance.synchronize(delay=5)

        spy_set_delay.assert_called_once_with(5)
        spy_arm_sync.assert_called_once()
        assert spy_sw_sync.call_count == 3
        # synchronize() pushes the header once so consumers see fresh
        # sync_time. The observe loop does not re-upload — header
        # publication is state-change driven.
        spy_upload_header.assert_called_once()

        # Round-trip: read corr_header back through the production
        # get_corr_header path and confirm sync_time survived the
        # json-encode → json-decode pipeline intact.
        header = fpga_instance.redis.corr_config.get_header()
        assert header["sync_time"] == 1234567890.0

    def test_synchronize_default_delay(self, fpga_instance):
        """synchronize() with default delay still publishes sync_time on the header."""
        with patch(
            "eigsep_observing.fpga.time.time",
            return_value=1111111111.0,
        ):
            fpga_instance.synchronize()

        header = fpga_instance.redis.corr_config.get_header()
        assert header["sync_time"] == 1111111111.0

    def test_synchronize_noise_mode(self, fpga_instance):
        """In noise mode, synchronize() arms the noise generator (not
        the external sync path) and skips set_delay — but still records
        sync_time and uploads the header so CorrWriter.add un-gates."""
        fpga_instance.cfg["use_noise"] = True
        sync = fpga_instance.sync
        with (
            patch.object(
                sync, "set_delay", wraps=sync.set_delay
            ) as spy_set_delay,
            patch.object(
                sync, "arm_sync", wraps=sync.arm_sync
            ) as spy_arm_sync,
            patch.object(
                sync, "arm_noise", wraps=sync.arm_noise
            ) as spy_arm_noise,
            patch.object(sync, "sw_sync", wraps=sync.sw_sync) as spy_sw_sync,
            patch(
                "eigsep_observing.fpga.time.time",
                return_value=2222222222.0,
            ),
        ):
            fpga_instance.synchronize(delay=5)

        spy_arm_noise.assert_called_once()
        spy_arm_sync.assert_not_called()
        spy_set_delay.assert_not_called()
        assert spy_sw_sync.call_count == 3
        assert fpga_instance.is_synchronized is True
        header = fpga_instance.redis.corr_config.get_header()
        assert header["sync_time"] == 2222222222.0

    def test_initialize_all_enabled(self, fpga_instance, caplog):
        """initialize() with sync=True calls synchronize and logs."""
        caplog.set_level(logging.DEBUG)
        with patch.object(fpga_instance, "synchronize") as mock_sync:
            fpga_instance.initialize(
                initialize_adc=True, initialize_fpga=True, sync=True
            )

            mock_sync.assert_called_once()

        assert "Synchronizing correlator clock." in caplog.text

    def test_initialize_sync_disabled_attach_path(self, fpga_instance):
        """initialize(initialize_fpga=False, sync=False) is the attach
        path: set_input runs but synchronize does not."""
        with patch.object(fpga_instance, "synchronize") as mock_sync:
            fpga_instance.initialize(
                initialize_adc=False, initialize_fpga=False, sync=False
            )
            mock_sync.assert_not_called()

    def test_initialize_fpga_without_sync_raises(self, fpga_instance):
        """Re-initializing FPGA registers invalidates the prior
        sync_time, so initialize_fpga=True with sync=False must raise
        rather than silently leave a stale sync anchor."""
        with pytest.raises(ValueError, match="requires sync=True"):
            fpga_instance.initialize(
                initialize_adc=False, initialize_fpga=True, sync=False
            )

    def test_initialize_adc_disabled(self, fpga_instance):
        """Test initialize with ADC initialization disabled."""
        # Track calls to synchronize method
        with patch.object(fpga_instance, "synchronize") as mock_sync:
            fpga_instance.initialize(initialize_adc=False, sync=True)

            # Verify that synchronize was called even with initialize_adc=False
            mock_sync.assert_called_once()

    def test_rehydrate_sync_from_header_restores_state(
        self, fpga_instance, caplog
    ):
        """A header with a non-zero sync_time rehydrates
        self.sync_time / self.is_synchronized without hitting the
        hardware sync path."""
        caplog.set_level(logging.INFO)
        with patch(
            "eigsep_observing.fpga.time.time",
            return_value=2222222222.0,
        ):
            fpga_instance.synchronize()
        # Fresh instance semantics: reset the in-process state so we
        # prove the rehydrate path does the work.
        fpga_instance.sync_time = 0
        fpga_instance.is_synchronized = False

        assert fpga_instance.rehydrate_sync_from_header() is True
        assert fpga_instance.sync_time == 2222222222.0
        assert fpga_instance.is_synchronized is True
        assert "Rehydrated sync_time" in caplog.text

    def test_rehydrate_sync_from_header_no_header(self, fpga_instance, caplog):
        """Cold-boot case: no header in Redis → returns False, warns,
        and leaves state untouched."""
        caplog.set_level(logging.WARNING)
        fpga_instance.sync_time = 0
        fpga_instance.is_synchronized = False

        assert fpga_instance.rehydrate_sync_from_header() is False
        assert fpga_instance.sync_time == 0
        assert fpga_instance.is_synchronized is False
        assert "cold boot" in caplog.text

    def test_rehydrate_sync_from_header_zero_sync_time(
        self, fpga_instance, caplog
    ):
        """Header present but sync_time=0 → refuse to rehydrate."""
        caplog.set_level(logging.WARNING)
        fpga_instance.redis.corr_config.upload_header({"sync_time": 0})
        fpga_instance.sync_time = 0
        fpga_instance.is_synchronized = False

        assert fpga_instance.rehydrate_sync_from_header() is False
        assert fpga_instance.is_synchronized is False
        assert "sync_time=0" in caplog.text

    def test_upload_header_stamps_header_upload_unix(self, fpga_instance):
        """Every CorrConfigStore.upload_header stamps the wallclock of
        publication under 'header_upload_unix'. Consumers (file headers)
        read this alongside sync_time to detect post-sync mutations
        that weren't re-published."""
        with patch("eigsep_observing.corr.time.time", return_value=1234.0):
            fpga_instance.redis.corr_config.upload_header({"sync_time": 42})

        header = fpga_instance.redis.corr_config.get_header()
        assert header["sync_time"] == 42
        assert header["header_upload_unix"] == 1234.0

    def test_upload_header_does_not_mutate_caller_dict(self, fpga_instance):
        """Stamping is done on a copy so the caller's dict (typically
        EigsepFpga.header, a fresh dict per property access, but still)
        is not surprised with extra keys."""
        payload = {"sync_time": 42, "nchan": 1024}
        fpga_instance.redis.corr_config.upload_header(payload)
        assert payload == {"sync_time": 42, "nchan": 1024}

    def test_initialize_publishes_header_at_boundary(self, fpga_instance):
        """initialize()'s trailing line publishes the header after all
        sub-mutators have run, independently of which of them also
        published. Called with initialize_adc=False / initialize_fpga=
        False / sync=True the only other publish is synchronize(), so
        the boundary publish is observable as the second of exactly two
        calls (redundant but harmless, freshest-wins). A fuller
        initialize() call additionally goes through set_pam_atten (once
        per configured antenna) and set_pol_delay, each of which also
        publishes — that shape is covered by the dedicated sub-mutator
        tests below, not this one."""
        with patch.object(
            fpga_instance.redis.corr_config,
            "upload_header",
            wraps=fpga_instance.redis.corr_config.upload_header,
        ) as spy:
            fpga_instance.initialize(
                initialize_adc=False, initialize_fpga=False, sync=True
            )
        assert spy.call_count == 2

    def test_set_pam_atten_publishes_header(self, fpga_instance):
        """set_pam_atten mutates header-relevant state (rf_chain atten),
        so it must re-publish."""
        fpga_instance.initialize_pams()
        ant = next(iter(fpga_instance.cfg["rf_chain"]["ants"]))
        with patch.object(
            fpga_instance.redis.corr_config,
            "upload_header",
            wraps=fpga_instance.redis.corr_config.upload_header,
        ) as spy:
            fpga_instance.set_pam_atten(ant, 4)
        spy.assert_called_once()

    def test_set_pam_atten_all_publishes_header(self, fpga_instance):
        """set_pam_atten_all mutates all PAMs at once; same contract."""
        fpga_instance.initialize_pams()
        with patch.object(
            fpga_instance.redis.corr_config,
            "upload_header",
            wraps=fpga_instance.redis.corr_config.upload_header,
        ) as spy:
            fpga_instance.set_pam_atten_all(6)
        spy.assert_called_once()

    def test_set_pol_delay_publishes_header(self, fpga_instance):
        """set_pol_delay mutates pol_delay (a top-level header field)
        so it must re-publish."""
        with patch.object(
            fpga_instance.redis.corr_config,
            "upload_header",
            wraps=fpga_instance.redis.corr_config.upload_header,
        ) as spy:
            fpga_instance.set_pol_delay({"01": 3, "23": 0, "45": 0})
        spy.assert_called_once()

    def test_read_integrations_no_new_data(self, fpga_instance):
        """No new cnt → loop exits via pre-set event, queue stays empty."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        # Pre-set so the loop's while-condition fails after the first
        # read_int, before it has a chance to enter the iteration body.
        fpga_instance.event.set()

        with patch.object(
            fpga_instance.fpga, "read_int", return_value=5
        ) as mock_read_int:
            fpga_instance._read_integrations(["0", "1"], timeout=0.1)

        assert fpga_instance.queue.empty()
        mock_read_int.assert_called_with("corr_acc_cnt")

    def test_read_integrations_new_data(self, fpga_instance, caplog):
        """New cnt → data read, queued, and logged."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        pairs = ["0", "1"]
        fake_data = utils.generate_data(
            ntimes=1, raw=True, reshape=False, return_time_freq=False
        )
        expected_data = {p: fake_data[p] for p in pairs}

        # _read_integrations calls read_int 3 times per successful
        # iteration: (1) initial cnt before the loop, (2) new-cnt
        # check inside the loop, (3) validation read after read_data.
        # We set the event during the validation read so the next
        # iteration's while-condition fails and the loop exits cleanly.
        calls = []

        def read_int(reg):
            calls.append(reg)
            n = len(calls)
            if n == 1:
                return 5  # initial cnt (before loop)
            if n == 2:
                return 6  # new cnt → triggers data read
            # n == 3: validation read; matches new cnt so no error log
            fpga_instance.event.set()
            return 6

        caplog.set_level(logging.INFO)
        with (
            patch.object(fpga_instance.fpga, "read_int", side_effect=read_int),
            patch.object(
                fpga_instance, "read_data", return_value=expected_data
            ) as mock_read_data,
        ):
            fpga_instance._read_integrations(pairs, timeout=1)

        assert not fpga_instance.queue.empty()
        queued_item = fpga_instance.queue.get()
        assert queued_item["data"] == expected_data
        assert queued_item["cnt"] == 6
        assert "Reading acc_cnt=6" in caplog.text
        mock_read_data.assert_called_once_with(pairs=pairs, unpack=False)

    def test_read_integrations_missed_integrations(
        self, fpga_instance, caplog
    ):
        """cnt jumps > 1 → warning log, data still read."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        pairs = ["0", "1"]
        fake_data = utils.generate_data(
            ntimes=1, raw=True, reshape=False, return_time_freq=False
        )
        expected_data = {p: fake_data[p] for p in pairs}

        calls = []

        def read_int(reg):
            calls.append(reg)
            n = len(calls)
            if n == 1:
                return 5
            if n == 2:
                return 8  # jumped from 5 to 8 (missed 2)
            fpga_instance.event.set()
            return 8  # validation matches

        caplog.set_level(logging.WARNING)
        with (
            patch.object(fpga_instance.fpga, "read_int", side_effect=read_int),
            patch.object(
                fpga_instance, "read_data", return_value=expected_data
            ),
        ):
            fpga_instance._read_integrations(pairs, timeout=1)

        assert not fpga_instance.queue.empty()
        queued_item = fpga_instance.queue.get()
        assert queued_item["data"] == expected_data
        assert queued_item["cnt"] == 8
        assert "Missed 2 integration(s)." in caplog.text

    def test_read_integrations_read_failure(self, fpga_instance, caplog):
        """Validation read returns different cnt → error log, data still queued."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        pairs = ["0", "1"]
        fake_data = utils.generate_data(
            ntimes=1, raw=True, reshape=False, return_time_freq=False
        )
        expected_data = {p: fake_data[p] for p in pairs}

        calls = []

        def read_int(reg):
            calls.append(reg)
            n = len(calls)
            if n == 1:
                return 5
            if n == 2:
                return 6
            # Validation read with a different cnt → error log path
            fpga_instance.event.set()
            return 7

        caplog.set_level(logging.ERROR)
        with (
            patch.object(fpga_instance.fpga, "read_int", side_effect=read_int),
            patch.object(
                fpga_instance, "read_data", return_value=expected_data
            ),
        ):
            fpga_instance._read_integrations(pairs, timeout=1)

        assert not fpga_instance.queue.empty()
        queued_item = fpga_instance.queue.get()
        assert queued_item["data"] == expected_data
        assert queued_item["cnt"] == 6
        assert (
            "Read of acc_cnt=6 FAILED to complete before next integration."
            in caplog.text
        )

    def test_end_observing(self, fpga_instance):
        """Test that end_observing method exists and can be called."""
        # Just verify the method exists and can be called without error
        # The actual implementation is in the parent class
        fpga_instance.end_observing()

    def test_observe_basic_functionality(self, fpga_instance):
        """Test basic observe functionality."""
        fpga_instance.upload_config = Mock()
        expected_dtype = fpga_instance.cfg["dtype"]

        pairs = ["0"]

        # Two normal items followed by a None sentinel to end the loop
        d1 = utils.generate_data(
            ntimes=1, raw=True, reshape=False, return_time_freq=False
        )
        d1 = {p: d1[p] for p in pairs}  # filter to the observed pair(s)
        d2 = utils.generate_data(
            ntimes=1, raw=True, reshape=False, return_time_freq=False
        )
        d2 = {p: d2[p] for p in pairs}
        items = [
            {"data": d1, "cnt": 1},
            {"data": d2, "cnt": 2},
            None,
        ]
        with (
            patch.object(fpga_instance.redis.corr, "add") as mock_add,
            _patch_observe_thread(fpga_instance, items),
        ):
            fpga_instance.observe(pairs=pairs, timeout=10)

        fpga_instance.upload_config.assert_called_once_with(validate=True)
        assert mock_add.call_count == 2
        # Not synchronized in this test → sync_time=0; the gate only
        # fires inside the real CorrWriter.add (which is mocked out
        # here), so the call is still made with sync_time=0.
        mock_add.assert_any_call(d1, 1, 0, dtype=expected_dtype)
        mock_add.assert_any_call(d2, 2, 0, dtype=expected_dtype)

    def test_observe_default_pairs(self, fpga_instance, caplog):
        """observe() with no pairs arg defaults to self.pairs."""
        expected_dtype = fpga_instance.cfg["dtype"]

        d1 = utils.generate_data(
            ntimes=1, raw=True, reshape=False, return_time_freq=False
        )
        items = [{"data": d1, "cnt": 1}, None]
        caplog.set_level(logging.INFO)
        with (
            patch.object(fpga_instance.redis.corr, "add") as mock_add,
            _patch_observe_thread(fpga_instance, items) as mock_thread_cls,
        ):
            fpga_instance.observe()  # no pairs arg → defaults to self.pairs

        # Producer thread was constructed with self.pairs
        call_kwargs = mock_thread_cls.call_args.kwargs
        assert call_kwargs["args"] == (fpga_instance.pairs,)
        assert call_kwargs["kwargs"] == {"timeout": 10}
        assert call_kwargs["target"] == fpga_instance._read_integrations

        # And the log line names self.pairs, not the literal "None"
        assert (
            f"Starting observation for pairs: {fpga_instance.pairs}."
            in caplog.text
        )
        # Data still drained normally
        mock_add.assert_called_once_with(d1, 1, 0, dtype=expected_dtype)

    def test_observe_timeout_immediate(self, fpga_instance, caplog):
        """
        observe() exits cleanly when the producer ends without ever
        pushing data (only the None sentinel arrives).

        This subsumes the old test_observe_continuous_no_data — the
        consumer can't tell whether the producer never had data or
        timed out without it; both surface as "sentinel only". The
        producer-side no-data path is covered separately by
        test_read_integrations_no_new_data.
        """
        caplog.set_level(logging.INFO)
        with (
            patch.object(fpga_instance.redis.corr, "add") as mock_add,
            _patch_observe_thread(fpga_instance, [None]),
        ):
            fpga_instance.observe(pairs=["0"], timeout=1)

        mock_add.assert_not_called()
        assert "End of queue, processing finished." in caplog.text

    def test_observe_logging(self, fpga_instance, caplog):
        """observe() emits the expected info log lines."""
        # The dummy provides real registers now, so header computes
        # integration_time without any helper-side setup.
        expected_t_int = fpga_instance.header["integration_time"]

        d1 = utils.generate_data(
            ntimes=1, raw=True, reshape=False, return_time_freq=False
        )
        items = [{"data": d1, "cnt": 1}, None]
        caplog.set_level(logging.INFO)
        with _patch_observe_thread(fpga_instance, items):
            fpga_instance.observe(pairs=["0"], timeout=10)

        assert f"Integration time is {expected_t_int} seconds." in caplog.text
        assert "Starting observation for pairs: ['0']." in caplog.text
        assert "End of queue, processing finished." in caplog.text

    def test_observe_integration_loop(self, fpga_instance):
        """
        Consumer drains every queued integration, in order, before
        exiting on the sentinel.
        """
        expected_dtype = fpga_instance.cfg["dtype"]

        items = [{"data": {"0": [i]}, "cnt": 10 + i} for i in range(5)]
        items.append(None)
        with (
            patch.object(fpga_instance.redis.corr, "add") as mock_add,
            _patch_observe_thread(fpga_instance, items),
        ):
            fpga_instance.observe(pairs=["0", "1"], timeout=10)

        assert mock_add.call_count == 5
        # Order matters — assert via call_args_list, not assert_any_call.
        for i, call in enumerate(mock_add.call_args_list):
            assert call.args == ({"0": [i]}, 10 + i, 0)
            assert call.kwargs == {"dtype": expected_dtype}

    def test_observe_passes_sync_time_when_synced(self, fpga_instance):
        """After synchronize(), observe() passes the real sync_time to
        the writer so downstream can derive valid timestamps.
        """
        expected_dtype = fpga_instance.cfg["dtype"]

        with patch(
            "eigsep_observing.fpga.time.time", return_value=1713200000.0
        ):
            fpga_instance.synchronize(delay=5)

        d1 = utils.generate_data(
            ntimes=1, raw=True, reshape=False, return_time_freq=False
        )
        items = [{"data": d1, "cnt": 1}, None]
        with (
            patch.object(fpga_instance.redis.corr, "add") as mock_add,
            _patch_observe_thread(fpga_instance, items),
        ):
            fpga_instance.observe(pairs=["0"], timeout=10)

        mock_add.assert_called_once_with(
            d1, 1, 1713200000.0, dtype=expected_dtype
        )

    def test_observe_skips_corr_stream_when_unsynced(self, fpga_instance):
        """End-to-end with the real CorrWriter: unsynced SNAP → stream
        stays empty. Structural guarantee that pre-sync integrations
        cannot be consumed downstream.
        """
        corr_mod._last_unsynced_log[0] = 0.0
        assert fpga_instance.is_synchronized is False

        d1 = utils.generate_data(
            ntimes=1, raw=True, reshape=False, return_time_freq=False
        )
        items = [{"data": d1, "cnt": 1}, {"data": d1, "cnt": 2}, None]
        with _patch_observe_thread(fpga_instance, items):
            fpga_instance.observe(pairs=["0"], timeout=10)

        # Real CorrWriter.add with sync_time=0 → dropped; nothing on
        # the stream.
        r = fpga_instance.redis.transport.r
        assert r.xlen(CORR_STREAM) == 0
