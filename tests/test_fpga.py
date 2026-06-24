import logging
import threading
import time
from contextlib import contextmanager
from copy import deepcopy
from queue import Queue
from threading import Event

import pytest
from unittest.mock import Mock, patch

from eigsep_observing import corr as corr_mod
from eigsep_observing.fpga import _FpgaLockProxy, default_config
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
        # observe() spawns more than one thread (the producer plus the
        # throttled diagnostics loop, and optionally the snapshot loop).
        # Only the producer's start() should inject items — keying off
        # the target avoids double-loading the queue when the auxiliary
        # threads are constructed.
        def make_thread(*args, **kwargs):
            m = Mock()
            target = kwargs.get("target")
            if target is not None and target.__name__ == "_read_integrations":

                def fake_start():
                    for item in items:
                        fpga.queue.put(item)
                    fpga.event.set()

                m.start = fake_start
            else:
                m.start = lambda: None
            return m

        mock_thread_class.side_effect = make_thread
        yield mock_thread_class


@pytest.fixture
def fpga_instance():
    """
    DummyEigsepFpga instance for FPGA tests.

    Uses the fakeredis-backed ``DummyTransport`` the dummy constructs
    in __init__ and the per-role ``CorrConfigStore`` / ``CorrWriter``
    surfaces the fpga builds on top of it. We do *not* mock
    fpga.fpga, fpga.logger, fpga.read_data, fpga.validate_config, or
    the corr surfaces — the dummy provides working implementations
    of all of those, and clobbering them with bare Mocks just hides
    what's there. Tests that need to assert on outbound corr calls
    should use a per-test ``patch.object(fpga_instance.corr_config,
    "<method>", wraps=...)`` spy scoped to the specific method — see
    ``test_upload_config_with_validation_success`` for the canonical
    pattern.

    Uses the default wiring shipped in ``config/wiring.yaml``, which has
    no ``pam:`` blocks — PAM-specific tests build their own fixture via
    ``fpga_with_pams`` below.
    """
    return DummyEigsepFpga(program=False)


# Wiring with PAMs declared on every antenna — used by tests that
# exercise ``initialize_pams`` / ``set_pam_atten`` / ``get_pam_atten``.
# Three antennas span all three PAM board positions (num 0/1/2) and
# both polarizations, so the tests cover the dual-pol path on PAM 0 as
# well as the single-pol path on PAM 1.
_WIRING_WITH_PAMS = {
    "snap_id": "C000069",
    "ants": {
        "ant0N": {
            "fem": {"id": 32, "pol": "N"},
            "pam": {"id": 376, "num": 0, "pol": "N", "atten": 8},
            "snap": {"input": 0, "label": "N0"},
        },
        "ant0E": {
            "fem": {"id": 32, "pol": "E"},
            "pam": {"id": 376, "num": 0, "pol": "E", "atten": 8},
            "snap": {"input": 1, "label": "E2"},
        },
        "ant1N": {
            "fem": {"id": 348, "pol": "N"},
            "pam": {"id": 377, "num": 1, "pol": "N", "atten": 8},
            "snap": {"input": 2, "label": "N4"},
        },
    },
}


@pytest.fixture
def fpga_with_pams():
    """DummyEigsepFpga whose wiring declares PAMs on every antenna."""
    return DummyEigsepFpga(wiring=_WIRING_WITH_PAMS, program=False)


def _cfg_for_version(major, minor, **overrides):
    """A corr config declaring firmware ``major.minor``.

    ``DummyEigsepFpga._make_fpga`` derives the dummy's
    ``version_version`` register from ``cfg["fpg_version"]``, so the
    dummy's "hardware" matches the declared firmware and the
    version->acc_bins derivation can be exercised for either firmware.
    """
    cfg = deepcopy(default_config)
    cfg["fpg_version"] = [major, minor]
    cfg.update(overrides)
    return cfg


class TestFirmwareVersionLayout:
    """acc_bins is derived from the firmware version register, not the
    yaml config (the bitstream determines the data layout)."""

    def test_acc_bins_v24_is_one(self):
        """Firmware >= 2.4 emits a single spectrum -> acc_bins == 1."""
        fpga = DummyEigsepFpga(cfg=_cfg_for_version(2, 4), program=False)
        assert fpga.acc_bins == 1

    def test_acc_bins_v23_is_two(self):
        """Firmware < 2.4 emits even/odd -> acc_bins == 2."""
        fpga = DummyEigsepFpga(cfg=_cfg_for_version(2, 3), program=False)
        assert fpga.acc_bins == 2

    def test_reconcile_stamps_layout_into_cfg_v24(self):
        """The version-derived layout is written onto self.cfg so the
        published config + header reflect the silicon."""
        fpga = DummyEigsepFpga(cfg=_cfg_for_version(2, 4), program=False)
        assert fpga.cfg["acc_bins"] == 1
        assert fpga.cfg["avg_even_odd"] is False

    def test_reconcile_stamps_layout_into_cfg_v23(self):
        fpga = DummyEigsepFpga(cfg=_cfg_for_version(2, 3), program=False)
        assert fpga.cfg["acc_bins"] == 2
        assert fpga.cfg["avg_even_odd"] is True

    def test_header_acc_bins_from_version(self):
        fpga = DummyEigsepFpga(cfg=_cfg_for_version(2, 4), program=False)
        header = fpga.header
        assert header["acc_bins"] == 1
        assert header["avg_even_odd"] is False

    def test_reconcile_warns_on_stale_yaml_acc_bins(self, caplog):
        """A yaml acc_bins that disagrees with the firmware is
        auto-corrected with a WARNING (emergency-revert ergonomics)."""
        cfg = _cfg_for_version(2, 4, acc_bins=2, avg_even_odd=True)
        with caplog.at_level(logging.WARNING):
            fpga = DummyEigsepFpga(cfg=cfg, program=False)
        assert fpga.acc_bins == 1
        assert any(
            "acc_bins" in r.message and r.levelno == logging.WARNING
            for r in caplog.records
        )


class TestEigsepFpga:
    """Test cases for EigsepFpga class."""

    def test_upload_config_with_validation_success(
        self, fpga_instance, caplog
    ):
        """upload_config(validate=True) runs real validate_config and uploads."""
        caplog.set_level(logging.DEBUG)

        with patch.object(
            fpga_instance.corr_config,
            "upload",
            wraps=fpga_instance.corr_config.upload,
        ) as spy:
            fpga_instance.upload_config(validate=True)

        # Implicit success: if validate_config had raised, the upload
        # below would never have happened.
        spy.assert_called_once_with(fpga_instance.cfg)
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
                fpga_instance.corr_config,
                "upload",
                wraps=fpga_instance.corr_config.upload,
            ) as upload_spy,
        ):
            fpga_instance.upload_config(validate=False)

        validate_spy.assert_not_called()
        upload_spy.assert_called_once_with(fpga_instance.cfg)

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
                fpga_instance.corr_config,
                "upload",
                wraps=fpga_instance.corr_config.upload,
            ) as upload_spy,
        ):
            with pytest.raises(
                RuntimeError, match="Configuration validation failed"
            ):
                fpga_instance.upload_config(validate=True)

        assert "Configuration validation failed: Config invalid" in caplog.text
        upload_spy.assert_not_called()

    def test_assert_config_matches_redis_match(self, fpga_instance):
        """assert_config_matches_redis is a no-op when cfg matches Redis."""
        fpga_instance.corr_config.upload(fpga_instance.cfg)
        fpga_instance.assert_config_matches_redis()

    def test_assert_config_matches_redis_missing(self, fpga_instance):
        """Cold boot (no cfg in Redis) → caller must run with --reinit."""
        with pytest.raises(RuntimeError, match="No corr config in Redis"):
            fpga_instance.assert_config_matches_redis()

    def test_assert_config_matches_redis_mismatch(self, fpga_instance):
        """
        cfg differs from Redis → refuse with a diff. Simulates the
        real-world failure: user edited the yaml and attached without
        --reinit.
        """
        fpga_instance.corr_config.upload(fpga_instance.cfg)
        # Perturb a scalar and a nested field to exercise the recursive
        # diff summary.
        fpga_instance.cfg["sample_rate"] = fpga_instance.cfg["sample_rate"] / 2
        fpga_instance.cfg["pol_delay"]["01"] += 1

        with pytest.raises(RuntimeError) as exc:
            fpga_instance.assert_config_matches_redis()

        msg = str(exc.value)
        assert "sample_rate" in msg
        assert "pol_delay.01" in msg
        assert "--reinit" in msg

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
                fpga_instance.corr_config,
                "upload_header",
                wraps=fpga_instance.corr_config.upload_header,
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
        header = fpga_instance.corr_config.get_header()
        assert header["sync_time"] == 1234567890.0

    def test_synchronize_default_delay(self, fpga_instance):
        """synchronize() with default delay still publishes sync_time on the header."""
        with patch(
            "eigsep_observing.fpga.time.time",
            return_value=1111111111.0,
        ):
            fpga_instance.synchronize()

        header = fpga_instance.corr_config.get_header()
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
        header = fpga_instance.corr_config.get_header()
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
        fpga_instance.corr_config.upload_header({"sync_time": 0})
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
            fpga_instance.corr_config.upload_header({"sync_time": 42})

        header = fpga_instance.corr_config.get_header()
        assert header["sync_time"] == 42
        assert header["header_upload_unix"] == 1234.0

    def test_upload_header_does_not_mutate_caller_dict(self, fpga_instance):
        """Stamping is done on a copy so the caller's dict (typically
        EigsepFpga.header, a fresh dict per property access, but still)
        is not surprised with extra keys."""
        payload = {"sync_time": 42, "nchan": 1024}
        fpga_instance.corr_config.upload_header(payload)
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
            fpga_instance.corr_config,
            "upload_header",
            wraps=fpga_instance.corr_config.upload_header,
        ) as spy:
            fpga_instance.initialize(
                initialize_adc=False, initialize_fpga=False, sync=True
            )
        assert spy.call_count == 2

    def test_set_pam_atten_publishes_header(self, fpga_with_pams):
        """set_pam_atten mutates header-relevant state (wiring atten),
        so it must re-publish."""
        fpga_with_pams.initialize_pams()
        ant = next(iter(fpga_with_pams.wiring["ants"]))
        with patch.object(
            fpga_with_pams.corr_config,
            "upload_header",
            wraps=fpga_with_pams.corr_config.upload_header,
        ) as spy:
            fpga_with_pams.set_pam_atten(ant, 4)
        spy.assert_called_once()

    def test_set_pam_atten_all_publishes_header(self, fpga_with_pams):
        """set_pam_atten_all mutates all PAMs at once; same contract."""
        fpga_with_pams.initialize_pams()
        with patch.object(
            fpga_with_pams.corr_config,
            "upload_header",
            wraps=fpga_with_pams.corr_config.upload_header,
        ) as spy:
            fpga_with_pams.set_pam_atten_all(6)
        spy.assert_called_once()

    def test_header_includes_wiring(self, fpga_instance):
        """The corr header carries the full wiring manifest."""
        header = fpga_instance.header
        assert "wiring" in header
        assert header["wiring"]["snap_id"] == fpga_instance.wiring["snap_id"]
        assert (
            header["wiring"]["ants"].keys()
            == fpga_instance.wiring["ants"].keys()
        )

    def test_header_no_rf_chain_key(self, fpga_instance):
        """Header carries ``wiring``, not the legacy ``rf_chain`` key."""
        assert "rf_chain" not in fpga_instance.header

    def test_initialize_pams_skipped_when_no_pams_in_wiring(
        self, fpga_instance, caplog
    ):
        """Wiring with no ``pam:`` blocks → initialize_pams is a no-op
        and ``pams_initialized`` stays False."""
        assert not any(
            "pam" in spec for spec in fpga_instance.wiring["ants"].values()
        )
        caplog.set_level(logging.INFO)
        fpga_instance.initialize_pams()
        assert fpga_instance.pams_initialized is False
        assert not hasattr(fpga_instance, "pams") or fpga_instance.pams == []
        assert "No PAMs declared in wiring" in caplog.text

    def test_initialize_pams_runs_when_pams_in_wiring(self, fpga_with_pams):
        """Wiring with ``pam:`` blocks → 3 PAMs built, per-ant atten set."""
        fpga_with_pams.initialize_pams()
        assert fpga_with_pams.pams_initialized is True
        assert len(fpga_with_pams.pams) == 3
        # Each declared ant's PAM reports the configured attenuation
        # back from hardware on its configured pol.
        for ant, spec in fpga_with_pams.wiring["ants"].items():
            assert fpga_with_pams.get_pam_atten(ant) == spec["pam"]["atten"]

    def test_set_pam_atten_reads_from_wiring(self, fpga_with_pams):
        """set_pam_atten reads num/pol from wiring (not cfg) and pokes
        the corresponding PAM's polarization channel."""
        fpga_with_pams.initialize_pams()
        ant = "ant1N"  # PAM num=1, pol=N
        fpga_with_pams.set_pam_atten(ant, 12)
        # Pam 1's N channel should now read 12; E channel unchanged (0).
        pam1 = fpga_with_pams.pams[1]
        assert pam1.get_attenuation() == (0, 12)

    def test_set_pam_atten_raises_without_pams(self, fpga_instance):
        """set_pam_atten on a fpga with no PAMs initialized raises."""
        assert fpga_instance.pams_initialized is False
        with pytest.raises(RuntimeError, match="PAMs not initialized"):
            fpga_instance.set_pam_atten("any", 4)

    def test_header_logs_warning_when_pam_declared_but_not_initialized(
        self, fpga_with_pams, caplog
    ):
        """Wiring declares PAMs but initialize_pams hasn't run → the
        header property emits a WARNING so the operator knows the
        published atten is declarative, not hardware-confirmed."""
        assert fpga_with_pams.pams_initialized is False
        caplog.set_level(logging.WARNING)
        _ = fpga_with_pams.header
        assert any(
            "Wiring declares PAMs" in rec.message
            and rec.levelno == logging.WARNING
            for rec in caplog.records
        )

    def test_validate_config_ignores_wiring(self, fpga_instance):
        """validate_config does not compare wiring against cfg — wiring
        is a separate dict, and a mismatch there should not fail a cfg
        validation."""
        # Perturb wiring (which is not in cfg at all); cfg matches
        # hardware so validate_config should succeed.
        fpga_instance.wiring["snap_id"] = "MISMATCHED"
        fpga_instance.validate_config()  # should not raise

    def test_set_pol_delay_publishes_header(self, fpga_instance):
        """set_pol_delay mutates pol_delay (a top-level header field)
        so it must re-publish."""
        with patch.object(
            fpga_instance.corr_config,
            "upload_header",
            wraps=fpga_instance.corr_config.upload_header,
        ) as spy:
            fpga_instance.set_pol_delay({"01": 3, "23": 0, "45": 0})
        spy.assert_called_once()

    def test_read_integrations_no_new_data(self, fpga_instance):
        """No new cnt + event pre-set → loop exits via the external-
        stop branch (``_producer_timeout`` stays False). Finally
        wakes the consumer with a single ``None`` sentinel."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        fpga_instance._producer_exc = None
        fpga_instance._producer_timeout = False
        # Pre-set so the loop's while-condition fails after the first
        # read_int, before it has a chance to enter the iteration body.
        fpga_instance.event.set()

        with patch.object(
            fpga_instance.fpga, "read_int", return_value=5
        ) as mock_read_int:
            fpga_instance._read_integrations(["0", "1"], timeout=0.1)

        assert fpga_instance.queue.get_nowait() is None
        assert fpga_instance.queue.empty()  # only the sentinel
        assert fpga_instance._producer_exc is None
        assert fpga_instance._producer_timeout is False
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
        """cnt jumps > 1 → "Dropped" warning, the latest cnt still read."""
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
        assert "Dropped 2 integration(s)" in caplog.text

    def test_read_integrations_torn_read_is_dropped(
        self, fpga_instance, caplog
    ):
        """Validation read returns a different cnt (the readout straddled
        an integration boundary) → the spliced row is NOT queued, the
        drop counter increments, and a "Dropped ... torn read" warning is
        logged. A torn row labels itself as one clean integration, so
        publishing it would write silently-corrupt data."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        assert fpga_instance._dropped_integrations == 0
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
            # Validation read returns a different cnt → torn read.
            fpga_instance.event.set()
            return 7

        caplog.set_level(logging.WARNING)
        with (
            patch.object(fpga_instance.fpga, "read_int", side_effect=read_int),
            patch.object(
                fpga_instance, "read_data", return_value=expected_data
            ),
        ):
            fpga_instance._read_integrations(pairs, timeout=1)

        # Only the producer's None sentinel is on the queue — no data row.
        assert fpga_instance.queue.get_nowait() is None
        assert fpga_instance.queue.empty()
        assert fpga_instance._dropped_integrations == 1
        assert "Dropped integration acc_cnt=6" in caplog.text
        assert "torn read" in caplog.text

    def test_read_integrations_increments_dropped_counter(self, fpga_instance):
        """A cnt jump > 1 accumulates into ``_dropped_integrations`` (the
        counter the corr_health diagnostic surfaces), on top of the
        existing warning."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        assert fpga_instance._dropped_integrations == 0
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
            return 8

        with (
            patch.object(fpga_instance.fpga, "read_int", side_effect=read_int),
            patch.object(
                fpga_instance, "read_data", return_value=expected_data
            ),
        ):
            fpga_instance._read_integrations(pairs, timeout=1)

        assert fpga_instance._dropped_integrations == 2

    def test_read_integrations_records_readout_time(self, fpga_instance):
        """A completed read stamps ``_last_readout_s`` so the diagnostics
        loop can surface the readout wall-time."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        assert fpga_instance._last_readout_s is None
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
            fpga_instance.event.set()
            return 6

        with (
            patch.object(fpga_instance.fpga, "read_int", side_effect=read_int),
            patch.object(
                fpga_instance, "read_data", return_value=expected_data
            ),
        ):
            fpga_instance._read_integrations(pairs, timeout=1)

        assert fpga_instance._last_readout_s is not None
        assert fpga_instance._last_readout_s >= 0.0

    def test_read_integrations_does_not_publish_adc_stats_inline(
        self, fpga_instance
    ):
        """adc_stats publishing moved off the hot read path to the
        throttled diagnostics loop — ``_read_integrations`` must not call
        it per integration anymore (it was an FPGA register read + Redis
        write inside the window that has to beat the BRAM overwrite)."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        fpga_instance.is_synchronized = True
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
            fpga_instance.event.set()
            return 6

        with (
            patch.object(fpga_instance.fpga, "read_int", side_effect=read_int),
            patch.object(
                fpga_instance, "read_data", return_value=expected_data
            ),
            patch.object(
                fpga_instance, "_publish_adc_stats"
            ) as mock_adc_stats,
        ):
            fpga_instance._read_integrations(pairs, timeout=1)

        mock_adc_stats.assert_not_called()

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
            patch.object(fpga_instance.corr, "add") as mock_add,
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
            patch.object(fpga_instance.corr, "add") as mock_add,
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
            patch.object(fpga_instance.corr, "add") as mock_add,
            _patch_observe_thread(fpga_instance, [None]),
        ):
            fpga_instance.observe(pairs=["0"], timeout=1)

        mock_add.assert_not_called()

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

    def test_observe_integration_loop(self, fpga_instance):
        """
        Consumer drains every queued integration, in order, before
        exiting on the sentinel.
        """
        expected_dtype = fpga_instance.cfg["dtype"]

        items = [{"data": {"0": [i]}, "cnt": 10 + i} for i in range(5)]
        items.append(None)
        with (
            patch.object(fpga_instance.corr, "add") as mock_add,
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
            patch.object(fpga_instance.corr, "add") as mock_add,
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
        r = fpga_instance.transport.r
        assert r.xlen(CORR_STREAM) == 0

    def test_end_observing_does_not_enqueue_sentinel(self, fpga_instance):
        """``end_observing`` only sets the event — the producer's
        ``finally`` is the sole source of the ``None`` sentinel. A
        second sentinel from ``end_observing`` would race the
        producer's mid-iteration ``queue.put`` and let the consumer
        exit before the last integration lands.
        """
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()

        fpga_instance.end_observing()

        assert fpga_instance.event.is_set()
        assert fpga_instance.queue.empty()

    def test_read_integrations_finally_signals_consumer_on_exception(
        self, fpga_instance
    ):
        """A hardware exception in the producer body (the SNAP-power-
        cut failure mode: casperfpga RuntimeError) must capture the
        exception, wake the consumer, and not propagate out of the
        thread. Pre-fix, the bare exception killed the thread and the
        consumer hung forever on ``queue.get()``.
        """
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        fpga_instance._producer_exc = None
        fpga_instance._producer_timeout = False

        boom = RuntimeError("Failed to read corr_cross_15_dout")
        with patch.object(fpga_instance.fpga, "read_int", side_effect=boom):
            # Captured, not propagated — observe() re-raises from the
            # main thread so journalctl sees one clean traceback.
            fpga_instance._read_integrations(["0"], timeout=1)

        assert fpga_instance.event.is_set()
        assert fpga_instance.queue.get_nowait() is None
        assert fpga_instance._producer_exc is boom
        assert fpga_instance._producer_timeout is False

    def test_read_integrations_no_progress_flags_producer_timeout(
        self, fpga_instance
    ):
        """``corr_acc_cnt`` frozen for >timeout → loop exits, finally
        wakes the consumer, and ``_producer_timeout`` flips True (the
        external-stop branch — event-set-by-end_observing — leaves
        it False; see test_read_integrations_no_new_data)."""
        fpga_instance.queue = Queue()
        fpga_instance.event = Event()
        fpga_instance._producer_exc = None
        fpga_instance._producer_timeout = False

        with patch.object(fpga_instance.fpga, "read_int", return_value=42):
            fpga_instance._read_integrations(["0"], timeout=0.05)

        assert fpga_instance.event.is_set()
        assert fpga_instance.queue.get_nowait() is None
        assert fpga_instance._producer_exc is None
        assert fpga_instance._producer_timeout is True

    def test_observe_re_raises_producer_exception(self, fpga_instance):
        """End-to-end with a real producer thread: a casperfpga
        ``RuntimeError`` propagates out of ``observe()`` so the
        supervisor (``eigsep-observe.service``) sees a non-zero exit
        and restarts with ``--reinit``.
        """
        boom = RuntimeError("Failed to read corr_cross_15_dout")

        with (
            patch.object(fpga_instance, "upload_config"),
            patch.object(fpga_instance.fpga, "read_int", side_effect=boom),
            patch.object(fpga_instance.corr, "add"),
            pytest.raises(RuntimeError, match="corr_cross_15_dout"),
        ):
            fpga_instance.observe(pairs=["0"], timeout=1)

    def test_observe_raises_timeouterror_on_producer_silence(
        self, fpga_instance
    ):
        """``corr_acc_cnt`` never advances → producer's no-progress
        window elapses → ``observe()`` raises ``TimeoutError`` so the
        supervisor restarts rather than hanging silently.
        """
        with (
            patch.object(fpga_instance, "upload_config"),
            patch.object(fpga_instance.fpga, "read_int", return_value=42),
            patch.object(fpga_instance.corr, "add"),
            pytest.raises(TimeoutError, match="SNAP appears unresponsive"),
        ):
            fpga_instance.observe(pairs=["0"], timeout=0.05)

    def test_observe_clean_stop_via_end_observing_does_not_raise(
        self, fpga_instance
    ):
        """External stop via ``end_observing`` (interactive shell
        exit, or SIGINT path in fpga_init.py) must remain a clean
        return — only producer-side failures should raise.
        """

        def stop_after_delay():
            time.sleep(0.05)
            fpga_instance.end_observing()

        stopper = threading.Thread(target=stop_after_delay, daemon=True)

        with (
            patch.object(fpga_instance, "upload_config"),
            patch.object(fpga_instance.fpga, "read_int", return_value=42),
            patch.object(fpga_instance.corr, "add"),
        ):
            stopper.start()
            # timeout=5 is the producer's no-progress window; if the
            # external stop is missed the test hangs ~5s and then
            # fails on TimeoutError, which is a louder symptom than
            # any silent skip.
            fpga_instance.observe(pairs=["0"], timeout=5)

        stopper.join(timeout=1)
        assert fpga_instance._producer_exc is None
        assert fpga_instance._producer_timeout is False


class TestFpgaLockProxy:
    """``EigsepFpga.fpga`` is wrapped in ``_FpgaLockProxy`` so the corr
    poll thread, the snapshot loop, and ``_publish_adc_stats`` cannot
    interleave TAPCP transactions on the casperfpga transport (which
    is not thread-safe). Without serialization, two concurrent threads
    can corrupt each other's responses and surface as ``RuntimeError(
    "Failed to read ... from register X")`` even when the register
    exists. These tests guard the structural contract and the
    serialization invariant."""

    def test_self_fpga_is_lock_proxy(self, fpga_instance):
        """``self.fpga`` is the proxy, not the raw casperfpga/DummyFpga."""
        assert isinstance(fpga_instance.fpga, _FpgaLockProxy)

    def test_blocks_share_the_proxied_fpga(self, fpga_instance):
        """Every block routes through the same proxied object so they
        all serialize on the same lock. If a block held a reference to
        the unwrapped fpga, its calls would bypass the lock entirely."""
        assert fpga_instance.sync.host is fpga_instance.fpga
        assert fpga_instance.noise.host is fpga_instance.fpga
        assert fpga_instance.inp.host is fpga_instance.fpga
        assert fpga_instance.pfb.host is fpga_instance.fpga

    def test_concurrent_calls_do_not_overlap(self, fpga_instance):
        """Two threads hammering the proxy never enter the underlying
        DummyFpga method concurrently. We slow the underlying call
        down so a missing lock would produce overlapping (enter, exit)
        intervals; with the lock all intervals must be sequential.
        """
        intervals = []
        intervals_lock = threading.Lock()

        def slow_read_int(reg, **kw):
            t_enter = time.perf_counter()
            time.sleep(0.005)  # widen the contention window
            t_exit = time.perf_counter()
            with intervals_lock:
                intervals.append((t_enter, t_exit))
            return 0

        # Patch the underlying DummyFpga's read_int — the proxy's
        # __getattr__ resolves this every call, so the patched version
        # is what runs inside the lock.
        with patch.object(
            fpga_instance.fpga._fpga, "read_int", side_effect=slow_read_int
        ):
            threads = [
                threading.Thread(
                    target=lambda: fpga_instance.fpga.read_int("foo")
                )
                for _ in range(8)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        assert len(intervals) == 8
        intervals.sort()
        # Each interval must end before the next begins. A small
        # epsilon avoids spurious failures from clock-resolution jitter
        # on heavily-loaded CI runners.
        eps = 1e-6
        for (e1, x1), (e2, x2) in zip(intervals, intervals[1:]):
            assert e2 + eps >= x1, (
                f"overlapping intervals: ({e1:.6f},{x1:.6f}) "
                f"and ({e2:.6f},{x2:.6f})"
            )

    def test_proxy_passes_non_callable_attributes_through(self, fpga_instance):
        """State-only attributes don't hit the lock — they're plain
        lookups on the underlying object."""
        # DummyFpga sets ``snap_ip`` as a plain attribute.
        assert fpga_instance.fpga.snap_ip == fpga_instance.fpga._fpga.snap_ip


class TestInputSnapSelCache:
    """``Input.get_adc_snapshot`` caches the ``snap_sel`` listdev
    dispatch instead of probing the FPGA on every call. Saves one UDP
    roundtrip per antenna per snapshot tick (six per tick at standard
    wiring)."""

    def test_listdev_called_once_across_many_snapshots(self, fpga_instance):
        """Calling ``get_adc_snapshot`` repeatedly hits ``listdev``
        exactly once."""
        # The Dummy doesn't seed snapshot_bram, but ``get_adc_snapshot``
        # works regardless because DummyFpga.read returns a fixed-fill
        # bytestring of the requested size.
        with patch.object(
            fpga_instance.inp,
            "listdev",
            wraps=fpga_instance.inp.listdev,
        ) as listdev_spy:
            for ant in range(3):
                fpga_instance.inp.get_adc_snapshot(ant)

        assert listdev_spy.call_count == 1

    def test_cache_initially_unset(self, fpga_instance):
        """The cache is lazy — fresh ``Input`` has no decision yet."""
        # Build a fresh fpga so we can inspect the cache before any
        # ``get_adc_snapshot`` runs.
        fresh = DummyEigsepFpga(program=False)
        assert fresh.inp._has_snap_sel is None
