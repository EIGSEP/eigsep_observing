import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest
from unittest.mock import Mock, patch

from eigsep_observing.utils import (
    calc_inttime,
    configure_eig_logger,
    get_config_path,
    load_config,
    require_panda,
    require_snap,
)


class TestCalcInttime:
    """Vacc dump-period semantics of calc_inttime.

    ``corr_acc_len`` counts FPGA fabric clocks, and the SNAP fabric
    runs at ``sample_rate / 2`` (demux-2: the ADC delivers two samples
    per fabric clock). The dump period is therefore
    ``acc_len * 2 / sample_rate`` regardless of whether the firmware
    emits even/odd banks (v2.3, acc_bins=2) or a single spectrum
    (v2.4, acc_bins=1). Measured on hardware 2026-07-09 (fpg v2.4):
    240-row files land every ~257.7 s = 240 x 1.0737 s.
    """

    def test_dump_period_production_registers(self):
        assert calc_inttime(500e6, 2**28) == pytest.approx(
            1.073741824, rel=1e-12
        )

    def test_readout_mode_cannot_enter_timing(self):
        # acc_bins describes the readout payload structure, not the
        # timing; the v2.4 header bug (t_int halved, times drifting to
        # 2x wall clock) came from multiplying it into this formula.
        with pytest.raises(TypeError):
            calc_inttime(500e6, 2**28, acc_bins=1)

    def test_load_config_computes_dump_period(self):
        # corr_config.yaml declares the v2.4 layout (acc_bins: 1); the
        # computed integration_time must still be the dump period
        # (2**26 fabric clocks -> 0.268 s tick).
        cfg = load_config(get_config_path("corr_config.yaml"))
        assert cfg["integration_time"] == pytest.approx(
            0.268435456, rel=1e-12
        )


class TestRequirePandaDecorator:
    """Test the require_panda decorator."""

    def test_require_panda_with_redis_panda(self):
        """Test require_panda when redis_panda is available."""

        class TestClass:
            def __init__(self):
                self.panda_connected = True

            @require_panda
            def test_method(self):
                return "success"

        obj = TestClass()
        result = obj.test_method()

        assert result == "success"

    def test_require_panda_without_redis_panda(self):
        """Test require_panda when redis_panda is None."""

        class TestClass:
            def __init__(self):
                self.panda_connected = False

            @require_panda
            def test_method(self):
                return "success"

        obj = TestClass()

        with pytest.raises(AttributeError):
            obj.test_method()

    def test_require_panda_missing_attribute(self):
        """Test require_panda when redis_panda attribute doesn't exist."""

        class TestClass:
            @require_panda
            def test_method(self):
                return "success"

        obj = TestClass()

        with pytest.raises(AttributeError):
            obj.test_method()

    def test_require_panda_with_arguments(self):
        """Test require_panda decorator with method arguments."""

        class TestClass:
            def __init__(self):
                self.panda_connected = True

            @require_panda
            def test_method(self, arg1, arg2=None):
                return f"{arg1}_{arg2}"

        obj = TestClass()
        result = obj.test_method("test", arg2="value")

        assert result == "test_value"

    def test_require_panda_preserves_method_attributes(self):
        """Test that require_panda preserves method attributes."""

        class TestClass:
            def __init__(self):
                self.panda_connected = True

            @require_panda
            def test_method(self):
                """Test method docstring."""
                return "success"

        obj = TestClass()

        # Should preserve method name and docstring
        assert obj.test_method.__name__ == "test_method"
        assert "Test method docstring" in obj.test_method.__doc__


class TestRequireSnapDecorator:
    """Test the require_snap decorator."""

    def test_require_snap_with_redis_snap(self):
        """Test require_snap when redis_snap is available."""

        class TestClass:
            def __init__(self):
                self.snap_connected = True

            @require_snap
            def test_method(self):
                return "success"

        obj = TestClass()
        result = obj.test_method()

        assert result == "success"

    def test_require_snap_without_redis_snap(self):
        """Test require_snap when redis_snap is None."""

        class TestClass:
            def __init__(self):
                self.snap_connected = False

            @require_snap
            def test_method(self):
                return "success"

        obj = TestClass()

        with pytest.raises(AttributeError):
            obj.test_method()

    def test_require_snap_missing_attribute(self):
        """Test require_snap when redis_snap attribute doesn't exist."""

        class TestClass:
            @require_snap
            def test_method(self):
                return "success"

        obj = TestClass()

        with pytest.raises(AttributeError):
            obj.test_method()

    def test_require_snap_with_arguments(self):
        """Test require_snap decorator with method arguments."""

        class TestClass:
            def __init__(self):
                self.snap_connected = True

            @require_snap
            def test_method(self, *args, **kwargs):
                return (args, kwargs)

        obj = TestClass()
        result = obj.test_method(1, 2, 3, key="value")

        assert result == ((1, 2, 3), {"key": "value"})

    def test_require_snap_preserves_method_attributes(self):
        """Test that require_snap preserves method attributes."""

        class TestClass:
            def __init__(self):
                self.snap_connected = True

            @require_snap
            def test_method(self):
                """Snap method docstring."""
                return "success"

        obj = TestClass()

        # Should preserve method name and docstring
        assert obj.test_method.__name__ == "test_method"
        assert "Snap method docstring" in obj.test_method.__doc__


class TestGetConfigPath:
    """Test the get_config_path function."""

    @patch("eigsep_observing.utils.resources.files")
    def test_get_config_path_basic(self, mock_files):
        """Test basic config path retrieval."""
        mock_path = Mock()
        mock_path.joinpath.return_value.joinpath.return_value = (
            "/path/to/config/test_config.yaml"
        )
        mock_files.return_value = mock_path

        result = get_config_path("test_config.yaml")

        mock_files.assert_called_once_with("eigsep_observing")
        assert result == "/path/to/config/test_config.yaml"

    @patch("eigsep_observing.utils.resources.files")
    def test_get_config_path_with_subdirectory(self, mock_files):
        """Test config path with subdirectory."""
        mock_path = Mock()
        mock_path.joinpath.return_value.joinpath.return_value = (
            "/path/to/config/subdir/config.yaml"
        )
        mock_files.return_value = mock_path

        result = get_config_path("subdir/config.yaml")

        mock_files.assert_called_once_with("eigsep_observing")
        assert result == "/path/to/config/subdir/config.yaml"

    @patch("eigsep_observing.utils.resources.files")
    def test_get_config_path_error_handling(self, mock_files):
        """Test config path error handling."""
        mock_files.side_effect = FileNotFoundError("Config not found")

        with pytest.raises(FileNotFoundError):
            get_config_path("nonexistent_config.yaml")

    @patch("eigsep_observing.utils.resources.files")
    def test_get_config_path_empty_filename(self, mock_files):
        """Test config path with empty filename."""
        mock_path = Mock()
        mock_path.joinpath.return_value.joinpath.return_value = (
            "/path/to/config/"
        )
        mock_files.return_value = mock_path

        result = get_config_path("")

        mock_files.assert_called_once_with("eigsep_observing")
        assert result == "/path/to/config/"


class TestDecoratorEdgeCases:
    """Test edge cases for decorators."""

    def test_require_panda_with_class_method(self):
        """Test require_panda on class method."""

        class TestClass:
            panda_connected = True

            @classmethod
            @require_panda
            def test_class_method(cls):
                return "class_success"

        result = TestClass.test_class_method()
        assert result == "class_success"

    def test_decorators_stacked(self):
        """Test stacking both decorators."""

        class TestClass:
            def __init__(self):
                self.panda_connected = True
                self.snap_connected = True

            @require_panda
            @require_snap
            def test_method(self):
                return "both_success"

        obj = TestClass()
        result = obj.test_method()

        assert result == "both_success"

    def test_decorators_stacked_missing_panda(self):
        """Test stacked decorators with missing panda."""

        class TestClass:
            def __init__(self):
                self.panda_connected = False
                self.snap_connected = True

            @require_panda
            @require_snap
            def test_method(self):
                return "both_success"

        obj = TestClass()

        with pytest.raises(AttributeError):
            obj.test_method()

    def test_decorators_stacked_missing_snap(self):
        """Test stacked decorators with missing snap."""

        class TestClass:
            def __init__(self):
                self.panda_connected = True
                self.snap_connected = False

            @require_snap
            @require_panda
            def test_method(self):
                return "both_success"

        obj = TestClass()

        with pytest.raises(AttributeError):
            obj.test_method()


class TestUtilsIntegration:
    """Test integration between utils functions."""

    def test_make_schedule_used_with_decorators(self):
        """Test make_schedule output used in decorated methods."""

        class TestObserver:
            def __init__(self):
                self.panda_connected = True
                self.snap_connected = True

            @require_panda
            @require_snap
            def start_observation(self, schedule):
                return f"Observing with {schedule['vna']} VNA measurements"

        observer = TestObserver()
        schedule = {
            "vna": 5,
            "snap_repeat": 10,
            "sky": 30,
            "load": 10,
            "noise": 5,
        }

        result = observer.start_observation(schedule)

        assert result == "Observing with 5 VNA measurements"

    def test_config_path_integration(self):
        """Test config path function integration."""
        with patch("eigsep_observing.utils.resources.files") as mock_files:
            mock_path = Mock()
            mock_path.joinpath.return_value.joinpath.return_value = (
                "/path/to/obs_config.yaml"
            )
            mock_files.return_value = mock_path

            config_path = get_config_path("obs_config.yaml")

            assert config_path == "/path/to/obs_config.yaml"
            mock_files.assert_called_once()


class TestConfigureEigLogger:
    """Test configure_eig_logger.

    pytest's logging plugin attaches a ``LogCaptureHandler`` to the
    root logger right before each test body. Most tests want to
    inspect the exact handler set after ``configure_eig_logger``, so
    each test clears ``root.handlers`` inline first and the autouse
    fixture restores the original handlers on teardown.
    """

    @pytest.fixture(autouse=True)
    def _isolate_root_logger(self):
        root = logging.getLogger()
        saved_handlers = list(root.handlers)
        saved_level = root.level
        try:
            yield
        finally:
            root.handlers.clear()
            for h in saved_handlers:
                root.addHandler(h)
            root.setLevel(saved_level)

    def test_default_log_file_is_absolute_under_home(self, tmp_path):
        """Default log_file must resolve to ``~/eigsep.log`` (absolute),
        not the CWD-relative ``eigsep.log``. Regression for the
        operator who can't find the file because it landed wherever
        the script was launched from."""
        logging.getLogger().handlers.clear()
        with patch("eigsep_observing.utils.Path.home", return_value=tmp_path):
            configure_eig_logger(console=False)
        file_handlers = [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, RotatingFileHandler)
        ]
        assert len(file_handlers) == 1
        path = Path(file_handlers[0].baseFilename)
        assert path.is_absolute()
        assert path == tmp_path / "eigsep.log"

    def test_console_handler_attached_by_default(self, tmp_path):
        """Console handler must be attached by default so operators
        see log lines in the terminal without tailing the file."""
        logging.getLogger().handlers.clear()
        with patch("eigsep_observing.utils.Path.home", return_value=tmp_path):
            configure_eig_logger()
        handler_types = {type(h) for h in logging.getLogger().handlers}
        assert RotatingFileHandler in handler_types
        assert logging.StreamHandler in handler_types

    def test_console_false_omits_stream_handler(self, tmp_path):
        logging.getLogger().handlers.clear()
        with patch("eigsep_observing.utils.Path.home", return_value=tmp_path):
            configure_eig_logger(console=False)
        handlers = logging.getLogger().handlers
        assert any(isinstance(h, RotatingFileHandler) for h in handlers)
        # A bare StreamHandler (not the rotating file handler, which is
        # also a StreamHandler subclass) must not be attached.
        assert not any(type(h) is logging.StreamHandler for h in handlers)

    def test_idempotent_no_duplicate_handlers(self, tmp_path):
        """A second call must not stack handlers. The function checks
        for an existing ``RotatingFileHandler`` and an existing plain
        ``StreamHandler`` separately and skips each if already present."""
        logging.getLogger().handlers.clear()
        with patch("eigsep_observing.utils.Path.home", return_value=tmp_path):
            configure_eig_logger()
            configure_eig_logger()
        handlers = logging.getLogger().handlers
        assert len(handlers) == 2  # rotating file + stream, no dupes

    def test_explicit_relative_path_resolved_under_home(self, tmp_path):
        """A caller who passes a relative path still gets an absolute
        location — a relative path defeats the point of the default."""
        logging.getLogger().handlers.clear()
        with patch("eigsep_observing.utils.Path.home", return_value=tmp_path):
            configure_eig_logger(log_file="custom.log", console=False)
        file_handlers = [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, RotatingFileHandler)
        ]
        assert Path(file_handlers[0].baseFilename) == tmp_path / "custom.log"

    def test_explicit_absolute_path_honored(self, tmp_path):
        target = tmp_path / "subdir" / "mylog.log"
        target.parent.mkdir()
        logging.getLogger().handlers.clear()
        configure_eig_logger(log_file=target, console=False)
        file_handlers = [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, RotatingFileHandler)
        ]
        assert Path(file_handlers[0].baseFilename) == target

    def test_console_false_strips_pre_existing_stream_handler(self, tmp_path):
        """A stray ``StreamHandler`` (e.g. from a leaked
        ``logging.basicConfig()`` call earlier in the import chain)
        must be removed when ``console=False`` so the repainting UI
        scripts get a clean stderr. Without this, ``console=False`` is
        a silent no-op behind the ``hasHandlers`` guard."""
        root = logging.getLogger()
        root.handlers.clear()
        leaked = logging.StreamHandler()
        root.addHandler(leaked)
        assert root.hasHandlers()
        with patch("eigsep_observing.utils.Path.home", return_value=tmp_path):
            configure_eig_logger(console=False)
        handlers = root.handlers
        assert any(isinstance(h, RotatingFileHandler) for h in handlers)
        assert not any(type(h) is logging.StreamHandler for h in handlers)

    def test_console_true_does_not_duplicate_pre_existing_stream(
        self, tmp_path
    ):
        """If a ``StreamHandler`` is already attached, ``console=True``
        must not stack a second one. Pairs with the strip behavior so
        the function is idempotent regardless of prior state."""
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(logging.StreamHandler())
        with patch("eigsep_observing.utils.Path.home", return_value=tmp_path):
            configure_eig_logger()
        stream_handlers = [
            h for h in root.handlers if type(h) is logging.StreamHandler
        ]
        assert len(stream_handlers) == 1

    def test_console_true_reconfigures_pre_existing_stream(self, tmp_path):
        root = logging.getLogger()
        root.handlers.clear()
        leaked = logging.StreamHandler()
        leaked.setLevel(logging.ERROR)
        leaked.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(leaked)
        with patch("eigsep_observing.utils.Path.home", return_value=tmp_path):
            configure_eig_logger(level=logging.DEBUG)
        stream_handlers = [
            h for h in root.handlers if type(h) is logging.StreamHandler
        ]
        assert stream_handlers == [leaked]
        assert leaked.level == logging.DEBUG
        assert leaked.formatter._fmt == (
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    def test_existing_rotating_file_handler_is_reconfigured(self, tmp_path):
        root = logging.getLogger()
        root.handlers.clear()
        target = tmp_path / "preexisting.log"
        existing = RotatingFileHandler(target, maxBytes=1, backupCount=1)
        existing.setLevel(logging.ERROR)
        existing.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(existing)
        configure_eig_logger(level=logging.WARNING, console=False)
        file_handlers = [
            h for h in root.handlers if isinstance(h, RotatingFileHandler)
        ]
        assert file_handlers == [existing]
        assert existing.level == logging.WARNING
        assert existing.formatter._fmt == (
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
