import logging
import pytest
from unittest.mock import Mock, patch

from eigsep_observing.utils import (
    require_panda,
    require_snap,
    get_config_path,
    get_path,
    configure_eig_logger,
    require_attr
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

    @patch('eigsep_observing.utils.resources.files')
    def test_get_config_path_basic(self, mock_files):
        """Test basic config path retrieval."""
        mock_path = Mock()
        mock_path.joinpath.return_value.joinpath.return_value = "/path/to/config/test_config.yaml"
        mock_files.return_value = mock_path
        
        result = get_config_path("test_config.yaml")
        
        mock_files.assert_called_once_with("eigsep_observing")
        assert result == "/path/to/config/test_config.yaml"

    @patch('eigsep_observing.utils.resources.files')
    def test_get_config_path_with_subdirectory(self, mock_files):
        """Test config path with subdirectory."""
        mock_path = Mock()
        mock_path.joinpath.return_value.joinpath.return_value = "/path/to/config/subdir/config.yaml"
        mock_files.return_value = mock_path
        
        result = get_config_path("subdir/config.yaml")
        
        mock_files.assert_called_once_with("eigsep_observing")
        assert result == "/path/to/config/subdir/config.yaml"

    @patch('eigsep_observing.utils.resources.files')
    def test_get_config_path_error_handling(self, mock_files):
        """Test config path error handling."""
        mock_files.side_effect = FileNotFoundError("Config not found")
        
        with pytest.raises(FileNotFoundError):
            get_config_path("nonexistent_config.yaml")

    @patch('eigsep_observing.utils.resources.files')
    def test_get_config_path_empty_filename(self, mock_files):
        """Test config path with empty filename."""
        mock_path = Mock()
        mock_path.joinpath.return_value.joinpath.return_value = "/path/to/config/"
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
        schedule = {"vna": 5, "snap_repeat": 10, "sky": 30, "load": 10, "noise": 5}
        
        result = observer.start_observation(schedule)
        
        assert result == "Observing with 5 VNA measurements"

    def test_config_path_integration(self):
        """Test config path function integration."""
        with patch('eigsep_observing.utils.resources.files') as mock_files:
            mock_path = Mock()
            mock_path.joinpath.return_value.joinpath.return_value = "/path/to/obs_config.yaml"
            mock_files.return_value = mock_path
            
            config_path = get_config_path("obs_config.yaml")
            
            assert config_path == "/path/to/obs_config.yaml"
            mock_files.assert_called_once()

