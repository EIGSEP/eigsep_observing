import pytest
import numpy as np
import json
import tempfile
import os
from unittest.mock import patch, mock_open

from eigsep_observing.capture import SpectrumCapture
from eigsep_observing.testing import DummyEigsepRedis


@pytest.fixture
def redis_client():
    import json

    client = DummyEigsepRedis()
    # Set up mock config
    corr_config = {"n_chans": 1024, "sample_rate": 500}
    client.add_raw("corr_config", json.dumps(corr_config))
    return client


@pytest.fixture
def spectrum_capture(redis_client):
    return SpectrumCapture(redis_client)


def test_spectrum_capture_init(redis_client):
    capture = SpectrumCapture(redis_client)

    assert capture.redis == redis_client
    assert capture.nchan == 1024
    assert capture.sample_rate == 500


def test_spectrum_capture_init_no_config():
    client = DummyEigsepRedis()

    capture = SpectrumCapture(client)

    # Should use defaults when config not available
    assert capture.nchan == 1024
    assert capture.sample_rate == 500


def test_get_spectrum_data_auto_correlation(spectrum_capture):
    # Add mock auto-correlation data
    auto_data = np.random.randint(0, 1000, 1024, dtype=np.int32)
    data_bytes = auto_data.astype(">i4").tobytes()
    spectrum_capture.redis.add_raw("data:0", data_bytes)
    spectrum_capture.redis.add_raw("ACC_CNT", "12345")

    data, metadata = spectrum_capture.get_spectrum_data(pairs=["0"])

    # Check metadata
    assert "timestamp" in metadata
    assert metadata["nchan"] == 1024
    assert metadata["sample_rate"] == 500
    assert metadata["pairs"] == ["0"]
    assert metadata["acc_cnt"] == "12345"

    # Check data
    assert "0" in data
    assert "magnitude" in data["0"]
    np.testing.assert_array_equal(data["0"]["magnitude"], auto_data)


def test_get_spectrum_data_cross_correlation(spectrum_capture):
    # Create mock cross-correlation data (interleaved real/imag)
    real_data = np.random.randint(-500, 500, 1024, dtype=np.int32)
    imag_data = np.random.randint(-500, 500, 1024, dtype=np.int32)

    cross_data = np.zeros(2048, dtype=np.int32)
    cross_data[::2] = real_data
    cross_data[1::2] = imag_data

    data_bytes = cross_data.astype(">i4").tobytes()
    spectrum_capture.redis.add_raw("data:02", data_bytes)

    data, metadata = spectrum_capture.get_spectrum_data(pairs=["02"])

    # Check data structure
    assert "02" in data
    assert "real" in data["02"]
    assert "imag" in data["02"]
    assert "magnitude" in data["02"]
    assert "phase" in data["02"]

    # Check calculations
    np.testing.assert_array_equal(data["02"]["real"], real_data)
    np.testing.assert_array_equal(data["02"]["imag"], imag_data)

    expected_mag = np.sqrt(
        real_data.astype(np.int64) ** 2 + imag_data.astype(np.int64) ** 2
    )
    np.testing.assert_array_equal(data["02"]["magnitude"], expected_mag)

    expected_phase = np.arctan2(imag_data, real_data)
    np.testing.assert_array_equal(data["02"]["phase"], expected_phase)


def test_get_spectrum_data_no_data(spectrum_capture):
    data, metadata = spectrum_capture.get_spectrum_data(pairs=["0"])

    # Should return empty data dict but valid metadata
    assert data == {}
    assert "timestamp" in metadata
    assert metadata["pairs"] == ["0"]


def test_get_spectrum_data_default_pairs(spectrum_capture):
    data, metadata = spectrum_capture.get_spectrum_data()

    # Should use default pairs
    expected_pairs = [
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "02",
        "04",
        "24",
        "13",
        "15",
        "35",
    ]
    assert metadata["pairs"] == expected_pairs


def test_get_spectrum_data_error_handling(spectrum_capture):
    # Add invalid data that will cause frombuffer to fail
    spectrum_capture.redis.add_raw("data:0", b"x")  # Too small

    with patch("builtins.print"):
        data, metadata = spectrum_capture.get_spectrum_data(pairs=["0"])

    # Should return empty data for failed pair
    assert data == {}


@patch("time.sleep")
@patch("builtins.open", new_callable=mock_open)
@patch("json.dump")
def test_save_last_n_spectra(
    mock_json_dump, mock_file, mock_sleep, spectrum_capture
):
    # Add mock data
    auto_data = np.random.randint(0, 1000, 1024, dtype=np.int32)
    data_bytes = auto_data.astype(">i4").tobytes()
    spectrum_capture.redis.add_raw("data:0", data_bytes)
    spectrum_capture.redis.add_raw("ACC_CNT", "12345")

    # Capture 3 spectra
    filename = spectrum_capture.save_last_n_spectra(
        n_spectra=3, pairs=["0"], filename="test_spectra.json", interval=0.1
    )

    # Check sleep was called correctly (n-1 times)
    assert mock_sleep.call_count == 2
    mock_sleep.assert_called_with(0.1)

    # Check file operations
    mock_file.assert_called_once_with("test_spectra.json", "w")
    mock_json_dump.assert_called_once()

    # Check return value
    assert filename == "test_spectra.json"


@patch("time.sleep")  
def test_save_last_n_spectra_auto_filename(mock_sleep, spectrum_capture):
    with patch("builtins.open", mock_open()):
        with patch("json.dump"):
            # Since datetime is imported inside the function, we need to mock the module
            import datetime as dt_module
            with patch.object(dt_module, 'datetime') as mock_datetime_class:
                mock_datetime_instance = Mock()
                mock_datetime_instance.strftime.return_value = "20231215_143022"
                mock_datetime_class.now.return_value = mock_datetime_instance
                
                filename = spectrum_capture.save_last_n_spectra(n_spectra=1)

                assert filename == "corr_spectra_20231215_143022.json"


def test_save_last_n_spectra_json_serialization(spectrum_capture):
    # Add mock data with numpy arrays
    auto_data = np.array([1, 2, 3, 4], dtype=np.int32)
    data_bytes = auto_data.astype(">i4").tobytes()
    spectrum_capture.redis.add_raw("data:0", data_bytes)

    # Use temporary file for real JSON writing test
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".json"
    ) as temp_file:
        temp_filename = temp_file.name

    try:
        with patch("time.sleep"):
            spectrum_capture.save_last_n_spectra(
                n_spectra=1, pairs=["0"], filename=temp_filename, interval=0.0
            )

        # Read back and verify JSON structure
        with open(temp_filename, "r") as f:
            saved_data = json.load(f)

        assert len(saved_data) == 1
        spectrum = saved_data[0]
        assert "metadata" in spectrum
        assert "data" in spectrum
        assert spectrum["metadata"]["sequence"] == 0
        assert "0" in spectrum["data"]
        assert "magnitude" in spectrum["data"]["0"]

        # Verify numpy arrays were converted to lists
        magnitude = spectrum["data"]["0"]["magnitude"]
        assert isinstance(magnitude, list)
        assert magnitude == auto_data.tolist()

    finally:
        # Clean up temp file
        if os.path.exists(temp_filename):
            os.unlink(temp_filename)


@patch("builtins.print")
def test_save_last_n_spectra_progress_messages(mock_print, spectrum_capture):
    with patch("time.sleep"):
        with patch("builtins.open", mock_open()):
            with patch("json.dump"):
                spectrum_capture.save_last_n_spectra(n_spectra=2)

    # Check progress messages were printed
    print_calls = [call[0][0] for call in mock_print.call_args_list]
    assert any("Capturing 2 spectra" in msg for msg in print_calls)
    assert any("Capturing spectrum 1/2" in msg for msg in print_calls)
    assert any("Capturing spectrum 2/2" in msg for msg in print_calls)
    assert any("Saved 2 spectra" in msg for msg in print_calls)
