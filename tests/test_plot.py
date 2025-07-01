import pytest
import numpy as np
from unittest.mock import Mock, patch

from eigsep_observing.plot import LivePlotter
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
def live_plotter(redis_client):
    with patch("eigsep_observing.plot.plt") as mock_plt:
        mock_fig = Mock()
        mock_ax1 = Mock()
        mock_ax2 = Mock()
        # Configure line creation to return a tuple containing a Mock
        mock_ax1.semilogy.return_value = (Mock(),)
        mock_ax1.plot.return_value = (Mock(),)
        mock_ax2.plot.return_value = (Mock(),)

        mock_plt.subplots.return_value = (mock_fig, [mock_ax1, mock_ax2])
        plotter = LivePlotter(
            redis_client=redis_client,
            pairs=["0", "1", "02"],
            plot_delay=False,
            log_scale=True,
            update_interval=100,
        )
    return plotter


def test_live_plotter_init(redis_client):
    with patch("eigsep_observing.plot.plt") as mock_plt:
        mock_fig = Mock()
        mock_ax1 = Mock()
        mock_ax2 = Mock()
        mock_ax1.semilogy.return_value = (Mock(),)
        mock_ax1.plot.return_value = (Mock(),)
        mock_ax2.plot.return_value = (Mock(),)
        mock_plt.subplots.return_value = (mock_fig, [mock_ax1, mock_ax2])

        plotter = LivePlotter(redis_client)

        assert plotter.redis == redis_client
        assert plotter.nchan == 1024
        assert plotter.sample_rate == 500
        assert len(plotter.x) == 1024
        assert plotter.log_scale is True
        assert plotter.plot_delay is False


def test_live_plotter_init_no_config():
    client = DummyEigsepRedis()

    with patch("eigsep_observing.plot.plt") as mock_plt:
        mock_fig = Mock()
        mock_ax1 = Mock()
        mock_ax2 = Mock()
        mock_ax1.semilogy.return_value = (Mock(),)
        mock_ax1.plot.return_value = (Mock(),)
        mock_ax2.plot.return_value = (Mock(),)
        mock_plt.subplots.return_value = (mock_fig, [mock_ax1, mock_ax2])

        plotter = LivePlotter(client)

        # Should use defaults when config not available
        assert plotter.nchan == 1024
        assert plotter.sample_rate == 500


def test_setup_colors(live_plotter):
    colors = live_plotter._setup_colors()

    assert colors["0"] == "black"
    assert colors["1"] == "lime"
    assert colors["02"] == "C0"


@patch("eigsep_observing.plot.plt")
def test_setup_plots(mock_plt, live_plotter):
    mock_fig = Mock()
    mock_axs = [Mock(), Mock()]
    mock_plt.subplots.return_value = (mock_fig, mock_axs)

    fig, axs = live_plotter._setup_plots()

    # Verify plot setup calls
    mock_plt.ion.assert_called_once()
    mock_plt.subplots.assert_called_once_with(figsize=(12, 8), nrows=2)

    # Verify axes configuration
    assert axs[0].grid.called
    assert axs[1].grid.called
    assert axs[0].set_ylabel.called
    assert axs[1].set_ylabel.called


def test_update_plot_auto_correlation(live_plotter):
    # Add mock data to Redis for auto-correlation
    auto_data = np.random.randint(0, 1000, 1024, dtype=np.int32)
    data_bytes = auto_data.astype(">i4").tobytes()
    live_plotter.redis.add_raw("data:0", data_bytes)

    # Mock the plot lines
    mock_line = Mock()
    live_plotter.lines = {"mag": {"0": mock_line}, "phase": {}, "delay": None}

    # Update plot
    live_plotter.update_plot(0)

    # Verify line was updated
    mock_line.set_ydata.assert_called_once()
    called_data = mock_line.set_ydata.call_args[0][0]
    np.testing.assert_array_equal(called_data, auto_data)


def test_update_plot_cross_correlation(live_plotter):
    # Create mock cross-correlation data (interleaved real/imag)
    real_data = np.random.randint(-500, 500, 1024, dtype=np.int32)
    imag_data = np.random.randint(-500, 500, 1024, dtype=np.int32)

    # Interleave real and imaginary parts
    cross_data = np.zeros(2048, dtype=np.int32)
    cross_data[::2] = real_data
    cross_data[1::2] = imag_data

    data_bytes = cross_data.astype(">i4").tobytes()
    live_plotter.redis.add_raw("data:02", data_bytes)

    # Mock the plot lines
    mock_mag_line = Mock()
    mock_phase_line = Mock()
    live_plotter.lines = {
        "mag": {"02": mock_mag_line},
        "phase": {"02": mock_phase_line},
        "delay": None,
    }

    # Update plot
    live_plotter.update_plot(0)

    # Verify lines were updated
    mock_mag_line.set_ydata.assert_called_once()
    mock_phase_line.set_ydata.assert_called_once()

    # Check magnitude calculation
    called_mag = mock_mag_line.set_ydata.call_args[0][0]
    expected_mag = np.sqrt(
        real_data.astype(np.int64) ** 2 + imag_data.astype(np.int64) ** 2
    )
    np.testing.assert_array_equal(called_mag, expected_mag)

    # Check phase calculation
    called_phase = mock_phase_line.set_ydata.call_args[0][0]
    expected_phase = np.arctan2(imag_data, real_data)
    np.testing.assert_array_equal(called_phase, expected_phase)


def test_update_plot_with_acc_cnt(live_plotter):
    # Add ACC_CNT to Redis
    live_plotter.redis.add_raw("ACC_CNT", "12345")

    # Mock figure and lines
    live_plotter.fig = Mock()
    live_plotter.lines = {"mag": {}, "phase": {}, "delay": None}

    # Update plot
    live_plotter.update_plot(0)

    # Verify title was set with ACC_CNT
    live_plotter.fig.suptitle.assert_called_with(
        "Live Correlation Spectra (ACC_CNT: 12345)"
    )


def test_update_plot_no_data(live_plotter):
    # Mock the plot lines
    mock_line = Mock()
    live_plotter.lines = {"mag": {"0": mock_line}, "phase": {}, "delay": None}

    # Update plot with no data in Redis
    live_plotter.update_plot(0)

    # Verify line was not updated
    mock_line.set_ydata.assert_not_called()


def test_update_plot_error_handling(live_plotter):
    # Add invalid data to Redis
    live_plotter.redis.add_raw("data:0", b"invalid_data")

    # Mock the plot lines
    mock_line = Mock()
    live_plotter.lines = {"mag": {"0": mock_line}, "phase": {}, "delay": None}

    # Update plot should handle errors gracefully
    with patch("builtins.print") as mock_print:
        live_plotter.update_plot(0)
        mock_print.assert_called()  # Should print error message

    # Line should not be updated
    mock_line.set_ydata.assert_not_called()


@patch("eigsep_observing.plot.FuncAnimation")
@patch("eigsep_observing.plot.plt")
def test_start(mock_plt, mock_animation, live_plotter):
    mock_animation_instance = Mock()
    mock_animation.return_value = mock_animation_instance

    # Mock the show and sleep to avoid blocking
    with patch("time.sleep", side_effect=KeyboardInterrupt):
        live_plotter.start()

    # Verify animation was created and started
    mock_animation.assert_called_once()
    mock_plt.show.assert_called_once()

    # Verify cleanup on KeyboardInterrupt
    mock_animation_instance.event_source.stop.assert_called_once()
    mock_plt.close.assert_called_once_with(live_plotter.fig)
