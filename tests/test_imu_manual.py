"""Tests for the live-plot helpers in ``scripts/imu_manual.py``.

``--plot`` opens a matplotlib window with rolling yaw/pitch/roll traces
fed from the same snapshot reads as the text readout. These tests cover
the pieces that don't need a display: the snapshot-to-readings helper,
the rolling history buffer (NaN gaps, window trim), the line-data
updates on an Agg figure, and the fail-loud guard for headless runs.
"""

import importlib.util
import math
from pathlib import Path

import matplotlib

# Deterministic headless backend: figure construction in _LivePlot must
# not depend on whatever GUI toolkit the test machine happens to have.
matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402  (must follow use("Agg"))
import pytest  # noqa: E402
from eigsep_redis import MetadataSnapshotReader, MetadataWriter  # noqa: E402


SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load(name):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _publish_imu(transport, name, **overrides):
    """Publish one full ``_IMU_SCHEMA``-shaped sample for ``name``.

    Matches the picohost 1.0.0 BNO085 UART RVC producer: scalar
    yaw/pitch/roll (deg) and accel_x/y/z (m/s²) plus the base
    sensor_name/status/app_id fields. ``overrides`` lets a test null
    out a field (sensor error nulls fields per the schema contract).
    """
    sample = {
        "sensor_name": name,
        "status": "update",
        "app_id": 3 if name == "imu_el" else 6,
        "yaw": 12.5,
        "pitch": -3.25,
        "roll": 1.75,
        "accel_x": 0.05,
        "accel_y": -0.12,
        "accel_z": 9.78,
    }
    sample.update(overrides)
    MetadataWriter(transport).add(name, sample)


def test_read_imus_present_and_missing(transport):
    """One snapshot read yields a dict per requested IMU; an IMU that
    has never published maps to None (the text readout's "no reading"
    branch and the plot's NaN gap both key off that)."""
    mod = _load("imu_manual")
    _publish_imu(transport, "imu_el")
    snapshot = MetadataSnapshotReader(transport)

    readings = mod._read_imus(snapshot, ["imu_el", "imu_az"])
    assert readings["imu_el"]["yaw"] == 12.5
    assert readings["imu_az"] is None


def test_plot_history_records_fields_and_elapsed(transport):
    """Each record() appends elapsed seconds since the first sample and
    the angle fields for every IMU."""
    mod = _load("imu_manual")
    _publish_imu(transport, "imu_el")
    _publish_imu(transport, "imu_az", yaw=200.0, pitch=45.0, roll=-90.0)
    snapshot = MetadataSnapshotReader(transport)

    history = mod._PlotHistory(["imu_el", "imu_az"])
    for i in range(3):
        readings = mod._read_imus(snapshot, ["imu_el", "imu_az"])
        history.record(readings, now=100.0 + i)

    assert len(history) == 3
    assert history.t == [0.0, 1.0, 2.0]
    assert history.values["imu_el"]["yaw"] == [12.5, 12.5, 12.5]
    assert history.values["imu_el"]["pitch"] == [-3.25, -3.25, -3.25]
    assert history.values["imu_az"]["roll"] == [-90.0, -90.0, -90.0]


def test_plot_history_gaps_to_nan(transport):
    """A silent IMU and a nulled-out field (sensor error nulls fields
    per _IMU_SCHEMA) both become NaN gaps, not zeros or crashes."""
    mod = _load("imu_manual")
    _publish_imu(transport, "imu_el", yaw=None)
    snapshot = MetadataSnapshotReader(transport)

    history = mod._PlotHistory(["imu_el", "imu_az"])
    readings = mod._read_imus(snapshot, ["imu_el", "imu_az"])
    history.record(readings, now=0.0)

    assert math.isnan(history.values["imu_el"]["yaw"][0])
    assert history.values["imu_el"]["pitch"] == [-3.25]
    for field in mod.PLOT_FIELDS:
        assert math.isnan(history.values["imu_az"][field][0])


def test_plot_history_trims_to_window(transport):
    """Samples older than window_s fall off the front so the plot stays
    a fixed-width rolling window."""
    mod = _load("imu_manual")
    _publish_imu(transport, "imu_el")
    snapshot = MetadataSnapshotReader(transport)

    history = mod._PlotHistory(["imu_el"], window_s=10.0)
    for i in range(16):
        readings = mod._read_imus(snapshot, ["imu_el"])
        history.record(readings, now=float(i))

    assert history.t == [float(i) for i in range(5, 16)]
    assert len(history.values["imu_el"]["yaw"]) == len(history.t)


def test_live_plot_update_sets_line_data(transport):
    """update() pushes the rolling history onto one line per (IMU,
    angle field), one panel per IMU."""
    mod = _load("imu_manual")
    _publish_imu(transport, "imu_el")
    _publish_imu(transport, "imu_az", yaw=200.0, pitch=45.0, roll=-90.0)
    snapshot = MetadataSnapshotReader(transport)

    plot = mod._LivePlot(["imu_el", "imu_az"])
    try:
        for i in range(2):
            readings = mod._read_imus(snapshot, ["imu_el", "imu_az"])
            plot.update(readings, now=float(i))

        assert len(plot.axes) == 2
        line = plot.lines["imu_az"]["pitch"]
        assert list(line.get_xdata()) == [0.0, 1.0]
        assert list(line.get_ydata()) == [45.0, 45.0]
        assert list(plot.lines["imu_el"]["yaw"].get_ydata()) == [12.5, 12.5]
    finally:
        plt.close(plot.fig)


def test_live_plot_all_missing_does_not_raise(transport):
    """An IMU that never publishes renders as an empty (all-NaN) panel
    rather than crashing the loop."""
    mod = _load("imu_manual")
    snapshot = MetadataSnapshotReader(transport)

    plot = mod._LivePlot(["imu_el"])
    try:
        for i in range(2):
            readings = mod._read_imus(snapshot, ["imu_el"])
            plot.update(readings, now=float(i))
        ydata = plot.lines["imu_el"]["roll"].get_ydata()
        assert all(math.isnan(v) for v in ydata)
    finally:
        plt.close(plot.fig)


def test_require_interactive_backend(monkeypatch):
    """Headless fallback to Agg exits with an operator-actionable error
    instead of a window that silently never appears; a GUI backend
    passes."""
    mod = _load("imu_manual")
    monkeypatch.setattr(mod.matplotlib, "get_backend", lambda: "agg")
    with pytest.raises(SystemExit) as exc:
        mod._require_interactive_backend()
    assert "--plot" in str(exc.value)
    assert "agg" in str(exc.value)

    monkeypatch.setattr(mod.matplotlib, "get_backend", lambda: "QtAgg")
    mod._require_interactive_backend()  # no raise
