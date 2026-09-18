import matplotlib
import numpy as np
import pytest

matplotlib.use("Agg")  # headless: no display needed for label assertions

from eigsep_redis import MetadataWriter
from eigsep_redis.testing import DummyTransport

from eigsep_observing.corr import CorrConfigStore, CorrWriter
from eigsep_observing.plot import LivePlotter

NCHAN = 1024
DTYPE = ">i4"


def _seed(transport, header):
    store = CorrConfigStore(transport)
    store.upload(
        {
            "sample_rate": 500.0,
            "nchan": NCHAN,
            "acc_bins": 1,
            "avg_even_odd": False,
        }
    )
    store.upload_header(header)


def test_plotter_labels_use_physical_baselines():
    transport = DummyTransport()
    _seed(
        transport,
        {
            "input_to_ant": {
                "0": "primA",
                "1": "primA",
                "2": "primB",
                "3": "primB",
            },
        },
    )
    plotter = LivePlotter(transport, pairs=["0", "02", "13"])
    assert plotter.labels["0"] == "primA [0]"
    assert plotter.labels["02"] == "primA / primB [02]"
    assert plotter.labels["13"] == "primA / primB [13]"
    assert plotter.lines["mag"]["02"].get_label() == "primA / primB [02]"


def test_plotter_falls_back_to_raw_key_when_unmapped():
    transport = DummyTransport()
    _seed(transport, {})  # header present but no mapping
    plotter = LivePlotter(transport, pairs=["02"])
    assert plotter.labels["02"] is None
    assert plotter.lines["mag"]["02"].get_label() == "02"


def test_metadata_key_requires_transport_panda():
    transport = DummyTransport()
    _seed(transport, {})
    with pytest.raises(ValueError, match="transport_panda"):
        LivePlotter(transport, pairs=["0"], metadata_key="tempctrl_load")


def test_channel_out_of_range_raises():
    transport = DummyTransport()
    _seed(transport, {})
    with pytest.raises(ValueError, match="channel"):
        LivePlotter(transport, pairs=["0"], channel=NCHAN)


def test_channel_history_disabled_by_default():
    transport = DummyTransport()
    _seed(transport, {})
    plotter = LivePlotter(transport, pairs=["0"])
    assert plotter.ax_channel_hist is None
    assert plotter.channel_history is None
    assert "channel_hist" not in plotter.lines


def _write_auto_row(writer, cnt, value):
    row = {"0": np.full(NCHAN, value, dtype=np.dtype(DTYPE)).tobytes()}
    writer.add(row, cnt=cnt, sync_time=1000.0, dtype=DTYPE)


def _rewind_corr(transport):
    """Rewind the corr stream cursor to the beginning.

    ``CorrReader``'s cursor defaults to ``$`` (Redis "now"), resolved
    lazily on the first ``.read()`` call — a test that writes rows
    before ever calling ``update_plot`` would have that first call
    swallow the pre-written entry as its baseline and then block
    forever (``timeout=0`` blocks indefinitely, matching production's
    "wait for the next real integration" design) waiting for a
    never-arriving *next* one. Mirrors the same pattern in
    ``test_live_status_aggregator.py``.
    """
    transport.set_last_read_id("stream:corr", "0")


def test_channel_history_tracks_new_integrations_only():
    transport = DummyTransport()
    _seed(transport, {})
    plotter = LivePlotter(transport, pairs=["0"], channel=3, history_len=5)
    writer = CorrWriter(transport)
    _rewind_corr(transport)

    _write_auto_row(writer, cnt=1, value=100)
    plotter.update_plot(0)
    assert list(plotter.channel_history["0"]) == [100.0]
    assert list(plotter.channel_history_x) == [1]

    _write_auto_row(writer, cnt=2, value=200)
    plotter.update_plot(0)
    assert list(plotter.channel_history["0"]) == [100.0, 200.0]
    assert list(plotter.channel_history_x) == [1, 2]


def test_metadata_history_reads_panda_snapshot():
    transport = DummyTransport()
    _seed(transport, {})
    panda = DummyTransport()
    plotter = LivePlotter(
        transport,
        pairs=["0"],
        transport_panda=panda,
        metadata_key="tempctrl_load",
        metadata_field="T_now",
        history_len=5,
    )
    writer = CorrWriter(transport)
    _rewind_corr(transport)
    MetadataWriter(panda).add(
        "tempctrl_load", {"T_now": 42.5, "status": "update"}
    )

    _write_auto_row(writer, cnt=1, value=1)
    plotter.update_plot(0)
    assert list(plotter.metadata_history) == [42.5]


def test_metadata_history_survives_missing_stream():
    """No producer has published tempctrl_load yet — benign, no crash."""
    transport = DummyTransport()
    _seed(transport, {})
    panda = DummyTransport()
    plotter = LivePlotter(
        transport,
        pairs=["0"],
        transport_panda=panda,
        metadata_key="tempctrl_load",
    )
    writer = CorrWriter(transport)
    _rewind_corr(transport)
    _write_auto_row(writer, cnt=1, value=1)
    plotter.update_plot(0)  # must not raise
    assert list(plotter.metadata_history) == []
