import matplotlib

matplotlib.use("Agg")  # headless: no display needed for label assertions

from eigsep_redis.testing import DummyTransport  # noqa: E402

from eigsep_observing.corr import CorrConfigStore  # noqa: E402
from eigsep_observing.plot import LivePlotter  # noqa: E402


def _seed(transport, header):
    store = CorrConfigStore(transport)
    store.upload(
        {
            "sample_rate": 500.0,
            "nchan": 1024,
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
