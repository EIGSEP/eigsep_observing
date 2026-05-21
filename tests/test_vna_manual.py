"""Tests for the local-HDF5 helper used by scripts/vna_manual.py."""

import json

import h5py
import numpy as np

from eigsep_observing.vna import save_vna_manual_h5


def _ant_payload(nfreq=8):
    """Synthesize a payload shaped like measure_s11('ant') would return.

    Six complex traces (ant/noise/load + three cal standards) plus a
    header carrying the freq axis and the production overlay keys."""
    rng = np.random.default_rng(0)
    s11 = {
        "ant": rng.standard_normal(nfreq) + 1j * rng.standard_normal(nfreq),
        "noise": rng.standard_normal(nfreq) + 1j * rng.standard_normal(nfreq),
        "load": rng.standard_normal(nfreq) + 1j * rng.standard_normal(nfreq),
        "cal:VNAO": np.ones(nfreq, dtype=complex) * 0.95,
        "cal:VNAS": -np.ones(nfreq, dtype=complex) * 0.95,
        "cal:VNAL": np.full(nfreq, 0.05 + 0j),
    }
    header = {
        "mode": "ant",
        "freqs": np.linspace(1e6, 250e6, nfreq).tolist(),
        "fstart": 1e6,
        "fstop": 250e6,
        "npoints": nfreq,
        "ifbw": 100.0,
        "power_dBm": 0.0,
        "metadata_snapshot_unix": 1716304212.0,
        "run_tag": "vna_manual_test",
        "run_started_at_unix": 1716304200.0,
        "obs_config": {"vna_ip": "127.0.0.1"},
    }
    metadata = {"rfswitch": {"sw_state_name": "VNAANT"}}
    return s11, header, metadata


def _rec_payload(nfreq=8):
    rng = np.random.default_rng(1)
    s11 = {
        "rec": rng.standard_normal(nfreq) + 1j * rng.standard_normal(nfreq),
        "cal:VNAO": np.ones(nfreq, dtype=complex) * 0.95,
        "cal:VNAS": -np.ones(nfreq, dtype=complex) * 0.95,
        "cal:VNAL": np.full(nfreq, 0.05 + 0j),
    }
    header = {
        "mode": "rec",
        "freqs": np.linspace(1e6, 250e6, nfreq).tolist(),
        "fstart": 1e6,
        "fstop": 250e6,
        "npoints": nfreq,
        "ifbw": 100.0,
        "power_dBm": -40.0,
        "metadata_snapshot_unix": 1716304300.0,
        "run_tag": "vna_manual_test",
        "run_started_at_unix": 1716304200.0,
        "obs_config": {"vna_ip": "127.0.0.1"},
    }
    metadata = {"rfswitch": {"sw_state_name": "VNARF"}}
    return s11, header, metadata


def test_save_vna_manual_h5_ant_mode_round_trips(tmp_path):
    s11, header, metadata = _ant_payload()
    path = save_vna_manual_h5(
        s11, header, metadata, save_dir=tmp_path, mode="ant"
    )
    assert path.parent == tmp_path
    assert path.name.startswith("vna_manual_ant_")
    assert path.suffix == ".h5"
    with h5py.File(path, "r") as f:
        for key in (
            "ant",
            "noise",
            "load",
            "cal:VNAO",
            "cal:VNAS",
            "cal:VNAL",
        ):
            np.testing.assert_array_equal(f[f"raw/{key}"][:], s11[key])
        for dut in ("ant", "noise", "load"):
            arr = f[f"calibrated/{dut}"][:]
            assert arr.shape == s11["ant"].shape
            assert arr.dtype == np.complex128
        assert "calibrated/cal:VNAO" not in f
        np.testing.assert_allclose(f["freqs"][:], header["freqs"])
        assert f.attrs["mode"] == "ant"
        assert f.attrs["vna_manual_script_version"] == "1"
        assert json.loads(f.attrs["obs_config"]) == header["obs_config"]
        assert (
            json.loads(f["metadata_snapshot"].attrs["rfswitch"])
            == metadata["rfswitch"]
        )


def test_save_vna_manual_h5_rec_mode_round_trips(tmp_path):
    s11, header, metadata = _rec_payload()
    path = save_vna_manual_h5(
        s11, header, metadata, save_dir=tmp_path, mode="rec"
    )
    assert path.name.startswith("vna_manual_rec_")
    with h5py.File(path, "r") as f:
        for key in ("rec", "cal:VNAO", "cal:VNAS", "cal:VNAL"):
            np.testing.assert_array_equal(f[f"raw/{key}"][:], s11[key])
        arr = f["calibrated/rec"][:]
        assert arr.shape == s11["rec"].shape
        assert arr.dtype == np.complex128
        assert list(f["calibrated"].keys()) == ["rec"]
        assert f.attrs["mode"] == "rec"


def test_save_vna_manual_h5_uses_mode_arg_for_attr(tmp_path):
    s11, header, metadata = _ant_payload()
    header["mode"] = "rec"
    path = save_vna_manual_h5(
        s11, header, metadata, save_dir=tmp_path, mode="ant"
    )
    with h5py.File(path, "r") as f:
        assert f.attrs["mode"] == "ant"


def test_save_vna_manual_h5_keeps_raw_when_calibration_fails(tmp_path, caplog):
    """Raw arrays are sacred: a calibration shape mismatch must not
    block the local file from being written. Skip the /calibrated/
    group instead, log loudly."""
    s11, header, metadata = _ant_payload(nfreq=8)
    s11["cal:VNAS"] = s11["cal:VNAS"][:4]  # force ValueError in calibrate_s11
    path = save_vna_manual_h5(
        s11, header, metadata, save_dir=tmp_path, mode="ant"
    )
    with h5py.File(path, "r") as f:
        np.testing.assert_array_equal(f["raw/ant"][:], s11["ant"])
        np.testing.assert_array_equal(f["raw/cal:VNAS"][:], s11["cal:VNAS"])
        assert "calibrated" not in f
    assert any(
        "calibration failed" in rec.getMessage().lower()
        for rec in caplog.records
    )
