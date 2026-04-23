"""Tests for eigsep_observing.live_status.signals + thresholds.

The two modules are exercised together because ``Thresholds`` composes
``default_thresholds`` and the signal registry; testing them as a unit
is closer to how the aggregator uses them than splitting would be.
"""

from __future__ import annotations

import pytest

from eigsep_observing.live_status import (
    SIGNAL_REGISTRY,
    Thresholds,
    default_thresholds,
    enabled_signals,
)


# Minimal subset of obs_config.yaml: only the fields that
# default_thresholds / enabled_signals read (corr_ntimes, use_tempctrl,
# tempctrl_settings). Production obs_config.yaml additionally carries
# rpi_ip, panda_ip, corr_save_dir, use_switches, switch_schedule,
# use_vna, use_motor, vna_*, motor_*, etc. — all irrelevant to threshold
# computation and therefore deliberately omitted so the fixture stays
# focused on what the code under test actually consumes.
OBS_CFG_TEMPCTRL_ON = {
    "use_tempctrl": True,
    "corr_ntimes": 240,
    "tempctrl_settings": {
        "LNA": {"target_C": 25.0, "hysteresis_C": 0.5, "clamp": 0.6},
        "LOAD": {"target_C": 25.0, "hysteresis_C": 0.5, "clamp": 0.6},
    },
}


# Same minimal-scope deviation as OBS_CFG_TEMPCTRL_ON; here
# use_tempctrl=False also exercises the path where tempctrl signals are
# filtered out of the registry entirely.
OBS_CFG_TEMPCTRL_OFF = {
    "use_tempctrl": False,
    "corr_ntimes": 240,
}


CORR_HEADER = {"integration_time": 0.27, "sync_time": 1.0}


# ---------------------------------------------------------------------
# signals.default_thresholds
# ---------------------------------------------------------------------


def test_default_thresholds_tempctrl_bands_from_config():
    """target_C ± 2*hysteresis_C should be the healthy band; clamp is
    the upper bound of the drive-level healthy band."""
    out = default_thresholds(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)

    assert out["tempctrl.LNA_T_now"]["healthy"] == [24.0, 26.0]
    assert out["tempctrl.LOAD_T_now"]["healthy"] == [24.0, 26.0]
    # danger is deferred to Thresholds where the YAML tuning knob is
    # resolved. default_thresholds leaves it None and carries
    # _target_C so the merger can fill it in.
    assert out["tempctrl.LNA_T_now"]["danger"] is None
    assert out["tempctrl.LNA_T_now"]["_target_C"] == 25.0

    assert out["tempctrl.LNA_drive_level"]["healthy"] == [0.0, 0.6]
    assert out["tempctrl.LOAD_drive_level"]["healthy"] == [0.0, 0.6]


def test_default_thresholds_corr_cadence_from_integration_time():
    out = default_thresholds(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    band = out["corr.acc_cadence_s"]
    assert band["healthy"] == pytest.approx([0.8 * 0.27, 1.2 * 0.27])
    assert band["danger"] == pytest.approx([0.5 * 0.27, 2.0 * 0.27])


def test_default_thresholds_file_heartbeat_from_ntimes():
    out = default_thresholds(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    file_dur = 0.27 * 240
    assert out["file.seconds_since_write"]["healthy"] == pytest.approx(
        [0.0, 1.5 * file_dur]
    )
    assert out["file.seconds_since_write"]["danger"] == pytest.approx(
        [0.0, 3.0 * file_dur]
    )


def test_default_thresholds_dropped_when_tempctrl_disabled():
    out = default_thresholds(OBS_CFG_TEMPCTRL_OFF, CORR_HEADER)
    for key in (
        "tempctrl.LNA_T_now",
        "tempctrl.LOAD_T_now",
        "tempctrl.LNA_drive_level",
        "tempctrl.LOAD_drive_level",
    ):
        assert key not in out


def test_default_thresholds_omits_cadence_without_header():
    out = default_thresholds(OBS_CFG_TEMPCTRL_ON, corr_header=None)
    assert "corr.acc_cadence_s" not in out
    assert "file.seconds_since_write" not in out


# ---------------------------------------------------------------------
# enabled_signals
# ---------------------------------------------------------------------


def test_enabled_signals_drops_disabled_subsystems():
    enabled = enabled_signals(OBS_CFG_TEMPCTRL_OFF)
    assert "tempctrl.LNA_T_now" not in enabled
    # Signals with enabled_by=None stay regardless.
    assert "adc.rms" in enabled
    assert "corr.acc_cadence_s" in enabled


def test_enabled_signals_includes_tempctrl_when_on():
    enabled = enabled_signals(OBS_CFG_TEMPCTRL_ON)
    assert "tempctrl.LNA_T_now" in enabled
    assert "tempctrl.LNA_drive_level" in enabled


# ---------------------------------------------------------------------
# Thresholds merge + classify
# ---------------------------------------------------------------------


def test_thresholds_merges_derived_and_yaml():
    yaml_overrides = {
        "adc.rms": {"healthy": [10.0, 20.0], "danger": [5.0, 30.0]},
        "tempctrl.danger_k_C": 10.0,
    }
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_overrides=yaml_overrides
    )

    adc = th.bands["adc.rms"]
    assert adc["healthy"] == [10.0, 20.0]
    assert adc["source"] == "yaml_override"

    lna = th.bands["tempctrl.LNA_T_now"]
    assert lna["healthy"] == [24.0, 26.0]
    # danger filled in from target_C +/- tempctrl.danger_k_C
    assert lna["danger"] == [15.0, 35.0]
    assert lna["source"] == "derived"


def test_thresholds_yaml_wins_over_derived():
    """An explicit YAML entry for a derived signal should override."""
    yaml_overrides = {
        "tempctrl.LNA_T_now": {
            "healthy": [22.0, 28.0],
            "danger": [18.0, 32.0],
        },
    }
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_overrides=yaml_overrides
    )
    lna = th.bands["tempctrl.LNA_T_now"]
    assert lna["healthy"] == [22.0, 28.0]
    assert lna["danger"] == [18.0, 32.0]
    assert lna["source"] == "yaml_override"


def test_thresholds_unregistered_signal_classifies_unknown():
    th = Thresholds(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    assert th.classify("not.a.signal", 42.0) == "unknown"


def test_thresholds_disabled_signal_classifies_unknown():
    """Signals filtered out by enabled_by should be unreachable."""
    th = Thresholds(OBS_CFG_TEMPCTRL_OFF, CORR_HEADER)
    assert th.classify("tempctrl.LNA_T_now", 25.0) == "unknown"


def test_thresholds_null_healthy_classifies_unknown():
    """A signal with ``healthy: null`` renders grey rather than green."""
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON,
        CORR_HEADER,
        yaml_overrides={"lidar.distance_m": {"healthy": None}},
    )
    assert th.classify("lidar.distance_m", 2.0) == "unknown"


def test_thresholds_classify_value_paths():
    yaml_overrides = {
        "adc.rms": {"healthy": [10.0, 20.0], "danger": [5.0, 30.0]},
    }
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_overrides=yaml_overrides
    )
    assert th.classify("adc.rms", 15.0) == "ok"
    assert (
        th.classify("adc.rms", 22.0) == "warn"
    )  # outside healthy, inside danger
    assert th.classify("adc.rms", 50.0) == "danger"
    assert th.classify("adc.rms", 2.0) == "danger"
    assert th.classify("adc.rms", None) == "unknown"


def test_thresholds_classify_stale_wins_over_value():
    yaml_overrides = {
        "tempctrl.LNA_T_now": {
            "healthy": [24.0, 26.0],
            "danger": [15.0, 35.0],
        },
    }
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_overrides=yaml_overrides
    )
    # In healthy range on value alone, but too old.
    assert th.classify("tempctrl.LNA_T_now", 25.0, age_s=120.0) == "stale"
    # max_age_s=None disables the check.
    assert th.classify("adc.rms", 15.0, age_s=9999.0) in {
        "ok",
        "unknown",
        # adc.rms has max_age_s=None, so staleness is not applied.
    }
    # Signal with max_age_s=None ignores age_s.
    assert SIGNAL_REGISTRY["adc.rms"].max_age_s is None


def test_thresholds_classify_warn_without_danger_band():
    """Derived tempctrl.LNA_drive_level has healthy=[0, clamp] but no
    danger band. Outside-healthy should warn, not raise."""
    th = Thresholds(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    assert th.classify("tempctrl.LNA_drive_level", 0.5) == "ok"
    assert th.classify("tempctrl.LNA_drive_level", 0.9) == "warn"


# ---------------------------------------------------------------------
# with_header (re-sync path)
# ---------------------------------------------------------------------


def test_thresholds_with_header_recomputes_cadence():
    th = Thresholds(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    old = th.bands["corr.acc_cadence_s"]["healthy"]

    th2 = th.with_header({"integration_time": 0.5, "sync_time": 1.0})
    new = th2.bands["corr.acc_cadence_s"]["healthy"]
    assert old != new
    assert new == pytest.approx([0.4, 0.6])


def test_thresholds_with_header_preserves_yaml_override():
    yaml_overrides = {
        "adc.rms": {"healthy": [10.0, 20.0], "danger": [5.0, 30.0]},
        "tempctrl.danger_k_C": 7.0,
    }
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_overrides=yaml_overrides
    )
    th2 = th.with_header({"integration_time": 0.5, "sync_time": 1.0})
    assert th2.bands["adc.rms"]["healthy"] == [10.0, 20.0]
    # Preserved non-default tempctrl_k (target 25 ± 7)
    assert th2.bands["tempctrl.LNA_T_now"]["danger"] == [18.0, 32.0]


# ---------------------------------------------------------------------
# as_dict (served by /api/config)
# ---------------------------------------------------------------------


def test_thresholds_as_dict_includes_provenance_and_metadata():
    yaml_overrides = {
        "adc.rms": {"healthy": [10.0, 20.0], "danger": [5.0, 30.0]},
    }
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_overrides=yaml_overrides
    )
    d = th.as_dict()

    assert d["adc.rms"]["source"] == "yaml_override"
    assert d["adc.rms"]["unit"] == "counts"
    assert d["tempctrl.LNA_T_now"]["source"] == "derived"
    # Signals with no band from either tier:
    assert d["corr.auto_mag_median"]["source"] == "default_null"
    assert d["corr.auto_mag_median"]["healthy"] is None


# ---------------------------------------------------------------------
# from_yaml integrates with the bundled config
# ---------------------------------------------------------------------


def test_thresholds_from_yaml_loads_bundled_defaults():
    th = Thresholds.from_yaml(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    # Bundled YAML defines adc.rms.
    assert th.bands["adc.rms"]["healthy"] == [10.0, 20.0]
    assert th.bands["adc.rms"]["source"] == "yaml_override"
    # Derived defaults still apply.
    assert th.bands["tempctrl.LNA_T_now"]["source"] == "derived"


def test_thresholds_from_yaml_with_explicit_path(tmp_path):
    path = tmp_path / "thresh.yaml"
    path.write_text("adc.rms:\n  healthy: [1.0, 2.0]\n  danger: [0.0, 3.0]\n")
    th = Thresholds.from_yaml(OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_path=path)
    assert th.bands["adc.rms"]["healthy"] == [1.0, 2.0]


def test_thresholds_invalid_band_raises():
    with pytest.raises(ValueError):
        Thresholds(
            OBS_CFG_TEMPCTRL_ON,
            CORR_HEADER,
            yaml_overrides={
                "adc.rms": {"healthy": [20.0, 10.0]},  # lo > hi
            },
        )
