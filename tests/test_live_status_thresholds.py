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
    effective_obs_cfg,
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

    assert out["tempctrl_lna.T_now"]["healthy"] == [24.0, 26.0]
    assert out["tempctrl_load.T_now"]["healthy"] == [24.0, 26.0]
    # danger is deferred to Thresholds where the YAML tuning knob is
    # resolved. default_thresholds leaves it None and carries
    # _target_C so the merger can fill it in.
    assert out["tempctrl_lna.T_now"]["danger"] is None
    assert out["tempctrl_lna.T_now"]["_target_C"] == 25.0

    assert out["tempctrl_lna.drive_level"]["healthy"] == [0.0, 0.6]
    assert out["tempctrl_load.drive_level"]["healthy"] == [0.0, 0.6]


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
        "tempctrl_lna.T_now",
        "tempctrl_load.T_now",
        "tempctrl_lna.drive_level",
        "tempctrl_load.drive_level",
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
    assert "tempctrl_lna.T_now" not in enabled
    # Signals with enabled_by=None stay regardless.
    assert "adc.rms" in enabled
    assert "corr.acc_cadence_s" in enabled


def test_enabled_signals_includes_tempctrl_when_on():
    enabled = enabled_signals(OBS_CFG_TEMPCTRL_ON)
    assert "tempctrl_lna.T_now" in enabled
    assert "tempctrl_lna.drive_level" in enabled


# LNA descoped (installed: false) but with setpoints still staged for a
# potential re-install/hot-swap — the realistic field shape. The channel
# publishes no stream, so its tiles/bands must disappear while LOAD's
# stay.
OBS_CFG_LNA_UNINSTALLED = {
    "use_tempctrl": True,
    "corr_ntimes": 240,
    "tempctrl_settings": {
        "LNA": {
            "installed": False,
            "enable": False,
            "target_C": 25.0,
            "hysteresis_C": 0.5,
            "clamp": 0.6,
        },
        "LOAD": {"target_C": 25.0, "hysteresis_C": 0.5, "clamp": 0.6},
    },
}


def test_enabled_signals_drops_uninstalled_channel():
    """A descoped channel publishes no stream — its tiles would sit
    permanently empty on the dashboard (the field's only alerting
    surface), so they're dropped per channel while LOAD's stay."""
    enabled = enabled_signals(OBS_CFG_LNA_UNINSTALLED)
    for key in (
        "tempctrl_lna.T_now",
        "tempctrl_lna.drive_level",
        "tempctrl_lna.Kp",
        "tempctrl_lna.Ki",
        "tempctrl_lna.integral",
    ):
        assert key not in enabled, key
    assert "tempctrl_load.T_now" in enabled
    assert "tempctrl_load.drive_level" in enabled


def test_default_thresholds_skips_uninstalled_channel():
    """Staged setpoints on a descoped channel must not produce bands —
    there is no stream for them to classify."""
    out = default_thresholds(OBS_CFG_LNA_UNINSTALLED, corr_header=CORR_HEADER)
    assert "tempctrl_lna.T_now" not in out
    assert "tempctrl_lna.drive_level" not in out
    assert "tempctrl_load.T_now" in out
    assert "tempctrl_load.drive_level" in out


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

    lna = th.bands["tempctrl_lna.T_now"]
    assert lna["healthy"] == [24.0, 26.0]
    # danger filled in from target_C +/- tempctrl.danger_k_C
    assert lna["danger"] == [15.0, 35.0]
    assert lna["source"] == "derived"


def test_thresholds_yaml_wins_over_derived():
    """An explicit YAML entry for a derived signal should override."""
    yaml_overrides = {
        "tempctrl_lna.T_now": {
            "healthy": [22.0, 28.0],
            "danger": [18.0, 32.0],
        },
    }
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_overrides=yaml_overrides
    )
    lna = th.bands["tempctrl_lna.T_now"]
    assert lna["healthy"] == [22.0, 28.0]
    assert lna["danger"] == [18.0, 32.0]
    assert lna["source"] == "yaml_override"


def test_thresholds_unregistered_signal_classifies_unknown():
    th = Thresholds(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    assert th.classify("not.a.signal", 42.0) == "unknown"


def test_thresholds_disabled_signal_classifies_unknown():
    """Signals filtered out by enabled_by should be unreachable."""
    th = Thresholds(OBS_CFG_TEMPCTRL_OFF, CORR_HEADER)
    assert th.classify("tempctrl_lna.T_now", 25.0) == "unknown"


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
        "tempctrl_lna.T_now": {
            "healthy": [24.0, 26.0],
            "danger": [15.0, 35.0],
        },
    }
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_overrides=yaml_overrides
    )
    # In healthy range on value alone, but too old.
    assert th.classify("tempctrl_lna.T_now", 25.0, age_s=120.0) == "stale"
    # max_age_s=None disables the check.
    assert th.classify("adc.rms", 15.0, age_s=9999.0) in {
        "ok",
        "unknown",
        # adc.rms has max_age_s=None, so staleness is not applied.
    }
    # Signal with max_age_s=None ignores age_s.
    assert SIGNAL_REGISTRY["adc.rms"].max_age_s is None


def test_thresholds_classify_warn_without_danger_band():
    """Derived tempctrl_lna.drive_level has healthy=[0, clamp] but no
    danger band. Outside-healthy should warn, not raise."""
    th = Thresholds(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    assert th.classify("tempctrl_lna.drive_level", 0.5) == "ok"
    assert th.classify("tempctrl_lna.drive_level", 0.9) == "warn"


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
    assert th2.bands["tempctrl_lna.T_now"]["danger"] == [18.0, 32.0]


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
    assert d["tempctrl_lna.T_now"]["source"] == "derived"
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
    assert th.bands["tempctrl_lna.T_now"]["source"] == "derived"


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


# ---------------------------------------------------------------------
# system_current signal + threshold band
# ---------------------------------------------------------------------


def test_system_current_signal_registered_and_always_enabled():
    sig = SIGNAL_REGISTRY["system_current.current_a"]
    assert sig.unit == "A"
    assert sig.enabled_by is None  # system-wide vital, never gated
    assert sig.max_age_s == 30.0
    # Present even when tempctrl (and other optional subsystems) are off.
    assert "system_current.current_a" in enabled_signals(OBS_CFG_TEMPCTRL_OFF)


def test_system_current_band_from_bundled_yaml():
    th = Thresholds.from_yaml(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    assert th.bands["system_current.current_a"]["healthy"] == [0.0, 5.0]
    assert th.bands["system_current.current_a"]["danger"] == [0.0, 8.0]
    assert th.classify("system_current.current_a", 3.0) == "ok"
    assert th.classify("system_current.current_a", 6.0) == "warn"
    assert th.classify("system_current.current_a", 9.0) == "danger"


# rfswitch_therm PCB thermistor signals
# ---------------------------------------------------------------------


def test_rfswitch_therm_signals_registered_and_always_enabled():
    for i in range(3):
        name = f"rfswitch_therm.temp_therm{i}"
        sig = SIGNAL_REGISTRY[name]
        assert sig.unit == "C"
        assert sig.enabled_by is None  # board vital, never gated
        # present even with optional subsystems off
        assert name in enabled_signals(OBS_CFG_TEMPCTRL_OFF)


def test_rfswitch_therm_classifies_unknown_without_band():
    # No config-derived and no bundled-YAML band -> grey "unknown" tile.
    th = Thresholds(OBS_CFG_TEMPCTRL_OFF, CORR_HEADER)
    assert th.classify("rfswitch_therm.temp_therm0", 30.0) == "unknown"


# ---------------------------------------------------------------------
# effective_obs_cfg — prefer-Redis-with-local-fallback (issue #194)
# ---------------------------------------------------------------------


# Local-file layer for the merge tests. Carries the panda-reality keys
# (use_* gates, tempctrl_settings, corr_ntimes, calibration) plus a
# dashboard-local key (corr_save_dir) and switch_schedule, which is
# deliberately NOT merged — the rfswitch payload reads the schedule
# straight from the upload with no local fallback.
OBS_CFG_LOCAL = {
    "corr_save_dir": "/media/eigsep/T7/data",
    "corr_ntimes": 240,
    "use_switches": True,
    "use_vna": True,
    "use_motor": False,
    "use_tempctrl": True,
    "switch_schedule": {"RFANT": 3600, "RFNON": 60},
    "calibration": {
        "noise_diode_enr_db": 35.0,
        "noise_source_atten_db": 30.0,
        "t_ns_stream": "rfswitch_therm",
        "t_ns_field": "temp_therm2",
        "t_amb_stream": "tempctrl_load",
        "t_amb_field": "T_now",
    },
    "tempctrl_settings": {
        "LNA": {"installed": True, "target_C": 25.0, "hysteresis_C": 0.5},
        "LOAD": {"installed": True, "target_C": 25.0, "hysteresis_C": 0.5},
    },
}


def _panda_upload(**overrides):
    """A full-obs_config upload as ``ConfigStore.get`` returns it: the
    panda uploads its entire config dict and ``Transport.upload_dict``
    stamps ``upload_time``. Built from the local fixture so
    unspecified keys agree — the realistic field case is two copies of
    the same file drifting on a few keys.
    """
    cfg = {**OBS_CFG_LOCAL, "upload_time": 1e9}
    cfg.update(overrides)
    return cfg


def test_effective_obs_cfg_no_upload_returns_local_copy():
    out = effective_obs_cfg(OBS_CFG_LOCAL, None)
    assert out == OBS_CFG_LOCAL
    assert out is not OBS_CFG_LOCAL  # copy, not alias
    # An empty dict (never a real upload shape) also means "no upload".
    assert effective_obs_cfg(OBS_CFG_LOCAL, {}) == OBS_CFG_LOCAL


def test_effective_obs_cfg_upload_wins_on_panda_reality_keys():
    upload = _panda_upload(
        use_tempctrl=False,
        use_motor=True,
        corr_ntimes=480,
        tempctrl_settings={
            "LNA": {"installed": False},
            "LOAD": {"installed": True, "target_C": 30.0},
        },
    )
    out = effective_obs_cfg(OBS_CFG_LOCAL, upload)
    assert out["use_tempctrl"] is False
    assert out["use_motor"] is True
    assert out["corr_ntimes"] == 480
    assert out["tempctrl_settings"] == upload["tempctrl_settings"]


def test_effective_obs_cfg_dashboard_local_keys_survive():
    # corr_save_dir is not panda reality (the corr writer runs on the
    # ground PC); the upload_time stamp must not leak into the merged
    # config either.
    upload = _panda_upload(corr_save_dir="/panda/side/path")
    out = effective_obs_cfg(OBS_CFG_LOCAL, upload)
    assert out["corr_save_dir"] == OBS_CFG_LOCAL["corr_save_dir"]
    assert "upload_time" not in out


def test_effective_obs_cfg_missing_upload_keys_fall_back_to_local():
    # Deliberately partial upload (production uploads carry the full
    # obs_config): models an older producer that predates a key — the
    # local value must survive rather than the field dropping.
    upload = {"upload_time": 1e9, "use_tempctrl": False}
    out = effective_obs_cfg(OBS_CFG_LOCAL, upload)
    assert out["use_tempctrl"] is False
    assert out["corr_ntimes"] == OBS_CFG_LOCAL["corr_ntimes"]
    assert out["tempctrl_settings"] == OBS_CFG_LOCAL["tempctrl_settings"]


def test_effective_obs_cfg_plucks_only_routing_knobs_from_calibration():
    upload = _panda_upload(
        calibration={
            "noise_diode_enr_db": 99.0,  # panda copy drifted
            "noise_source_atten_db": 20.0,  # panda copy drifted
            "t_ns_stream": "rfswitch_therm",
            "t_ns_field": "temp_therm0",  # pad moved to another channel
            "t_amb_stream": "tempctrl_lna",  # hot-swap step 3
            "t_amb_field": "T_now",
        }
    )
    out = effective_obs_cfg(OBS_CFG_LOCAL, upload)
    cal = out["calibration"]
    # The reference-temperature routing follows the upload...
    assert cal["t_amb_stream"] == "tempctrl_lna"
    assert cal["t_ns_field"] == "temp_therm0"
    # ...but the physical constants are dashboard-local display-cal
    # knobs, not panda reality.
    assert cal["noise_diode_enr_db"] == 35.0
    assert cal["noise_source_atten_db"] == 30.0
    # The local dict's nested section was copied, not mutated in place.
    assert OBS_CFG_LOCAL["calibration"]["t_amb_stream"] == "tempctrl_load"


def test_effective_obs_cfg_switch_schedule_not_merged():
    # switch_schedule keeps its stricter no-fallback contract in
    # _rfswitch_payload (read straight from the upload); the merged
    # config must not paper over that with the upload's copy.
    upload = _panda_upload(switch_schedule={"RFANT": 60})
    out = effective_obs_cfg(OBS_CFG_LOCAL, upload)
    assert out["switch_schedule"] == OBS_CFG_LOCAL["switch_schedule"]


# ---------------------------------------------------------------------
# with_obs_cfg (panda config upload path)
# ---------------------------------------------------------------------


def test_thresholds_with_obs_cfg_recomputes_bands_and_gating():
    th = Thresholds(OBS_CFG_TEMPCTRL_ON, CORR_HEADER)
    assert "tempctrl_lna.T_now" in th.registry

    new_cfg = {
        "use_tempctrl": True,
        "corr_ntimes": 240,
        "tempctrl_settings": {
            "LNA": {
                "installed": False,
                "enable": False,
                "target_C": 25.0,
                "hysteresis_C": 0.5,
                "clamp": 0.6,
            },
            "LOAD": {"target_C": 30.0, "hysteresis_C": 0.5, "clamp": 0.6},
        },
    }
    th2 = th.with_obs_cfg(new_cfg)
    # Gating followed the new config: the descoped channel is gone...
    assert "tempctrl_lna.T_now" not in th2.registry
    # ...and the live channel's band moved to the new setpoint.
    assert th2.bands["tempctrl_load.T_now"]["healthy"] == [29.0, 31.0]
    # The corr header carried over — cadence band unchanged.
    assert th2.bands["corr.acc_cadence_s"] == th.bands["corr.acc_cadence_s"]


def test_thresholds_with_obs_cfg_preserves_yaml_override_and_danger_k():
    yaml_overrides = {
        "adc.rms": {"healthy": [10.0, 20.0], "danger": [5.0, 30.0]},
        "tempctrl.danger_k_C": 7.0,
    }
    th = Thresholds(
        OBS_CFG_TEMPCTRL_ON, CORR_HEADER, yaml_overrides=yaml_overrides
    )
    new_cfg = {
        "use_tempctrl": True,
        "corr_ntimes": 240,
        "tempctrl_settings": {
            "LNA": {"target_C": 25.0, "hysteresis_C": 0.5, "clamp": 0.6},
            "LOAD": {"target_C": 30.0, "hysteresis_C": 0.5, "clamp": 0.6},
        },
    }
    th2 = th.with_obs_cfg(new_cfg)
    assert th2.bands["adc.rms"]["healthy"] == [10.0, 20.0]
    # Non-default danger half-width survived the rebuild (30 ± 7).
    assert th2.bands["tempctrl_load.T_now"]["danger"] == [23.0, 37.0]
