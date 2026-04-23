"""
Signal registry for the live-status dashboard.

A "signal" is a named quantity the dashboard can classify against a
healthy/danger band. The registry declares what's observable; the
``Thresholds`` class (see ``thresholds.py``) holds the per-signal bands
and runs the classifier.

Two tiers of bands:

1. **Derived** — ``default_thresholds(obs_cfg, corr_header)`` computes
   bands that follow from live config (tempctrl setpoints/clamp, corr
   integration time, file duration).
2. **YAML override** — loaded in ``Thresholds`` from
   ``config/live_status_thresholds.yaml`` (or ``--thresholds PATH``).
   Wins over derived where both are present.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Signal:
    """Metadata describing a dashboard signal.

    Attributes
    ----------
    name
        Dotted signal identifier (e.g. ``tempctrl.LNA_T_now``).
    description
        Short human-readable label for the tile.
    unit
        Display unit string (may be empty).
    max_age_s
        Freshness budget. ``None`` disables the staleness check for this
        signal (used for on/off streams like ``adc_stats`` and for
        file-mtime signals that classify ``age`` rather than value).
    enabled_by
        Optional ``obs_config`` flag that must be truthy for this signal
        to be rendered. ``None`` = always enabled.
    """

    name: str
    description: str
    unit: str = ""
    max_age_s: Optional[float] = 30.0
    enabled_by: Optional[str] = None


SIGNAL_REGISTRY: dict[str, Signal] = {
    # ADC — empirical bands only (YAML).
    "adc.rms": Signal(
        "adc.rms",
        "ADC RMS (counts)",
        unit="counts",
        max_age_s=None,
    ),
    "adc.clipping_fraction": Signal(
        "adc.clipping_fraction",
        "ADC clipping fraction",
        max_age_s=None,
    ),
    # Corr — derived bands from integration_time.
    "corr.acc_cadence_s": Signal(
        "corr.acc_cadence_s",
        "Integration cadence",
        unit="s",
        max_age_s=None,
    ),
    "corr.auto_mag_median": Signal(
        "corr.auto_mag_median",
        "Auto-correlation median magnitude",
        max_age_s=None,
    ),
    # File-writing heartbeat — derived from integration_time * corr_ntimes.
    "file.seconds_since_write": Signal(
        "file.seconds_since_write",
        "Seconds since last corr file written",
        unit="s",
        max_age_s=None,
    ),
    # Tempctrl — derived from target_C / hysteresis_C / clamp.
    "tempctrl.LNA_T_now": Signal(
        "tempctrl.LNA_T_now",
        "LNA temperature",
        unit="C",
        enabled_by="use_tempctrl",
    ),
    "tempctrl.LOAD_T_now": Signal(
        "tempctrl.LOAD_T_now",
        "LOAD temperature",
        unit="C",
        enabled_by="use_tempctrl",
    ),
    "tempctrl.LNA_drive_level": Signal(
        "tempctrl.LNA_drive_level",
        "LNA drive level",
        enabled_by="use_tempctrl",
    ),
    "tempctrl.LOAD_drive_level": Signal(
        "tempctrl.LOAD_drive_level",
        "LOAD drive level",
        enabled_by="use_tempctrl",
    ),
    # Site-geometry signals — YAML override only (TODO after deploy).
    "lidar.distance_m": Signal(
        "lidar.distance_m",
        "Lidar distance",
        unit="m",
    ),
    "potmon.pot_el_angle": Signal(
        "potmon.pot_el_angle",
        "Potmon elevation angle",
        unit="deg",
    ),
    "potmon.pot_az_angle": Signal(
        "potmon.pot_az_angle",
        "Potmon azimuth angle",
        unit="deg",
    ),
}


def enabled_signals(
    obs_cfg: dict, registry: Optional[dict[str, Signal]] = None
) -> dict[str, Signal]:
    """Return the subset of the registry whose ``enabled_by`` flag is
    set (or missing) in ``obs_cfg``.

    Signals with ``enabled_by=None`` are always included. A signal with
    ``enabled_by="use_tempctrl"`` is included iff ``obs_cfg`` contains
    ``use_tempctrl: true``. A missing key is treated as disabled so the
    dashboard doesn't render tiles for subsystems the observer isn't
    running.
    """
    reg = registry if registry is not None else SIGNAL_REGISTRY
    out: dict[str, Signal] = {}
    for name, sig in reg.items():
        if sig.enabled_by is None or obs_cfg.get(sig.enabled_by):
            out[name] = sig
    return out


def default_thresholds(
    obs_cfg: dict, corr_header: Optional[dict] = None
) -> dict[str, dict]:
    """Compute config-derived threshold bands.

    Returned dict is keyed by signal name; each value is
    ``{"healthy": [lo, hi] | None, "danger": [lo, hi] | None}``. Only
    signals whose bands fall out of live config are populated here —
    empirical signals (ADC RMS, lidar, potmon angles) are left to the
    YAML override layer.

    Parameters
    ----------
    obs_cfg
        Loaded ``obs_config.yaml``. Read: ``use_tempctrl``,
        ``tempctrl_settings.{LNA,LOAD}.{target_C, hysteresis_C, clamp}``,
        ``corr_ntimes``.
    corr_header
        Loaded ``CorrConfigStore.get_header()`` output. Read:
        ``integration_time``. Pass ``None`` if the header isn't
        available yet (e.g. FPGA not synchronized) — cadence and
        file-heartbeat bands will be omitted until it is.

    Notes
    -----
    Tempctrl danger bands are not derived here. This function only
    computes the healthy tempctrl band from ``tempctrl_settings`` and
    leaves ``danger`` unset. The danger-band half-width is filled in
    later by :class:`Thresholds` using the YAML override tuning key
    ``tempctrl.danger_k_C`` (default
    :data:`Thresholds._DEFAULT_TEMPCTRL_DANGER_K_C`).
    """
    out: dict[str, dict] = {}
    int_time = None
    if corr_header is not None:
        int_time = corr_header.get("integration_time")

    if int_time is not None and int_time > 0:
        out["corr.acc_cadence_s"] = {
            "healthy": [0.8 * int_time, 1.2 * int_time],
            "danger": [0.5 * int_time, 2.0 * int_time],
        }
        corr_ntimes = obs_cfg.get("corr_ntimes")
        if corr_ntimes:
            file_dur = int_time * corr_ntimes
            out["file.seconds_since_write"] = {
                "healthy": [0.0, 1.5 * file_dur],
                "danger": [0.0, 3.0 * file_dur],
            }

    if obs_cfg.get("use_tempctrl"):
        settings = obs_cfg.get("tempctrl_settings", {}) or {}
        for channel in ("LNA", "LOAD"):
            ch_cfg = settings.get(channel, {}) or {}
            target = ch_cfg.get("target_C")
            hyst = ch_cfg.get("hysteresis_C")
            clamp = ch_cfg.get("clamp")
            if target is not None and hyst is not None:
                out[f"tempctrl.{channel}_T_now"] = {
                    "healthy": [target - 2 * hyst, target + 2 * hyst],
                    # danger band filled in by Thresholds using
                    # tempctrl.danger_k_C from the YAML override
                    # (defaulted there, not here).
                    "danger": None,
                    "_target_C": target,
                }
            if clamp is not None:
                out[f"tempctrl.{channel}_drive_level"] = {
                    "healthy": [0.0, float(clamp)],
                    "danger": None,
                }

    return out
