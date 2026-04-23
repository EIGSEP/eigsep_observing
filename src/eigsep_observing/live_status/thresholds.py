"""
Threshold classifier for the live-status dashboard.

``Thresholds`` merges three sources into one band per signal:

1. Config-derived defaults from :func:`signals.default_thresholds`.
2. YAML override file (``config/live_status_thresholds.yaml`` or a
   user-specified path).
3. A fallback for unregistered signals, which always classify as
   ``"unknown"`` — rendered as a grey tile rather than silently green.

``classify(signal, value, age_s=None)`` returns one of
``"ok" | "warn" | "danger" | "stale" | "unknown"``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Union

import yaml

from .signals import (
    SIGNAL_REGISTRY,
    Signal,
    default_thresholds,
    enabled_signals,
)


_DEFAULT_TEMPCTRL_DANGER_K_C = 10.0


def _as_band(value: Any) -> Optional[list[float]]:
    """Normalize a YAML band entry to ``[lo, hi]`` or ``None``."""
    if value is None:
        return None
    if (
        not isinstance(value, (list, tuple))
        or len(value) != 2
        or any(v is None for v in value)
    ):
        raise ValueError(f"invalid band {value!r}; expected [lo, hi] or null")
    lo, hi = float(value[0]), float(value[1])
    if lo > hi:
        raise ValueError(f"band {value!r} has lo > hi")
    return [lo, hi]


def _load_yaml_overrides(path: Union[str, Path]) -> dict:
    """Load a thresholds override YAML file, returning an empty dict
    if the file is empty (but not if it's missing)."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data or {}


class Thresholds:
    """Per-signal healthy/danger bands + a classifier.

    Instances are effectively immutable once built; use
    :meth:`with_header` to produce a refreshed instance after the corr
    header (``integration_time``) changes.

    Provenance tracking.
    --------------------
    ``self.bands`` stores the merged band under each signal name,
    alongside a ``source`` tag in ``{"derived", "yaml_override",
    "default_null"}``. ``/api/config`` surfaces this so an operator can
    tell at a glance where a band came from.
    """

    def __init__(
        self,
        obs_cfg: dict,
        corr_header: Optional[dict] = None,
        yaml_overrides: Optional[dict] = None,
        registry: Optional[dict[str, Signal]] = None,
    ):
        self.obs_cfg = obs_cfg
        self.corr_header = corr_header
        self._yaml_overrides = dict(yaml_overrides or {})
        # Remember the caller's registry argument (pre-enable-filter) so
        # with_header() can rebuild with the same customization.
        self._registry_arg = registry
        self.registry = enabled_signals(
            obs_cfg, registry if registry is not None else SIGNAL_REGISTRY
        )

        tempctrl_k = self._yaml_overrides.pop(
            "tempctrl.danger_k_C", _DEFAULT_TEMPCTRL_DANGER_K_C
        )
        tempctrl_k = float(tempctrl_k)

        derived = default_thresholds(obs_cfg, corr_header)

        # Fill tempctrl danger bands using the per-channel target_C
        # carried through by default_thresholds, with the YAML-tunable
        # half-width.
        for sig_name, band in derived.items():
            target = band.pop("_target_C", None)
            if target is not None and band.get("danger") is None:
                band["danger"] = [
                    target - tempctrl_k,
                    target + tempctrl_k,
                ]

        merged: dict[str, dict] = {}
        for name in self.registry:
            band = self._resolve_band(name, derived)
            merged[name] = band
        self.bands = merged
        self.tempctrl_danger_k_C = tempctrl_k

    @classmethod
    def from_yaml(
        cls,
        obs_cfg: dict,
        corr_header: Optional[dict] = None,
        yaml_path: Union[str, Path, None] = None,
        registry: Optional[dict[str, Signal]] = None,
    ) -> "Thresholds":
        """Load a YAML override file and build a Thresholds instance.

        ``yaml_path=None`` loads the package-bundled file at
        ``config/live_status_thresholds.yaml``.
        """
        if yaml_path is None:
            from ..utils import get_config_path

            yaml_path = get_config_path("live_status_thresholds.yaml")
        overrides = _load_yaml_overrides(yaml_path)
        return cls(
            obs_cfg=obs_cfg,
            corr_header=corr_header,
            yaml_overrides=overrides,
            registry=registry,
        )

    def with_header(self, corr_header: dict) -> "Thresholds":
        """Return a new Thresholds reflecting an updated corr header.

        Used by the aggregator when ``integration_time`` changes (e.g.
        after a re-sync). The old instance is discarded.
        """
        # Reconstruct self._yaml_overrides with the danger-k entry if it
        # was supplied — __init__ consumes it via dict.pop.
        overrides = dict(self._yaml_overrides)
        if self.tempctrl_danger_k_C != _DEFAULT_TEMPCTRL_DANGER_K_C:
            overrides["tempctrl.danger_k_C"] = self.tempctrl_danger_k_C
        return Thresholds(
            obs_cfg=self.obs_cfg,
            corr_header=corr_header,
            yaml_overrides=overrides,
            registry=self._registry_arg,
        )

    def _resolve_band(self, name: str, derived: dict) -> dict:
        """Merge derived + YAML override for one signal."""
        yaml_entry = self._yaml_overrides.get(name)
        derived_entry = derived.get(name)

        if yaml_entry is not None:
            healthy = _as_band(yaml_entry.get("healthy"))
            danger = _as_band(yaml_entry.get("danger"))
            return {
                "healthy": healthy,
                "danger": danger,
                "source": "yaml_override",
            }
        if derived_entry is not None:
            return {
                "healthy": derived_entry.get("healthy"),
                "danger": derived_entry.get("danger"),
                "source": "derived",
            }
        return {"healthy": None, "danger": None, "source": "default_null"}

    def classify(
        self,
        signal: str,
        value: Optional[float],
        age_s: Optional[float] = None,
    ) -> str:
        """Return one of ``"ok"|"warn"|"danger"|"stale"|"unknown"``.

        Semantics:

        - ``signal`` not registered (or disabled) → ``"unknown"``.
        - ``age_s`` present and exceeds the signal's ``max_age_s`` →
          ``"stale"`` (takes precedence over value-based classification).
        - ``value is None`` or ``healthy`` is ``None`` → ``"unknown"``.
        - Value inside ``healthy`` → ``"ok"``.
        - Value outside ``danger`` → ``"danger"``.
        - Otherwise → ``"warn"``.

        A signal without a ``danger`` band that falls outside ``healthy``
        classifies as ``"warn"`` (there is no upper escalation tier).
        """
        sig = self.registry.get(signal)
        if sig is None:
            return "unknown"
        if age_s is not None and sig.max_age_s is not None:
            if age_s > sig.max_age_s:
                return "stale"

        band = self.bands.get(signal)
        if band is None or value is None:
            return "unknown"
        healthy = band.get("healthy")
        danger = band.get("danger")
        if healthy is None:
            return "unknown"
        if healthy[0] <= value <= healthy[1]:
            return "ok"
        if danger is not None and not (danger[0] <= value <= danger[1]):
            return "danger"
        return "warn"

    def as_dict(self) -> dict[str, dict]:
        """Return the merged bands with provenance, for ``/api/config``.

        Each signal entry also carries its ``description``, ``unit``,
        and ``max_age_s`` from the registry so the front-end can render
        tile labels directly.
        """
        out: dict[str, dict] = {}
        for name, sig in self.registry.items():
            band = self.bands.get(name, {})
            out[name] = {
                "description": sig.description,
                "unit": sig.unit,
                "max_age_s": sig.max_age_s,
                "healthy": band.get("healthy"),
                "danger": band.get("danger"),
                "source": band.get("source", "default_null"),
            }
        return out
