"""Tests for ``scripts/republish_header.py``.

The script is standalone — it does not import ``EigsepFpga`` and does
not touch the FPGA. Tests exercise the happy path (wiring edit in
Redis-side header, all other fields preserved) and the cold-state path
(no header in Redis → exits nonzero).
"""

import importlib.util
from pathlib import Path

import pytest
import yaml

from eigsep_observing.corr import CorrConfigStore
from eigsep_observing.utils import get_config_path
from eigsep_redis.testing import DummyTransport

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load(name):
    """Import a script module by file path (not on sys.path)."""
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _seed_header(transport, **overrides):
    """Upload a minimal but realistic corr header into the
    fakeredis-backed transport, matching the shape ``EigsepFpga.header``
    produces today."""
    header = {
        "snap_ip": "10.10.10.12",
        "fpg_file": "eigsep_fengine.fpg",
        "fpg_version": [2, 3],
        "sample_rate": 500.0,
        "nchan": 1024,
        "use_ref": False,
        "use_noise": False,
        "adc_gain": 4,
        "fft_shift": 0x015F,
        "corr_acc_len": 2**28,
        "corr_scalar": 2**9,
        "corr_word": 4,
        "acc_bins": 2,
        "avg_even_odd": True,
        "dtype": ">i4",
        "pol_delay": {"01": 0, "23": 0, "45": 0},
        "redis": {"host": "localhost", "port": 6379},
        "sync_time": 1713200000.0,
        "integration_time": 0.5368709120000001,
        "wiring": {
            "snap_id": "C000069",
            "ants": {
                "old": {
                    "fem": {"id": 1, "pol": "N"},
                    "snap": {"input": 0, "label": "N0"},
                },
            },
        },
    }
    header.update(overrides)
    CorrConfigStore(transport).upload_header(header)
    return header


def test_republish_header_swaps_wiring_preserves_rest(tmp_path):
    """Happy path: corrected wiring.yaml replaces the header's wiring
    block; every other field — notably ``sync_time`` — is preserved."""
    transport = DummyTransport()
    original = _seed_header(transport)

    new_wiring = {
        "snap_id": "C000069",
        "ants": {
            "new_ant": {
                "fem": {"id": 42, "pol": "E"},
                "snap": {"input": 7, "label": "E14"},
            },
        },
    }
    wiring_path = tmp_path / "wiring.yaml"
    wiring_path.write_text(yaml.safe_dump(new_wiring))

    # republish_header.main needs cfg only for the redis block, which
    # the injected transport makes irrelevant. Point it at the shipped
    # corr_config.yaml.
    mod = _load("republish_header")
    mod.main(
        argv=[
            "--wiring_file",
            str(wiring_path),
            "--config_file",
            str(get_config_path("corr_config.yaml")),
        ],
        transport=transport,
    )

    fetched = CorrConfigStore(transport).get_header()
    assert fetched["wiring"] == new_wiring
    # Sync time and all other fields unchanged.
    assert fetched["sync_time"] == original["sync_time"]
    assert fetched["sample_rate"] == original["sample_rate"]
    assert fetched["pol_delay"] == original["pol_delay"]
    # header_upload_unix was re-stamped by upload_header.
    assert "header_upload_unix" in fetched


@pytest.mark.parametrize(
    "wiring_yaml",
    [
        "",  # empty file → safe_load returns None
        "snap_id: C000069\n",  # dict missing 'ants'
        "[1, 2, 3]\n",  # not a dict at all
    ],
    ids=["empty", "missing_ants", "not_a_dict"],
)
def test_republish_header_rejects_invalid_wiring(tmp_path, wiring_yaml):
    """Malformed wiring.yaml exits before touching Redis. The header
    rewriter must not turn an operator typo into the bug it was meant
    to fix."""
    transport = DummyTransport()
    _seed_header(transport)

    wiring_path = tmp_path / "wiring.yaml"
    wiring_path.write_text(wiring_yaml)

    mod = _load("republish_header")
    with pytest.raises(SystemExit) as exc:
        mod.main(
            argv=[
                "--wiring_file",
                str(wiring_path),
                "--config_file",
                str(get_config_path("corr_config.yaml")),
            ],
            transport=transport,
        )
    assert "Invalid wiring" in str(exc.value)


def test_republish_header_cold_state_exits(tmp_path):
    """No header in Redis → script exits nonzero with a cold-boot
    message. Operator should run fpga_init.py first."""
    transport = DummyTransport()
    # Intentionally no upload_header call.

    new_wiring = {"snap_id": "C000069", "ants": {}}
    wiring_path = tmp_path / "wiring.yaml"
    wiring_path.write_text(yaml.safe_dump(new_wiring))

    mod = _load("republish_header")
    with pytest.raises(SystemExit) as exc:
        mod.main(
            argv=[
                "--wiring_file",
                str(wiring_path),
                "--config_file",
                str(get_config_path("corr_config.yaml")),
            ],
            transport=transport,
        )
    assert "No corr header" in str(exc.value)
