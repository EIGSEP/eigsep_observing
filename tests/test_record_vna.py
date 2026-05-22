"""Smoke tests for scripts/record_vna.py.

Mirrors test_vna_manual.py: the script lives under ``scripts/`` so we
import it by file location. The loop is driven against a
``build_vna_subsystem(dummy=True)`` (in-process dummy ``PicoManager``
+ ``DummyVNA``) so ``measure_s11`` runs end-to-end without a real
``PandaClient``.
"""

import importlib.util
import threading
from pathlib import Path

import pytest

from eigsep_observing.vna import build_vna_subsystem


SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load():
    path = SCRIPTS_DIR / "record_vna.py"
    spec = importlib.util.spec_from_file_location("record_vna", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def vna_subsystem(transport, dummy_cfg):
    """Minimal VNA subsystem with ``DummyVNA`` + in-process dummy picos."""
    sub = build_vna_subsystem(
        transport, dummy_cfg, source="test_record_vna", dummy=True
    )
    yield sub
    sub.cleanup()


def test_loop_runs_each_bundle_then_stops(
    vna_subsystem, transport, dummy_cfg, tmp_path, monkeypatch
):
    """One full pass through ``bundles`` writes one .h5 per bundle,
    after which the stop_event flips and the loop exits without
    sleeping the full interval."""
    rv = _load()
    save_dir = tmp_path
    stop_event = threading.Event()
    calls = []
    real_run = rv._run_bundle

    def _tracker(subsystem_, cfg_, transport_, mode, save_dir_):
        result = real_run(subsystem_, cfg_, transport_, mode, save_dir_)
        calls.append(mode)
        if calls == ["ant", "rec"]:
            stop_event.set()
        return result

    monkeypatch.setattr(rv, "_run_bundle", _tracker)

    rv._loop(
        subsystem=vna_subsystem,
        cfg=dummy_cfg,
        transport=transport,
        save_dir=save_dir,
        bundles=["ant", "rec"],
        interval=0,
        stop_event=stop_event,
    )

    assert calls == ["ant", "rec"]
    h5_files = sorted(p.name for p in save_dir.glob("vna_manual_*.h5"))
    assert any(name.startswith("vna_manual_ant_") for name in h5_files), (
        h5_files
    )
    assert any(name.startswith("vna_manual_rec_") for name in h5_files), (
        h5_files
    )


def test_parse_bundles_validates():
    rv = _load()
    assert rv._parse_bundles("ant") == ["ant"]
    assert rv._parse_bundles("ant,rec") == ["ant", "rec"]
    assert rv._parse_bundles("rec,ant") == ["rec", "ant"]


def test_parse_bundles_rejects_unknown():
    rv = _load()
    with pytest.raises(SystemExit, match="unknown entries"):
        rv._parse_bundles("ant,trouble")
    with pytest.raises(SystemExit, match="at least one"):
        rv._parse_bundles("")
