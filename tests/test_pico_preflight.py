"""Tests for the alive-but-absent annotation in ``scripts/pico_preflight.py``.

A live device heartbeat with no published stream is either a
deliberately descoped tempctrl channel (firmware ``installed=false``
publishes nothing) or a producer fault. The preflight table must say
so instead of leaving the reading column blank — without false-failing
a descoped field rig.
"""

import importlib.util
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


def _load(name):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_absent_stream_note_fires_when_alive_and_silent():
    mod = _load("pico_preflight")
    note = mod._absent_stream_note(alive=True, ts=None, reading=None)
    assert "no stream" in note
    assert "uninstalled" in note


def test_absent_stream_note_silent_otherwise():
    mod = _load("pico_preflight")
    # Device dead: the alive column already tells the story.
    assert mod._absent_stream_note(alive=False, ts=None, reading=None) == ""
    # Stream publishing: nothing to annotate.
    assert (
        mod._absent_stream_note(alive=True, ts=123.0, reading={"T_now": 25.0})
        == ""
    )
