"""Structural guards on the obs_config upload + run_tag.session contract.

AST-level invariants on the ``scripts/`` and
``src/eigsep_observing/scripts/`` directories. They fail closed so a
future PR can't quietly add an uploader, drop a session wrap from an
active driver, let an exempt (passive/coexisting) script start claiming
the tag, or let an uploader stop publishing its owner.
"""

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT_DIRS = (
    _REPO_ROOT / "scripts",
    _REPO_ROOT / "src" / "eigsep_observing" / "scripts",
)

# Authorized scripts that may mutate persistent obs_config in Redis.
UPLOADER_SCRIPTS = {
    "panda_observe.py",
    "no_switch_observation.py",
    "vna_position_sweep.py",
}

# Panda-side ACTIVE driver scripts that must enter run_tag.session.
# These send commands or write VNA files, so they change physical state
# that affects the always-recording corr/VNA data; the run_tag overlay
# flags those files with the active driver's identity ("hand-driven, not
# autonomous science"). refuse-on-conflict makes them mutually exclusive
# with the autonomous driver and each other — one active driver of the
# physical state at a time (combined motor+VNA runs use the dedicated
# alt-mode vna_position_sweep). Passive readouts are NOT here; they only
# read snapshots and must coexist, so they live in RUN_TAG_EXEMPT.
ACTIVE_DRIVER_SCRIPTS = {
    "field_zero.py",
    "motor_home.py",
    "vna_manual.py",
    "rfswitch_manual.py",
    "tempctrl_manual.py",
    "motor_manual.py",
    "motor_scan.py",
    "record_vna.py",
}

# Scripts explicitly exempt from publishing run_tag, with the reason:
#   - Passive readouts (imu_manual.py, watch_sensors.py,
#     potmon_manual.py, lidar_manual.py, pico_preflight.py):
#     MetadataSnapshotReader-only — no commands, no files — so they
#     change no physical state and have no provenance to record. They
#     must coexist with whatever active driver is running, so they must
#     NOT claim the refuse-on-conflict tag. See imu_manual.py /
#     scripts/CLAUDE.md for the active-vs-passive rule.
#   - live_status.py / live_plotter.py: long-running dashboards that
#     may run alongside any driver; publishing would trample or be
#     refused by the session check.
#   - record_metadata.py: test-bench metadata recorder that runs
#     alongside the active driver(s); it only reads/relays metadata and
#     drives no hardware, so it coexists like the dashboards.
#   - observe.py: ground-PC eigsep-observe writer, a consumer of the
#     panda transport (it reads obs_config but never publishes run_tag).
#   - SNAP-side scripts (republish_header.py, adc_snapshot*.py,
#     capture_spectrum.py, fpga_init.py): use the SNAP transport, not
#     the panda transport that hosts the run_tag key.
#   - clear_run_tag.py: the stale-lock recovery tool; it inspects and
#     clears the run_tag key, and entering run_tag.session would be
#     refused by the very stale tag it exists to clear.
#   - host_health.py: always-on per-pi vitals publisher (the
#     eigsep-host-health systemd service). Publishes a dashboard-only
#     K/V to the local Redis, drives no hardware, and must keep
#     running through manual sessions and corr-only operation — the
#     same coexistence reasoning as the dashboards.
#   - set_motor_limits.py: rig-wide motor-limit admin tool. Writes a
#     dedicated MotorLimitStore K/V (not obs_config), drives no motor,
#     and writes no files, so it changes none of the always-recording
#     physical state and has no provenance to record. A one-shot config
#     tool that must coexist with whatever is running, so it must not
#     claim the refuse-on-conflict tag.
RUN_TAG_EXEMPT = {
    "live_status.py",
    "live_plotter.py",
    "record_metadata.py",
    "observe.py",
    "republish_header.py",
    "adc_snapshot.py",
    "adc_snapshot_bench.py",
    "capture_spectrum.py",
    "fpga_init.py",
    "imu_manual.py",
    "watch_sensors.py",
    "potmon_manual.py",
    "lidar_manual.py",
    "pico_preflight.py",
    "clear_run_tag.py",
    "host_health.py",
    "set_motor_limits.py",
}


def _iter_script_files():
    for d in _SCRIPT_DIRS:
        for f in sorted(d.glob("*.py")):
            if f.name == "__init__.py":
                continue
            yield f


def _parse(path):
    with open(path) as f:
        return ast.parse(f.read(), filename=str(path))


def _dotted_call_name(call):
    """Best-effort dotted name for ``call.func`` (e.g. 'run_tag.session')."""
    func = call.func
    parts = []
    while isinstance(func, ast.Attribute):
        parts.append(func.attr)
        func = func.value
    if isinstance(func, ast.Name):
        parts.append(func.id)
    return ".".join(reversed(parts))


def _has_call_named(tree, target):
    """True if any Call's dotted name equals or ends with '.<target>'."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _dotted_call_name(node)
        if name == target or name.endswith("." + target):
            return True
    return False


def _has_configstore_upload(tree):
    """True if the AST has any ``ConfigStore(...).upload(...)`` call."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "upload"):
            continue
        inner = func.value
        if not isinstance(inner, ast.Call):
            continue
        inner_func = inner.func
        if isinstance(inner_func, ast.Name) and inner_func.id == "ConfigStore":
            return True
    return False


def test_obs_config_upload_whitelist():
    """Only authorized uploader scripts may call ``ConfigStore.upload``.

    Bring-up tools like ``vna_manual`` route their local cfg through
    ``PandaClient(..., cfg=...)`` instead of mutating the persistent
    Redis cfg, so the cfg corr-file headers stamp keeps reflecting the
    rig's true operating intent set by the active driver script.
    """
    offenders = []
    for path in _iter_script_files():
        tree = _parse(path)
        if _has_configstore_upload(tree) and path.name not in UPLOADER_SCRIPTS:
            offenders.append(str(path.relative_to(_REPO_ROOT)))
    assert not offenders, (
        f"Unauthorized scripts call ConfigStore.upload: {offenders}. "
        f"Only {sorted(UPLOADER_SCRIPTS)} may mutate persistent Redis cfg."
    )


def test_active_driver_scripts_use_run_tag_session():
    """Every active panda-side driver enters ``run_tag.session``.

    The active manual drivers plus the autonomous uploaders. Together
    with the refuse-on-conflict policy in
    :func:`eigsep_observing.run_tag.session`, this means corr / VNA
    files written during a driver's run always carry that driver's
    identity in the ``run_tag`` overlay.
    """
    must_publish = ACTIVE_DRIVER_SCRIPTS | UPLOADER_SCRIPTS
    missing = []
    for path in _iter_script_files():
        if path.name not in must_publish:
            continue
        tree = _parse(path)
        if not _has_call_named(tree, "run_tag.session"):
            missing.append(str(path.relative_to(_REPO_ROOT)))
    assert not missing, (
        f"Scripts must wrap their main work in run_tag.session(...): {missing}"
    )


def test_exempt_scripts_do_not_claim_run_tag():
    """Exempt scripts must NOT enter ``run_tag.session``.

    The bidirectional partner to
    :func:`test_active_driver_scripts_use_run_tag_session`: a script is
    in ``RUN_TAG_EXEMPT`` *because* it must coexist (passive readout,
    dashboard, recorder, or SNAP-side tool), so claiming the
    refuse-on-conflict tag would block that coexistence. Fails closed —
    would have caught ``record_vna`` sitting in ``RUN_TAG_EXEMPT`` while
    still calling ``run_tag.session``.
    """
    offenders = []
    for path in _iter_script_files():
        if path.name not in RUN_TAG_EXEMPT:
            continue
        tree = _parse(path)
        if _has_call_named(tree, "run_tag.session"):
            offenders.append(str(path.relative_to(_REPO_ROOT)))
    assert not offenders, (
        "Exempt scripts must not call run_tag.session (they must coexist "
        f"with the active driver): {offenders}"
    )


def test_uploader_scripts_publish_owner():
    """Authorized uploaders publish ``obs_config_owner`` alongside upload.

    Binds the two writes at source level so a future PR can't drop
    ``publish_owner`` without dropping the ``ConfigStore.upload`` it
    accompanies — the persistent owner key would otherwise go stale
    and downstream's trust check (``obs_config_owner == run_tag``)
    would silently break.
    """
    missing = []
    for path in _iter_script_files():
        if path.name not in UPLOADER_SCRIPTS:
            continue
        tree = _parse(path)
        if not _has_call_named(tree, "publish_owner"):
            missing.append(str(path.relative_to(_REPO_ROOT)))
    assert not missing, (
        "Uploader scripts must call publish_owner alongside "
        f"ConfigStore.upload: {missing}"
    )


def test_script_directory_partition_is_total():
    """No orphans: every panda-relevant script is categorized.

    A new bring-up script committed without being added to
    ``BRING_UP_SCRIPTS`` (or, explicitly, ``RUN_TAG_EXEMPT``) would
    silently bypass the previous two guards. This check forces the
    author to update the partition.
    """
    known = ACTIVE_DRIVER_SCRIPTS | UPLOADER_SCRIPTS | RUN_TAG_EXEMPT
    orphans = []
    for path in _iter_script_files():
        if path.name not in known:
            orphans.append(str(path.relative_to(_REPO_ROOT)))
    assert not orphans, (
        "New scripts must be added to ACTIVE_DRIVER_SCRIPTS, "
        "UPLOADER_SCRIPTS, or RUN_TAG_EXEMPT in "
        f"{Path(__file__).name}: {orphans}"
    )
