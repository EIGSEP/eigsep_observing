# Bring-up scripts contract

The scripts in this directory (`*_manual.py`, `record_*.py`,
`motor_control.py`, `pico_preflight.py`, etc.) are operator tools
for **field verification, lab bring-up, and ad-hoc debugging**. The
production observing path is in `src/eigsep_observing/scripts/`
(`fpga_init`, `observe`, `panda_observe`) and is *not* governed by
this contract — it has its own architecture documented in the
top-level `CLAUDE.md`.

The same exclusion applies to **alternative observing modes** —
scripts that take exclusive control of the panda for the duration
of their run (own the heartbeat, drive their own switch / motor /
VNA orchestration to a defined endpoint, are mutually exclusive
with `panda_observe`). These are structurally `panda_observe`-like:
they build a full :class:`PandaClient`, upload `obs_config`, and
coordinate state. They live alongside `panda_observe` in
`src/eigsep_observing/scripts/`, not here. Current examples:
``no_switch_observation``, ``vna_position_sweep``.

The test for which category a new script belongs in: **does it need
exclusive control of the panda for its run?** If yes (it starts its
own loops, pins switch/motor state across operations, or makes any
assumption about what else is/isn't running), write it under
`src/eigsep_observing/scripts/`. If no (commands fired inline from
one terminal, composable with sibling scripts), it belongs here.
Touching multiple picos does not by itself push a script into the
alt-mode category — driving motor + rfswitch + VNA inline through
:class:`PicoProxy` / :class:`MotorClient` /
:func:`build_vna_subsystem` is still bring-up.

Bring-up scripts must coexist with a production observer running on
the panda, and with each other (one operator may run a motor script
in one terminal and a VNA script in another). The whole point is
**seamless, opportunistic, defensive** measurement: an operator must
never have to stop `observe`, `Ctrl+C` an unrelated script, or
unload pico services to take a measurement.

We also want the same bring-up scripts available unchanged in lab
and field, so that "the system works the same way in the field as
in the lab" is a property we can verify directly with the same
tooling. That means the scripts cannot assume anything about what
*else* is running on the rig — only that the picos are up and the
Redis transport is reachable.

## What bring-up scripts MUST do

1. **Build only the producer surface for the one bus they touch.**
   VNA scripts → `VnaWriter` (via `build_vna_subsystem`). Metadata-
   producing scripts → `MetadataWriter`. Never build a
   :class:`PandaClient`; never build a
   :class:`MotionSwitchCoordinator`.
2. **Stamp captures with `MetadataSnapshotReader.get(...)`** so
   artifacts inherit the running picos' state — even though *this*
   script didn't start those picos. The snapshot reader is
   read-only and doesn't compete with the corr loop's
   `MetadataStreamReader.drain`, so it is the right reader for any
   point-in-time bring-up capture (see top-level CLAUDE.md
   "Metadata flow: streaming for corr, snapshot for VNA").
3. **Talk to hardware exclusively through
   `picohost.proxy.PicoProxy`.** The Pico's command stream is the
   cross-process arbiter for switch state, motor moves, etc. — two
   processes both calling `PicoProxy("rfswitch").send_command(...)`
   get serviced in order by the pico-manager service, so no
   in-process lock is needed.
4. **Call `require_pico(...)` (from `_scripts_util`) before issuing
   any command,** so a missing `pico-manager.service` surfaces as a
   one-line operator-actionable error rather than a silent
   `send_command` no-op.
5. **Use `build_transport_bare(dummy)` (not `build_transport`) when
   the script builds its own dummy producer surface.** The default
   `build_transport(dummy=True)` auto-attaches a
   `DummyPandaClient`, which would double-register dummy picos and
   start a competing heartbeat.

## What bring-up scripts MUST NOT do

- **No heartbeat thread.** `panda:hb*` belongs to the real panda
  process. A duplicate heartbeat confuses `live_status` and the
  ground-side watchdog.
- **No boot-time force-switch.** Don't drive RFANT (or any state)
  at startup; you'll fight whatever the production observer is
  doing.
- **No `ConfigStore.upload(...)`.** The observing config is owned
  by the production observer (`obs_config_owner`). Read parameters
  out of `cfg` in memory if you need them; do not overwrite the
  store from a transient tool.
- **No in-process `coord` lock** (`MotionSwitchCoordinator`,
  per-script `RLock`, etc.). It only serializes against threads in
  your own process, of which there are none. It does not protect
  against the production observer on the panda, nor against a
  sibling bring-up script in another terminal. Cross-process
  arbitration happens at the Pico level (rfswitch command stream)
  or at the operator level (terminal-orchestrated motion before
  VNA, like `vna_sweep`).

## run_tag: active drivers claim it, passive readouts don't

`run_tag` (`eigsep_observing.run_tag`) is the single Redis key naming
which driver owns the panda's physical state right now; it is stamped
into every corr / VNA file header for offline provenance. Bring-up
scripts split into two classes by whether they change that state:

- **Active drivers** — send commands or write VNA files
  (`motor_manual`, `motor_control`, `rfswitch_manual`,
  `tempctrl_manual`, `vna_manual`, `record_vna`). They **claim**
  `run_tag.session(...)`. Because the ground PC is always recording
  corr, the tag flags the spectra captured during a hand-driven
  session as "not autonomous science." `run_tag.session` is
  refuse-on-conflict, which here is a **safety feature**: it refuses to
  start while another driver (the autonomous `panda_observe`, or
  another active tool) owns the state, forcing the operator to stop the
  autonomous driver before hand-driving shared hardware. One active
  driver of the physical state at a time; combined motor+VNA runs use
  the dedicated alt-mode `vna_position_sweep` in
  `src/eigsep_observing/scripts/`.
- **Passive readouts** — `MetadataSnapshotReader`-only, no commands, no
  files (`imu_manual`, `monitor_meta`, `potmon_manual`, `lidar_manual`,
  `pico_preflight`). They **never** claim `run_tag`: they change no
  physical state, have no provenance to record, and must coexist with
  whatever active driver is running. Claiming would block that
  coexistence and misattribute a concurrent driver's files.

`tests/test_obs_config_uploaders.py` enforces the split both ways:
active drivers (plus the autonomous uploaders) must enter
`run_tag.session`; exempt (passive / coexisting) scripts must not.

## Cross-process coordination is YAGNI

If a future workflow genuinely needs "pause the production
observer's switch cycling for 30 s while I capture," that's a
Redis-backed pause flag *on the observer*, not a per-script coord
lock. Don't pre-build it. Today the Pico-level serialization plus
operator terminal-ordering covers the workflows we have in mind
(operator moves motor in terminal A, sees the move complete,
triggers VNA in terminal B).
