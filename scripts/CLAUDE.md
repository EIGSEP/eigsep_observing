# Bring-up scripts contract

The scripts in this directory (`*_manual.py`, `record_*.py`,
`vna_sweep.py`, `vna_position_sweep.py`, `no_switch_observation.py`,
`motor_control.py`, `pico_preflight.py`, etc.) are operator tools
for **field verification, lab bring-up, and ad-hoc debugging**. The
production observing path is in
`src/eigsep_observing/scripts/` (`fpga_init`, `observe`,
`panda_observe`) and is *not* governed by this contract — those have
their own architecture documented in the top-level `CLAUDE.md`.

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

## Cross-process coordination is YAGNI

If a future workflow genuinely needs "pause the production
observer's switch cycling for 30 s while I capture," that's a
Redis-backed pause flag *on the observer*, not a per-script coord
lock. Don't pre-build it. Today the Pico-level serialization plus
operator terminal-ordering covers the workflows we have in mind
(operator moves motor in terminal A, sees the move complete,
triggers VNA in terminal B).
