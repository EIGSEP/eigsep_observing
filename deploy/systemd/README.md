# EIGSEP systemd units

Two units for the ground-PC side of an EIGSEP deployment:

- `eigsep-observe.service` — runs `scripts/fpga_init.py --reinit -p`,
  the SNAP supervisor. Restarts on hardware failure (e.g. heat-induced
  casperfpga register-read timeout); each restart performs a full
  `--reinit -p` so the SNAP comes back with a fresh `sync_time` and a
  re-uploaded bitstream.
- `eigsep-observe-writer.service` — runs `scripts/observe.py`, the
  corr-data HDF5 file writer. Restarts only on genuine crashes; the
  consumer-side liveness watchdog now logs persistent SNAP silence
  instead of suiciding, so the writer survives ordinary supervisor
  restarts of the SNAP unit.

The panda-side processes (motors, tempctrl, VNA, RF switch) run on a
different machine and are out of scope for these units.

## Install

The unit files assume the canonical layout:

| What        | Where                                  |
| ----------- | -------------------------------------- |
| Repo root   | `/home/eigsep/eigsep_observing`        |
| venv        | `/home/eigsep/eigsep_observing/.venv`  |
| Run-as user | `eigsep` (UID/GID with hardware ACLs)  |

If your layout differs, override the paths via a drop-in:

```bash
sudo systemctl edit eigsep-observe.service
sudo systemctl edit eigsep-observe-writer.service
```

and supply only the keys you need to change, e.g.:

```ini
[Service]
WorkingDirectory=/srv/eigsep_observing
ExecStart=
ExecStart=/srv/eigsep_observing/.venv/bin/python scripts/fpga_init.py --reinit -p
```

(The empty `ExecStart=` resets the inherited value before the new one;
required for `ExecStart=` overrides specifically.)

### Symlink and enable

```bash
sudo ln -s "$(pwd)/deploy/systemd/eigsep-observe.service" \
    /etc/systemd/system/eigsep-observe.service
sudo ln -s "$(pwd)/deploy/systemd/eigsep-observe-writer.service" \
    /etc/systemd/system/eigsep-observe-writer.service
sudo systemctl daemon-reload
sudo systemctl enable --now eigsep-observe.service
sudo systemctl enable --now eigsep-observe-writer.service
```

Verify before enabling:

```bash
sudo systemd-analyze verify deploy/systemd/eigsep-observe.service
sudo systemd-analyze verify deploy/systemd/eigsep-observe-writer.service
```

On a dev box where `/home/eigsep/eigsep_observing/.venv/bin/python`
does not exist, `systemd-analyze` will report
`Command ... is not executable` and exit non-zero. That is expected —
the syntax of the unit is fine, the path just resolves on the target
host. Re-run on the deployment machine (or after a `systemctl edit`
override has rewritten `ExecStart=`) to get a clean exit.

## Operate

### Tail live logs

```bash
journalctl -u eigsep-observe.service -f
journalctl -u eigsep-observe-writer.service -f
```

### Restart manually

```bash
sudo systemctl restart eigsep-observe.service
```

The supervisor passes `--reinit -p` on every start, so a manual
restart cuts the current observing block and starts a fresh one. For
non-destructive interventions (header-only fixes, config tweaks),
stop the unit and run `scripts/fpga_init.py` (no flag) by hand from
the venv to attach without re-syncing.

### Disable while debugging

```bash
sudo systemctl stop eigsep-observe.service
sudo systemctl mask eigsep-observe.service   # prevent auto-start until unmasked
```

## Recovery model

Both units use `Restart=on-failure` with `StartLimitIntervalSec=0` —
unbounded retries with a 30-second backoff. Rationale:

- The deployment is offline (no wifi). Operators are on-site and
  check in a few times per day. Bounded retries that "give up" risk
  losing a whole night of observation when overnight thermal
  cool-down would self-heal.
- The cost of a restart is one Python process startup every 30s —
  negligible CPU/power, no RFI source.
- The visibility signal lives on the live-status dashboard:
  - `Reinits: N (Ts ago)` — bumped by `fpga_init.py` on every
    successful supervised re-init. A high count overnight means
    the SNAP was thermal-cycling.
  - `Last file: Ts ago` — `observe.py` heartbeat. Goes stale when
    the writer or producer is genuinely down for longer than one
    file's worth of integrations.

If the dashboard tiles agree the system is broken, the operator
intervenes manually. If they don't, systemd's restart loop is doing
its job.

## Why `--reinit -p` (not the no-flag attach path)

The observed crash signature is an uncaught casperfpga exception on
a register-read timeout. That means the SNAP went through a boot;
the bitstream is gone, registers are reset, the cached `sync_time`
in Redis is stale. Attaching with a stale `sync_time` would publish
data that downstream consumers tag with the wrong alignment. A full
`--reinit -p` is the honest recovery: fresh ADC alignment, fresh
bitstream upload, fresh `sync_time` per recovery, new file per
observing block.

Manual operator runs of `fpga_init.py` (without `--reinit`) still
attach to existing state. Only the systemd-managed copy passes
`--reinit -p`, and it only fires when the SNAP has already failed.
