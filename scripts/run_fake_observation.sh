#!/usr/bin/env bash
# run_fake_observation.sh
#
# Dev-only harness: launches an end-to-end fake EIGSEP observation
# pipeline locally (panda_observe --dummy, fpga_init --dummy --reinit,
# observe --dummy) with all three processes sharing a single Redis on
# localhost:6380. Intended for dogfooding the live-status dashboard
# against a running fake pipeline. NOT for production.
#
# Starts redis-server on :6380 in daemon mode if nothing is listening.
# The daemon persists after this script exits; stop it with:
#     redis-cli -p 6380 shutdown

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
REDIS_PORT=6380

mkdir -p "${LOG_DIR}"

# --- Redis ---------------------------------------------------------------
if redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
    echo "Redis already listening on :${REDIS_PORT}"
else
    echo "Starting redis-server on :${REDIS_PORT} (daemonized)"
    # Disable persistence: this is a dev harness, no snapshots or AOF
    # files should land in the repo root.
    redis-server --port "${REDIS_PORT}" --daemonize yes \
        --save "" --appendonly no
    # Give the daemon a moment to bind the port.
    sleep 1
    if ! redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
        echo "ERROR: redis-server failed to start on :${REDIS_PORT}" >&2
        exit 1
    fi
fi

# --- Process launches ----------------------------------------------------
# Order matters: panda uploads obs_config that observe.py waits on,
# then fpga_init uploads corr_config + header, then observe.py attaches.

PANDA_LOG="${LOG_DIR}/panda_observe.log"
FPGA_LOG="${LOG_DIR}/fpga_init.log"
OBS_LOG="${LOG_DIR}/observe.log"

# --- Shutdown handling ---------------------------------------------------
# Install trap BEFORE launching children so a Ctrl-C, kill, SSH
# disconnect (HUP), or set -e abort during the startup window cannot
# leave Python children orphaned. PIDs start empty; `${VAR:-}` guards
# in cleanup handle the pre-launch window. Trap disarms itself on
# entry so re-entry from EXIT after INT/TERM/HUP is a no-op.
# Order: observer first so its HDF5 file flushes, then FPGA, then panda.
PANDA_PID=""
FPGA_PID=""
OBS_PID=""

cleanup() {
    trap - INT TERM HUP EXIT
    echo ""
    echo "Shutting down fake observation pipeline..."
    kill -TERM "${OBS_PID:-}" 2>/dev/null || true
    wait "${OBS_PID:-}" 2>/dev/null || true
    kill -TERM "${FPGA_PID:-}" 2>/dev/null || true
    wait "${FPGA_PID:-}" 2>/dev/null || true
    kill -TERM "${PANDA_PID:-}" 2>/dev/null || true
    wait "${PANDA_PID:-}" 2>/dev/null || true
    echo "All processes stopped. Redis daemon still running on :${REDIS_PORT}."
    echo "Stop it with: redis-cli -p ${REDIS_PORT} shutdown"
}
trap cleanup INT TERM HUP EXIT

echo "Launching panda_observe --dummy -> ${PANDA_LOG}"
python "${SCRIPT_DIR}/panda_observe.py" --dummy \
    >"${PANDA_LOG}" 2>&1 &
PANDA_PID=$!

sleep 2

echo "Launching fpga_init --dummy --reinit -> ${FPGA_LOG}"
python "${SCRIPT_DIR}/fpga_init.py" --dummy --reinit \
    >"${FPGA_LOG}" 2>&1 &
FPGA_PID=$!

sleep 2

echo "Launching observe --dummy -> ${OBS_LOG}"
python "${SCRIPT_DIR}/observe.py" --dummy \
    >"${OBS_LOG}" 2>&1 &
OBS_PID=$!

echo ""
echo "================================================================"
echo "  Fake observation pipeline running:"
echo "    panda_observe.py    PID ${PANDA_PID}"
echo "    fpga_init.py        PID ${FPGA_PID}"
echo "    observe.py          PID ${OBS_PID}"
echo "  Logs in ${LOG_DIR}/"
echo "  Press Ctrl-C to stop (observer is flushed first)."
echo "================================================================"

# Block until any child exits or we get a signal. The EXIT trap
# will run cleanup on fall-through.
wait -n "${PANDA_PID}" "${FPGA_PID}" "${OBS_PID}" || true
