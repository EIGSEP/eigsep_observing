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
    redis-server --port "${REDIS_PORT}" --daemonize yes
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

# --- Shutdown handling ---------------------------------------------------
# Observer first so its HDF5 file flushes cleanly, then FPGA, then panda.
cleanup() {
    echo ""
    echo "Shutting down fake observation pipeline..."
    kill -TERM "${OBS_PID}" 2>/dev/null || true
    wait "${OBS_PID}" 2>/dev/null || true
    kill -TERM "${FPGA_PID}" 2>/dev/null || true
    wait "${FPGA_PID}" 2>/dev/null || true
    kill -TERM "${PANDA_PID}" 2>/dev/null || true
    wait "${PANDA_PID}" 2>/dev/null || true
    echo "All processes stopped. Redis daemon still running on :${REDIS_PORT}."
    echo "Stop it with: redis-cli -p ${REDIS_PORT} shutdown"
}
trap cleanup INT TERM

# Block until any child exits or we get a signal.
wait -n "${PANDA_PID}" "${FPGA_PID}" "${OBS_PID}" || true
cleanup
