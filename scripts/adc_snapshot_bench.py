"""ADC snapshot bench: measure corr-poll latency under contention.

Spins up the real ``EigsepFpga`` corr poll thread (and optionally the
ADC snapshot thread) against a live SNAP, while a timing-instrumented
``_FpgaLockProxy`` records every TAPCP transaction. Compares two
runs — baseline (``--snapshot-period 0``) and loaded (``--snapshot-
period 1.0``) — to verify that enabling the snapshot publisher does
not regress the corr-poll latency past the acceptance bar:

  * Loaded ``corr_acc_cnt`` p99 within ~2x baseline.
  * Loaded corr-thread lock-wait p99 <= 30 ms.
  * Zero ``Missed N integration(s)`` warnings.
  * Zero ``Read of acc_cnt=X FAILED to complete`` errors.
  * Zero ``Disabling adc_*_publisher`` warnings.

Pre-conditions: SNAP is reachable at ``--snap-ip``, has been programmed
and synced (e.g. via ``eigsep-fpga-init --reinit``), and is producing
correlator data. The bench wires Redis I/O through a fakeredis
``DummyTransport`` so the real Redis bus is left alone.
"""

import argparse
import logging
import queue
import sys
import threading
import time
from collections import Counter, deque

import numpy as np

from eigsep_redis.testing import DummyTransport

from eigsep_observing.fpga import (
    EigsepFpga,
    _FpgaLockProxy,
    default_config,
    default_wiring,
)
from eigsep_observing.utils import configure_eig_logger


class _TimingFpgaLockProxy(_FpgaLockProxy):
    """Lock proxy that appends ``(thread, method, reg_or_arg, t_request,
    t_acquire, t_complete)`` to a thread-safe ``deque`` per call.

    ``t_request`` is captured before the lock acquire so
    ``t_acquire - t_request`` measures contention; ``t_complete -
    t_acquire`` is the TAPCP-only walltime.
    """

    def __init__(self, fpga, lock, collector):
        super().__init__(fpga, lock)
        self._collector = collector

    def __getattr__(self, name):
        attr = getattr(self._fpga, name)
        if not callable(attr):
            return attr
        lock = self._lock
        collector = self._collector

        def wrapped(*args, **kwargs):
            reg = args[0] if args else ""
            t_request = time.perf_counter()
            with lock:
                t_acquire = time.perf_counter()
                try:
                    return attr(*args, **kwargs)
                finally:
                    collector.append(
                        (
                            threading.current_thread().name,
                            name,
                            str(reg),
                            t_request,
                            t_acquire,
                            time.perf_counter(),
                        )
                    )

        return wrapped


class _BenchFpga(EigsepFpga):
    """``EigsepFpga`` whose lock proxy records timings."""

    def __init__(self, *args, collector, **kwargs):
        # Set before super().__init__ so _wrap_fpga can see it.
        self._bench_collector = collector
        super().__init__(*args, **kwargs)

    def _wrap_fpga(self, raw):
        return _TimingFpgaLockProxy(
            raw, self._fpga_lock, self._bench_collector
        )


class _LogCounter(logging.Handler):
    """Logging handler that counts records by message-prefix substring.

    The acceptance criteria look for specific phrases emitted by
    ``_read_integrations`` and the f177dff sticky-disable path, so
    a substring match is sufficient.
    """

    PHRASES = (
        "Missed",
        "FAILED to complete",
        "Disabling adc_stats publisher",
        "Disabling adc_snapshot publisher",
    )

    def __init__(self):
        super().__init__()
        self.counts = Counter()

    def emit(self, record):
        msg = record.getMessage()
        for phrase in self.PHRASES:
            if phrase in msg:
                self.counts[phrase] += 1


def _percentiles_ms(values_s):
    if not values_s:
        return {f"p{p}": float("nan") for p in (50, 95, 99, 100)}
    arr = np.asarray(values_s) * 1000.0
    return {f"p{p}": float(np.percentile(arr, p)) for p in (50, 95, 99, 100)}


def _summarize(rows, label, log_counts):
    print(f"\n=== {label} ===")
    print(f"total tapcp ops: {len(rows)}")

    corr_polls = [
        r for r in rows if r[1] == "read_int" and r[2] == "corr_acc_cnt"
    ]
    snap_reads = [
        r for r in rows if r[1] == "read" and r[2] == "input_snapshot_bram"
    ]

    def report(name, rs):
        if not rs:
            print(f"  {name}: 0 calls")
            return
        latency = [r[5] - r[3] for r in rs]
        wait = [r[4] - r[3] for r in rs]
        txn = [r[5] - r[4] for r in rs]
        latency_pct = _percentiles_ms(latency)
        wait_pct = _percentiles_ms(wait)
        txn_pct = _percentiles_ms(txn)
        print(
            f"  {name}: count={len(rs)} "
            f"latency_ms_p50/p95/p99/max="
            f"{latency_pct['p50']:.2f}/"
            f"{latency_pct['p95']:.2f}/"
            f"{latency_pct['p99']:.2f}/"
            f"{latency_pct['p100']:.2f} "
            f"wait_ms_p50/p95/p99/max="
            f"{wait_pct['p50']:.2f}/"
            f"{wait_pct['p95']:.2f}/"
            f"{wait_pct['p99']:.2f}/"
            f"{wait_pct['p100']:.2f} "
            f"txn_ms_p50/p95/p99/max="
            f"{txn_pct['p50']:.2f}/"
            f"{txn_pct['p95']:.2f}/"
            f"{txn_pct['p99']:.2f}/"
            f"{txn_pct['p100']:.2f}"
        )

    report("corr_acc_cnt polls", corr_polls)
    report("snapshot bram reads", snap_reads)
    print("log warnings:")
    for phrase in _LogCounter.PHRASES:
        print(f"  {phrase!r}: {log_counts.get(phrase, 0)}")


def _build_parser():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--snap-ip",
        required=True,
        help="IP of the SNAP correlator board (already programmed/synced).",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=600.0,
        help="Seconds to run before stopping the corr/snapshot threads.",
    )
    p.add_argument(
        "--snapshot-period",
        type=float,
        default=0.0,
        help=(
            "adc_snapshot_period_s in seconds. 0 disables the snapshot "
            "thread (baseline run); typical loaded value is 1.0."
        ),
    )
    p.add_argument(
        "--out",
        default="adc_snapshot_bench.npz",
        help="npz path for raw timing rows (one row per TAPCP op).",
    )
    return p


def main():
    configure_eig_logger(level=logging.INFO)
    args = _build_parser().parse_args()

    # Capture eigsep_observing's logger so we count warnings emitted
    # from inside the fpga module.
    log_counter = _LogCounter()
    eig_logger = logging.getLogger("eigsep_observing")
    eig_logger.addHandler(log_counter)

    collector = deque()
    cfg = dict(default_config)
    cfg["snap_ip"] = args.snap_ip
    cfg["adc_snapshot_period_s"] = args.snapshot_period

    fpga = _BenchFpga(
        cfg=cfg,
        wiring=default_wiring,
        transport=DummyTransport(),
        program=False,
        collector=collector,
    )

    # The corr loop is gated on is_synchronized for the stats publisher
    # branch; assume the SNAP was synced by eigsep-fpga-init and stamp
    # the cached value. sync_time only needs to be present, not exact —
    # it's not used in the timing measurement itself.
    fpga.is_synchronized = True
    fpga.sync_time = time.time()

    # Drive _read_integrations and (optionally) _publish_snapshots_loop
    # directly. We intentionally skip ``observe()``'s upload_config /
    # CorrWriter dance — the bench is measuring FPGA contention, not
    # the file-writer pipeline.
    fpga.queue = queue.Queue(maxsize=0)
    fpga.event = threading.Event()

    poll_thd = threading.Thread(
        target=fpga._read_integrations,
        args=(["0"],),
        # Larger than --duration so the loop doesn't time out before we
        # set the stop event.
        kwargs={"timeout": args.duration + 60},
        name="corr-poll",
    )
    poll_thd.start()

    snap_thd = None
    if args.snapshot_period > 0:
        snap_thd = threading.Thread(
            target=fpga._publish_snapshots_loop,
            args=(args.snapshot_period,),
            daemon=True,
            name="adc-snap",
        )
        snap_thd.start()

    # Drain the queue in a sidecar so the corr thread isn't backpressured
    # waiting on an unbounded queue.
    drain_stop = threading.Event()

    def _drain():
        while not drain_stop.is_set():
            try:
                fpga.queue.get(timeout=0.5)
            except Exception as e:
                eig_logger.warning("Exception while draining queue: %s", e)
                continue

    drain_thd = threading.Thread(target=_drain, daemon=True, name="drain")
    drain_thd.start()

    eig_logger.info(
        "bench started: duration=%.1fs snapshot_period=%.2fs",
        args.duration,
        args.snapshot_period,
    )
    time.sleep(args.duration)

    fpga.event.set()
    poll_thd.join(timeout=30)
    if snap_thd is not None:
        snap_thd.join(timeout=30)
    drain_stop.set()
    drain_thd.join(timeout=5)

    rows = list(collector)
    if not rows:
        print("WARNING: no timing rows recorded; check FPGA reachability")
        return 1

    label = f"snapshot_period={args.snapshot_period}"
    _summarize(rows, label, log_counter.counts)

    threads = np.array([r[0] for r in rows])
    methods = np.array([r[1] for r in rows])
    regs = np.array([r[2] for r in rows])
    t_request = np.asarray([r[3] for r in rows])
    t_acquire = np.asarray([r[4] for r in rows])
    t_complete = np.asarray([r[5] for r in rows])
    log_warnings = {
        p: log_counter.counts.get(p, 0) for p in _LogCounter.PHRASES
    }
    np.savez(
        args.out,
        threads=threads,
        methods=methods,
        regs=regs,
        t_request=t_request,
        t_acquire=t_acquire,
        t_complete=t_complete,
        snapshot_period=args.snapshot_period,
        duration=args.duration,
        log_warnings=log_warnings,
    )
    print(f"\nWrote {len(rows)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
