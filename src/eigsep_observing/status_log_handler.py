import logging
import logging.handlers
import queue

from eigsep_redis import StatusWriter

GROUND_LOGGER_ROOT = "eigsep_observing"
AGGREGATOR_LOGGER_PREFIX = "eigsep_observing.live_status"
PANDA_RELAY_LOGGER = "eigsep_observing.observer.panda_relay"

# Handler-internal failures (queue full, XADD raised) are logged via
# this logger — deliberately *outside* the ``eigsep_observing``
# hierarchy so :class:`_StatusStreamFilter` cannot re-queue them and
# form a feedback loop. Records propagate to the root logger; under
# systemd that means journald via stderr, which is the durable signal
# path (the panda transport is the dashboard path, not the source of
# truth). CLAUDE.md priority-2: "log loudly at ERROR level" for
# safety-net failures — this logger is that signal.
_handler_logger = logging.getLogger("eigsep_status_handler_errors")

_QUEUE_MAXSIZE = 1024
_SHUTDOWN_TIMEOUT_S = 2.0


def _is_under(name, root):
    """Strict logger-hierarchy membership.

    ``name == root or name.startswith(root + ".")`` — rejects sibling
    roots like ``eigsep_observing_foo`` that bare ``startswith`` would
    falsely match.
    """
    return name == root or name.startswith(root + ".")


class _StatusStreamFilter(logging.Filter):
    """Logger-name exclusions for the panda status stream.

    Exclude aggregator-owned loggers (``eigsep_observing.live_status.*``):
    they run in the dashboard process, are redundant with the operator's
    live_status terminal, and would form a self-feeding loop
    (aggregator-error → status stream → aggregator read → re-error).

    Exclude ``PANDA_RELAY_LOGGER``: the dedicated child logger
    ``EigObserver.status_logger`` uses to re-emit panda status messages
    locally. Without this, every panda ERROR would be re-published to
    the panda's own stream and immediately re-read by the observer,
    looping forever.
    """

    def filter(self, record):
        name = record.name
        if not _is_under(name, GROUND_LOGGER_ROOT):
            return False
        if _is_under(name, AGGREGATOR_LOGGER_PREFIX):
            return False
        if name == PANDA_RELAY_LOGGER:
            return False
        return True


class _StatusStreamEmitter(logging.Handler):
    """Inner handler that runs on the ``QueueListener`` thread.

    Owns the ``StatusWriter`` and performs the synchronous ``XADD`` —
    keeping that off the caller's thread is the whole point of the
    queue. Level filter is ``NOTSET`` here because the outer
    ``QueueHandler`` already enforces ``ERROR``; lowering the gate
    twice would mask bugs.
    """

    def __init__(self, transport_panda):
        super().__init__(level=logging.NOTSET)
        self._status = StatusWriter(transport_panda)

    def emit(self, record):
        try:
            # ``QueueHandler.prepare`` already ran the formatter and
            # baked exception info into ``record.msg``; ``getMessage``
            # is the formatted string at this point.
            msg = f"[{record.name}] {record.getMessage()}"
            self._status.send(msg, level=record.levelno)
        except Exception:
            try:
                _handler_logger.error(
                    "StatusStreamHandler listener failed to publish "
                    "status record onto panda stream "
                    "(record=%r); record dropped",
                    record.getMessage(),
                    exc_info=True,
                )
            except Exception:
                # Logging system itself is broken (or
                # ``raiseExceptions`` is off and ``_handler_logger``
                # has no handlers); fall back to stderr trace via the
                # Handler default so we don't lose the signal entirely.
                self.handleError(record)


class StatusStreamHandler(logging.handlers.QueueHandler):
    """Mirror ground-side ERROR records onto the panda status stream.

    The live-status dashboard drains a ``StatusReader`` on the panda
    transport (see ``live_status/aggregator.py``), so ground-side
    faults are otherwise invisible from the field deployment (no
    Slack/email fallback). This handler is the symmetric counterpart
    to panda's ``PandaClient._log_with_status`` — every
    ``logger.error`` inside the ``eigsep_observing`` hierarchy is
    auto-mirrored.

    Caller-thread ``emit()`` is a non-blocking ``queue.put_nowait``
    inherited from :class:`logging.handlers.QueueHandler`; the actual
    ``XADD`` runs on a background :class:`logging.handlers.QueueListener`
    thread. This matters because ``eigsep_redis.Transport`` constructs
    its Redis client with ``socket_timeout=None`` — a half-open TCP to
    panda on a synchronous ``XADD`` blocks the calling thread
    indefinitely. The handler is attached to the ``eigsep_observing``
    root logger, which is shared by the corr-recording path
    (``record_corr_data`` emits ERRORs on header-fetch failure,
    ``sync_time=0``, corr-read timeouts). The CLAUDE.md priority-1
    rule — "corr data is sacred. Under no circumstances should
    failures in any other data product prevent corr data from being
    saved" — requires that mirroring cannot stall corr writes. The
    queue is that guarantee.

    Bounded queue (``maxsize=_QUEUE_MAXSIZE``): if panda is
    unreachable and the listener thread parks on a hung ``XADD``,
    subsequent enqueues raise ``queue.Full`` and the overridden
    :meth:`emit` routes them through :data:`_handler_logger` at ERROR
    level (with ``handleError``'s stderr trace as a last-resort
    fallback if logging itself raises). The CLAUDE.md priority-2 rule
    requires safety-net failures to log loudly at ERROR so the
    upstream contract violation is visible; the dedicated logger sits
    outside the ``eigsep_observing`` hierarchy specifically so its
    record cannot re-enter this handler's filter and loop. Records
    dropped under that condition remain durable in the local rotating
    log file — the panda is the dashboard transport, not the source
    of truth.

    Level filter is ``ERROR`` (on the outer QueueHandler). WARNING-
    level events in this codebase include sites that can fire at ~4 Hz
    corr cadence (missed integrations, averaging fallbacks), which
    would blow ``StatusWriter.maxlen``. Promote a specific WARNING to
    ERROR at its call site if you need it on the dashboard.

    Two logger-name exclusions live on :class:`_StatusStreamFilter`.

    The ``StatusWriter`` is constructed and held *inside* the inner
    emitter rather than being passed in as an attribute on the outer
    handler. This keeps the writer encapsulated behind the log-handler
    subsystem so it never lands on ``EigObserver.__dict__``,
    preserving the consumer-role invariant documented in
    ``CLAUDE.md`` (``EigObserver`` has no writer surfaces). The only
    path from observer code to the panda status stream is
    ``logger.error(...)`` → handler → queue → listener → ``XADD``.
    """

    def __init__(self, transport_panda, queue_maxsize=_QUEUE_MAXSIZE):
        q = queue.Queue(maxsize=queue_maxsize)
        super().__init__(q)
        self.setLevel(logging.ERROR)
        self.addFilter(_StatusStreamFilter())
        self._emitter = _StatusStreamEmitter(transport_panda)
        self._listener = logging.handlers.QueueListener(q, self._emitter)
        self._listener.start()

    def emit(self, record):
        try:
            self.enqueue(self.prepare(record))
        except Exception:
            try:
                _handler_logger.error(
                    "StatusStreamHandler failed to enqueue record "
                    "(queue full or prepare failed); record dropped",
                    exc_info=True,
                )
            except Exception:
                # Fall back to stderr trace if logging itself failed —
                # see ``_StatusStreamEmitter.emit`` for the rationale.
                self.handleError(record)

    def flush(self):
        """Block until the listener has processed all queued records.

        ``QueueListener._monitor`` calls ``queue.task_done()`` after
        each ``handle()``, so ``queue.join()`` is the deterministic
        flush primitive. Tests call this between ``logger.error(...)``
        and reading the status stream to remove the race.
        """
        self.queue.join()

    def close(self):
        """Drain pending records (best-effort) and stop the listener.

        Uses a bounded thread-join so a hung panda socket cannot keep
        ``close()`` from returning. The listener thread is a daemon
        (``QueueListener.start`` sets ``daemon=True``), so a stalled
        worker doesn't hold up process exit either way. Records still
        pending behind the sentinel when the timeout expires are lost
        on the dashboard side; the rotating local log file is the
        durable record.
        """
        try:
            self._listener.enqueue_sentinel()
            thread = self._listener._thread
            if thread is not None:
                thread.join(timeout=_SHUTDOWN_TIMEOUT_S)
        finally:
            try:
                self._emitter.close()
            finally:
                super().close()
