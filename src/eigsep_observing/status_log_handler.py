import logging

from eigsep_redis import StatusWriter

GROUND_LOGGER_ROOT = "eigsep_observing"
AGGREGATOR_LOGGER_PREFIX = "eigsep_observing.live_status"
PANDA_RELAY_LOGGER = "eigsep_observing.observer.panda_relay"


class StatusStreamHandler(logging.Handler):
    """Mirror ground-side ERROR records onto the panda status stream.

    The live-status dashboard drains a ``StatusReader`` on the panda
    transport (see ``live_status/aggregator.py``), so ground-side
    faults are otherwise invisible from the field deployment (no
    Slack/email fallback). This handler is the symmetric counterpart
    to panda's ``PandaClient._log_with_status`` — every ``logger.error``
    inside the ``eigsep_observing`` hierarchy is auto-mirrored.

    Two exclusions:

    - Aggregator-owned loggers (``eigsep_observing.live_status.*``)
      run in the same process as the dashboard; their errors are
      redundant with the operator's live_status terminal and would
      also create a circular path (aggregator-error → status stream
      → aggregator read → re-error).
    - ``PANDA_RELAY_LOGGER`` is the dedicated child logger
      ``EigObserver.status_logger`` uses to re-emit panda status
      messages locally. Without this exclusion every panda ERROR
      would be re-published to the panda's own stream and immediately
      re-read by the observer, looping forever.

    Level filter is ``ERROR``. WARNING-level events in this codebase
    include sites that can fire at ~4 Hz corr cadence (missed
    integrations, averaging fallbacks), which would blow
    ``StatusWriter.maxlen``. Promote a specific WARNING to ERROR at
    its call site if you need it on the dashboard.

    ``emit`` must never raise (Python logging contract). A
    ``transport_panda`` outage delegates to ``self.handleError`` —
    stderr trace, no propagation — so a dead panda cannot break
    observer logging or stall corr writes.

    The ``StatusWriter`` is constructed and held *inside* the handler
    rather than being passed in. This keeps the writer encapsulated
    behind the log-handler subsystem so it never lands on
    ``EigObserver.__dict__``, preserving the consumer-role invariant
    documented in ``CLAUDE.md`` (``EigObserver`` has no writer
    surfaces). The only path from observer code to the panda status
    stream is ``logger.error(...)`` → handler.
    """

    def __init__(self, transport_panda):
        super().__init__(level=logging.ERROR)
        self._status = StatusWriter(transport_panda)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            name = record.name
            if not name.startswith(GROUND_LOGGER_ROOT):
                return
            if name.startswith(AGGREGATOR_LOGGER_PREFIX):
                return
            if name == PANDA_RELAY_LOGGER:
                return
            formatted = self.format(record)
            msg = f"[{name}] {formatted}"
            self._status.send(msg, level=record.levelno)
        except Exception:
            self.handleError(record)
