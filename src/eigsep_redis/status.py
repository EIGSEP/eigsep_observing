import logging

from .keys import STATUS_STREAM

logger = logging.getLogger(__name__)


class StatusWriter:
    """
    Publish status messages onto the status stream.

    Producers call ``send(level, status)`` to emit a human-readable
    status line tagged with a Python logging level. The stream is
    bounded via ``maxlen`` so a dead consumer can't grow it without
    limit. The bound is sized to survive a brief ground-reader
    outage without dropping diverse event types — the durable record
    of every event remains the panda's rotating log file.
    """

    maxlen = 100

    def __init__(self, transport):
        self.transport = transport

    def send(self, status, level=logging.INFO):
        """
        Publish a status message.

        Parameters
        ----------
        status : str
            Status message.
        level : int
            Python logging level.
        """
        self.transport.r.xadd(
            STATUS_STREAM,
            {"level": level, "status": status},
            maxlen=self.maxlen,
        )


class StatusReader:
    """
    Consume status messages from the status stream.

    ``read`` is a blocking XREAD scoped to ``stream:status`` only; it
    cannot be coerced to read any other stream (the stream name is
    hard-coded in :data:`STATUS_STREAM`).
    """

    def __init__(self, transport):
        self.transport = transport

    @property
    def stream(self):
        """``{STATUS_STREAM: last_read_id}`` — view, used for blocking reads."""
        return {STATUS_STREAM: self.transport._get_last_read_id(STATUS_STREAM)}

    def read(self, timeout=None):
        """
        Blocking read of the next status message.

        Parameters
        ----------
        timeout : float or None
            Timeout in seconds. ``None`` blocks indefinitely.

        Returns
        -------
        (level, status) : tuple
            ``(int, str)`` on success, ``(None, None)`` on timeout.
        """
        block_time = 0 if timeout is None else int(timeout * 1000)
        msg = self.transport.r.xread(self.stream, count=1, block=block_time)
        if not msg:
            return None, None
        entry_id, status_dict = msg[0][1][0]
        self.transport._set_last_read_id(STATUS_STREAM, entry_id)
        status = status_dict.get(b"status").decode("utf-8")
        raw_level = status_dict.get(b"level")
        if raw_level is None:
            level = logging.INFO
        else:
            level = int(raw_level.decode("utf-8"))
        return level, status
