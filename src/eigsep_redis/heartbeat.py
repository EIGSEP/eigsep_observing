from .keys import HEARTBEAT_KEY


class HeartbeatWriter:
    """
    Set/clear the client-liveness heartbeat.

    The panda-side ``PandaClient`` calls ``set`` periodically (with a
    short ``ex`` TTL) to prove it's still running; it calls
    ``set(alive=False)`` on graceful shutdown. The ground-side observer
    reads the heartbeat via :class:`HeartbeatReader.check`.
    """

    def __init__(self, transport):
        self.transport = transport

    def set(self, ex=None, alive=True):
        """
        Publish a heartbeat tick.

        Parameters
        ----------
        ex : int or None
            Optional TTL in seconds. Typical pattern: set with
            ``ex=60`` on a ~1 Hz cadence so a crashed client is
            detected within 60s.
        alive : bool
            ``True`` to mark the client alive, ``False`` to mark it
            down (shutdown).
        """
        self.transport.add_raw(HEARTBEAT_KEY, int(alive), ex=ex)


class HeartbeatReader:
    """Read-only view of the client-liveness heartbeat."""

    def __init__(self, transport):
        self.transport = transport

    def check(self):
        """
        Return ``True`` if the client is alive, ``False`` otherwise.

        A missing key (TTL expired, never set) returns ``False``.
        """
        raw = self.transport.get_raw(HEARTBEAT_KEY)
        if raw is None:
            return False
        return int(raw) == 1
