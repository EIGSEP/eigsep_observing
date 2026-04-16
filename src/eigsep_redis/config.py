import json

from .keys import CONFIG_KEY


class ConfigStore:
    """
    Persistent single-key store for the panda-side configuration.

    ``upload`` serializes the config (plus an ``upload_time``) under a
    well-known Redis key; ``get`` reads it back. This is the generic
    panda config — the SNAP-side correlator config lives in a separate
    store in ``eigsep_observing``.

    YAML file loading is the caller's responsibility. Keeping the store
    dict-only means it has no dependency on ``eigsep_observing.utils``
    and no divergence in how the two stores parse files.
    """

    def __init__(self, transport):
        self.transport = transport

    def upload(self, config):
        """
        Upload the panda configuration to Redis.

        Parameters
        ----------
        config : dict
        """
        self.transport._upload_dict(config, CONFIG_KEY)

    def get(self):
        """
        Return the panda configuration.

        Raises
        ------
        ValueError
            If no configuration is present.
        """
        raw = self.transport.get_raw(CONFIG_KEY)
        if raw is None:
            raise ValueError("No configuration found in Redis.")
        return json.loads(raw)
