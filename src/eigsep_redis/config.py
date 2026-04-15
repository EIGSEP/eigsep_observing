import json

import yaml

from .keys import CONFIG_KEY


class ConfigStore:
    """
    Persistent single-key store for the panda-side YAML configuration.

    ``upload`` serializes the config (plus an ``upload_time``) under a
    well-known Redis key; ``get`` reads it back. This is the generic
    panda config — the SNAP-side correlator config lives in a separate
    store in ``eigsep_observing``.
    """

    def __init__(self, transport):
        self.transport = transport

    def upload(self, config, from_file=True):
        """
        Upload the panda configuration to Redis.

        Parameters
        ----------
        config : str or dict
            Path to a YAML file if ``from_file`` is True, else a dict.
        from_file : bool
        """
        if from_file:
            with open(config, "r") as f:
                config = yaml.safe_load(f)
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
