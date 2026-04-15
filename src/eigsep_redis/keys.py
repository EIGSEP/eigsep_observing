"""
Central registry of Redis keys owned by ``eigsep_redis``.

Every key/stream/set constant touched by this package lives here so
that collisions are visible at import time and new names can be
audited in one place. Observer-side keys (corr, vna) live in
``eigsep_observing.keys``; both modules are checked for cross-package
uniqueness by ``tests/test_key_uniqueness.py``.
"""

METADATA_HASH = "metadata"
METADATA_STREAMS_SET = "metadata_streams"
DATA_STREAMS_SET = "data_streams"
STATUS_STREAM = "stream:status"
CONFIG_KEY = "config"
HEARTBEAT_KEY = "heartbeat:client"
