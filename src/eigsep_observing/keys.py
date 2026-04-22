"""
Central registry of Redis keys owned by ``eigsep_observing``.

Complements ``eigsep_redis.keys`` — this module holds observer-side
keys (corr, vna) that don't belong in the shared lower-level package.
Cross-package uniqueness is enforced by
``src/eigsep_observing/contract_tests/test_key_uniqueness.py``.
"""

CORR_STREAM = "stream:corr"
CORR_CONFIG_KEY = "corr_config"
CORR_HEADER_KEY = "corr_header"
CORR_PAIRS_SET = "corr_pairs"
VNA_STREAM = "stream:vna"
