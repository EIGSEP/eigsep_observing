"""Cross-package Redis-key uniqueness guard.

Every Redis key/stream/set this repo writes to lives in
``eigsep_redis.keys`` or ``eigsep_observing.keys``. Two constants
resolving to the same string would silently cross buses (e.g. a
status writer stomping on a metadata stream). This test asserts that
never happens.

If this test fails, pick a new name — do not "fix" it by deleting one
of the duplicates. The right fix depends on which key is actually in
use at runtime.

This module ships under ``src/`` (alongside the producer-contract
suite) rather than ``tests/`` so that ``eigsep-field verify`` can run
it on wheel-only installs via
``pytest --pyargs eigsep_observing.contract_tests``. The uniqueness
check also catches mixed-version pins that CI's pre-release gate
cannot see (e.g. an on-Pi hand upgrade of ``eigsep_redis``).
"""

from eigsep_observing import keys as obs_keys
from eigsep_redis import keys as redis_keys


def _public_string_constants(mod):
    return {
        name: value
        for name, value in vars(mod).items()
        if name.isupper()
        and not name.startswith("_")
        and isinstance(value, str)
    }


def test_redis_keys_module_unique():
    consts = _public_string_constants(redis_keys)
    values = list(consts.values())
    assert len(values) == len(set(values)), (
        f"duplicate values in eigsep_redis.keys: {consts}"
    )


def test_observing_keys_module_unique():
    consts = _public_string_constants(obs_keys)
    values = list(consts.values())
    assert len(values) == len(set(values)), (
        f"duplicate values in eigsep_observing.keys: {consts}"
    )


def test_no_cross_package_collisions():
    redis_consts = _public_string_constants(redis_keys)
    obs_consts = _public_string_constants(obs_keys)
    overlap = set(redis_consts.values()) & set(obs_consts.values())
    assert not overlap, (
        f"keys defined in both packages collide: {overlap}. "
        "Rename one side or move the constant to eigsep_redis.keys."
    )
