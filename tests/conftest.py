"""Shared test fixtures for the eigsep_observing test suite.

The golden data constants (``HEADER``, ``IMU_READING``, ``CORR_METADATA``,
``VNA_METADATA``, ``S11_HEADER``, ``NTIMES``, ``ERROR_INTEGRATION_INDEX``)
now live in ``eigsep_observing._test_fixtures`` — under ``src/`` rather
than here — so the producer-contract tests under
``eigsep_observing.contract_tests`` can import them from the installed
wheel. They are re-exported from this module so existing
``from conftest import HEADER`` imports in ``test_io.py`` keep working
unchanged; pytest's default ``importmode=prepend`` puts the tests
directory on ``sys.path`` during collection, which is what makes that
import pattern work.

This file retains only the ``@pytest.fixture`` definitions — these stay
here because they depend on pytest's fixture-injection machinery and
are not useful outside a test run.
"""

import pytest
import yaml

from eigsep_redis.testing import DummyTransport

import eigsep_observing
from eigsep_observing._test_fixtures import (  # noqa: F401 (re-exported)
    CORR_METADATA,
    ERROR_INTEGRATION_INDEX,
    FILE_TIME,
    HEADER,
    IMU_READING,
    INTEGRATION_TIME,
    NTIMES,
    S11_HEADER,
    VNA_METADATA,
)
from eigsep_observing.testing import DummyPandaClient


# ---------------------------------------------------------------------
# Dummy-transport / dummy-client fixtures.
#
# Previously private to test_client.py; promoted here so tests that
# exercise other dummy-manager-backed subjects (MotorScanner,
# MotorZeroer, ...) can reuse the same in-process PicoManager setup.
# ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def module_tmpdir(tmp_path_factory):
    """Module-scoped temp dir for VNA save paths and similar."""
    return tmp_path_factory.mktemp("module_tmpdir")


@pytest.fixture()
def dummy_cfg(module_tmpdir):
    path = eigsep_observing.utils.get_config_path("dummy_config.yaml")
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg["vna_save_dir"] = str(module_tmpdir)
    return cfg


@pytest.fixture
def transport():
    return DummyTransport()


@pytest.fixture
def client(transport, dummy_cfg):
    c = DummyPandaClient(transport, default_cfg=dummy_cfg)
    yield c
    c.stop()
