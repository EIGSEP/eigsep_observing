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

from eigsep_redis.testing import DummyTransport

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
# exercise other dummy-manager-backed subjects (MotorClient,
# MotorZeroer, ...) can reuse the same in-process PicoManager setup.
# ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def module_tmpdir(tmp_path_factory):
    """Module-scoped temp dir for VNA save paths and similar."""
    return tmp_path_factory.mktemp("module_tmpdir")


@pytest.fixture()
def dummy_cfg(module_tmpdir):
    return {
        "rpi_ip": "localhost",
        "panda_ip": "localhost",
        "corr_save_dir": str(module_tmpdir),
        "corr_ntimes": NTIMES,
        "use_switches": True,
        "switch_schedule": {"RFANT": 0, "RFNOFF": 20, "RFNON": 20},
        "use_vna": False,
        "vna_interval": 10,
        "vna_ip": "127.0.0.1",
        "vna_port": 5025,
        "vna_timeout": 1,
        "vna_settings": {
            "fstart": 1e6,
            "fstop": 250e6,
            "npoints": 1000,
            "ifbw": 100.0,
            "power_dBm": {"ant": 0.0, "rec": -40.0},
        },
        "vna_save_dir": str(module_tmpdir),
        "use_motor": False,
        "motor_interval": 1,
        "motor_failure_retry_s": 0.5,
        "motor_scan": {},
        "serialize_motion_and_switching": False,
        "use_tempctrl": True,
        "tempctrl_interval": 1,
        "tempctrl_settings": {
            "watchdog_timeout_ms": 30000,
            "LNA": {
                "enable": True,
                "target_C": 25.0,
                "hysteresis_C": 0.5,
                "clamp": 0.6,
            },
            "LOAD": {
                "enable": True,
                "target_C": 25.0,
                "hysteresis_C": 0.5,
                "clamp": 0.6,
            },
        },
    }


@pytest.fixture
def transport():
    return DummyTransport()


@pytest.fixture
def client(transport, dummy_cfg):
    c = DummyPandaClient(transport, default_cfg=dummy_cfg)
    yield c
    c.stop()
