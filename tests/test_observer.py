import itertools
import pytest
import time

from eigsep_corr.testing import DummyEigsepFpga
from eigsep_observing.config import default_obs_config
from eigsep_observing.observer import make_schedule, EigObserver
from eigsep_observing.testing import DummyEigsepRedis


def take_schedule(schedule, n):
    """
    Helper function to take the first n elements from a schedule.
    """
    return list(itertools.islice(schedule, n))


def test_make_empty_schedule():
    """
    Test that an empty schedule raises an error.
    """
    with pytest.raises(ValueError):
        make_schedule({})

    # if we only have snap_repeat, but not sky/load/noise, it is still
    # considered an empty schedule
    with pytest.raises(ValueError):
        make_schedule({"snap_repeat": 1})

    # similarly if snap_repeat is 0
    with pytest.raises(ValueError):
        make_schedule({"snap_repeat": 0, "sky": 1, "load": 1, "noise": 1})

    # default snap repeat is 1, so this should not raise an error
    make_schedule({"sky": 1, "load": 1, "noise": 1})


@pytest.mark.parametrize("vna_remaining", [1, 3, 100])
def test_make_vna_schedule(vna_remaining):
    """
    Make a schedule with only VNA observations.
    """
    schedule = make_schedule({"vna": vna_remaining})
    n = 4
    assert take_schedule(schedule, n) == [("vna", vna_remaining)] * n

    # itering over the schedule yields all "vna" independent of vna_remaining
    for _ in range(10):
        mode, remaining = next(schedule)
        assert mode == "vna"
        assert remaining == vna_remaining


def test_invalid_key_schedule():
    """
    Test that an invalid key in the schedule raises an error.
    """
    with pytest.raises(KeyError):
        make_schedule({"invalid_key": 1})


def test_make_schedule():
    switch_schedule = {
        "vna": 1,
        "snap_repeat": 2,
        "sky": 1,
        "load": 1,
        "noise": 1,
    }
    sched = make_schedule(switch_schedule)
    # expected schedule is:
    first_pass = [("vna", 1)] + 2 * [("sky", 1), ("load", 1), ("noise", 1)]
    # grab first two passes from sched
    result = take_schedule(sched, 2 * len(first_pass))
    assert result[: len(first_pass)] == first_pass
    assert result[len(first_pass) :] == first_pass

    # iterate
    expected_modes = ["vna"] + 2 * ["sky", "load", "noise"]
    for i in range(2 * len(expected_modes)):
        mode = next(sched)[0]
        assert mode == expected_modes[i % len(expected_modes)]


# test the EigObserver class
@pytest.fixture
def fpga():
    return DummyEigsepFpga()

@pytest.mark.skip("Fix issues in eigsep_corr")
@pytest.fixture
def obs(fpga):
    eig_obs = EigObserver(fpga, cfg=default_obs_config)
    eig_obs.redis = DummyEigsepRedis()  # replace redis with a dummy
    return eig_obs


@pytest.mark.skip("Fix issues in eigsep_corr")
def test_send_heartbeat(obs):
    """
    Test that the EigObserver sends a heartbeat message.
    """
    assert not obs.redis.is_server_alive()  # initially no heartbeat
    ex = 1  # heartbeat expiration time in seconds
    obs.start_heartbeat(ex=ex)
    assert obs.redis.is_server_alive()
    # stop the heartbeat
    obs.stop_heartbeat_event.set()
    # wait for the heartbeat to stop
    time.sleep(ex + 0.1)
    assert not obs.redis.is_server_alive()  # heartbeat should be stopped


@pytest.mark.skip("Fix issues in eigsep_corr")
def test_start_client(obs):
    """
    Test that the EigObserver starts the client correctly.
    """
    obs.start_client()
    # logic in client.read_init_commands:
    eid, msg = obs.redis.read_ctrl()
    assert eid is not None
    assert msg is not None
    cmd, pico_ids = msg
    assert cmd in obs.redis.init_commands
    switch_pico = pico_ids.pop("switch_pico", None)
    assert switch_pico == obs.cfg.switch_pico
    assert pico_ids == obs.cfg.sensors
