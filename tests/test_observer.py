import itertools
import pytest

from eigsep_observing.config import default_obs_config
from eigsep_observing.observer import make_schedule, EigObserver


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
