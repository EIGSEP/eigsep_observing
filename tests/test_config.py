from eigsep_observing.config import CorrConfig, ObsConfig


def test_inttime():
    # defaults
    sample_rate = 500
    corr_acc_len = 2**26
    acc_bins = 2
    ntimes = 240
    cfg = CorrConfig(
        sample_rate=sample_rate,
        corr_acc_len=corr_acc_len,
        acc_bins=acc_bins,
        ntimes=ntimes,
    )
    t0 = cfg.inttime
    assert cfg.file_time == ntimes * cfg.inttime
    # double sample rate
    cfg.sample_rate = 2 * sample_rate
    assert cfg.inttime == t0 / 2
    assert cfg.file_time == ntimes * cfg.inttime
    # change corr_acc_len
    cfg.sample_rate = sample_rate
    cfg.corr_acc_len = 4 * corr_acc_len
    assert cfg.inttime == t0 * 4
    assert cfg.file_time == ntimes * cfg.inttime
    # change acc_bins
    cfg.corr_acc_len = corr_acc_len
    cfg.acc_bins = 1
    assert cfg.inttime == t0 / 2
    assert cfg.file_time == ntimes * cfg.inttime
    # change ntimes
    cfg.acc_bins = acc_bins
    for ntimes in [30, 60, 120, 240, 480]:
        cfg.ntimes = ntimes
        assert cfg.file_time == ntimes * cfg.inttime


# test ObsConfig
switch_schedule = {
    "vna": 1,
    "snap_repeat": 1200,
    "sky": 100,
    "load": 100,
    "noise": 100,
}


def test_use_snap():
    cfg = ObsConfig(switch_schedule=switch_schedule)
    assert cfg.use_snap is True
    no_snap = switch_schedule.copy()
    no_snap["snap_repeat"] = 0
    cfg = ObsConfig(switch_schedule=no_snap)
    assert cfg.use_snap is False


def test_use_vna():
    cfg = ObsConfig(switch_schedule=switch_schedule)
    assert cfg.use_vna is True
    no_vna = switch_schedule.copy()
    no_vna["vna"] = 0
    cfg = ObsConfig(switch_schedule=no_vna)
    assert cfg.use_vna is False


def test_use_switches():
    cfg = ObsConfig(switch_schedule=switch_schedule)
    assert cfg.use_switches is True
    only_vna = switch_schedule.copy()
    only_vna["sky"] = 0
    only_vna["load"] = 0
    only_vna["noise"] = 0
    cfg = ObsConfig(switch_schedule=only_vna)
    assert cfg.use_switches is True
    only_snap = switch_schedule.copy()
    only_snap["vna"] = 0
    cfg = ObsConfig(switch_schedule=only_snap)
    assert cfg.use_switches is True
    only_snap["sky"] = 0
    cfg = ObsConfig(switch_schedule=only_snap)
    assert cfg.use_switches is True  # still switch load and noise
    only_snap["load"] = 0
    cfg = ObsConfig(switch_schedule=only_snap)
    assert cfg.use_switches is False  # no switching left to do
