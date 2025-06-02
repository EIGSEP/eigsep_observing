from eigsep_observing.config import ObsConfig

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
