from dataclasses import dataclass, field
from pathlib import Path
from .data import DATA_PATH


@dataclass
class CorrConfig:
    """
    Configuration for EigsepFpga and observing with the SNAP correlator.

    """

    snap_ip: str = "10.10.10.13"
    sample_rate: float = 500  # in MHz
    fpg_file: str = str(
        (
            Path(DATA_PATH) / "eigsep_fengine_1g_v2_3_2024-07-08_1858.fpg"
        ).resolve()
    )
    fpg_version: tuple[int, int] = (2, 3)  # major, minor
    adc_gain: float = 4
    fft_shift: int = 0x055
    corr_acc_len: int = 2**28  # increment corr_acc_cnt by ~1/second
    corr_scalar: int = 2**9  # 8 bits after binary point so 2**9 = 1
    corr_word: int = 4  # 4 bytes per word
    dtype: tuple[str, str] = ("int32", ">")  # dtype, endian
    acc_bins: int = 2
    pam_atten: dict[int, tuple[int, int]] = field(
        default_factory=lambda: {0: (8, 8), 1: (8, 8), 2: (8, 8)}
    )
    pol_delay: dict[str, int] = field(
        default_factory=lambda: {"01": 0, "23": 0, "45": 0}
    )
    nchan: int = 1024
    save_dir: str = "media/eigsep/T7/data"
    ntimes: int = 60  # number of times per file


default_corr_config = CorrConfig()

# config for Dummy SNAP interface (not connected to SNAP)
dummy_corr_config = CorrConfig(
    snap_ip="",
    fpg_file="",
    fpg_version=(0, 0),
    save_dir="./test_data",
)


@dataclass
class ObsConfig:
    """
    High-level configuation for EIGSEP observations. Sets switch schedules,
    sensors, and other high-level parameters.

    """

    # XXX not sure yet what to do here
    sensors: list[str] = field(
        default_factory=lambda: [
            "imu_az",
            "imu_el",
            "therm_load",
            "therm_lna",
            "therm_vna_load",
            "peltier",
            "lidar",
        ]
    )
    # XXX this is an alternative
    pico_id: dict[str, str] =   field(
        default_factory=lambda: {
            "imu_az": "/dev/pico_imu_az",
            "imu_el": "/dev/pico_imu_el",
            "therm_load": "/dev/pico_therm_load",
            "therm_lna": "/dev/pico_therm_lna",
            "therm_vna_load": "/dev/pico_therm_vna_load",
            "peltier": "/dev/pico_peltier",
            "lidar": "/dev/pico_lidar",
            "switch": "/dev/pico_switch",
        }
    )

    switch_schedule: dict[str, int] = field(
        default_factory=lambda: {
            "vna": 1,
            "snap_repeat": 1200,
            "sky": 100,
            "load": 100,
            "noise": 100,
        }
    )


default_obs_config = ObsConfig()
