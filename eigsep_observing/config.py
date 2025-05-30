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
    use_ref: bool = True  # use synth on snap to generate adc clock
    use_noise: bool = False  # use digital noise instead of ADC data
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
    save_dir: str = "/media/eigsep/T7/data"
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

    sensors: dict[str, str] = field(
        default_factory=lambda: {
            "imu_az": "/dev/pico_imu_az",
            "imu_el": "/dev/pico_imu_el",
            "therm_load": "/dev/pico_therm_load",
            "therm_lna": "/dev/pico_therm_lna",
            "therm_vna_load": "/dev/pico_therm_vna_load",
            "peltier": "/dev/pico_peltier",
            "lidar": "/dev/pico_lidar",
        }
    )
    switch_pico: str = "/dev/pico_switch"

    switch_schedule: dict[str, int] = field(
        default_factory=lambda: {
            "vna": 1,
            "snap_repeat": 1200,
            "sky": 100,
            "load": 100,
            "noise": 100,
        }
    )

    vna_ip: str = "127.0.0.1"
    vna_port: int = 5025
    vna_timeout: int = 1000  # in seconds
    vna_fstart: float = 1e6  # in Hz
    vna_fstop: float = 250e6  # in Hz
    vna_npoints: int = 1000
    vna_ifbw: float = 100  # in Hz
    # power in dBm
    vna_power: dict[str, float] = field(
        default_factory=lambda: {
            "ant": 0,
            "rec": -40,
        }
    )
    vna_save_dir: str = "/media/eigsep/T7/data/s11_data"

    @property
    def use_snap(self) -> bool:
        """
        Whether to use the SNAP correlator for this observation.

        Returns
        -------
        bool
            True if the SNAP correlator should be used, False otherwise.

        """
        return self.switch_schedule.get("snap_repeat", 0) > 0

    @property
    def use_vna(self) -> bool:
        """
        Whether to use the VNA for this observation.

        Returns
        -------
        bool
            True if the VNA should be used, False otherwise.

        """
        return self.switch_schedule.get("vna", 0) > 0

    @property
    def use_switches(self) -> bool:
        """
        Whether to use the switches for this observation. This is true if
        we use the VNA or at least two modes of sky/load/noise.

        Returns
        -------
        bool
            True if the switches should be used, False otherwise.

        """
        if self.use_vna:
            return True
        nmodes = 0
        for k in ["sky", "load", "noise"]:
            if self.switch_schedule.get(k, 0) > 0:
                nmodes += 1
        return nmodes > 1


default_obs_config = ObsConfig()
