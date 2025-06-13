from dataclasses import dataclass, field
from pathlib import Path
import yaml
from typing import Dict, Any

from eigsep_corr.data import DATA_PATH
from eigsep_corr.utils import calc_inttime


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
    corr_acc_len: int = 2**26  # increment corr_acc_cnt by ~4/second
    corr_scalar: int = 2**9  # 8 bits after binary point so 2**9 = 1
    # note that corr_word and dtype must be consistent with each other
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
    ntimes: int = 240  # number of times per file

    @property
    def inttime(self) -> float:
        """
        Integration time in seconds.

        Returns
        -------
        float
            Integration time in seconds.

        """
        return calc_inttime(
            self.sample_rate * 1e6,  # in Hz
            self.corr_acc_len,
            acc_bins=self.acc_bins,
        )

    @property
    def file_time(self) -> float:
        """
        Time covered by each file in seconds.

        Returns
        -------
        float
            Time covered by each file in seconds.

        """
        return self.inttime * self.ntimes


def load_corr_config(config_name: str = "default") -> CorrConfig:
    """
    Load CorrConfig from YAML file.
    
    Parameters
    ----------
    config_name : str
        Configuration name ("default" or "dummy")
        
    Returns
    -------
    CorrConfig
        Loaded configuration
    """
    config_path = Path(__file__).parent / "corr_config.yaml"
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    
    data = config_data[config_name]
    
    # Handle fpg_file path construction for default config
    if config_name == "default" and data["fpg_file"] is None:
        data["fpg_file"] = str(
            (Path(DATA_PATH) / "eigsep_fengine_1g_v2_3_2024-07-08_1858.fpg").resolve()
        )
    
    # Convert lists to tuples where needed
    data["fpg_version"] = tuple(data["fpg_version"])
    data["dtype"] = tuple(data["dtype"])
    
    # Convert pam_atten and pol_delay to proper dict format
    data["pam_atten"] = {k: tuple(v) for k, v in data["pam_atten"].items()}
    
    return CorrConfig(**data)


default_corr_config = load_corr_config("default")
dummy_corr_config = load_corr_config("dummy")


@dataclass
class ObsConfig:
    """
    High-level configuation for EIGSEP observations. Sets switch schedules,
    sensors, and other high-level parameters.

    """

    rpi_ip: str = "10.10.10.10"
    panda_ip: str = "10.10.10.12"
    sensors: dict[str, str] = field(
        default_factory=lambda: {
            "imu_az": "/dev/pico_imu_az",
            "imu_el": "/dev/pico_imu_el",
            "therm": "/dev/pico_therm",
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


def load_obs_config(config_name: str = "default") -> ObsConfig:
    """
    Load ObsConfig from YAML file.
    
    Parameters
    ----------
    config_name : str
        Configuration name ("default")
        
    Returns
    -------
    ObsConfig
        Loaded configuration
    """
    config_path = Path(__file__).parent / "obs_config.yaml"
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    
    data = config_data[config_name]
    
    return ObsConfig(**data)


default_obs_config = load_obs_config("default")
