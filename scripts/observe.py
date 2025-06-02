"""
Observing script for Eigsep observing, using SNAP correlator,
a VNA, Dicke switching, and motor rotations. This script runs on the main
single board computer.
"""

import argparse
import logging
from pathlib import Path
import subprocess

from eigsep_corr.fpga import add_args
from eigsep_observing.config import CorrConfig, ObsConfig
from eigsep_observing import EigObserver

LOG_LEVEL = logging.DEBUG
logger = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL)

# panda config
PANDA_USERNAME = "eigsep"
CLIENT_PATH = "/home/eigsep/eigsep_observing/scripts/client.py"  # on panda

# command line arguments
parser = argparse.ArgumentParser(
    description="Eigsep Observer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
add_args(parser, eig_observing=True)
args = parser.parse_args()
save_dir = Path(args.save_dir).resolve()
vna_save_dir = save_dir / "s11_data"

# SNAP config
corr_cfg = CorrConfig(
    snap_ip="10.10.10.13",
    sample_rate=500,
    use_ref=True,
    use_noise=False,
    fpg_file=args.fpg_file,
    fpg_version=(2, 3),
    adc_gain=4,
    fft_shift=0x00FF,
    corr_acc_len=2**28,
    corr_scalar=2**9,
    corr_word=4,
    dtype=("int32", ">"),
    acc_bins=2,
    pam_atten={"0": (8, 8), "1": (8, 8), "2": (8, 8)},
    pol_delay={"01": 0, "23": 0, "45": 0},
    nchan=1024,
    save_dir=str(save_dir),
    ntimes=args.ntimes,
)

# observing config
obs_cfg = ObsConfig(
    pi_ip="10.10.10.10",
    panda_ip="10.10.10.12",
    sensors={
        "imu_az": "/dev/pico_imu_az",
        "imu_el": "/dev/pico_imu_el",
        "therm_load": "/dev/pico_therm_load",
        "therm_lna": "/dev/pico_therm_lna",
        "therm_vna_load": "/dev/pico_therm_vna_load",
        "peltier": "/dev/pico_peltier",
        "lidar": "/dev/pico_lidar",
    },
    switch_pico="/dev/pico_switch",
    switch_schedule={
        "vna": 1,
        "snap_repeat": 1200,
        "sky": 100,
        "load": 100,
        "noise": 100,
    },
    vna_ip="127.0.0.1",
    vna_port=5025,
    vna_timeout=1000,
    vna_fstart=1e6,
    vna_fstop=250e6,
    vna_npoints=1000,
    vna_ifbw=100,
    vna_power={"amt": 0, "rec": -40},
    vna_save_dir=str(vna_save_dir),
)

if not obs_cfg.use_snap:
    error_msg = "SNAP correlator is not configured for observing."
    logger.error(error_msg)
    raise RuntimeError(error_msg)

if args.dummy_mode:
    logger.warning("Running in DUMMY mode")
    from eigsep_corr.testing import DummyEigsepFpga

    fpga = DummyEigsepFpga(logger=logger)
else:
    from eigsep_corr.fpga import EigsepFpga

    if args.force_program:
        program = True
        force_program = True
    elif args.program:
        program = True
        force_program = False
    else:
        program = False
        force_program = False
    fpga = EigsepFpga(
        cfg=corr_cfg,
        program=program,
        logger=logger,
        force_program=force_program,
    )

# initialize the FPGA
if args.initialize_adc:
    fpga.initialize_adc()
if args.initialize_fpga:
    fpga.initialize_fpga()
fpga.check_version()
fpga.set_input()
if args.sync:
    fpga.synchronize(delay=0, update_redis=args.update_redis)


observer = EigObserver(fpga, cfg=obs_cfg, logger=logger)
observer.redis.reset()  # clear redis at the start of observing
observer.start_heartbeat(ex=60)  # heartbeat thread
# start the client
observer.start_client()
subprocess.run(
    [
        "ssh",
        f"{PANDA_USERNAME}@{obs_cfg.panda_ip}",
        "nohup python3 {CLIENT_PATH} > client.log 2>&1 &",
    ]
)
logger.info("Observing.")
try:
    observer.observe(
        pairs=None,
        timeout=10,
        update_redis=args.update_redis,
        write_files=args.write_files,
    )
except KeyboardInterrupt:
    logger.info("Exiting.")
finally:
    observer.end_observing()
