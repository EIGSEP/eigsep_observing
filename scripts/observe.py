import argparse
import logging

from eigsep_corr.fpga import add_args
from eigsep_corr.config import CorrConfig

LOG_LEVEL = logging.DEBUG

parser = argparse.ArgumentParser(
    description="Eigsep Observer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
add_args(parser)
args = parser.parse_args()

cfg = CorrConfig(
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
    save_dir=args.save_dir,
    ntimes=args.ntimes,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL)

if args.dummy_mode:
    logger.warning("Running in DUMMY mode")
    from eigsep_corr.testing import DummyEigsepFpga

    fpga = DummyEigsepFpga(ref=ref, logger=logger)
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
        cfg=cfg,
        program=program,
        logger=logger,
        force_program=force_program,
    )


if args.initialize_adc:
    fpga.initialize_adc()

if args.initialize_fpga:
    fpga.initialize_fpga()

fpga.check_version()

# set input
fpga.set_input()

# synchronize
if args.sync:
    fpga.synchronize(delay=0, update_redis=args.update_redis)

logger.info("Observing ...")
try:
    fpga.observe(
        pairs=None,
        timeout=10,
        update_redis=args.update_redis,
        write_files=args.write_files,
    )
except KeyboardInterrupt:
    logger.info("Exiting.")
finally:
    fpga.end_observing()
