from eigsep_corr.testing import DummyEigsepFpga as CorrDummyEigsepFpga

from .. import EigsepFpga


class DummyEigsepFpga(EigsepFpga, CorrDummyEigsepFpga):
    """
    DummyEigsepFpga class that inherits from eigsep_observing.EigsepFpga
    and eigsep_corr.DummyEigsepFpga.
    """

    pass
