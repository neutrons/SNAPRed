import tempfile

from mantid.api import mtd
from mantid.simpleapi import (
    CreateSampleWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    RebinRagged,
    ReheatLeftovers,
    ResampleX,
    WrapLeftovers,
)
from mantid.testing import assert_almost_equal
from snapred.meta.Config import Config

NUM_BINS = Config["constants.ResampleX.NumberBins"]
LOG_BINNING = True


def test_saveLoad():
    # Load focussed data
    CreateSampleWorkspace(
        OutputWorkspace="raw",
        Function="One Peak",
        NumBanks=1,
        NumMonitors=1,
        BankPixelWidth=3,
        NumEvents=500,
        Random=True,
        XUnit="TOF",
        XMin=0,
        XMax=8000,
        BinWidth=100,
    )
    LoadInstrument(Workspace="raw", RewriteSpectraMap=False, InstrumentName="SNAP")

    numFillerSpectra = 10 - 6
    xMin = [0.05] * numFillerSpectra
    xMin.extend([0.05, 0.06, 0.1, 0.07, 0.04, 0.04])
    xMax = [0.36] * numFillerSpectra
    xMax.extend([0.36, 0.41, 0.64, 0.48, 0.48, 0.48])
    delta = [-0.000401475] * numFillerSpectra
    delta.extend([-0.000401475, -0.000277182, -0.000323453, -0.000430986, -0.000430986, -0.000430986])

    RebinRagged(InputWorkspace="raw", XMin=xMin, XMax=xMax, Delta=delta, PreserveEvents=False, OutputWorkspace="raw")

    with tempfile.TemporaryDirectory(prefix="/tmp/") as extractPath:
        filename = f"{extractPath}/leftovers.nxs.h5"
        WrapLeftovers(
            InputWorkspace="raw",
            Filename=filename,
        )

        ReheatLeftovers(
            OutputWorkspace="reheated",
            Filename=filename,
        )

    ResampleX(
        InputWorkspace="raw",
        NumberBins=NUM_BINS,
        LogBinning=LOG_BINNING,
        OutputWorkspace="expected",
    )
    assert_almost_equal(
        Workspace1=mtd["expected"],
        Workspace2=mtd["reheated"],
        atol=0.0,
    )
    DeleteWorkspace("raw")
    DeleteWorkspace("expected")
    DeleteWorkspace("reheated")
