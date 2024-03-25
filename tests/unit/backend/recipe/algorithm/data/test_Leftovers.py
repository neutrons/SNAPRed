import tempfile

import numpy as np
from mantid.api import mtd
from mantid.simpleapi import *
from snapred.backend.recipe.algorithm.data.ReheatLeftovers import ReheatLeftovers
from snapred.backend.recipe.algorithm.data.WrapLeftovers import WrapLeftovers


def compareRaggedWorkspaces(ws1, ws2):
    assert ws1.getNumberHistograms() == ws2.getNumberHistograms()

    assert ws1.isRaggedWorkspace()
    assert ws2.isRaggedWorkspace()

    for i in range(ws1.getNumberHistograms()):
        np.testing.assert_allclose(ws1.readX(i), ws2.readX(i))
        np.testing.assert_allclose(ws1.readY(i), ws2.readY(i))
        np.testing.assert_allclose(ws1.readE(i), ws2.readE(i))


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
        filename = f"{extractPath}/leftovers.tar"
        wrapLeftovers = WrapLeftovers()
        wrapLeftovers.initialize()
        wrapLeftovers.setPropertyValue("InputWorkspace", "raw")
        wrapLeftovers.setPropertyValue("Filename", filename)
        wrapLeftovers.execute()

        reheatLeftovers = ReheatLeftovers()
        reheatLeftovers.initialize()
        reheatLeftovers.setPropertyValue("OutputWorkspace", "reheated")
        reheatLeftovers.setPropertyValue("Filename", filename)
        reheatLeftovers.execute()
        compareRaggedWorkspaces(mtd["raw"], mtd["reheated"])
        DeleteWorkspace("raw")
        DeleteWorkspace("reheated")
    # CompareWorkspaces(Workspace1="raw", Workspace2="reheated") Doesnt work with ragged!
