import os
import unittest.mock as mock

import pytest
from mantid.simpleapi import DeleteWorkspace, mtd
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo
from snapred.meta.Config import Resource

HAVE_MOUNT_SNAP = os.path.exists("/SNS/SNAP/")


@pytest.fixture(autouse=True)
def _setup_teardown():
    """Clear all workspaces before and after tests."""
    workspaces = mtd.getObjectNames()
    for workspace in workspaces:
        try:
            DeleteWorkspace(workspace)
        except ValueError:
            print(f"Workspace {workspace} doesn't exist!")


@pytest.mark.skipif(not HAVE_MOUNT_SNAP, reason="Mount SNAP not available")
def test_LiteDataCreationAlgo_invalid_input():
    """Test how the algorithm handles an invalid input workspace."""
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setPropertyValue("AutoDeleteNonLiteWS", "1")
    with pytest.raises(RuntimeError):
        liteDataCreationAlgo.execute()


def test_fakeInstrument():
    from mantid.simpleapi import (
        ConvertToEventWorkspace,
        ConvertToHistogram,
        CreateWorkspace,
        LoadDetectorsGroupingFile,
        LoadInstrument,
        mtd,
    )

    fullInstrumentWS = "_test_lite_algo_native"
    liteInstrumentWS = "_test_lite_algo_lite"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")

    # create simle event data with a different number in each pixel
    CreateWorkspace(
        OutputWorkspace=fullInstrumentWS,
        DataX=[0.5, 1.5] * 16,
        DataY=range(16),
        DataE=[0.01] * 16,
        NSpec=16,
    )
    ConvertToEventWorkspace(
        InputWorkspace=fullInstrumentWS,
        OutputWorkspace=fullInstrumentWS,
    )
    # load the instrument, and load the grouping file
    LoadInstrument(
        Workspace=fullInstrumentWS,
        Filename=fullInstrumentFile,
        RewriteSpectraMap=True,
    )
    LoadDetectorsGroupingFile(
        InputFile=liteInstrumentMap,
        InputWorkspace=fullInstrumentWS,
        OutputWorkspace=focusWS,
    )

    # run the algorithm
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", fullInstrumentWS)
    liteDataCreationAlgo.setPropertyValue("OutputWorkspace", liteInstrumentWS)
    liteDataCreationAlgo.setPropertyValue("LiteDataMapWorkspace", focusWS)
    liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
    liteDataCreationAlgo.execute()

    # check that the lite data is correct
    # 1. check the lite data has four histograms
    # 2. check each histograms has a single pixel
    # 3. check the pixel ids of histograms are 0, 1, 2, 3 in order
    # 4. check each superpixel has the sum corresponding to the four banks
    nHst = 4
    liteWS = mtd[liteInstrumentWS]
    fullWS = mtd[fullInstrumentWS]
    assert liteWS.getNumberHistograms() == nHst
    assert liteWS.getSpectrumNumbers() == list(range(1, nHst + 1))
    for i in range(nHst):
        el = liteWS.getSpectrum(i)
        assert list(el.getDetectorIDs()) == [i]

    for i in range(nHst):
        assert liteWS.getDetector(i).getID() == i

    summedPixels = [0] * nHst
    for i in range(nHst):
        for j in range(nHst):
            summedPixels[i] += fullWS.readY(nHst * i + j)[0]
        assert summedPixels[i] == liteWS.readY(i)[0]
