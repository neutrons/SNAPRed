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
        CreateSampleWorkspace,
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

    CreateSampleWorkspace(
        OutputWorkspace=fullInstrumentWS,
        WorkspaceType="Event",
        Function="Flat background",
        Xmin=0,
        Xmax=1,
        BinWidth=1,
        XUnit="TOF",
        NumBanks=4,
        BankPixelWidth=2,
    )
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

    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", fullInstrumentWS)
    liteDataCreationAlgo.setPropertyValue("OutputWorkspace", liteInstrumentWS)
    liteDataCreationAlgo.setPropertyValue("LiteDataMapWorkspace", focusWS)
    liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
    liteDataCreationAlgo.execute()

    liteWS = mtd[liteInstrumentWS]
    fullWS = mtd[fullInstrumentWS]
    summedPixels = [0, 0, 0, 0]
    for i in range(4):
        for j in range(4):
            summedPixels[i] += fullWS.readY(4 * i + j)[0]
        assert summedPixels[i] == liteWS.readY(i)[0]
