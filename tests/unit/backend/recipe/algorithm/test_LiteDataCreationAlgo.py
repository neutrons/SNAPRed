import os
import unittest.mock as mock

import pytest
from mantid.simpleapi import DeleteWorkspace, mtd
from snapred.backend.dao.state.InstrumentState import InstrumentState
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
        CreateWorkspace,
        DeleteWorkspace,
        LoadDetectorsGroupingFile,
        LoadInstrument,
        mtd,
    )

    fullInstrumentWS = "_test_lite_algo_native"
    liteInstrumentWS = "_test_lite_algo_lite"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
    instrumentState = InstrumentState.model_validate_json(Resource.read("inputs/diffcal/fakeInstrumentState.json"))

    fullResolution: int = 16
    liteResolution: int = 4

    # create simple event data with a different number in each pixel
    CreateWorkspace(
        OutputWorkspace=fullInstrumentWS,
        DataX=[0.5, 1.5] * fullResolution,
        DataY=range(fullResolution),
        DataE=[0.01] * fullResolution,
        NSpec=fullResolution,
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
    liteDataCreationAlgo.setPropertyValue("Ingredients", instrumentState.model_dump_json())
    liteDataCreationAlgo.execute()

    # check that the lite data is correct
    # 1. check the lite data has four histograms
    # 2. check each histograms has a single pixel
    # 3. check the pixel ids of histograms are 0, 1, 2, 3 in order
    # 4. check each superpixel has the sum corresponding to the four banks
    liteWS = mtd[liteInstrumentWS]
    fullWS = mtd[fullInstrumentWS]
    assert liteWS.getNumberHistograms() == liteResolution
    assert liteWS.getSpectrumNumbers() == list(range(1, liteResolution + 1))
    for i in range(liteResolution):
        el = liteWS.getSpectrum(i)
        assert list(el.getDetectorIDs()) == [i]

    for i in range(liteResolution):
        assert liteWS.getDetector(i).getID() == i

    summedPixels = [0] * liteResolution
    for i in range(liteResolution):
        for j in range(liteResolution):
            summedPixels[i] += fullWS.readY(liteResolution * i + j)[0]
        assert summedPixels[i] == liteWS.readY(i)[0]

    # check that the data has been flagged as lite
    assert "Lite" in liteWS.getComment()

    # clean up
    DeleteWorkspace(fullInstrumentWS)
    DeleteWorkspace(liteInstrumentWS)
    DeleteWorkspace(focusWS)


def test_fail_with_no_output():
    from mantid.simpleapi import (
        ConvertToEventWorkspace,
        CreateWorkspace,
        DeleteWorkspace,
        LoadDetectorsGroupingFile,
        LoadInstrument,
    )

    fullInstrumentWS = "_test_lite_algo_native"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")

    fullResolution: int = 16

    # create simple event data with a different number in each pixel
    CreateWorkspace(
        OutputWorkspace=fullInstrumentWS,
        DataX=[0.5, 1.5] * fullResolution,
        DataY=range(fullResolution),
        DataE=[0.01] * fullResolution,
        NSpec=fullResolution,
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

    # will fail with no output
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", fullInstrumentWS)
    liteDataCreationAlgo.setPropertyValue("LiteDataMapWorkspace", focusWS)
    liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
    with pytest.raises(RuntimeError) as e:
        liteDataCreationAlgo.execute()
    assert "invalid Properties" in str(e.value)

    DeleteWorkspace(fullInstrumentWS)
    DeleteWorkspace(focusWS)


def test_fail_to_validate():
    from mantid.simpleapi import (
        CreateWorkspace,
        DeleteWorkspace,
        LoadDetectorsGroupingFile,
        LoadEmptyInstrument,
    )

    instrumentWorkspace: str = "_test_lite_idf"
    invalidWorkspace: str = "_test_lite_algo_invalid"
    liteInstrumentWS: str = "_test_lite_algo_lite"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")

    # load the instrument, and load the grouping file
    LoadEmptyInstrument(
        OutputWorkspace=instrumentWorkspace,
        Filename=fullInstrumentFile,
    )
    LoadDetectorsGroupingFile(
        InputFile=liteInstrumentMap,
        InputWorkspace=instrumentWorkspace,
        OutputWorkspace=focusWS,
    )

    fullResolution: int = 16
    liteResolution: int = 4

    # setup the algorithm
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setPropertyValue("OutputWorkspace", liteInstrumentWS)
    liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)

    # try executing without an input workspace -- should complain
    with pytest.raises(RuntimeError) as e:
        liteDataCreationAlgo.execute()
    assert "some invalid properties found" in str(e.value).lower()

    # try running with the incorrect number of spectra
    invalidResolution: int = fullResolution + 1
    CreateWorkspace(
        OutputWorkspace=invalidWorkspace,
        DataX=[1] * invalidResolution,
        DataY=[1] * invalidResolution,
        DataE=[1] * invalidResolution,
        NSpec=invalidResolution,
    )
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", invalidWorkspace)
    # try executing without a map workspace -- should complain
    with pytest.raises(RuntimeError) as e:
        liteDataCreationAlgo.execute()
    assert "some invalid properties found" in str(e.value).lower()

    # should complain inconsistent resolution with lite data map
    liteDataCreationAlgo.setPropertyValue("LiteDataMapWorkspace", focusWS)
    with pytest.raises(RuntimeError) as e:
        liteDataCreationAlgo.execute()
    print(str(e))
    assert "InputWorkspace" in str(e.value)
    assert "LiteDataMapWorkspace" in str(e.value)
    assert invalidWorkspace in str(e.value)
    assert focusWS in str(e.value)
    assert liteDataCreationAlgo._liteModeResolution == liteResolution

    # cleanup
    DeleteWorkspace(invalidWorkspace)
    DeleteWorkspace(instrumentWorkspace)
    DeleteWorkspace(focusWS)


def test_no_run_twice():
    from mantid.simpleapi import (
        CloneWorkspace,
        ConvertToEventWorkspace,
        CreateWorkspace,
        DeleteWorkspace,
        LoadDetectorsGroupingFile,
        LoadEmptyInstrument,
        LoadInstrument,
    )

    instrumentWorkspace: str = "_test_lite_idf"
    inputWorkspace: str = "_test_lite_algo_input"
    outputWorkspace: str = "_test_lite_algo_output"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
    instrumentState = InstrumentState.model_validate_json(Resource.read("inputs/diffcal/fakeInstrumentState.json"))

    LoadEmptyInstrument(
        OutputWorkspace=instrumentWorkspace,
        Filename=fullInstrumentFile,
    )
    LoadDetectorsGroupingFile(
        InputFile=liteInstrumentMap,
        InputWorkspace=instrumentWorkspace,
        OutputWorkspace=focusWS,
    )

    fullResolution: int = 16
    liteResolution: int = 4

    # setup the algorithm
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setPropertyValue("OutputWorkspace", outputWorkspace)
    liteDataCreationAlgo.setPropertyValue("LiteDataMapWorkspace", focusWS)
    liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
    liteDataCreationAlgo.setPropertyValue("Ingredients", instrumentState.model_dump_json())

    # try reducing native resolution, then running again with the Lite output
    CreateWorkspace(
        OutputWorkspace=inputWorkspace,
        DataX=[0.5, 1.5] * fullResolution,
        DataY=[1] * fullResolution,
        DataE=[1] * fullResolution,
        NSpec=fullResolution,
    )
    ConvertToEventWorkspace(
        InputWorkspace=inputWorkspace,
        OutputWorkspace=inputWorkspace,
    )
    LoadInstrument(
        Workspace=inputWorkspace,
        Filename=fullInstrumentFile,
        RewriteSpectraMap=True,
    )
    # check that it runs
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", inputWorkspace)
    assert liteDataCreationAlgo.execute()
    assert liteDataCreationAlgo._liteModeResolution == liteResolution

    # check the data is reduced and marked as lite
    liteWS = mtd[outputWorkspace]
    assert liteWS.getNumberHistograms() == liteResolution
    assert "Lite" in liteWS.getComment()

    # copy the reduced data, and run it again as the input
    CloneWorkspace(
        InputWorkspace=outputWorkspace,
        outputWorkspace=inputWorkspace,
    )

    # ensure it is reduced data being sent as the input
    liteWS = mtd[inputWorkspace]
    assert liteWS.getNumberHistograms() == liteResolution
    assert "Lite" in liteWS.getComment()

    # mock GroupDetectors -- should never be reached
    liteDataCreationAlgo.mantidSnapper.GroupDetectors = mock.Mock()

    # run the algo with this reduced workspace and check it never got to GroupDetectors
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", inputWorkspace)
    assert liteDataCreationAlgo.execute()
    assert liteDataCreationAlgo._liteModeResolution == liteResolution
    liteWS = mtd[outputWorkspace]
    assert liteWS.getNumberHistograms() == liteResolution
    assert "Lite" in liteWS.getComment()
    liteDataCreationAlgo.mantidSnapper.GroupDetectors.assert_not_called()

    # reset the comment so it does not have the word "Lite"
    # but it does have lite resolution
    # run, and make sure not further reduced
    liteWS.setComment("No comment")
    assert "Lite" not in liteWS.getComment()
    CloneWorkspace(
        InputWorkspace=liteWS,
        OutputWorkspace=inputWorkspace,
    )
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", inputWorkspace)
    assert liteDataCreationAlgo.execute()
    assert liteDataCreationAlgo._liteModeResolution == liteResolution
    liteDataCreationAlgo.mantidSnapper.GroupDetectors.assert_not_called()

    # try running with data already at Lite resolution
    CreateWorkspace(
        OutputWorkspace=inputWorkspace,
        DataX=[1] * liteResolution,
        DataY=[1] * liteResolution,
        DataE=[1] * liteResolution,
        NSpec=liteResolution,
    )
    #
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", inputWorkspace)
    assert liteDataCreationAlgo.execute()
    assert liteDataCreationAlgo._liteModeResolution == liteResolution
    liteDataCreationAlgo.mantidSnapper.GroupDetectors.assert_not_called()

    # to make sure the issue isn't some error caused before GroupDetectors
    # run with full resolution data and make sure it executes the whole thing
    CreateWorkspace(
        OutputWorkspace=inputWorkspace,
        DataX=[0.5, 1.5] * fullResolution,
        DataY=[1] * fullResolution,
        DataE=[1] * fullResolution,
        NSpec=fullResolution,
    )
    ConvertToEventWorkspace(
        InputWorkspace=inputWorkspace,
        OutputWorkspace=inputWorkspace,
    )
    LoadInstrument(
        Workspace=inputWorkspace,
        Filename=fullInstrumentFile,
        RewriteSpectraMap=True,
    )
    #
    liteDataCreationAlgo.mantidSnapper = mock.MagicMock()
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setPropertyValue("OutputWorkspace", outputWorkspace)
    liteDataCreationAlgo.setPropertyValue("LiteDataMapWorkspace", focusWS)
    liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
    liteDataCreationAlgo.setPropertyValue("Ingredients", instrumentState.model_dump_json())
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", inputWorkspace)
    assert liteDataCreationAlgo.execute()

    # cleanup
    DeleteWorkspace(inputWorkspace)
    DeleteWorkspace(outputWorkspace)
    DeleteWorkspace(instrumentWorkspace)
    DeleteWorkspace(focusWS)


def getWorkspaceMetrics(workspace):
    memorySize = workspace.getMemorySize()
    totalEvents = workspace.getNEvents()
    return memorySize, totalEvents


def compareMetrics(initialMetrics, finalMetrics):
    initialMemorySize, initialTotalEvents = initialMetrics
    finalMemorySize, finalTotalEvents = finalMetrics

    return finalMemorySize < initialMemorySize and finalTotalEvents < initialTotalEvents


def testLiteDataCreationAlgoWithCompressionCheck():
    from mantid.simpleapi import (
        ConvertToEventWorkspace,
        CreateWorkspace,
        DeleteWorkspace,
        LoadDetectorsGroupingFile,
        LoadInstrument,
        mtd,
    )

    fullInstrumentWS = "_test_lite_algo_native"
    liteInstrumentWS = "_test_lite_algo_lite"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
    instrumentState = InstrumentState.model_validate_json(Resource.read("inputs/diffcal/fakeInstrumentState.json"))

    fullResolution: int = 16
    liteResolution: int = 4

    # create simple event data with a different number in each pixel
    CreateWorkspace(
        OutputWorkspace=fullInstrumentWS,
        DataX=[0.5, 1.5] * fullResolution,
        DataY=range(fullResolution),
        DataE=[0.01] * fullResolution,
        NSpec=fullResolution,
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

    # Get initial metrics before compression
    initialMetrics = getWorkspaceMetrics(mtd[fullInstrumentWS])

    # run the algorithm
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", fullInstrumentWS)
    liteDataCreationAlgo.setPropertyValue("OutputWorkspace", liteInstrumentWS)
    liteDataCreationAlgo.setPropertyValue("LiteDataMapWorkspace", focusWS)
    liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
    liteDataCreationAlgo.setPropertyValue("Ingredients", instrumentState.model_dump_json())
    liteDataCreationAlgo.execute()

    # Get final metrics after compression
    finalMetrics = getWorkspaceMetrics(mtd[liteInstrumentWS])

    # Compare metrics to verify compression effectiveness
    assert compareMetrics(initialMetrics, finalMetrics), "Compression was not effective"

    # check that the lite data is correct
    # 1. check the lite data has four histograms
    # 2. check each histograms has a single pixel
    # 3. check the pixel ids of histograms are 0, 1, 2, 3 in order
    # 4. check each superpixel has the sum corresponding to the four banks
    liteWS = mtd[liteInstrumentWS]
    fullWS = mtd[fullInstrumentWS]
    assert liteWS.getNumberHistograms() == liteResolution
    assert liteWS.getSpectrumNumbers() == list(range(1, liteResolution + 1))
    for i in range(liteResolution):
        el = liteWS.getSpectrum(i)
        assert list(el.getDetectorIDs()) == [i]

    for i in range(liteResolution):
        assert liteWS.getDetector(i).getID() == i

    summedPixels = [0] * liteResolution
    for i in range(liteResolution):
        for j in range(liteResolution):
            summedPixels[i] += fullWS.readY(liteResolution * i + j)[0]
        assert summedPixels[i] == liteWS.readY(i)[0]

    # check that the data has been flagged as lite
    assert "Lite" in liteWS.getComment()

    # clean up
    DeleteWorkspace(fullInstrumentWS)
    DeleteWorkspace(liteInstrumentWS)
    DeleteWorkspace(focusWS)
