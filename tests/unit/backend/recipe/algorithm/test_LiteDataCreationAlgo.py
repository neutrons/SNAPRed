import pytest
from mantid.simpleapi import (
    CloneWorkspace,
    ConvertToEventWorkspace,
    CreateWorkspace,
    LoadDetectorsGroupingFile,
    LoadEmptyInstrument,
    LoadInstrument,
    mtd,
)
from util.Config_helpers import Config_override
from util.dao import DAOFactory

from snapred.backend.dao.ingredients import LiteDataCreationIngredients
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo
from snapred.meta.Config import Resource


@pytest.fixture(autouse=True)
def _setup_teardown():
    """Clear all workspaces before and after tests."""
    mtd.clear()
    yield

    # teardown:
    mtd.clear()


def test_LiteDataCreationAlgo_mandatory_properties():
    """Test mandatory input properties."""
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    with pytest.raises(RuntimeError):
        liteDataCreationAlgo.execute()


def test_fakeInstrument():
    fullInstrumentWS = "_test_lite_algo_native"
    liteInstrumentWS = "_test_lite_algo_lite"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
    ingredients = LiteDataCreationIngredients(instrumentState=DAOFactory.synthetic_instrument_state.copy())

    fullResolution: int = 16
    liteResolution: int = 4

    with (
        Config_override("instrument.native.pixelResolution", fullResolution),
        Config_override("instrument.lite.pixelResolution", liteResolution),
    ):
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
        liteDataCreationAlgo.setPropertyValue("Ingredients", ingredients.model_dump_json())
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


def test_fail_with_no_output():
    # 'OutputWorkspace' must be specified.

    fullInstrumentWS = "_test_lite_algo_native"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
    ingredients = LiteDataCreationIngredients(instrumentState=DAOFactory.synthetic_instrument_state.copy())

    fullResolution: int = 16
    liteResolution: int = 4

    with (
        Config_override("instrument.native.pixelResolution", fullResolution),
        Config_override("instrument.lite.pixelResolution", liteResolution),
    ):
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
        liteDataCreationAlgo.setPropertyValue("Ingredients", ingredients.model_dump_json())
        with pytest.raises(RuntimeError) as e:
            liteDataCreationAlgo.execute()
        assert "invalid Properties" in str(e.value)


def test_fail_to_validate_missing_properties():
    # 'InputWorkspace' must be specified.
    # 'LiteDataMapWorkspace' must be specified.

    instrumentWorkspace: str = "_test_lite_idf"
    liteInstrumentWS: str = "_test_lite_algo_lite"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
    ingredients = LiteDataCreationIngredients(instrumentState=DAOFactory.synthetic_instrument_state.copy())

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

    with (
        Config_override("instrument.native.pixelResolution", fullResolution),
        Config_override("instrument.lite.pixelResolution", liteResolution),
    ):
        # setup the algorithm
        liteDataCreationAlgo = LiteDataCreationAlgo()
        liteDataCreationAlgo.initialize()
        liteDataCreationAlgo.setPropertyValue("OutputWorkspace", liteInstrumentWS)
        liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
        liteDataCreationAlgo.setPropertyValue("Ingredients", ingredients.model_dump_json())

        # try executing without an input workspace -- should complain
        with pytest.raises(RuntimeError) as e:
            liteDataCreationAlgo.execute()
        assert "some invalid properties found" in str(e.value).lower()

    with (
        Config_override("instrument.native.pixelResolution", fullResolution),
        Config_override("instrument.lite.pixelResolution", liteResolution),
    ):
        # setup the algorithm
        liteDataCreationAlgo = LiteDataCreationAlgo()
        liteDataCreationAlgo.initialize()
        liteDataCreationAlgo.setPropertyValue("OutputWorkspace", liteInstrumentWS)
        liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
        liteDataCreationAlgo.setPropertyValue("Ingredients", ingredients.model_dump_json())

        # try executing without a map workspace -- should complain
        with pytest.raises(RuntimeError) as e:
            liteDataCreationAlgo.execute()
        assert "some invalid properties found" in str(e.value).lower()


def test_fail_to_validate_input_format():
    # 'InputWorkspace' must have the correct number of spectra.

    instrumentWorkspace: str = "_test_lite_idf"
    invalidWorkspace: str = "_test_lite_algo_invalid"
    liteInstrumentWS: str = "_test_lite_algo_lite"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
    ingredients = LiteDataCreationIngredients(instrumentState=DAOFactory.synthetic_instrument_state.copy())

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

    with (
        Config_override("instrument.native.pixelResolution", fullResolution),
        Config_override("instrument.lite.pixelResolution", liteResolution),
    ):
        # setup the algorithm
        liteDataCreationAlgo = LiteDataCreationAlgo()
        liteDataCreationAlgo.initialize()
        liteDataCreationAlgo.setPropertyValue("OutputWorkspace", liteInstrumentWS)
        liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
        liteDataCreationAlgo.setPropertyValue("Ingredients", ingredients.model_dump_json())

        # Create an event workspace with an incorrect number of spectra.
        invalidResolution = fullResolution + 1
        CreateWorkspace(
            OutputWorkspace=invalidWorkspace,
            DataX=[0.5, 1.5] * invalidResolution,
            DataY=range(invalidResolution),
            DataE=[0.01] * invalidResolution,
            NSpec=invalidResolution,
        )
        ConvertToEventWorkspace(InputWorkspace=invalidWorkspace, OutputWorkspace=invalidWorkspace)
        LoadInstrument(Workspace=invalidWorkspace, Filename=fullInstrumentFile, RewriteSpectraMap=True)

        liteDataCreationAlgo.setPropertyValue("InputWorkspace", invalidWorkspace)

        # should complain inconsistent resolution with lite data map
        liteDataCreationAlgo.setPropertyValue("LiteDataMapWorkspace", focusWS)
        with pytest.raises(RuntimeError, match=r".*one spectrum per non-monitor pixel.*"):
            liteDataCreationAlgo.execute()


def test_input_format_already_lite():
    # Test: converted to lite-mode data is marked correctly as 'Lite'.
    # Test: Usage error: input data already in lite-mode and marked as such.
    # Test: Usage error: input data already in lite-mode and not marked.

    instrumentWorkspace: str = "_test_lite_idf"
    inputWorkspace: str = "_test_lite_algo_input"
    outputWorkspace: str = "_test_lite_algo_output"
    focusWS = "_test_lite_data_map"

    fullInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    liteInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
    liteInstrumentMap = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
    ingredients = LiteDataCreationIngredients(instrumentState=DAOFactory.synthetic_instrument_state.copy())

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

    with (
        Config_override("instrument.native.pixelResolution", fullResolution),
        Config_override("instrument.lite.pixelResolution", liteResolution),
    ):
        # setup the algorithm
        liteDataCreationAlgo = LiteDataCreationAlgo()
        liteDataCreationAlgo.initialize()
        liteDataCreationAlgo.setPropertyValue("OutputWorkspace", outputWorkspace)
        liteDataCreationAlgo.setPropertyValue("LiteDataMapWorkspace", focusWS)
        liteDataCreationAlgo.setPropertyValue("LiteInstrumentDefinitionFile", liteInstrumentFile)
        liteDataCreationAlgo.setPropertyValue("Ingredients", ingredients.model_dump_json())

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

        # ensure it is lite-mode data being sent as the input
        liteWS = mtd[inputWorkspace]
        assert liteWS.getNumberHistograms() == liteResolution
        assert "Lite" in liteWS.getComment()

        # run the algo with this lite-mode workspace
        liteDataCreationAlgo.setPropertyValue("InputWorkspace", inputWorkspace)
        with pytest.raises(
            RuntimeError, match="Usage error: the input workspace has already been converted to lite mode."
        ):
            liteDataCreationAlgo.execute()

        # try running with data already at Lite resolution
        CreateWorkspace(
            OutputWorkspace=inputWorkspace,
            DataX=[1] * liteResolution,
            DataY=[1] * liteResolution,
            DataE=[1] * liteResolution,
            NSpec=liteResolution,
        )
        liteDataCreationAlgo.setPropertyValue("InputWorkspace", inputWorkspace)
        with pytest.raises(
            RuntimeError, match="Usage error: the input workspace has already been converted to lite mode."
        ):
            liteDataCreationAlgo.execute()


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
    ingredients = LiteDataCreationIngredients(instrumentState=DAOFactory.synthetic_instrument_state.copy())

    fullResolution: int = 16
    liteResolution: int = 4

    with (
        Config_override("instrument.native.pixelResolution", fullResolution),
        Config_override("instrument.lite.pixelResolution", liteResolution),
    ):
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
        liteDataCreationAlgo.setPropertyValue("Ingredients", ingredients.model_dump_json())
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
