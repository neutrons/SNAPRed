import json
import socket
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import DeleteWorkspace, mtd
    from pydantic import parse_file_as
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.state.InstrumentState import InstrumentState
    from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
    from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
        PixelGroupingParametersCalculationAlgorithm as ThisAlgo,
    )
    from snapred.meta.Config import Resource

    IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

    def setup():
        """Common setup before each test"""
        pass

    def teardown():
        """Common teardown after each test"""
        if not IS_ON_ANALYSIS_MACHINE:  # noqa: F821
            return
        # collect list of all workspaces
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for workspace in workspaces:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")

    @pytest.fixture(autouse=True)
    def _setup_teardown():
        """Setup before each test, teardown after each test"""
        setup()
        yield
        teardown()

    def getInstrumentState(isLocalTest):
        if isLocalTest:
            calibrationPath = Resource.getPath("inputs/pixel_grouping/CalibrationParameters.json")
        else:
            calibrationPath = (
                "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/CalibrationParameters.json"
            )

        return parse_file_as(Calibration, calibrationPath).instrumentState.json()

    def getInstrumentDefinitionFilePath(isLocalTest, isLiteInstrument):
        if isLocalTest:
            if isLiteInstrument:
                return Resource.getPath("inputs/pixel_grouping/SNAPLite_Definition.xml")
            else:
                return Resource.getPath("inputs/pixel_grouping/SNAP_Definition.xml")
        else:
            if isLiteInstrument:
                return "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
            else:
                return "/opt/anaconda/envs/mantid-dev/instrument/SNAP_Definition.xml"

    def getFakeInstrumentState():
        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("inputs/calibration/sampleInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 10
        fakeInstrumentState.particleBounds.tof.maximum = 1000
        return fakeInstrumentState

    def mockRaidPantry(algo):
        groupingFilePath = algo.getProperty("GroupingFile").value
        idf = algo.getProperty("InstrumentDefinitionFile").value
        algo.mantidSnapper.CreateWorkspace(
            "Create workspace to hold IDF",
            OutputWorkspace="idf",
            DataX=1,
            DataY=1,
        )
        algo.mantidSnapper.LoadInstrument(
            "Load a fake instrument for testing",
            Workspace="idf",
            Filename=idf,
            RewriteSpectraMap=False,
        )
        algo.mantidSnapper.LoadGroupingDefinition(
            "Load a fake grouping  file for testing",
            GroupingFilename=groupingFilePath,
            InstrumentDonor="idf",
            OutputWorkspace=algo.grouping_ws_name,
        )

        algo.mantidSnapper.DeleteWorkspace(
            "Remove temporary IDF workspace",
            Workspace="idf",
        )
        algo.mantidSnapper.executeQueue()

    def test_chop_ingredients():
        algo = ThisAlgo()
        algo.initialize()
        fakeInstrumentState = getFakeInstrumentState()
        algo.chopIngredients(fakeInstrumentState)
        assert algo.tofMin is not None
        assert algo.tofMin == fakeInstrumentState.particleBounds.tof.minimum
        assert algo.tofMax == fakeInstrumentState.particleBounds.tof.maximum
        assert algo.deltaTOverT == fakeInstrumentState.instrumentConfig.delTOverT
        assert algo.delLOverL == fakeInstrumentState.instrumentConfig.delLOverL
        assert algo.L == fakeInstrumentState.instrumentConfig.L1 + fakeInstrumentState.instrumentConfig.L2
        assert algo.delL == algo.L * algo.delLOverL
        assert algo.delTheta == fakeInstrumentState.instrumentConfig.delThWithGuide
        assert algo.delL is not None
        assert algo.deltaTOverT is not None
        assert algo.delTheta is not None

    # @mock.patch.object(ThisAlgo, "raidPantry", mockRaidPantry)
    # def test_local_fake_grouping_file():
    #     run_test(
    #         instrumentDefinitionFile=Resource.getPath("inputs/calibration/fakeSNAPLite.xml"),
    #         instrumentState=Resource.read("/inputs/calibration/sampleInstrumentState.json"),
    #         groupingFile=Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml"),
    #         referenceParametersFile=Resource.getPath("outputs/calibration/output.json"),
    #     )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_column_full():
        isLocalTest = False
        isLiteInstrument = False
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.xml"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/Column_parameters_newCalc.json"
        )

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    def test_local_column_full():
        isLocalTest = True
        isLiteInstrument = False
        groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.hdf")
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/Column_parameters.json")

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_column_lite():
        isLocalTest = False
        isLiteInstrument = True
        groupingFile = (
            "/SNS/SNAP/shared/Calibration_Prototype/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
        )
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/Column_parameters_newCalc.lite.json"
        )

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=False),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    def test_local_column_lite():
        isLocalTest = True
        isLiteInstrument = True
        groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.lite.hdf")
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/Column_parameters.lite.json")

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_bank_full():
        isLocalTest = False
        isLiteInstrument = False
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Bank.xml"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/Bank_parameters_newCalc.json"
        )

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    def test_local_bank_full():
        isLocalTest = True
        isLiteInstrument = False
        groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Bank.hdf")
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/Bank_parameters.json")

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_bank_lite():
        isLocalTest = False
        isLiteInstrument = True
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Bank.lite.nxs"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/Bank_parameters_newCalc.lite.json"
        )

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    def test_local_bank_lite():
        isLocalTest = True
        isLiteInstrument = True
        groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Bank.lite.hdf")
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/Bank_parameters.lite.json")

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_all_full():
        isLocalTest = False
        isLiteInstrument = False
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_All.xml"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/All_parameters_newCalc.json"
        )

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    def test_local_all_full():
        isLocalTest = True
        isLiteInstrument = False
        groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_All.hdf")
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/All_parameters.json")

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_all_lite():
        isLocalTest = False
        isLiteInstrument = True
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_All.lite.nxs"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/All_parameters_newCalc.lite.json"
        )

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    def test_local_all_lite():
        isLocalTest = True
        isLiteInstrument = True
        groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_All.lite.hdf")
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/All_parameters.lite.json")

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
            ),
            instrumentState=getInstrumentState(isLocalTest=isLocalTest),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    def test_local_wrong_idf():
        isLocalTest = True
        groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.lite.hdf")
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/Column_parameters.lite.json")

        with pytest.raises(RuntimeError) as excinfo:
            run_test(
                instrumentDefinitionFile="junk",
                instrumentState=getInstrumentState(isLocalTest=isLocalTest),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
            )
        assert 'Instrument file name "junk" has an invalid extension' in str(excinfo.value)
        
    def test_local_wrong_grouping_file():
        isLocalTest = True
        isLiteInstrument = True
        groupingFile = "junk"
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/Column_parameters.lite.json")

        with pytest.raises(RuntimeError) as excinfo:
            run_test(
                instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                    isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
                ),
                instrumentState=getInstrumentState(isLocalTest=isLocalTest),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
            )
        assert "LoadGroupingDefinition" in str(excinfo.value)

    def test_local_wrong_calibration_file():
        isLocalTest = True
        isLiteInstrument = True
        groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.lite.hdf")
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/Column_parameters.lite.json")

        with pytest.raises(RuntimeError) as excinfo:
            run_test(
                instrumentDefinitionFile=getInstrumentDefinitionFilePath(
                    isLocalTest=isLocalTest, isLiteInstrument=isLiteInstrument
                ),
                instrumentState="junk",
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
            )
        assert "InstrumentState" in str(excinfo.value)

    def run_test(instrumentDefinitionFile, instrumentState, groupingFile, referenceParametersFile):
        """Test execution of PixelGroupingParametersCalculationAlgorithm"""
        pixelGroupingAlgo = ThisAlgo()
        pixelGroupingAlgo.initialize()

        pixelGroupingAlgo.setProperty("InstrumentState", instrumentState)
        pixelGroupingAlgo.setProperty("InstrumentDefinitionFile", instrumentDefinitionFile)
        pixelGroupingAlgo.setProperty("GroupingFile", groupingFile)

        assert pixelGroupingAlgo.execute()

        # parse the algorithm output and create a list of PixelGroupingParameters
        pixelGroupingParams_json = json.loads(pixelGroupingAlgo.getProperty("OutputParameters").value)
        pixelGroupingParams_calc = []
        for item in pixelGroupingParams_json:
            pixelGroupingParams_calc.append(PixelGroupingParameters.parse_raw(item))

        # parse the reference file. Note, in the reference file each kind of parameter is grouped into its own list
        with open(referenceParametersFile) as f:
            pixelGroupingParams_ref = json.load(f)

        # compare calculated and reference parameters
        number_of_groupings_calc = len(pixelGroupingParams_calc)
        assert len(pixelGroupingParams_ref["twoTheta"]) == number_of_groupings_calc
        assert len(pixelGroupingParams_ref["dMin"]) == number_of_groupings_calc
        assert len(pixelGroupingParams_ref["dMax"]) == number_of_groupings_calc
        assert len(pixelGroupingParams_ref["delDOverD"]) == number_of_groupings_calc

        index = 0
        for param in pixelGroupingParams_ref["twoTheta"]:
            assert abs(float(param) - pixelGroupingParams_calc[index].twoTheta) == 0
            index += 1

        index = 0
        for param in pixelGroupingParams_ref["dMin"]:
            assert abs(float(param) - pixelGroupingParams_calc[index].dResolution.minimum) == 0
            index += 1

        index = 0
        for param in pixelGroupingParams_ref["dMax"]:
            assert abs(float(param) - pixelGroupingParams_calc[index].dResolution.maximum) == 0
            index += 1

        index = 0
        for param in pixelGroupingParams_ref["delDOverD"]:
            assert abs(float(param) - pixelGroupingParams_calc[index].dRelativeResolution) < 1.0e-3
            index += 1
