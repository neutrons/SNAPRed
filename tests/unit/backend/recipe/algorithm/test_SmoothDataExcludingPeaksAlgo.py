import json
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import (
        DeleteWorkspace,
        LoadNexus,
        mtd,
    )
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
    from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import IngestCrystallographicInfoAlgorithm
    from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
        PixelGroupingParametersCalculationAlgorithm,
    )
    from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaks

    def setup():
        pass

    def teardown():
        workspaces = mtd.getObjectNames()

        for workspace in workspaces:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")

    @pytest.fixture(autouse=True)
    def _setup_teardown():
        setup()
        yield
        teardown()

    def test_SmoothDataExcludingPeaksAlgo():
        # input data
        inputWorkspaceFile = "/home/dzj/Documents/Work/csaps/TestData/DSP_58882_cal_CC_Column.nxs"
        calibrationFile = "/home/dzj/Documents/Work/csaps/TestData/CalibrationParameters.json"
        instrumentDefinitionFile = "/home/dzj/Documents/Work/csaps/TestData/SNAPLite.xml"
        groupingFile = "/home/dzj/Documents/Work/csaps/TestData/SNAPFocGroup_Column.lite.nxs"
        cifPath = "/home/dzj/Documents/Work/csaps/TestData/EntryWithCollCode51688.cif"

        # load a test workspace
        test_ws_name = "test_ws"
        LoadNexus(Filename=inputWorkspaceFile, OutputWorkspace=test_ws_name)

        # setup other algos involved within SmoothData Algo
        pixelGroupingAlgo = PixelGroupingParametersCalculationAlgorithm()
        pixelGroupingAlgo.initialize()
        pixelGroupingAlgo.setProperty("InputState", parse_file_as(Calibration, calibrationFile).json())
        pixelGroupingAlgo.setProperty("InstrumentDefinitionFile", instrumentDefinitionFile)
        pixelGroupingAlgo.setProperty("GroupingFile", groupingFile)
        assert pixelGroupingAlgo.execute()
        pixelGroupingParams_json = json.loads(pixelGroupingAlgo.getProperty("OutputParameters").value)

        calibrationState = parse_file_as(Calibration, calibrationFile)
        instrumentState = calibrationState.instrumentState
        instrumentState.pixelGroupingInstrumentParameters = []
        for index in pixelGroupingParams_json:
            instrumentState.pixelGroupingInstrumentParameters.append(PixelGroupingParameters.parse_raw(index))

        ingestAlgo = IngestCrystallographicInfoAlgorithm()
        ingestAlgo.intialize()
        ingestAlgo.setProperty("cifPath", cifPath)
        assert ingestAlgo.execute()

        # set the inputs for test algo
        SmoothAlgo = SmoothDataExcludingPeaks()
        SmoothAlgo.intialize()
        SmoothAlgo.setProperty("InputWorkspace", test_ws_name)

        assert SmoothAlgo.execute()
