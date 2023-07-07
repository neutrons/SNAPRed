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
    from mantid.simpleapi import (
        DeleteWorkspace,
        LoadNexusProcessed,
        mtd,
    )
    from pydantic import parse_file_as
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
    from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
    from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import IngestCrystallographicInfoAlgorithm
    from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
        PixelGroupingParametersCalculationAlgorithm,
    )

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

    def test_DiffractionSpectrumWeightCalculator():
        """Test execution of DiffractionSpectrumWeightCalculator"""

        inputWorkspaceFile = "/SNS/SNAP/shared/Malcolm/Temp/DSP_58882_cal_CC_Column.nxs"
        calibrationFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/CalibrationParameters.json"
        instrumentDefinitionFile = "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
        cifFile = "/SNS/SNAP/shared/Calibration/CalibrantSamples/EntryWithCollCode51688.cif"

        # calculate pixel grouping parameters
        pixelGroupingAlgo = PixelGroupingParametersCalculationAlgorithm()
        pixelGroupingAlgo.initialize()
        pixelGroupingAlgo.setProperty("InputState", parse_file_as(Calibration, calibrationFile).json())
        pixelGroupingAlgo.setProperty("InstrumentDefinitionFile", instrumentDefinitionFile)
        pixelGroupingAlgo.setProperty("GroupingFile", groupingFile)

        assert pixelGroupingAlgo.execute()
        pixelGroupingParams_json = json.loads(pixelGroupingAlgo.getProperty("OutputParameters").value)

        # parse and update instrument state
        calibrationState = parse_file_as(Calibration, calibrationFile)
        instrumentState = calibrationState.instrumentState
        instrumentState.pixelGroupingInstrumentParameters = []
        for item in pixelGroupingParams_json:
            instrumentState.pixelGroupingInstrumentParameters.append(PixelGroupingParameters.parse_raw(item))

        # extract crystal info from the cif file
        ingestAlgo = IngestCrystallographicInfoAlgorithm()
        ingestAlgo.initialize()
        ingestAlgo.setProperty("cifPath", cifFile)
        assert ingestAlgo.execute()

        # load a test workspace
        input_ws_name = "input_ws"
        LoadNexusProcessed(Filename=inputWorkspaceFile, OutputWorkspace=input_ws_name)

        # initialize and run the weight algo
        weight_ws_name = "weight_ws"
        weightCalculatorAlgo = DiffractionSpectrumWeightCalculator()
        weightCalculatorAlgo.initialize()
        weightCalculatorAlgo.setProperty("InputWorkspace", input_ws_name)
        weightCalculatorAlgo.setProperty("InstrumentState", instrumentState.json())
        weightCalculatorAlgo.setProperty("CrystalInfo", ingestAlgo.getProperty("CrystalInfo").value)
        weightCalculatorAlgo.setProperty("WeightWorkspace", weight_ws_name)

        assert weightCalculatorAlgo.execute()
