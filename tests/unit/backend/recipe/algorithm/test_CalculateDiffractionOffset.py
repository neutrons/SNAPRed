import random
import json
import unittest.mock as mock

import numpy as np

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import (
        CompareWorkspaces,
        CreateWorkspace,
    )
    from snapred.backend.recipe.algorithm.CalculateDiffractionOffsets import (
        CalculateDiffractionOffsets as ThisAlgo,  # noqa: E402
    )
    from snapred.backend.dao.state.InstrumentState import InstrumentState
    from snapred.meta.Config import Resource

    def mock_ingredients(dBin, runNumber):
        fakeDBin = abs(dBin)
        fakeRunNumber = runNumber
        fakeIngredients = mock.Mock()
        fakeIngredients.dBin = fakeDBin
        fakeIngredients.runConfig.runNumber = fakeRunNumber
        fakeIngredients.groupingFile = "fake_grouping_file"

        instrumentJSON = json.loads(Resource.read("/inputs/calibration/CalibrationParameters.json"))
        fakeIngredients.instrumentState = instrumentJSON["instrumentState"]

        return fakeIngredients

    def test_chop_ingredients():
        """Test that properties can be initialized"""
        fakeDBin = abs(0.002)
        fakeRunNumber = 555
        fakeIngredients = mock_ingredients(fakeDBin, fakeRunNumber)

        algo = ThisAlgo()
        algo.chopIngredients(fakeIngredients)
        assert algo.dBin == -fakeDBin
        assert algo.runNumber == fakeRunNumber
        assert algo.TOFMin < 0
        assert algo.TOFMax > 0

    # def test_set_properties():
    #     """Test that properties can be initialized"""
    #     fakeIngredients = json.json({"dBin": abs(0.002)})

    #     # fakeIngredients.dBin = abs(0.002)
    #     fakeInputWS = "fake_input_ws"
    #     fakeOutputWS = "fake_output_ws"
    #     algo = ThisAlgo()
    #     algo.initialize()
    #     algo.setProperty("DiffractionCalibrationIngredients", fakeIngredients)
    #     algo.setProperty("InputWorkspace", fakeInputWS)
    #     algo.setProperty("OutputWorkspace", fakeOutputWS)
    #     algo.chopIngredients(fakeIngredients)
    #     assert algo.dBin == -fakeIngredients.dbin
    #     assert fakeInputWS == algo.getProperty("InputWorkspace").value
    #     assert fakeOutputWS == algo.getProperty("OutputWorkspace").value
    #     assert fakeBinWidth == algo.getProperty("BinWidth").value

    

    # def test_zero_bin():
    #     """Test that a zero bin width results in no change"""
    #     offsetWS = "offsets"
    #     prevCal = "previouscalibration"
    #     outputWS = "outputWS"
    #     dBin = 0.0
    #     lenTest = 10
    #     dataX = range(lenTest)
    #     # create an offset workspace
    #     CreateWorkspace(OutputWorkspace=offsetWS, DataX=dataX, DataY=[0.1 for i in range(lenTest)])
    #     # create a previous calibration workspace
    #     CreateWorkspace(
    #         OutputWorkspace=prevCal,
    #         DataX=dataX,
    #         DataY=[1] * lenTest,
    #     )
    #     algo = ThisAlgo()
    #     algo.initialize()
    #     algo.setProperty("OffsetsWorkspace", offsetWS)
    #     algo.setProperty("PreviousCalibration", prevCal)
    #     algo.setProperty("OutputWorkspace", outputWS)
    #     algo.setProperty("BinWidth", dBin)
    #     assert algo.execute()
    #     assert CompareWorkspaces(Workspace1=prevCal, Workspace2=outputWS)


    # def test_random_values():
    #     """
    #     Test that the calculation works with random values
    #     """
    #     offsetWS = "offsets"
    #     prevCal = "previouscalibration"
    #     outputWS = "outputWS"
    #     testWS = "testresult"
    #     dBin = random.random()
    #     lenTest = 100
    #     dataX = range(lenTest)
    #     # create workspace of random offsets
    #     offsets = np.array([random.random() for i in range(lenTest)])
    #     CreateWorkspace(
    #         OutputWorkspace=offsetWS,
    #         DataX=dataX,
    #         DataY=offsets,
    #     )
    #     # create workspace of random previous calibrations
    #     dataYin = np.array([random.random() for i in range(lenTest)])
    #     CreateWorkspace(
    #         OutputWorkspace=prevCal,
    #         DataX=dataX,
    #         DataY=dataYin,
    #     )
    #     # calculate expected values of calculation for comparison
    #     multFactor = np.power(np.ones(lenTest) + np.abs(dBin), -1 * offsets)
    #     dataYout = np.multiply(dataYin, multFactor)
    #     CreateWorkspace(
    #         OutputWorkspace=testWS,
    #         DataX=dataX,
    #         DataY=dataYout,
    #     )
    #     algo = ThisAlgo()
    #     algo.initialize()
    #     algo.setProperty("OffsetsWorkspace", offsetWS)
    #     algo.setProperty("PreviousCalibration", prevCal)
    #     algo.setProperty("OutputWorkspace", outputWS)
    #     algo.setProperty("BinWidth", dBin)
    #     assert algo.execute()
    #     assert CompareWorkspaces(Workspace1=testWS, Workspace2=outputWS)


