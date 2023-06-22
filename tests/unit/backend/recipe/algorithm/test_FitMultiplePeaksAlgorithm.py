import os
import unittest.mock as mock

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import LoadNexusProcessed, mtd
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
    from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients
    from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import (
        FitMultiplePeaksAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    def test_init():
        """Test ability to initialize fit multiple peaks algo"""
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/purge_peaks/input_parameters.json")
        ).instrumentState
        crystalInfo = CrystallographicInfo.parse_raw(Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        wsName = "testWS"
        fitIngredients = FitMultiplePeaksIngredients(
            InstrumentState=instrumentState, CrystalInfo=crystalInfo, InputWorkspace=wsName
        )
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setProperty("FitMultiplePeaksIngredients", fitIngredients.json())
        assert fmpAlgo.getProperty("FitMultiplePeaksIngredients").value == fitIngredients.json()

    def test_execute():
        inputFile = os.path.join(Resource._resourcesPath, "FitMultiplePeaksTestWS.nxs").replace("tests", "src/snapred")
        LoadNexusProcessed(Filename=inputFile, OutputWorkspace="testWS")
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/purge_peaks/input_parameters.json")
        ).instrumentState
        crystalInfo = CrystallographicInfo.parse_raw(Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        wsName = "testWS"
        fitIngredients = FitMultiplePeaksIngredients(
            InstrumentState=instrumentState, CrystalInfo=crystalInfo, InputWorkspace=wsName
        )
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setProperty("FitMultiplePeaksIngredients", fitIngredients.json())
        fmpAlgo.execute()
        wsGroupName = fmpAlgo.getProperty("OutputWorkspaceGroup").value
        assert wsGroupName == "fitPeaksWSGroup"
        wsGroup = mtd[wsGroupName].getNames()
        expected = [
            "ws_fitted_peakpositions_0",
            "ws_fitted_params_0",
            "ws_fitted_0",
            "ws_fitted_params_err_0",
            "ws_fitted_peakpositions_1",
            "ws_fitted_params_1",
            "ws_fitted_1",
            "ws_fitted_params_err_1",
            "ws_fitted_peakpositions_2",
            "ws_fitted_params_2",
            "ws_fitted_2",
            "ws_fitted_params_err_2",
            "ws_fitted_peakpositions_3",
            "ws_fitted_params_3",
            "ws_fitted_3",
            "ws_fitted_params_err_3",
            "ws_fitted_peakpositions_4",
            "ws_fitted_params_4",
            "ws_fitted_4",
            "ws_fitted_params_err_4",
            "ws_fitted_peakpositions_5",
            "ws_fitted_params_5",
            "ws_fitted_5",
            "ws_fitted_params_err_5",
        ]
        assert wsGroup == expected
