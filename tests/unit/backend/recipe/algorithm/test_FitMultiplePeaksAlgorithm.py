import os
import unittest.mock as mock

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import CreateSingleValuedWorkspace, CreateWorkspace, LoadNexusProcessed, mtd
    from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
    from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import (
        FitMultiplePeaksAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    def test_init():
        """Test ability to initialize fit multiple peaks algo"""
        wsName = "testWS"
        CreateSingleValuedWorkspace(OutputWorkspace=wsName, DataValue=1)
        ingredients = Ingredients.parse_file(Resource.getPath("/inputs/predict_peaks/input_good_ingredients.json"))
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setPropertyValue("InputWorkspace", wsName)
        fmpAlgo.setProperty("DetectorPeakIngredients", ingredients.json())
        assert fmpAlgo.getPropertyValue("InputWorkspace") == wsName
        assert fmpAlgo.getPropertyValue("DetectorPeakIngredients") == ingredients.json()

    def test_execute():
        inputFile = os.path.join(Resource._resourcesPath, "inputs", "fitMultPeaks", "FitMultiplePeaksTestWS.nxs")
        LoadNexusProcessed(Filename=inputFile, OutputWorkspace="testWS")
        wsName = "testWS"
        CreateWorkspace(
            OutputWorkspace=wsName,
            DataX=[1] * 6,
            DataY=[1] * 6,
            NSpec=6,
        )
        fitIngredients = Ingredients.parse_file(Resource.getPath("inputs/predict_peaks/input_good_ingredients.json"))
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setPropertyValue("InputWorkspace", wsName)
        fmpAlgo.setProperty("DetectorPeakIngredients", fitIngredients.json())
        fmpAlgo.execute()
        wsGroupName = fmpAlgo.getProperty("OutputWorkspaceGroup").value
        assert wsGroupName == "fitPeaksWSGroup"
        wsGroup = list(mtd[wsGroupName].getNames())
        expected = [
            "testWS_fitted_peakpositions_0",
            "testWS_fitted_params_0",
            "testWS_fitted_0",
            "testWS_fitted_params_err_0",
            "testWS_fitted_peakpositions_1",
            "testWS_fitted_params_1",
            "testWS_fitted_1",
            "testWS_fitted_params_err_1",
            "testWS_fitted_peakpositions_2",
            "testWS_fitted_params_2",
            "testWS_fitted_2",
            "testWS_fitted_params_err_2",
            "testWS_fitted_peakpositions_3",
            "testWS_fitted_params_3",
            "testWS_fitted_3",
            "testWS_fitted_params_err_3",
            "testWS_fitted_peakpositions_4",
            "testWS_fitted_params_4",
            "testWS_fitted_4",
            "testWS_fitted_params_err_4",
            "testWS_fitted_peakpositions_5",
            "testWS_fitted_params_5",
            "testWS_fitted_5",
            "testWS_fitted_params_err_5",
        ]
        assert wsGroup == expected
