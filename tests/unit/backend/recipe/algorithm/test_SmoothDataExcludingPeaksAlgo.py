import json
import unittest
import unittest.mock as mock
from typing import List

import pytest
from mantid.simpleapi import (
    CreateSingleValuedWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    LoadNexusProcessed,
    mtd,
)
from pydantic import parse_raw_as
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo as Algo
from snapred.meta.Config import Resource


class TestSmoothDataAlgo(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        for workspace in mtd.getObjectNames():
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")

    def test_unbag_groceries(self):
        testWS = CreateSingleValuedWorkspace()
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", testWS)
        algo.setPropertyValue("OutputWorkspace", "test_out_ws")
        algo.unbagGroceries()
        assert algo.getPropertyValue("InputWorkspace") == testWS.name()
        assert algo.getPropertyValue("Outputworkspace") == "test_out_ws"

    def test_validate_success_with_peaks(self):
        testWS = CreateSingleValuedWorkspace()
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", testWS)
        algo.setPropertyValue("OutputWorkspace", "test_out_ws")
        algo.setProperty("DetectorPeaks", "[{}]")
        algo.setProperty("SmoothingParameter", 0.0)
        errors = algo.validateInputs()
        assert errors == {}

    def test_validate_success_with_ingredients(self):
        testWS = CreateSingleValuedWorkspace()
        ingredientString = Resource.read("inputs/predict_peaks/input_fake_ingredients.json")
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", testWS)
        algo.setPropertyValue("OutputWorkspace", "test_out_ws")
        algo.setPropertyValue("DetectorPeakIngredients", ingredientString)
        algo.setProperty("SmoothingParameter", 0.0)
        errors = algo.validateInputs()
        assert errors == {}

    def test_validate_fail_no_peaks(self):
        # check that it will fail if it is not given a way to find peaks
        testWS = CreateSingleValuedWorkspace()
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", testWS)
        algo.setPropertyValue("OutputWorkspace", "test_out_ws")
        algo.setProperty("SmoothingParameter", 0.0)
        errors = algo.validateInputs()
        assert errors.get("DetectorPeaks") is not None
        assert errors.get("DetectorPeakIngredients") is not None

    def test_validate_fail_no_smoothing(self):
        # check that it will fail if it is not given a way to find peaks
        testWS = CreateSingleValuedWorkspace()
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", testWS)
        algo.setPropertyValue("OutputWorkspace", "test_out_ws")
        algo.setProperty("DetectorPeaks", "[{}]")
        errors = algo.validateInputs()
        assert errors.get("DetectorPeakIngredients") is not None
        assert errors.get("SmoothingParameter")

    def test_execute_with_peaks(self):
        # input data
        testWS = CreateWorkspace(DataX=[0, 1, 2, 3, 4, 5, 6], DataY=[2, 2, 2, 2, 2, 2])
        jsonString = '[{"groupID": 1, "peaks": [{"position": {"value":1, "minimum":0, "maximum":2} }]}]'
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", testWS)
        algo.setPropertyValue("OutputWorkspace", "test_out_ws")
        algo.setProperty("DetectorPeaks", jsonString)
        algo.setProperty("SmoothingParameter", 0.0)
        assert algo.execute()

    def test_SmoothDataExcludingPeaksAlgo(self):
        # input data
        testWorkspaceFile = "inputs/strip_peaks/DSP_58882_cal_CC_Column_spectra.nxs"

        # loading test workspace
        test_ws_name = "test_ws"
        LoadNexusProcessed(
            Filename=Resource.getPath(testWorkspaceFile),
            OutputWorkspace=test_ws_name,
        )

        # populate ingredients
        ingredients = Ingredients.parse_file(Resource.getPath("inputs/predict_peaks/input_fake_ingredients.json"))

        # initialize and run smoothdata algo
        smoothDataAlgo = Algo()
        smoothDataAlgo.initialize()
        smoothDataAlgo.setPropertyValue("InputWorkspace", test_ws_name)
        smoothDataAlgo.setPropertyValue("OutputWorkspace", "_output")
        smoothDataAlgo.setPropertyValue("DetectorPeakIngredients", ingredients.json())
        with pytest.raises(RuntimeError) as e:
            smoothDataAlgo.execute()
        assert "DetectorPeakIngredients" in str(e.value)
        assert "SmoothingParameter" in str(e.value)

        ingredients.smoothingParameter = 0.9
        smoothDataAlgo.setProperty("DetectorPeakIngredients", ingredients.json())
        assert smoothDataAlgo.execute()
