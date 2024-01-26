import unittest
import unittest.mock as mock

import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    LoadNexusProcessed,
    mtd,
)
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo
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
        smoothDataAlgo = SmoothDataExcludingPeaksAlgo()
        smoothDataAlgo.initialize()
        smoothDataAlgo.setPropertyValue("InputWorkspace", test_ws_name)
        smoothDataAlgo.setProperty("OutputWorkspace", "_output")
        smoothDataAlgo.setPropertyValue("DetectorPeakIngredients", ingredients.json())
        with pytest.raises(RuntimeError) as e:
            smoothDataAlgo.execute()
        assert "DetectorPeakIngredients" in str(e.value)
        assert "SmoothingParameter" in str(e.value)

        ingredients.smoothingParameter = 0.9
        smoothDataAlgo.setProperty("DetectorPeakIngredients", ingredients.json())
        assert smoothDataAlgo.execute()
