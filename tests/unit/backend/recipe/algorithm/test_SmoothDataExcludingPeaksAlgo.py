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
from snapred.backend.dao.ingredients import SmoothDataExcludingPeaksIngredients
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

        # load crystal info for testing
        crystalInfo = CrystallographicInfo.parse_raw(Resource.read("inputs/purge_peaks/input_crystalInfo.json"))

        # load instrument state for testing
        instrumentState = Calibration.parse_raw(
            Resource.read("inputs/purge_peaks/input_parameters.json")
        ).instrumentState

        # populate ingredients
        smoothDataIngredients = SmoothDataExcludingPeaksIngredients(
            crystalInfo=crystalInfo,
            instrumentState=instrumentState,
            smoothingParameter=0.5,
        )

        # initialize and run smoothdata algo
        smoothDataAlgo = SmoothDataExcludingPeaksAlgo()
        smoothDataAlgo.initialize()
        smoothDataAlgo.setPropertyValue("InputWorkspace", test_ws_name)
        smoothDataAlgo.setProperty("OutputWorkspace", "_output")
        smoothDataAlgo.setPropertyValue("Ingredients", smoothDataIngredients.json())
        smoothDataAlgo.execute()

        assert smoothDataAlgo.getPropertyValue("InputWorkspace") == "test_ws"
