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
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo as Algo
from snapred.meta.Config import Resource
from snapred.meta.redantic import list_to_raw
from util.SculleryBoy import SculleryBoy


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
        peaks = SculleryBoy().prepDetectorPeaks({})

        # initialize and run smoothdata algo
        smoothDataAlgo = Algo()
        smoothDataAlgo.initialize()
        smoothDataAlgo.setPropertyValue("InputWorkspace", test_ws_name)
        smoothDataAlgo.setPropertyValue("OutputWorkspace", "_output")
        smoothDataAlgo.setPropertyValue("DetectorPeaks", list_to_raw(peaks))
        smoothDataAlgo.setPropertyValue("SmoothingParameter", "0.9")
        assert smoothDataAlgo.execute()
