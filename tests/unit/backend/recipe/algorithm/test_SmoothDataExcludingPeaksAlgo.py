import unittest
from unittest import mock

from mantid.simpleapi import (
    CreateWorkspace,
    DeleteWorkspace,
    LoadNexusProcessed,
    mtd,
)
from util.SculleryBoy import SculleryBoy

from snapred.backend.dao.request import FarmFreshIngredients
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo as Algo
from snapred.meta.Config import Resource
from snapred.meta.pointer import create_pointer


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
        testWS = CreateWorkspace(DataX=[0, 1, 2, 3, 4, 5, 6], DataY=[2, 2, 2, 2, 2, 2], UnitX="dSpacing")
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", testWS)
        algo.setPropertyValue("OutputWorkspace", "test_out_ws")
        algo.unbagGroceries()
        assert algo.getPropertyValue("InputWorkspace") == testWS.name()
        assert algo.getPropertyValue("Outputworkspace") == "test_out_ws"

    def test_execute_with_peaks(self):
        # input data
        testWS = CreateWorkspace(DataX=[0, 1, 2, 3, 4, 5, 6], DataY=[2, 2, 2, 2, 2, 2], UnitX="dSpacing")
        peaks = [
            {
                "groupID": 1,
                "peaks": [
                    {
                        "position": {"value": 1, "minimum": 0, "maximum": 2},
                        "peak": {
                            "hkl": [1, 1, 1],
                            "dSpacing": 3.13592994862768,
                            "fSquared": 535.9619564273586,
                            "multiplicity": 8,
                        },
                    }
                ],
            }
        ]
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", testWS)
        algo.setPropertyValue("OutputWorkspace", "test_out_ws")
        algo.setProperty("DetectorPeaks", create_pointer(peaks))
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
        mockFarmFresh = mock.Mock(spec_set=FarmFreshIngredients)
        peaks = SculleryBoy().prepDetectorPeaks(mockFarmFresh)

        # initialize and run smoothdata algo
        smoothDataAlgo = Algo()
        smoothDataAlgo.initialize()
        smoothDataAlgo.setPropertyValue("InputWorkspace", test_ws_name)
        smoothDataAlgo.setPropertyValue("OutputWorkspace", "_output")
        smoothDataAlgo.setProperty("DetectorPeaks", create_pointer(peaks))
        smoothDataAlgo.setProperty("SmoothingParameter", 0.9)
        assert smoothDataAlgo.execute()
