import unittest
from typing import Any, Dict, List
from unittest import mock

import pytest
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList

# needed to make mocked ingredients
# the algorithm to test
from snapred.backend.recipe.algorithm.RemoveEventBackground import (
    RemoveEventBackground as Algo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestRemoveEventBackground(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        from mantid.simpleapi import DeleteWorkspace, mtd

        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        return super().tearDownClass()

    def createFakeNeutronData(self):
        from mantid.simpleapi import (
            CreateSampleWorkspace,
        )

        TOFMin = 1
        TOFMax = 10
        CreateSampleWorkspace(
            OutputWorkspace=input_ws_name,
            WorkspaceType="Event",
            Function="Flat background",
            XMin=TOFMin,
            XMax=TOFMax,
            BinWidth=1,
            NumEvents=1,
            XUnit="TOF",
            NumBanks=4,
            BankPixelWidth=2,
            Random=False,
        )
        LoadInstrument()
        LoadGroupingDefinition()

    def test_init(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("InputWorkspace", inWS)
        algo.setPropertyValue("InputWorkspace", inWS)
        assert inWS == algo.getPropertyValue("InputWorkspace")

    def test_only_event_workspace(self):
        from mantid.simpleapi import (
            CreateWorkspace,
            DeleteWorkspace,
        )

        CreateWorkspace(
            OutputWorkspace="_tmp_histogram_workspace",
            DataX=[1],
            DataY=[1],
        )
        assert "EventWorkspace" not in mtd["_tmp_histogram_workspace"].id()
        algo = Algo()
        algo.initialize()
        with pytest.raises(RuntimeError):
            algo.setPropertyValue("InputWorkspace", inWS)
        DeleteWorkspace("_tmp_histogram_workspace")

    def test_chop_ingredients(self):
        algo = Algo()
        algo.chopIngredients(self.ingredients)
        assert algo.blanks == {1: [(), (), ()], 2: [(), (), ()]}
        assert algo.groupIDs == [1, 2]
        assert algo.groupDetectorIDs == {1: [], 2: []}

    def test_unbag_groceries(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("InputWorkspace", inWS)
        algo.setPropertyValue("InputWorkspace", inWS)
        algo.unbagGroceries()
        assert algo.inputWorkspaceName == inWS

    def test_execute(self):
        pass

    def test_SmoothDataExcludingPeaksAlgo():
        # input data
        testWorkspaceFile = "inputs/strip_peaks/DSP_58882_cal_CC_Column_spectra.nxs"

        # loading test workspace
        test_ws_name = "test_ws"
        LoadNexusProcessed(Filename=Resource.getPath(testWorkspaceFile), OutputWorkspace=test_ws_name)

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
        smoothDataAlgo = SmoothDataExcludingPeaks()
        smoothDataAlgo.initialize()
        smoothDataAlgo.setPropertyValue("InputWorkspace", test_ws_name)
        smoothDataAlgo.setPropertyValue("Ingredients", smoothDataIngredients.json())
        smoothDataAlgo.execute()

        assert smoothDataAlgo.getPropertyValue("InputWorkspace") == "test_ws"
