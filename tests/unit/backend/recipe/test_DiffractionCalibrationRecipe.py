import json
import unittest
from typing import List
from unittest import mock

import pytest
from mantid.api import PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import (
    ConvertUnits,
    CreateSampleWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    LoadDetectorsGroupingFile,
    LoadInstrument,
    Rebin,
    RebinRagged,
    mtd,
)
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as TheseIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as Recipe
from snapred.meta.Config import Resource

PixelCalAlgo: str = "snapred.backend.recipe.DiffractionCalibrationRecipe.PixelDiffractionCalibration"
GroupCalAlgo: str = "snapred.backend.recipe.DiffractionCalibrationRecipe.GroupDiffractionCalibration"
TheAlgorithmManager = "hello"


class TestDiffractionCalibtationRecipe(unittest.TestCase):
    def setUp(self):
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/calibration/sampleInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 1
        fakeInstrumentState.particleBounds.tof.maximum = 10

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("/inputs/diffcal/fakeFocusGroup.json"))
        fakeFocusGroup.definition = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")

        peakList = [
            DetectorPeak.parse_obj({"position": {"value": 2, "minimum": 1, "maximum": 3}}),
            DetectorPeak.parse_obj({"position": {"value": 5, "minimum": 4, "maximum": 6}}),
        ]

        self.fakeIngredients = TheseIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[
                GroupPeakList(groupID=3, peaks=peakList, maxfwhm=0.01),
                GroupPeakList(groupID=7, peaks=peakList, maxfwhm=0.02),
                GroupPeakList(groupID=2, peaks=peakList, maxfwhm=0.02),
                GroupPeakList(groupID=11, peaks=peakList, maxfwhm=0.02),
            ],
            convergenceThreshold=0.5,
            calPath=Resource.getPath("outputs/calibration/"),
        )

        self.fakeRawData = "_test_diffcal_rx"
        CreateWorkspace(
            OutputWorkspace=self.fakeRawData,
            DataX=[1, 2, 3, 4, 5, 6] * 16,
            DataY=[2, 11, 2, 2, 11, 2] * 16,
            UnitX="TOF",
            NSpec=16,
        )
        Rebin(
            InputWorkspace=self.fakeRawData,
            Params=(1, -0.001, 6),
            BinningMode="Logarithmic",
            OutputWorkspace=self.fakeRawData,
        )
        LoadInstrument(
            Workspace=self.fakeRawData,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )
        self.fakeGroupingWorkspace = "_test_diffcal_rx_grouping"
        LoadDetectorsGroupingFile(
            InputFile=Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml"),
            InputWorkspace=self.fakeRawData,
            OutputWorkspace=self.fakeGroupingWorkspace,
        )

        self.groceryList = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
        }
        self.recipe = Recipe()

    def tearDown(self) -> None:
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for workspace in workspaces:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")
        return super().tearDown

    def test_chop_ingredients(self):
        self.recipe.chopIngredients(self.fakeIngredients)
        assert self.recipe.runNumber == self.fakeIngredients.runConfig.runNumber
        assert self.recipe.threshold == self.fakeIngredients.convergenceThreshold
        self.recipe.unbagGroceries(self.groceryList)
        assert self.recipe.rawInput == self.fakeRawData
        assert self.recipe.groupingWS == self.fakeGroupingWorkspace

    # a scoped dummy algorithm to test the recipe's behavior
    class DummyCalibrationAlgorithm(PythonAlgorithm):
        def PyInit(self):
            # declare properties of both algos
            self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
            self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
            self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
            self.declareProperty("GroupingWorkspace", defaultValue="", direction=Direction.Input)
            # declare properties of offset algo
            self.declareProperty("CalibrationTable", defaultValue="", direction=Direction.Output)
            self.declareProperty("data", defaultValue="", direction=Direction.Output)
            # declare properties of calibration algo
            self.declareProperty("PreviousCalibrationTable", defaultValue="", direction=Direction.Input)
            self.declareProperty("FinalCalibrationTable", defaultValue="", direction=Direction.Output)
            self.calls: int = 0
            self.medianOffset: float = 4.0

        def PyExec(self):
            self.reexecute()
            self.setProperty("PreviousCalibrationTable", self.getProperty("CalibrationTable").value)
            self.setProperty("OutputWorkspace", self.getProperty("InputWorkspace").value)
            self.setProperty("FinalCalibrationTable", self.getProperty("PreviousCalibrationTable").value)

        def reexecute(self):
            self.calls += 1
            self.medianOffset *= 0.5
            data = {"medianOffset": self.medianOffset, "calls": self.calls}
            self.setProperty("data", json.dumps(data))
            self.setProperty("CalibrationTable", "fake calibration table")

    @mock.patch(PixelCalAlgo, DummyCalibrationAlgorithm)
    @mock.patch(GroupCalAlgo, DummyCalibrationAlgorithm)
    @mock.patch.object(Recipe, "restockShelves", mock.Mock())
    def test_execute_successful(self):
        result = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert result["result"]
        assert len(result["steps"]) == 3

    # a scoped dummy algorithm to test the recipe's behavior
    class DummyPixelCalAlgo(PythonAlgorithm):
        def PyInit(self):
            self.setRethrows(True)
            # declare properties of both algos
            self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
            self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
            self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
            self.declareProperty("GroupingWorkspace", defaultValue="", direction=Direction.Input)
            # declare properties of offset algo
            self.declareProperty("CalibrationTable", defaultValue="", direction=Direction.Output)
            self.declareProperty("data", defaultValue="", direction=Direction.Output)
            self.calls: int = 0

        def PyExec(self):
            self.calls += 1
            self.setProperty("data", json.dumps({"medianOffset": 7, "calls": self.calls}))
            raise RuntimeError("passed")

    @mock.patch(PixelCalAlgo, DummyPixelCalAlgo)
    @mock.patch.object(Recipe, "restockShelves", mock.Mock())
    def test_execute_unsuccessful_pixel_cal(self):
        try:
            self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

    # a scoped dummy algorithm to test the recipe's behavior
    class DummyGroupCalAlgo(PythonAlgorithm):
        def PyInit(self):
            self.setRethrows(True)
            # declare properties of both algos
            self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
            self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
            self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
            self.declareProperty("GroupingWorkspace", defaultValue="", direction=Direction.Input)
            # declare properties of offset algo
            self.declareProperty("PreviousCalibrationTable", defaultValue="", direction=Direction.Input)
            self.declareProperty("FinalCalibrationTable", defaultValue="", direction=Direction.Output)

        def PyExec(self):
            raise RuntimeError("passed")

    @mock.patch(PixelCalAlgo, DummyCalibrationAlgorithm)
    @mock.patch(GroupCalAlgo, DummyGroupCalAlgo)
    @mock.patch.object(Recipe, "restockShelves", mock.Mock())
    def test_execute_unsuccessful_group_cal(self):
        try:
            self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

    # a scoped dummy algorithm to test all three try/except blocks
    class DummyFailingAlgo(PythonAlgorithm):
        fails: int = 0

        def PyInit(self):
            self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
            self.declareProperty("GroupingWorkspace", defaultValue="", direction=Direction.Input)
            self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
            self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
            self.declareProperty("CalibrationTable", defaultValue="", direction=Direction.InOut)
            self.declareProperty("PreviousCalibrationTable", defaultValue="", direction=Direction.InOut)
            self.declareProperty("FinalCalibrationTable", defaultValue="", direction=Direction.InOut)
            self.declareProperty("times", defaultValue=0, direction=Direction.InOut)
            self.declareProperty("data", defaultValue="", direction=Direction.Output)
            self.setRethrows(True)

        def PyExec(self):
            times = self.getProperty("times").value
            self.setProperty("data", json.dumps({"medianOffset": 2 * 2 ** (-times)}))
            times += 1
            if times >= self.fails:
                raise RuntimeError(f"passed {times} - {self.fails}")
            self.setProperty("times", times)

    @mock.patch(PixelCalAlgo, DummyFailingAlgo)
    @mock.patch(GroupCalAlgo, DummyFailingAlgo)
    @mock.patch.object(Recipe, "restockShelves", mock.Mock())
    def test_execute_unsuccessful_later_calls(self):
        for i in range(1, 4):
            self.DummyFailingAlgo.fails = i
            try:
                result = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
            except Exception as e:  # noqa: E722 BLE001
                assert str(e) == f"passed {i} - {i}"  # noqa: PT017
            else:
                # fail if execute did not raise an exception
                pytest.fail(f"Test should have raised RuntimeError, but no error raised: {len(result['steps'])} - {i}")

    def makeFakeNeutronData(self, rawWS, groupingWS):
        """Will cause algorithm to execute with sample data, instead of loading from file"""

        TOFMin = self.fakeIngredients.instrumentState.particleBounds.tof.minimum
        TOFMax = self.fakeIngredients.instrumentState.particleBounds.tof.maximum

        # prepare with test data
        CreateSampleWorkspace(
            OutputWorkspace=rawWS,
            Function="Powder Diffraction",
            Xmin=0.2,
            Xmax=5,
            BinWidth=0.001,
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )
        LoadInstrument(
            Workspace=rawWS,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
            RewriteSpectraMap=True,
        )
        LoadDetectorsGroupingFile(
            InputFile=Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml"),
            InputWorkspace=rawWS,
            OutputWorkspace=groupingWS,
        )
        focWS = mtd[groupingWS]
        allXmins = [0] * 16
        allXmaxs = [0] * 16
        allDelta = [0] * 16
        for i, gid in enumerate(focWS.getGroupIDs()):
            for detid in focWS.getDetectorIDsOfGroup(int(gid)):
                allXmins[detid] = self.fakeIngredients.focusGroup.dMin[i]
                allXmaxs[detid] = self.fakeIngredients.focusGroup.dMax[i]
                allDelta[detid] = self.fakeIngredients.focusGroup.dBin[i]
        RebinRagged(
            InputWorkspace=rawWS,
            OutputWorkspace=rawWS,
            XMin=allXmins,
            XMax=allXmaxs,
            Delta=allDelta,
        )
        ConvertUnits(
            InputWorkspace=rawWS,
            OutputWorkspace=rawWS,
            Target="TOF",
        )
        Rebin(
            InputWorkspace=rawWS,
            OutputWorkspace=rawWS,
            Params=(TOFMin, -0.001, TOFMax),
            BinningMode="Logarithmic",
        )

    @mock.patch("snapred.backend.data.LocalDataService.LocalDataService")
    @mock.patch("mantid.simpleapi.SaveDiffCal")
    def test_execute_with_algos(self, mockSaveDiffCal, mockLDS):
        # create sample data
        rawWS = "_test_diffcal_rx_data"
        groupingWS = "_test_diffcal_grouping"
        self.makeFakeNeutronData(rawWS, groupingWS)
        self.groceryList["inputWorkspace"] = rawWS
        self.groceryList["groupingWorkspace"] = groupingWS
        try:
            res = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        except ValueError:
            print(res)
        assert res["result"]
        assert res["steps"][-1]["medianOffset"] <= self.fakeIngredients.convergenceThreshold
        mockLDS.assert_called_once()
        mockSaveDiffCal.assert_called_once_with(
            CalibrationWorkspace=res["calibrationTable"],
            GroupingWorkspace=self.recipe.groupingWS,
            MaskWorkspace="",
            Filename=mockLDS()._getCalibrationDataPath() + "/difcal.h5",
        )


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
