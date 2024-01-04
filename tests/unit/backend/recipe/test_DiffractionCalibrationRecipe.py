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
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as Recipe
from snapred.meta.Config import Config, Resource

ThisRecipe: str = "snapred.backend.recipe.DiffractionCalibrationRecipe"
PixelCalAlgo: str = ThisRecipe + ".PixelDiffractionCalibration"
GroupCalAlgo: str = ThisRecipe + ".GroupDiffractionCalibration"


class TestDiffractionCalibtationRecipe(unittest.TestCase):
    def setUp(self):
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(
            runNumber=str(self.fakeRunNumber),
            IPTS="",
        )

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
            pixelGroup=fakeInstrumentState.pixelGroup,
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

    @mock.patch(PixelCalAlgo)
    @mock.patch(GroupCalAlgo)
    def test_execute_successful(self, mockGroupCalAlgo, mockPixelCalAlgo):
        # produce 4, 2, 1, 0.5
        mockPixelAlgo = mock.Mock()
        mockPixelAlgo.getPropertyValue.side_effect = [f'{{"medianOffset": {4 * 2**(-i)}}}' for i in range(10)]
        mockPixelCalAlgo.return_value = mockPixelAlgo
        mockGroupCalAlgo.return_value.getPropertyValue.return_value = "passed"
        result = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert result["result"]
        assert result["steps"] == [{"medianOffset": x} for x in [4, 2, 1, 0.5]]
        assert result["calibrationTable"] == "passed"
        assert result["outputWorkspace"] == "passed"

    @mock.patch(PixelCalAlgo)
    def test_execute_unsuccessful_pixel_cal(self, mockPixelCalAlgo):
        mockPixelAlgo = mock.Mock()
        mockPixelAlgo.execute.side_effect = RuntimeError("failure in pixel algo")
        mockPixelCalAlgo.return_value = mockPixelAlgo
        with pytest.raises(RuntimeError) as e:
            self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert str(e.value) == "failure in pixel algo"

    @mock.patch(PixelCalAlgo)
    @mock.patch(GroupCalAlgo)
    def test_execute_unsuccessful_group_cal(self, mockGroupCalAlgo, mockPixelCalAlgo):
        mockPixelAlgo = mock.Mock()
        mockPixelAlgo.getPropertyValue.return_value = '{"medianOffset": 0}'
        mockPixelCalAlgo.return_value = mockPixelAlgo
        mockGroupAlgo = mock.Mock()
        mockGroupAlgo.execute.side_effect = RuntimeError("failure in group algo")
        mockGroupCalAlgo.return_value = mockGroupAlgo
        with pytest.raises(RuntimeError) as e:
            self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert str(e.value) == "failure in group algo"

    @mock.patch(PixelCalAlgo)
    @mock.patch(GroupCalAlgo)
    def test_execute_unsuccessful_later_calls(self, mockGroupCalAlgo, mockPixelCalAlgo):
        # this will check that errors are raised in each of the three try-catch blocks
        # the mocked median offset will follow pattern 1, 0.5, 0.25, etc.
        # first time, algo fails (first try block)
        # second time, algo succeeds with 1, then fails (second try block)
        # second time, algo succeeds with 1, succeeds with 0.5, then fails (third try block)
        mockAlgo = mock.Mock()
        mockGroupCalAlgo.return_value = mockAlgo
        mockPixelCalAlgo.return_value = mockAlgo
        for i in range(3):
            # algo succeeds once then raises an error
            listOfFails = [1] * (i)
            listOfFails.append(RuntimeError(f"passed {i}"))
            mockAlgo.execute.side_effect = listOfFails
            # algo will return 1, 0.5, 0.25, etc.
            mockAlgo.getPropertyValue.side_effect = [f'{{"medianOffset": {2**(-i)}}}' for i in range(10)]
            with pytest.raises(RuntimeError) as e:
                self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
            assert str(e.value).split("\n")[0] == f"passed {i}"

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
        dMin = self.fakeIngredients.pixelGroup.dMin()
        dMax = self.fakeIngredients.pixelGroup.dMax()
        dBin = self.fakeIngredients.pixelGroup.dBin(1)
        focWS = mtd[groupingWS]
        allXmins = [0] * 16
        allXmaxs = [0] * 16
        allDelta = [0] * 16
        for i, gid in enumerate(focWS.getGroupIDs()):
            for detid in focWS.getDetectorIDsOfGroup(int(gid)):
                allXmins[detid] = dMin[i]
                allXmaxs[detid] = dMax[i]
                allDelta[detid] = dBin[i]
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

    def test_execute_with_algos(self):
        # create sample data
        from datetime import date

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

    @mock.patch(PixelCalAlgo)
    @mock.patch(GroupCalAlgo)
    def test_hard_cap_at_five(self, mockGroupAlgo, mockPixelAlgo):
        mockAlgo = mock.Mock()
        mockAlgo.getPropertyValue.side_effect = [f'{{"medianOffset": {11-i}}}' for i in range(10)]
        mockPixelAlgo.return_value = mockAlgo
        mockGroupAlgo.return_value.getPropertyValue.return_value = "fake"
        result = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert result["result"]
        maxIterations = Config["calibration.diffraction.maximumIterations"]
        assert result["steps"] == [{"medianOffset": 11 - i} for i in range(maxIterations)]
        assert result["calibrationTable"] == "fake"
        assert result["outputWorkspace"] == "fake"
        # change the config then run again
        maxIterations = 7
        mockAlgo.getPropertyValue.side_effect = [f'{{"medianOffset": {11-i}}}' for i in range(10)]
        Config._config["calibration"]["diffraction"]["maximumIterations"] = maxIterations
        result = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert result["result"]
        assert result["steps"] == [{"medianOffset": 11 - i} for i in range(maxIterations)]
        assert result["calibrationTable"] == "fake"
        assert result["outputWorkspace"] == "fake"

    @mock.patch(PixelCalAlgo)
    @mock.patch(GroupCalAlgo)
    def test_ensure_monotonic(self, mockGroupAlgo, mockPixelAlgo):
        mockAlgo = mock.Mock()
        mockAlgo.getPropertyValue.side_effect = [f'{{"medianOffset": {i}}}' for i in [4, 3, 2, 1, 4, 0]]
        mockPixelAlgo.return_value = mockAlgo
        mockGroupAlgo.return_value.getPropertyValue.return_value = "fake"
        result = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert result["result"]
        assert result["steps"] == [{"medianOffset": i} for i in [4, 3, 2, 1]]
        assert result["calibrationTable"] == "fake"
        assert result["outputWorkspace"] == "fake"


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
