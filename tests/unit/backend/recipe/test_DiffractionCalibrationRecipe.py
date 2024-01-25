import json
import unittest
from typing import List
from unittest import mock

import pytest
from mantid.api import PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import mtd
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as TheseIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as Recipe
from snapred.meta.Config import Config, Resource
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import deleteWorkspaceNoThrow

ThisRecipe: str = "snapred.backend.recipe.DiffractionCalibrationRecipe"
PixelCalAlgo: str = ThisRecipe + ".PixelDiffractionCalibration"
GroupCalAlgo: str = ThisRecipe + ".GroupDiffractionCalibration"


class TestDiffractionCalibrationRecipe(unittest.TestCase):
    def setUp(self):
        self.syntheticInputs = SyntheticData()
        self.fakeIngredients = self.syntheticInputs.ingredients

        self.fakeRawData = "_test_diffcal_rx"
        self.fakeGroupingWorkspace = "_test_diffcal_rx_grouping"
        self.fakeOutputWorkspace = "_test_diffcal_rx_output"
        self.fakeTableWorkspace = "_test_diffcal_rx_table"
        self.fakeMaskWorkspace = "_test_diffcal_rx_mask"
        self.syntheticInputs.generateWorkspaces(self.fakeRawData, self.fakeGroupingWorkspace, self.fakeMaskWorkspace)

        self.groceryList = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "outputWorkspace": self.fakeOutputWorkspace,
            "calibrationTable": self.fakeTableWorkspace,
            "maskWorkspace": self.fakeMaskWorkspace,
        }
        self.recipe = Recipe()

    def tearDown(self) -> None:
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for ws in workspaces:
            deleteWorkspaceNoThrow(ws)
        return super().tearDown

    def test_chop_ingredients(self):
        self.recipe.chopIngredients(self.fakeIngredients)
        assert self.recipe.runNumber == self.fakeIngredients.runConfig.runNumber
        assert self.recipe.threshold == self.fakeIngredients.convergenceThreshold
        self.recipe.unbagGroceries(self.groceryList)
        assert self.recipe.rawInput == self.fakeRawData
        assert self.recipe.groupingWS == self.fakeGroupingWorkspace
        assert self.recipe.maskWS == self.fakeMaskWorkspace

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
        assert result["maskWorkspace"] == "passed"

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

    def test_execute_with_algos(self):
        # create sample data
        from datetime import date

        rawWS = "_test_diffcal_rx_data"
        groupingWS = "_test_diffcal_grouping"
        maskWS = "_test_diffcal_mask"
        self.syntheticInputs.generateWorkspaces(rawWS, groupingWS, maskWS)

        self.groceryList["inputWorkspace"] = rawWS
        self.groceryList["groupingWorkspace"] = groupingWS
        self.groceryList["maskWorkspace"] = maskWS
        try:
            res = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        except ValueError:
            print(res)
        assert res["result"]

        assert res["maskWorkspace"]
        mask = mtd[res["maskWorkspace"]]
        assert mask.getNumberMasked() == 0
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
        assert result["maskWorkspace"] == "fake"
        # change the config then run again
        maxIterations = 7
        mockAlgo.getPropertyValue.side_effect = [f'{{"medianOffset": {11-i}}}' for i in range(10)]
        Config._config["calibration"]["diffraction"]["maximumIterations"] = maxIterations
        result = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert result["result"]
        assert result["steps"] == [{"medianOffset": 11 - i} for i in range(maxIterations)]
        assert result["calibrationTable"] == "fake"
        assert result["outputWorkspace"] == "fake"
        assert result["maskWorkspace"] == "fake"

    @mock.patch(PixelCalAlgo)
    @mock.patch(GroupCalAlgo)
    def test_ensure_monotonic(self, mockGroupAlgo, mockPixelAlgo):
        mockAlgo = mock.Mock()
        mockAlgo.getPropertyValue.side_effect = [f'{{"medianOffset": {i}}}' for i in [2, 1, 2, 0]]
        mockPixelAlgo.return_value = mockAlgo
        mockGroupAlgo.return_value.getPropertyValue.return_value = "fake"
        result = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert result["result"]
        assert result["steps"] == [{"medianOffset": i} for i in [2, 1]]
        assert result["calibrationTable"] == "fake"
        assert result["outputWorkspace"] == "fake"
        assert result["maskWorkspace"] == "fake"


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    yield  # ... teardown follows:
    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
