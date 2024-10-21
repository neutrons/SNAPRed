import unittest
from unittest import mock

import pytest
from mantid.simpleapi import mtd
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as Recipe
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import deleteWorkspaceNoThrow

ThisRecipe: str = "snapred.backend.recipe.DiffractionCalibrationRecipe"
PixelCalRx: str = ThisRecipe + ".PixelDiffCalRecipe"
GroupCalRx: str = ThisRecipe + ".GroupDiffCalRecipe"


class TestDiffractionCalibrationRecipe(unittest.TestCase):
    def setUp(self):
        self.syntheticInputs = SyntheticData()
        self.fakeIngredients = self.syntheticInputs.ingredients

        self.fakeRawData = "_test_diffcal_rx"
        self.fakeGroupingWorkspace = "_test_diffcal_rx_grouping"
        self.fakeDiagnosticWorkspace = "_test_diffcal_rx_diagnostic"
        self.fakeOutputWorkspace = "_test_diffcal_rx_dsp_output"
        self.fakeTableWorkspace = "_test_diffcal_rx_table"
        self.fakeMaskWorkspace = "_test_diffcal_rx_mask"
        self.syntheticInputs.generateWorkspaces(self.fakeRawData, self.fakeGroupingWorkspace, self.fakeMaskWorkspace)

        self.groceryList = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "outputWorkspace": self.fakeOutputWorkspace,
            "diagnosticWorkspace": self.fakeDiagnosticWorkspace,
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

    @mock.patch(PixelCalRx)
    @mock.patch(GroupCalRx)
    def test_execute_successful(self, mockGroupRx, mockPixelRx):
        # produce 4, 2, 1, 0.5
        mockPixelRx.return_value.cook.return_value = mock.Mock(
            result=True,
            medianOffsets=[0],
        )
        mockGroupRx.return_value.cook.return_value = mock.Mock(
            result=True,
            diagnosticWorkspace="",
            outputWorkspace="",
            calibrationTable="",
            maskWorkspace="",
        )
        result = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert result["result"]
        assert result["steps"] == [0]

    @mock.patch(PixelCalRx)
    def test_execute_unsuccessful_pixel_cal(self, mockPixelRx):
        mockPixelRx.return_value.cook.return_value = mock.Mock(result=False)
        with pytest.raises(RuntimeError) as e:
            self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert str(e.value) == "Pixel Calibration failed"

    @mock.patch(PixelCalRx)
    @mock.patch(GroupCalRx)
    def test_execute_unsuccessful_group_cal(self, mockGroupRx, mockPixelRx):
        mockPixelRx.return_value.cook.return_value = mock.Mock(result=True, medianOffsets=[0])
        mockGroupRx.return_value.cook.return_value = mock.Mock(result=False)
        with pytest.raises(RuntimeError) as e:
            self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)
        assert str(e.value) == "Group Calibration failed"

    def test_execute_with_algos(self):
        """
        Run the recipe with no mocks, to ensure both calculations can work.
        """

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
        assert res["steps"][-1] <= self.fakeIngredients.convergenceThreshold
