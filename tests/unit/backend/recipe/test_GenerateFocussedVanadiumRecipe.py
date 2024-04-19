import unittest
from unittest import mock

from mantid.simpleapi import (
    CreateWorkspace,
    LoadNexusProcessed,
    mtd,
)
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import GenerateFocussedVanadiumIngredients as Ingredients
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo
from snapred.backend.recipe.GenerateFocussedVanadiumRecipe import GenerateFocussedVanadiumRecipe as Recipe
from snapred.meta.Config import Resource
from util.helpers import deleteWorkspaceNoThrow
from util.SculleryBoy import SculleryBoy

ThisRecipe: str = "snapred.backend.recipe.GenerateFocussedVanadiumRecipe"
SmoothAlgo: str = ThisRecipe + ".SmoothDataExcludingPeaksAlgo"


class TestGenerateFocussedVanadiumRecipe(unittest.TestCase):
    def setUp(self):
        testWorkspaceFile = "inputs/strip_peaks/DSP_58882_cal_CC_Column_spectra.nxs"

        self.fakeInputWorkspace = "_test_input_workspace"
        self.fakeOutputWorkspace = "_test_smoothed_output"

        LoadNexusProcessed(
            Filename=Resource.getPath(testWorkspaceFile),
            outputWorkspace=self.fakeInputWorkspace,
        )
        peaks = SculleryBoy().prepDetectorPeaks({})

        self.fakeIngredients = Ingredients(
            smoothingParameter=0.1,
            detectorPeaks=peaks,
        )
        self.groceryList = {
            "inputWorkspace": self.fakeInputWorkspace,
            "outputWorkspace": self.fakeOutputWorkspace,
        }
        self.recipe = Recipe()

    def tearDown(self) -> None:
        workspaces = mtd.getObjectNames()
        for ws in workspaces:
            deleteWorkspaceNoThrow(ws)
        return super().tearDown()

    @mock.patch(SmoothAlgo)
    def test_execute_successful(self, mockSmoothAlgo):
        mock_instance = mockSmoothAlgo.return_value
        mock_instance.execute.return_value = None
        mock_instance.getPropertyValue.return_value = self.fakeOutputWorkspace

        expected_output = {"outputWorkspace": self.fakeOutputWorkspace, "result": True}

        output = self.recipe.executeRecipe(self.fakeIngredients, self.groceryList)

        self.assertEqual(output, expected_output)  # noqa: PT009
