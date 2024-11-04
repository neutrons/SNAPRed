import unittest
from unittest import mock

import pytest
from mantid.simpleapi import (
    LoadNexusProcessed,
    mtd,
)
from util.helpers import deleteWorkspaceNoThrow
from util.SculleryBoy import SculleryBoy

from snapred.backend.dao.ingredients import GenerateFocussedVanadiumIngredients as Ingredients
from snapred.backend.dao.request import FarmFreshIngredients
from snapred.backend.recipe.GenerateFocussedVanadiumRecipe import GenerateFocussedVanadiumRecipe as Recipe
from snapred.meta.Config import Resource

ThisRecipe: str = "snapred.backend.recipe.GenerateFocussedVanadiumRecipe"


class TestGenerateFocussedVanadiumRecipe(unittest.TestCase):
    def setUp(self):
        testWorkspaceFile = "inputs/strip_peaks/DSP_58882_cal_CC_Column_spectra.nxs"

        self.fakeInputWorkspace = "_test_input_workspace"
        self.fakeOutputWorkspace = "_test_smoothed_output"

        LoadNexusProcessed(
            Filename=Resource.getPath(testWorkspaceFile),
            outputWorkspace=self.fakeInputWorkspace,
        )

        mockFarmFresh = mock.Mock(spec_set=FarmFreshIngredients)
        peaks = SculleryBoy().prepDetectorPeaks(mockFarmFresh)

        self.fakeIngredients = Ingredients(
            smoothingParameter=0.1,
            pixelGroup=SculleryBoy().prepPixelGroup(None),
            detectorPeaks=peaks,
        )
        self.groceryList = {
            "inputWorkspace": self.fakeInputWorkspace,
            "outputWorkspace": self.fakeOutputWorkspace,
        }
        self.errorGroceryList = {
            "inputWorkspace": None,
            "outputWorkspace": self.fakeOutputWorkspace,
        }
        self.recipe = Recipe()
        self.recipe.mantidSnapper = mock.Mock()
        self.mockSnapper = self.recipe.mantidSnapper

    def tearDown(self) -> None:
        workspaces = mtd.getObjectNames()
        for ws in workspaces:
            deleteWorkspaceNoThrow(ws)
        return super().tearDown()

    def test_execute_successful(self):
        mock_instance = self.mockSnapper.SmoothDataExcludingPeaksAlgo.return_value
        mock_instance.execute.return_value = None
        mock_instance.getPropertyValue.return_value = self.fakeOutputWorkspace

        expected_output = self.fakeOutputWorkspace

        output = self.recipe.cook(self.fakeIngredients, self.groceryList)

        self.assertEqual(output, expected_output)  # noqa: PT009

    def test_execute_unsuccessful(self):
        mock_instance = self.mockSnapper.SmoothDataExcludingPeaksAlgo.return_value
        mock_instance.execute.return_value = None
        mock_instance.getPropertyValue.return_value = None

        with pytest.raises(NotImplementedError) as e:
            self.recipe.cook(self.fakeIngredients, self.errorGroceryList)
        assert str(e.value) == "Fake Vanadium not implemented yet."  # noqa: PT009

    def test_catering(self):
        self.recipe.cook = mock.Mock()
        pallet = (self.fakeIngredients, self.groceryList)
        shipment = [pallet]
        output = self.recipe.cater(shipment)

        assert self.recipe.cook.called
        assert output[0] == self.recipe.cook.return_value
