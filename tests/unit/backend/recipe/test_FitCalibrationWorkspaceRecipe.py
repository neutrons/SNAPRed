import unittest
from unittest.mock import MagicMock, patch

from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
from snapred.backend.recipe.FitCalibrationWorkspaceRecipe import FitCalibrationWorkspaceRecipe

thisRecipe = "snapred.backend.recipe.FitCalibrationWorkspaceRecipe."


class TestFitCalibrationWorkspaceRecipe(unittest.TestCase):
    @patch(thisRecipe + "SmoothDataExcludingPeaksRecipe", return_value=MagicMock(executeRecipe=MagicMock()))
    @patch(thisRecipe + "FitMultiplePeaksRecipe")
    def test_executeRecipe(
        self,
        FitMultiplePeaksRecipe,
        SmoothDataExcludingPeaksRecipe,
    ):
        # Mock input data
        ingredients = MagicMock()
        ingredients.instrumentState = "mock_instrument_state"
        ingredients.crystalInfo = "mock_crystal_info"
        ingredients.pixelGroup = {"param1": 1, "param2": 2}
        ingredients.smoothingParameter = 0.2

        # Mock the necessary method calls
        recipe_instance = FitCalibrationWorkspaceRecipe()

        # Call the method to test
        result = recipe_instance.executeRecipe(ingredients, "mock_workspace_name")
        assert result

        # Assert the method calls and return value
        SmoothDataExcludingPeaksRecipe().executeRecipe.assert_called_once_with(
            InputWorkspace="mock_workspace_name",
            DetectorPeakIngredients=ingredients,
            OutputWorkspace="mock_workspace_name",
        )
        FitMultiplePeaksRecipe().executeRecipe.assert_called_once_with(
            InputWorkspace="mock_workspace_name",
            DetectorPeakIngredients=ingredients,
            OutputWorkspaceGroup="fitted_mock_workspace_name",
        )
