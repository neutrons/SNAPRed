import unittest
from unittest.mock import MagicMock, patch

from snapred.backend.dao.ingredients import PeakIngredients
from snapred.backend.recipe.FitCalibrationWorkspaceRecipe import FitCalibrationWorkspaceRecipe


class TestFitCalibrationWorkspaceRecipe(unittest.TestCase):
    @patch("snapred.backend.recipe.FitCalibrationWorkspaceRecipe.PeakIngredients")
    @patch(
        "snapred.backend.recipe.FitCalibrationWorkspaceRecipe.SmoothDataExcludingPeaksRecipe",
        return_value=MagicMock(executeRecipe=MagicMock()),
    )
    @patch("snapred.backend.recipe.FitCalibrationWorkspaceRecipe.FitMultiplePeaksRecipe")
    def test_executeRecipe(
        self,
        FitMultiplePeaksRecipe,
        SmoothDataExcludingPeaksRecipe,
        PeakIngredients,
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
            Ingredients=PeakIngredients(),
            OutputWorkspace=PeakIngredients().InputWorkspace,
        )
        FitMultiplePeaksRecipe().executeRecipe.assert_called_once_with(
            InputWorkspace="mock_workspace_name",
            Ingredients=PeakIngredients(),
            OutputWorkspaceGroup="fitted_mock_workspace_name",
        )
