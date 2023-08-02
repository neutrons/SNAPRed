import unittest
from unittest.mock import MagicMock, patch

from snapred.backend.dao.ingredients.FitCalibrationWorkspaceIngredients import FitCalibrationWorkspaceIngredients
from snapred.backend.recipe.FitCalibrationWorkspaceRecipe import FitCalibrationWorkspaceRecipe


class TestFitCalibrationWorkspaceRecipe(unittest.TestCase):
    @patch("snapred.backend.recipe.FitCalibrationWorkspaceRecipe.FitMultiplePeaksIngredients")
    @patch("snapred.backend.recipe.FitCalibrationWorkspaceRecipe.SmoothDataExcludingPeaksIngredients")
    @patch(
        "snapred.backend.recipe.FitCalibrationWorkspaceRecipe.SmoothDataExcludingPeaksRecipe",
        return_value=MagicMock(executeRecipe=MagicMock()),
    )
    @patch("snapred.backend.recipe.FitCalibrationWorkspaceRecipe.FitMultiplePeaksRecipe")
    def test_executeRecipe(
        self,
        FitMultiplePeaksRecipe,
        SmoothDataExcludingPeaksRecipe,
        SmoothDataExcludingPeaksIngredients,
        FitMultiplePeaksIngredients,
    ):
        # Mock input data
        ingredients = MagicMock()
        ingredients.instrumentState = "mock_instrument_state"
        ingredients.crystalInfo = "mock_crystal_info"
        ingredients.workspaceName = "mock_workspace_name"
        ingredients.pixelGroupingParameters = {"param1": 1, "param2": 2}

        # Mock the necessary method calls
        recipe_instance = FitCalibrationWorkspaceRecipe()

        # Call the method to test
        result = recipe_instance.executeRecipe(ingredients)

        # Assert the method calls and return value
        SmoothDataExcludingPeaksRecipe().executeRecipe.assert_called_once_with(
            InputWorkspace="mock_workspace_name",
            SmoothDataExcludingPeaksIngredients=SmoothDataExcludingPeaksIngredients(),
            OutputWorkspace=FitMultiplePeaksIngredients().InputWorkspace,
        )
        FitMultiplePeaksRecipe().executeRecipe.assert_called_once_with(
            FitMultiplePeaksIngredients=FitMultiplePeaksIngredients(),
            OutputWorkspaceGroup=f"fitted_{FitMultiplePeaksIngredients().InputWorkspace}",
        )
        assert result == FitCalibrationWorkspaceRecipe().executeRecipe()
