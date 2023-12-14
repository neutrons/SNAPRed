import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import (
    NormalizationCalibrationIngredients as Ingredients,
)
from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import CalibrationNormalizationAlgo
from snapred.backend.recipe.algorithm.WashDishes import WashDishes
from snapred.backend.recipe.CalibrationNormalizationRecipe import CalibrationNormalizationRecipe
from snapred.meta.decorators.Singleton import Singleton


class TestCalibrationNormalizationRecipe(unittest.TestCase):
    def setUp(self):
        self.ingredientsMock = mock.create_autospec(Ingredients)
        self.calib_norm_algo_mock = mock.create_autospec(CalibrationNormalizationAlgo)
        self.recipe = CalibrationNormalizationRecipe()

    def test_chop_ingredients(self):
        self.ingredients_mock = mock.Mock()
        self.ingredients_mock.reductionIngredients.runConfig.runNumber = "123"
        self.ingredients_mock.backgroundReductionIngredients.runConfig.runNumber = "456"

        recipe = CalibrationNormalizationRecipe()

        recipe.chopIngredients(self.ingredients_mock)

        assert recipe.runNumber == "123"
        assert recipe.backgroundRunNumber == "456"

    def test_unbag_groceries(self):
        self.grocery_list = {
            "inputWorkspace": "InputData",
            "backgroundWorkspace": "BackgroundData",
            "groupingWorkspace": "GroupingData",
            "outputWorkspace": "OutputData",
            "smoothedOutput": "SmoothedData",
        }

        recipe = CalibrationNormalizationRecipe()

        recipe.unbagGroceries(self.grocery_list)

        assert recipe.rawInput == "InputData"
        assert recipe.rawBackgroundInput == "BackgroundData"
        assert recipe.groupingWS == "GroupingData"
        assert recipe.outputWS == "OutputData"
        assert recipe.smoothWS == "SmoothedData"

    @patch("snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo", return_value=None)
    def test_execute_recipe(self, calib_norm_algo_mock):
        calib_norm_algo_mock.return_value = self.calib_norm_algo_mock
        self.calib_norm_algo_mock.execute.return_value = None
        self.calib_norm_algo_mock.getPropertyValue.side_effect = lambda key: f"Mocked_{key}"

        recipe = CalibrationNormalizationRecipe()
        self.calib_norm_algo_mock = MagicMock()
        ingredients_mock = MagicMock(spec=Ingredients)
        ingredients_mock.json.return_value = "mocked_ingredients_json"

        grocery_list = {
            "inputWorkspace": "mocked_input_workspace",
            "backgroundWorkspace": "mocked_background_workspace",
            "groupingWorkspace": "mocked_grouping_workspace",
            "outputWorkspace": "mocked_output_workspace",
            "smoothedOutput": "mocked_smoothed_output",
        }

        result = recipe.executeRecipe(ingredients_mock, grocery_list)

        self.calib_norm_algo_mock.initialize.assert_called_once()
        self.calib_norm_algo_mock.execute.assert_called_once()
        self.calib_norm_algo_mock.setPropertyValue.assert_any_call("InputWorkspace", "mocked_input_workspace")
        self.calib_norm_algo_mock.setPropertyValue.assert_any_call("BackgroundWorkspace", "mocked_background_workspace")
        self.calib_norm_algo_mock.setPropertyValue.assert_any_call("GroupingWorkspace", "mocked_grouping_workspace")
        self.calib_norm_algo_mock.setPropertyValue.assert_any_call("OutputWorkspace", "mocked_output_workspace")
        self.calib_norm_algo_mock.setPropertyValue.assert_any_call("SmoothedOutput", "mocked_smoothed_output")
        self.calib_norm_algo_mock.setPropertyValue.assert_any_call("Ingredients", "mocked_ingredients_json")

        assert result["result"]
