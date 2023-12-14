import unittest
from unittest import mock
from unittest.mock import patch

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

    # @patch('snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo', return_value=None)
    # def test_execute_recipe(self, calib_norm_algo_mock):
    #     calib_norm_algo_mock.return_value = self.calib_norm_algo_mock
    #     self.calib_norm_algo_mock.execute.return_value = None
    #     self.calib_norm_algo_mock.getPropertyValue.side_effect = lambda key: f"Mocked_{key}"

    #     recipe = CalibrationNormalizationRecipe()

    #     result = recipe.executeRecipe(self.ingredients_mock, self.grocery_list)

    #     self.calib_norm_algo_mock.initialize.assert_called_once()
    #     self.calib_norm_algo_mock.execute.assert_called_once()
    #     self.assertEqual(result["outputWorkspace"], "Mocked_OutputWorkspace")
    #     self.assertEqual(result["smoothedOutput"], "Mocked_SmoothedOutput")
    #     self.assertTrue(result["result"])
