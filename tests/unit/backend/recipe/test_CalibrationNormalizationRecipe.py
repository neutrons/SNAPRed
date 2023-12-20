import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import (
    NormalizationCalibrationIngredients as Ingredients,
)
from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import CalibrationNormalizationAlgo
from snapred.backend.recipe.algorithm.WashDishes import WashDishes
from snapred.backend.recipe.CalibrationNormalizationRecipe import CalibrationNormalizationRecipe
from snapred.meta.decorators.Singleton import Singleton


class TestCalibrationNormalizationRecipe(unittest.TestCase):
    def setUp(self):
        self.ingredientsMock = mock.Mock()
        self.recipe = CalibrationNormalizationRecipe()

    def test_chop_ingredients(self):
        recipe = CalibrationNormalizationRecipe()
        recipe.chopIngredients(self.ingredientsMock)
        assert recipe.runNumber == self.ingredientsMock.reductionIngredients.runConfig.runNumber
        assert recipe.backgroundRunNumber == self.ingredientsMock.backgroundReductionIngredients.runConfig.runNumber

    def test_unbag_groceries(self):
        grocery_list = {
            "inputWorkspace": "InputData",
            "backgroundWorkspace": "BackgroundData",
            "groupingWorkspace": "GroupingData",
        }

        recipe = CalibrationNormalizationRecipe()

        recipe.unbagGroceries(grocery_list)

        assert recipe.rawInput == "InputData"
        assert recipe.rawBackgroundInput == "BackgroundData"
        assert recipe.groupingWS == "GroupingData"
        assert recipe.outputWS == ""
        assert recipe.smoothWS == ""

        grocery_list.update(
            {
                "outputWorkspace": "OutputData",
                "smoothedOutput": "SmoothedData",
            }
        )
        recipe.unbagGroceries(grocery_list)
        assert recipe.outputWS == "OutputData"
        assert recipe.smoothWS == "SmoothedData"

    @patch("snapred.backend.recipe.CalibrationNormalizationRecipe.CalibrationNormalizationAlgo")
    def test_execute_recipe(self, calib_norm_algo_mock):
        calib_norm_algo = mock.Mock()
        calib_norm_algo.getPropertyValue.side_effect = lambda key: f"Mocked_{key}"
        calib_norm_algo_mock.return_value = calib_norm_algo

        grocery_list = {
            "inputWorkspace": "InputData",
            "backgroundWorkspace": "BackgroundData",
            "groupingWorkspace": "GroupingData",
            "outputWorkspace": "OutputData",
            "smoothedOutput": "SmoothedData",
        }

        recipe = CalibrationNormalizationRecipe()

        result = recipe.executeRecipe(self.ingredientsMock, grocery_list)

        calib_norm_algo.initialize.assert_called_once()
        calib_norm_algo.execute.assert_called_once()
        calib_norm_algo.setPropertyValue.assert_any_call("InputWorkspace", grocery_list["inputWorkspace"])
        calib_norm_algo.setPropertyValue.assert_any_call("BackgroundWorkspace", grocery_list["backgroundWorkspace"])
        calib_norm_algo.setPropertyValue.assert_any_call("GroupingWorkspace", grocery_list["groupingWorkspace"])
        calib_norm_algo.setPropertyValue.assert_any_call("OutputWorkspace", grocery_list["outputWorkspace"])
        calib_norm_algo.setPropertyValue.assert_any_call("SmoothedOutput", grocery_list["smoothedOutput"])
        calib_norm_algo.setPropertyValue.assert_any_call("Ingredients", self.ingredientsMock.json())

        assert result["outputWorkspace"] == "Mocked_OutputWorkspace"
        assert result["smoothedOutput"] == "Mocked_SmoothedOutput"
        assert result["result"] is True

    @patch("snapred.backend.recipe.CalibrationNormalizationRecipe.CalibrationNormalizationAlgo")
    def test_fail_execute_recipe(self, calib_norm_algo_mock):
        calib_norm_algo = mock.Mock()
        calib_norm_algo.execute.side_effect = RuntimeError("mock")
        calib_norm_algo_mock.return_value = calib_norm_algo

        grocery_list = {
            "inputWorkspace": "InputData",
            "backgroundWorkspace": "BackgroundData",
            "groupingWorkspace": "GroupingData",
            "outputWorkspace": "OutputData",
            "smoothedOutput": "SmoothedData",
        }

        recipe = CalibrationNormalizationRecipe()

        with pytest.raises(RuntimeError) as e:
            recipe.executeRecipe(self.ingredientsMock, grocery_list)
        assert "mock" == str(e.value)
