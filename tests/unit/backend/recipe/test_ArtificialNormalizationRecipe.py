import json
import unittest
from unittest import mock

import pytest
from mantid.simpleapi import (
    CreateSingleValuedWorkspace,
    mtd,
)
from snapred.backend.dao.ingredients import ArtificialNormalizationIngredients as Ingredients
from snapred.backend.recipe.ArtificialNormalizationRecipe import ArtificialNormalizationRecipe as Recipe
from util.SculleryBoy import SculleryBoy


class TestArtificialNormalizationRecipe(unittest.TestCase):
    sculleryBoy = SculleryBoy()

    def __make_groceries(self):
        inputWorkspace = mtd.unique_name(prefix="inputworkspace")
        outputWorkspace = "outputworkspace"  # noqa: F841
        CreateSingleValuedWorkspace(OutputWorkspace=inputWorkspace)

    def test_chopIngredients(self):
        recipe = Recipe()
        ingredients = mock.Mock()
        recipe.chopIngredients(ingredients)
        assert ingredients.copy() == recipe.ingredients

    def test_unbagGroceries(self):
        recipe = Recipe()

        groceries = {
            "inputWorkspace": "sample",
            "outputWorkspace": "output",
        }
        recipe.unbagGroceries(groceries)
        assert recipe.inputWS == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["outputWorkspace"]

        groceries = {
            "inputWorkspace": "sample",
            "outputWorkspace": "output",
        }
        recipe.unbagGroceries(groceries)
        assert recipe.inputWS == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["outputWorkspace"]
        groceries = {}
        with pytest.raises(KeyError):
            recipe.unbagGroceries(groceries)

    def test_queueAlgos(self):
        recipe = Recipe()
        recipe.mantidSnapper = mock.Mock()
        ingredients = Ingredients(peakWindowClippingSize=10, smoothingParameter=0.5, decreaseParameter=True, lss=True)
        groceries = {"inputWorkspace": "inputworkspace", "outputWorkspace": "outputworkspace"}

        recipe.chopIngredients(ingredients)
        recipe.unbagGroceries(groceries)

        recipe.queueAlgos()

        expected_ingredients_dict = {
            "peakWindowClippingSize": 10,
            "smoothingParameter": 0.5,
            "decreaseParameter": True,
            "lss": True,
        }
        expected_ingredients_str = json.dumps(expected_ingredients_dict)

        recipe.mantidSnapper.CreateArtificialNormalizationAlgo.assert_called_once_with(
            "Creating artificial normalization...",
            InputWorkspace="inputworkspace",
            OutputWorkspace="outputworkspace",
            Ingredients=expected_ingredients_str,
        )

    def test_cook(self):
        recipe = Recipe()
        recipe.prep = mock.Mock()
        recipe.execute = mock.Mock()
        recipe.cook(None, None)
        recipe.prep.assert_called()
        recipe.execute.assert_called()

    def test_cater(self):
        recipe = Recipe()
        recipe.cook = mock.Mock()
        mockIngredients = mock.Mock()
        mockGroceries = mock.Mock()
        pallet = (mockIngredients, mockGroceries)
        shipment = [pallet]
        output = recipe.cater(shipment)
        recipe.cook.assert_called_once_with(mockIngredients, mockGroceries)
        assert output[0] == recipe.cook.return_value
