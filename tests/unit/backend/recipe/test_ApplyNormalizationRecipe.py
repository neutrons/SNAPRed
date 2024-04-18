import unittest

import pytest
from snapred.backend.recipe.ApplyNormalizationRecipe import ApplyNormalizationRecipe, Ingredients
from util.SculleryBoy import SculleryBoy


class ApplyNormalizationRecipeTest(unittest.TestCase):
    sculleryBoy = SculleryBoy()

    def test_init(self):
        ApplyNormalizationRecipe()

    def test_chopIngredients(self):
        recipe = ApplyNormalizationRecipe()

        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        recipe.chopIngredients(ingredients)
        assert recipe.pixelGroup == ingredients.pixelGroup
        assert recipe.dMin == ingredients.pixelGroup.dMin
        assert recipe.dMax == ingredients.pixelGroup.dMax

    def test_unbagGroceries(self):
        recipe = ApplyNormalizationRecipe()

        groceries = {"inputWorkspace": "sample", "normalizationWorkspace": "norm", "backgroundWorkspace": "bg"}
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.normalizationWs == groceries["normalizationWorkspace"]
        assert recipe.backgroundWs == groceries["backgroundWorkspace"]

        groceries = {
            "inputWorkspace": "sample",
            "normalizationWorkspace": "norm",
        }
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.normalizationWs == groceries["normalizationWorkspace"]
        assert recipe.backgroundWs == ""

        groceries = {
            "inputWorkspace": "sample",
        }
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.normalizationWs == ""
        assert recipe.backgroundWs == ""

    def test_validateInputs(self):
        recipe = ApplyNormalizationRecipe()

        recipe.backgroundWs = "bg"
        with pytest.raises(NotImplementedError):
            recipe.validateInputs()

        recipe.backgroundWs = ""
        recipe.validateInputs()

    def test_queueAlgos(self):
        recipe = ApplyNormalizationRecipe()
        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        groceries = {
            "inputWorkspace": "sample",
            "normalizationWorkspace": "norm",
        }
        recipe.prep(ingredients, groceries)
        recipe.queueAlgos()

        queuedAlgos = recipe.mantidSnapper._algorithmQueue
        divideTuple = queuedAlgos[0]
        resampleTuple = queuedAlgos[1]

        assert divideTuple[0] == "Divide"
        assert resampleTuple[0] == "ResampleX"
        # Excessive testing maybe?
        assert divideTuple[1] == "Dividing out the normalization.."
        assert resampleTuple[1] == "Resampling X-axis..."
        assert divideTuple[2]["LHSWorkspace"] == groceries["inputWorkspace"]
        assert divideTuple[2]["RHSWorkspace"] == groceries["normalizationWorkspace"]
        assert resampleTuple[2]["InputWorkspace"] == groceries["inputWorkspace"]
        assert resampleTuple[2]["XMin"] == ingredients.pixelGroup.dMin
        assert resampleTuple[2]["XMax"] == ingredients.pixelGroup.dMax

    @unittest.mock.patch(
        "snapred.backend.recipe.ApplyNormalizationRecipe.MantidSnapper", return_value=unittest.mock.Mock()
    )
    def test_cook(self, mockSnapper):
        recipe = ApplyNormalizationRecipe()
        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        groceries = {
            "inputWorkspace": "sample",
            "normalizationWorkspace": "norm",
        }

        output = recipe.cook(ingredients, groceries)

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.normalizationWs == groceries["normalizationWorkspace"]
        assert recipe.backgroundWs == ""
        assert output == groceries["inputWorkspace"]
        assert mockSnapper.called
        assert mockSnapper.return_value.executeQueue.called
        assert mockSnapper.return_value.Divide.called
        assert mockSnapper.return_value.ResampleX.called

    @unittest.mock.patch(
        "snapred.backend.recipe.ApplyNormalizationRecipe.MantidSnapper", return_value=unittest.mock.Mock()
    )
    def test_cater(self, mockSnapper):
        recipe = ApplyNormalizationRecipe()
        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        groceries = {
            "inputWorkspace": "sample",
            "normalizationWorkspace": "norm",
        }

        output = recipe.cater([(ingredients, groceries)])

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.normalizationWs == groceries["normalizationWorkspace"]
        assert recipe.backgroundWs == ""
        assert output[0] == groceries["inputWorkspace"]
        assert mockSnapper.called
        assert mockSnapper.return_value.executeQueue.called
        assert mockSnapper.return_value.Divide.called
        assert mockSnapper.return_value.ResampleX.called
