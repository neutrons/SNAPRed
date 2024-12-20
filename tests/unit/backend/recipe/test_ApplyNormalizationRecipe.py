import unittest

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from util.SculleryBoy import SculleryBoy

from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.ApplyNormalizationRecipe import ApplyNormalizationRecipe, Ingredients


class ApplyNormalizationRecipeTest(unittest.TestCase):
    sculleryBoy = SculleryBoy()

    def _make_groceries(self):
        sampleWS = mtd.unique_name(prefix="test_applynorm")
        normWS = mtd.unique_name(prefix="test_applynorm")
        CreateSingleValuedWorkspace(OutputWorkspace=sampleWS)
        CreateSingleValuedWorkspace(OutputWorkspace=normWS)
        return {
            "inputWorkspace": sampleWS,
            "normalizationWorkspace": normWS,
        }

    def test_init(self):
        ApplyNormalizationRecipe()

    def test_init_reuseUtensils(self):
        utensils = Utensils()
        utensils.PyInit()
        recipe = ApplyNormalizationRecipe(utensils=utensils)
        assert recipe.mantidSnapper == utensils.mantidSnapper

    @unittest.mock.patch("snapred.backend.recipe.ApplyNormalizationRecipe.RebinFocussedGroupDataRecipe")
    def test_chopIngredients(self, mockRebinRecipe):  # noqa: ARG002
        recipe = ApplyNormalizationRecipe()

        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        recipe.chopIngredients(ingredients)
        assert recipe.pixelGroup == ingredients.pixelGroup

    def test_unbagGroceries(self):
        recipe = ApplyNormalizationRecipe()

        groceries = {
            "inputWorkspace": "sample",
            "normalizationWorkspace": "norm",
            "backgroundWorkspace": "bg",
        }
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

    def test_stirInputs(self):
        recipe = ApplyNormalizationRecipe()

        recipe.backgroundWs = "bg"
        with pytest.raises(NotImplementedError):
            recipe.stirInputs()

        recipe.backgroundWs = ""
        recipe.stirInputs()

    def test_queueAlgos(self):
        recipe = ApplyNormalizationRecipe()
        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        groceries = self._make_groceries()
        recipe.prep(ingredients, groceries)
        recipe.queueAlgos()

        queuedAlgos = recipe.mantidSnapper._algorithmQueue
        divideTuple = queuedAlgos[0]

        assert divideTuple[0] == "Divide"

    @unittest.mock.patch("snapred.backend.recipe.ApplyNormalizationRecipe.RebinFocussedGroupDataRecipe")
    def test_cook(self, mockRebinRecipe):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        mockSnapper.mtd = unittest.mock.MagicMock()
        untensils.mantidSnapper = mockSnapper
        recipe = ApplyNormalizationRecipe(utensils=untensils)
        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        groceries = self._make_groceries()

        output = recipe.cook(ingredients, groceries)

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.normalizationWs == groceries["normalizationWorkspace"]
        assert recipe.backgroundWs == ""
        assert output == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.Divide.called
        assert mockRebinRecipe().cook.called

    @unittest.mock.patch("snapred.backend.recipe.ApplyNormalizationRecipe.RebinFocussedGroupDataRecipe")
    def test_cater(self, mockRebinRecipe):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        untensils.mantidSnapper = mockSnapper
        recipe = ApplyNormalizationRecipe(utensils=untensils)
        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        groceries = self._make_groceries()

        output = recipe.cater([(ingredients, groceries)])

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.normalizationWs == groceries["normalizationWorkspace"]
        assert recipe.backgroundWs == ""
        assert output[0] == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.Divide.called
        assert mockRebinRecipe().cook.called
