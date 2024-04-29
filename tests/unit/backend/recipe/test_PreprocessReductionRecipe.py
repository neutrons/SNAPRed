import unittest

import pytest
from mantid.simpleapi import CreateEmptyTableWorkspace, CreateSingleValuedWorkspace, mtd
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.PreprocessReductionRecipe import Ingredients, PreprocessReductionRecipe
from util.SculleryBoy import SculleryBoy


class PreprocessReductionRecipeTest(unittest.TestCase):
    sculleryBoy = SculleryBoy()

    def _make_groceries(self):
        sampleWS = mtd.unique_name(prefix="test_applynorm")
        calibWS = mtd.unique_name(prefix="test_applynorm")
        CreateSingleValuedWorkspace(OutputWorkspace=sampleWS)
        CreateEmptyTableWorkspace(OutputWorkspace=calibWS)
        return {
            "inputWorkspace": sampleWS,
            "diffcalWorkspace": calibWS,
        }

    def test_chopIngredients(self):
        recipe = PreprocessReductionRecipe()

        ingredients = Ingredients(maskList=[])
        recipe.chopIngredients(ingredients)
        assert recipe.maskList == ingredients.maskList

        ingredients = Ingredients(maskList=["mask1", "mask2"])
        recipe.chopIngredients(ingredients)
        assert recipe.maskList == ingredients.maskList

        ingredients = Ingredients(maskList=None)
        recipe.chopIngredients(ingredients)
        assert recipe.maskList == []

    def test_unbagGroceries(self):
        recipe = PreprocessReductionRecipe()

        groceries = {
            "inputWorkspace": "sample",
            "diffcalWorkspace": "diffcal",
        }
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]

        groceries = {"inputWorkspace": "sample"}
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == ""

    def test_queueAlgos(self):
        recipe = PreprocessReductionRecipe()
        ingredients = Ingredients(maskList=["mask1", "mask2"])
        groceries = self._make_groceries()
        recipe.prep(ingredients, groceries)
        recipe.queueAlgos()

        queuedAlgos = recipe.mantidSnapper._algorithmQueue
        applyDiffCalTuple = queuedAlgos[0]
        maskDetectorFlagsTuple1 = queuedAlgos[1]
        maskDetectorFlagsTuple2 = queuedAlgos[2]

        assert applyDiffCalTuple[0] == "ApplyDiffCal"
        assert maskDetectorFlagsTuple1[0] == "MaskDetectorFlags"
        assert maskDetectorFlagsTuple2[0] == "MaskDetectorFlags"
        # Excessive testing maybe?
        assert applyDiffCalTuple[2]["InstrumentWorkspace"] == groceries["inputWorkspace"]
        assert applyDiffCalTuple[2]["CalibrationWorkspace"] == groceries["diffcalWorkspace"]
        assert maskDetectorFlagsTuple1[2]["MaskWorkspace"] == "mask1"
        assert maskDetectorFlagsTuple1[2]["OutputWorkspace"] == groceries["inputWorkspace"]
        assert maskDetectorFlagsTuple2[2]["MaskWorkspace"] == "mask2"
        assert maskDetectorFlagsTuple2[2]["OutputWorkspace"] == groceries["inputWorkspace"]

    def test_cook(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        untensils.mantidSnapper = mockSnapper
        recipe = PreprocessReductionRecipe(utensils=untensils)
        ingredients = Ingredients(maskList=None)
        groceries = self._make_groceries()

        output = recipe.cook(ingredients, groceries)

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert output == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called
        assert not mockSnapper.MaskDetectorFlags.called

        ingredients = Ingredients(maskList=["mask1", "mask2"])
        output = recipe.cook(ingredients, groceries)

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert output == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called
        assert mockSnapper.MaskDetectorFlags.called

    def test_cater(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        untensils.mantidSnapper = mockSnapper
        recipe = PreprocessReductionRecipe(utensils=untensils)
        ingredients = Ingredients(maskList=["mask1", "mask2"])
        groceries = self._make_groceries()

        output = recipe.cater([(ingredients, groceries)])

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert output[0] == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called
        assert mockSnapper.MaskDetectorFlags.called
