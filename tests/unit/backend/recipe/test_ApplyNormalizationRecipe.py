import unittest

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.ApplyNormalizationRecipe import ApplyNormalizationRecipe, Ingredients
from snapred.meta.Config import Config
from util.Config_helpers import Config_override
from util.SculleryBoy import SculleryBoy


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

    def test_chopIngredients(self):
        recipe = ApplyNormalizationRecipe()

        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        recipe.chopIngredients(ingredients)
        assert recipe.pixelGroup == ingredients.pixelGroup
        # Apply adjustment that happens in chopIngredients step
        dMin = [x + Config["constants.CropFactors.lowdSpacingCrop"] for x in ingredients.pixelGroup.dMin()]
        dMax = [x - Config["constants.CropFactors.highdSpacingCrop"] for x in ingredients.pixelGroup.dMax()]
        assert recipe.dMin == dMin
        assert recipe.dMax == dMax

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
        rebinRaggedTuple = queuedAlgos[1]

        assert divideTuple[0] == "Divide"
        assert rebinRaggedTuple[0] == "RebinRagged"
        # Excessive testing maybe?
        assert divideTuple[1] == "Dividing out the normalization.."
        assert rebinRaggedTuple[1] == "Resampling X-axis..."
        assert divideTuple[2]["LHSWorkspace"] == groceries["inputWorkspace"]
        assert divideTuple[2]["RHSWorkspace"] == groceries["normalizationWorkspace"]
        assert rebinRaggedTuple[2]["InputWorkspace"] == groceries["inputWorkspace"]
        # Apply adjustment that happens in chopIngredients step
        dMin = [x + Config["constants.CropFactors.lowdSpacingCrop"] for x in ingredients.pixelGroup.dMin()]
        dMax = [x - Config["constants.CropFactors.highdSpacingCrop"] for x in ingredients.pixelGroup.dMax()]
        assert rebinRaggedTuple[2]["XMin"] == dMin
        assert rebinRaggedTuple[2]["XMax"] == dMax

    def test_cook(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
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
        assert mockSnapper.RebinRagged.called

    def test_cater(self):
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
        assert mockSnapper.RebinRagged.called

    def test_badChopIngredients(self):
        recipe = ApplyNormalizationRecipe()
        ingredients = Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        with (
            Config_override("constants.CropFactors.lowdSpacingCrop", 500.0),
            Config_override("constants.CropFactors.highdSpacingCrop", 1000.0),
            pytest.raises(ValueError, match="d-spacing crop factors are too large"),
        ):
            recipe.chopIngredients(ingredients)

        with (
            Config_override("constants.CropFactors.lowdSpacingCrop", -10.0),
            pytest.raises(ValueError, match="Low d-spacing crop factor must be positive"),
        ):
            recipe.chopIngredients(ingredients)

        with (
            Config_override("constants.CropFactors.highdSpacingCrop", -10.0),
            pytest.raises(ValueError, match="High d-spacing crop factor must be positive"),
        ):
            recipe.chopIngredients(ingredients)
