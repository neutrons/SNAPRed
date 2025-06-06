import unittest
from unittest import mock

import pytest

from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe

ModulePatch = "snapred.backend.recipe.ReductionGroupProcessingRecipe.{0}"


# TODO: Add/update tests when EWM 4798 is complete
class ReductionGroupProcessingRecipeTest(unittest.TestCase):
    def test_init(self):
        ReductionGroupProcessingRecipe()

    def mockIngredients(self):
        ingredients = mock.Mock()
        ingredients.pixelGroup = mock.Mock()
        return ingredients

    def test_init_reuseUtensils(self):
        utensils = Utensils()
        utensils.PyInit()
        recipe = ReductionGroupProcessingRecipe(utensils=utensils)
        assert recipe.mantidSnapper == utensils.mantidSnapper

    def test_unbagGroceries(self):
        recipe = ReductionGroupProcessingRecipe()

        groceries = {
            "inputWorkspace": "input",
            "outputWorkspace": "output",
            "groupingWorkspace": "groupingWS",
        }
        recipe.unbagGroceries(groceries)
        assert recipe.rawInput == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["outputWorkspace"]
        assert recipe.groupingWS == groceries["groupingWorkspace"]

    def test_queueAlgos(self):
        recipe = ReductionGroupProcessingRecipe()
        recipe._validateIngredients = unittest.mock.Mock(return_value=True)
        recipe._validateGrocery = unittest.mock.Mock(return_value=True)
        groceries = {
            "inputWorkspace": "input",
            "groupingWorkspace": "groupingWS",
        }
        recipe.prep(self.mockIngredients(), groceries)
        recipe.queueAlgos()

        queuedAlgos = recipe.mantidSnapper._algorithmQueue
        i = 0
        diffFoc = queuedAlgos[i]
        normCurr = queuedAlgos[i := i + 1]

        assert diffFoc[0] == "FocusSpectraAlgorithm"
        assert normCurr[0] == "NormalizeByCurrentButTheCorrectWay"
        assert diffFoc[1] == "Focusing Spectra..."
        assert normCurr[1] == "Normalizing Current ... but the correct way!"
        assert diffFoc[2]["InputWorkspace"] == groceries["inputWorkspace"]
        assert diffFoc[2]["GroupingWorkspace"] == groceries["groupingWorkspace"]
        assert diffFoc[2]["OutputWorkspace"] == groceries["inputWorkspace"]
        assert normCurr[2]["InputWorkspace"] == groceries["inputWorkspace"]
        assert normCurr[2]["OutputWorkspace"] == groceries["inputWorkspace"]

    def test_cook(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()

        untensils.mantidSnapper = mockSnapper
        recipe = ReductionGroupProcessingRecipe(utensils=untensils)
        recipe._validateIngredients = unittest.mock.Mock(return_value=True)
        recipe._validateWSUnits = unittest.mock.Mock()
        groceries = {
            "inputWorkspace": "input",
            "outputWorkspace": "output",
            "groupingWorkspace": "groupingWS",
        }
        with mock.patch(ModulePatch.format("WriteWorkspaceMetadata")) as mockMetadataRecipe:
            output = recipe.cook(self.mockIngredients(), groceries)
            assert mockMetadataRecipe.called

        assert recipe.rawInput == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["outputWorkspace"]
        assert recipe.groupingWS == groceries["groupingWorkspace"]
        assert output == groceries["outputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.FocusSpectraAlgorithm.called
        assert mockSnapper.NormalizeByCurrentButTheCorrectWay.called

    def test_cater(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()

        untensils.mantidSnapper = mockSnapper
        recipe = ReductionGroupProcessingRecipe(utensils=untensils)
        recipe._validateWSUnits = unittest.mock.Mock()
        recipe._validateIngredients = unittest.mock.Mock(return_value=True)
        groceries = {
            "inputWorkspace": "input",
            "outputWorkspace": "output",
            "groupingWorkspace": "groupingWS",
        }
        with mock.patch(ModulePatch.format("WriteWorkspaceMetadata")) as mockMetadataRecipe:
            output = recipe.cater([(self.mockIngredients(), groceries)])
            assert mockMetadataRecipe.called

        assert recipe.rawInput == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["outputWorkspace"]
        assert recipe.groupingWS == groceries["groupingWorkspace"]
        assert output[0] == groceries["outputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.FocusSpectraAlgorithm.called
        assert mockSnapper.NormalizeByCurrentButTheCorrectWay.called

    def test_validateWSUnits(self):
        recipe = ReductionGroupProcessingRecipe()
        mockSnapper = unittest.mock.Mock()
        recipe.mantidSnapper = mockSnapper
        mockSnapper.mtd = unittest.mock.MagicMock()
        mockSnapper.mtd.__getitem__ = unittest.mock.Mock()
        mockWorkspace = mockSnapper.mtd["input"]
        mockWorkspace.getAxis().getUnit().unitID = unittest.mock.Mock(return_value="dSpacing")
        recipe._validateWSUnits("inputWorkspace", "input")
        assert mockWorkspace.getAxis().getUnit().unitID.called

    def test_validateWSUnits_invalid(self):
        recipe = ReductionGroupProcessingRecipe()
        mockSnapper = unittest.mock.Mock()
        recipe.mantidSnapper = mockSnapper
        mockSnapper.mtd = unittest.mock.MagicMock()
        mockSnapper.mtd.__getitem__ = unittest.mock.Mock()
        mockWorkspace = mockSnapper.mtd["input"]
        mockWorkspace.getAxis().getUnit().unitID = unittest.mock.Mock(return_value="TOF")

        with pytest.raises(
            RuntimeError,
            match="Input workspace input is of units TOF. Please convert it to dSpacing before using this recipe.",
        ):
            recipe._validateWSUnits("inputWorkspace", "input")
