import unittest
from unittest import mock

from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe


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
            "geometryOutputWorkspace": "geoWS",
            "diffFocOutputWorkspace": "diffFocWS",
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
        convertUnits = queuedAlgos[i]
        diffFoc = queuedAlgos[i := i + 1]
        normCurr = queuedAlgos[i := i + 1]

        assert convertUnits[0] == "ConvertUnits"
        assert diffFoc[0] == "FocusSpectraAlgorithm"
        assert normCurr[0] == "NormalizeByCurrentButTheCorrectWay"
        assert convertUnits[1] == "Converting to TOF..."
        assert diffFoc[1] == "Focusing Spectra..."
        assert normCurr[1] == "Normalizing Current ... but the correct way!"
        assert convertUnits[2]["InputWorkspace"] == groceries["inputWorkspace"]
        assert convertUnits[2]["OutputWorkspace"] == groceries["inputWorkspace"]
        assert convertUnits[2]["Target"] == "TOF"
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
        groceries = {
            "inputWorkspace": "input",
            "outputWorkspace": "output",
            "geometryOutputWorkspace": "geoWS",
            "diffFocOutputWorkspace": "diffFocWS",
            "groupingWorkspace": "groupingWS",
        }

        output = recipe.cook(self.mockIngredients(), groceries)

        assert recipe.rawInput == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["inputWorkspace"]
        assert recipe.groupingWS == groceries["groupingWorkspace"]
        assert output == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.FocusSpectraAlgorithm.called
        assert mockSnapper.NormalizeByCurrentButTheCorrectWay.called

    def test_cater(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        untensils.mantidSnapper = mockSnapper
        recipe = ReductionGroupProcessingRecipe(utensils=untensils)
        recipe._validateIngredients = unittest.mock.Mock(return_value=True)
        groceries = {
            "inputWorkspace": "input",
            "outputWorkspace": "output",
            "geometryOutputWorkspace": "geoWS",
            "diffFocOutputWorkspace": "diffFocWS",
            "groupingWorkspace": "groupingWS",
        }

        output = recipe.cater([(self.mockIngredients(), groceries)])

        assert recipe.rawInput == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["inputWorkspace"]
        assert recipe.groupingWS == groceries["groupingWorkspace"]
        assert output[0] == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.FocusSpectraAlgorithm.called
        assert mockSnapper.NormalizeByCurrentButTheCorrectWay.called
