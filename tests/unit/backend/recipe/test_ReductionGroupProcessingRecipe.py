import unittest

from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe, Utensils


# TODO: Add/update tests when EWM 4798 is complete
class ReductionGroupProcessingRecipeTest(unittest.TestCase):
    def test_init(self):
        ReductionGroupProcessingRecipe()

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
        assert recipe.geometryOutputWS == groceries["geometryOutputWorkspace"]
        assert recipe.diffFocOutputWS == groceries["diffFocOutputWorkspace"]
        assert recipe.groupingWS == groceries["groupingWorkspace"]

    def test_queueAlgos(self):
        recipe = ReductionGroupProcessingRecipe()
        groceries = {
            "inputWorkspace": "input",
            "outputWorkspace": "output",
            "geometryOutputWorkspace": "geoWS",
            "diffFocOutputWorkspace": "diffFocWS",
            "groupingWorkspace": "groupingWS",
        }
        recipe.prep(groceries)
        recipe.queueAlgos()

        queuedAlgos = recipe.mantidSnapper._algorithmQueue
        diffFoc = queuedAlgos[0]
        normCurr = queuedAlgos[1]

        assert diffFoc[0] == "DiffractionFocussing"
        assert normCurr[0] == "NormaliseByCurrent"
        assert diffFoc[1] == "Applying Diffraction Focussing..."
        assert normCurr[1] == "Normalizing Current ..."
        assert diffFoc[2]["InputWorkspace"] == groceries["geometryOutputWorkspace"]
        assert diffFoc[2]["GroupingWorkspace"] == groceries["groupingWorkspace"]
        assert diffFoc[2]["OutputWorkspace"] == groceries["diffFocOutputWorkspace"]
        assert normCurr[2]["InputWorkspace"] == groceries["diffFocOutputWorkspace"]
        assert normCurr[2]["OutputWorkspace"] == groceries["outputWorkspace"]

    def test_cook(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        untensils.mantidSnapper = mockSnapper
        recipe = ReductionGroupProcessingRecipe(utensils=untensils)
        groceries = {
            "inputWorkspace": "input",
            "outputWorkspace": "output",
            "geometryOutputWorkspace": "geoWS",
            "diffFocOutputWorkspace": "diffFocWS",
            "groupingWorkspace": "groupingWS",
        }

        output = recipe.cook(groceries)

        assert recipe.rawInput == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["outputWorkspace"]
        assert recipe.geometryOutputWS == groceries["geometryOutputWorkspace"]
        assert recipe.diffFocOutputWS == groceries["diffFocOutputWorkspace"]
        assert recipe.groupingWS == groceries["groupingWorkspace"]
        assert output == groceries["outputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.DiffractionFocussing.called
        assert mockSnapper.NormaliseByCurrent.called

    def test_cater(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        untensils.mantidSnapper = mockSnapper
        recipe = ReductionGroupProcessingRecipe(utensils=untensils)
        groceries = {
            "inputWorkspace": "input",
            "outputWorkspace": "output",
            "geometryOutputWorkspace": "geoWS",
            "diffFocOutputWorkspace": "diffFocWS",
            "groupingWorkspace": "groupingWS",
        }

        output = recipe.cater([(groceries)])

        assert recipe.rawInput == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["outputWorkspace"]
        assert recipe.geometryOutputWS == groceries["geometryOutputWorkspace"]
        assert recipe.diffFocOutputWS == groceries["diffFocOutputWorkspace"]
        assert recipe.groupingWS == groceries["groupingWorkspace"]
        assert output[0] == groceries["outputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.DiffractionFocussing.called
        assert mockSnapper.NormaliseByCurrent.called
