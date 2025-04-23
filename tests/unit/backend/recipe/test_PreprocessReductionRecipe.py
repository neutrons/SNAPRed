import unittest

from mantid.simpleapi import (
    CreateEmptyTableWorkspace,
    CreateSampleWorkspace,
    LoadInstrument,
    mtd,
)
from util.SculleryBoy import SculleryBoy

from snapred.backend.dao.ingredients import PreprocessReductionIngredients as Ingredients
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.PreprocessReductionRecipe import PreprocessReductionRecipe
from snapred.meta.Config import Resource


class PreprocessReductionRecipeTest(unittest.TestCase):
    fakeInstrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    sculleryBoy = SculleryBoy()

    def _make_groceries(self):
        sampleWS = mtd.unique_name(prefix="test_preprocess_reduction")
        calibWS = mtd.unique_name(prefix="test_preprocess_reduction")

        # Create sample workspace:
        #   * warning: `createCompatibleMask` does not work correctly with
        #     single-valued workspaces.
        CreateSampleWorkspace(
            OutputWorkspace=sampleWS,
            Function="One Peak",
            NumBanks=1,
            NumMonitors=1,
            BankPixelWidth=5,
            NumEvents=500,
            Random=True,
            XUnit="TOF",
            XMin=0,
            XMax=8000,
            BinWidth=100,
        )
        LoadInstrument(
            Workspace=sampleWS,
            Filename=self.fakeInstrumentFilePath,
            RewriteSpectraMap=True,
        )

        CreateEmptyTableWorkspace(OutputWorkspace=calibWS)

        return {
            "inputWorkspace": sampleWS,
            "diffcalWorkspace": calibWS,
        }

    def test_chopIngredients(self):
        """
        recipe = PreprocessReductionRecipe()
        ingredients = Ingredients(...)
        recipe.chopIngredients(ingredients)
        """
        pass

    def test_unbagGroceries(self):
        recipe = PreprocessReductionRecipe()

        groceries = {"inputWorkspace": "sample", "diffcalWorkspace": "diffcal"}
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]

        groceries = {"inputWorkspace": "sample"}
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == ""

    def test_queueAlgos(self):
        recipe = PreprocessReductionRecipe()
        ingredients = Ingredients()
        groceries = self._make_groceries()
        recipe.prep(ingredients, groceries)
        recipe.queueAlgos()

        queuedAlgos = recipe.mantidSnapper._algorithmQueue
        applyDiffCalTuple = queuedAlgos[0]

        assert applyDiffCalTuple[0] == "ApplyDiffCal"

        # Excessive testing maybe?
        assert applyDiffCalTuple[2]["InstrumentWorkspace"] == groceries["inputWorkspace"]
        assert applyDiffCalTuple[2]["CalibrationWorkspace"] == groceries["diffcalWorkspace"]

    def test_cook(self):
        utensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        utensils.mantidSnapper = mockSnapper
        recipe = PreprocessReductionRecipe(utensils=utensils)
        ingredients = Ingredients()

        groceries = self._make_groceries()
        output = recipe.cook(ingredients, groceries)

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert output == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called

        groceries["outputWorkspace"] = "output"
        output = recipe.cook(ingredients, groceries)

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert recipe.outputWs == groceries["outputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called
        assert mockSnapper.CloneWorkspace.called

    def test_cater(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        untensils.mantidSnapper = mockSnapper
        recipe = PreprocessReductionRecipe(utensils=untensils)
        ingredients = Ingredients()
        groceries = self._make_groceries()

        output = recipe.cater([(ingredients, groceries)])

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert output[0] == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called
