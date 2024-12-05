import unittest

from mantid.simpleapi import (
    CreateEmptyTableWorkspace,
    CreateSampleWorkspace,
    LoadInstrument,
    mtd,
)
from util.helpers import createCompatibleMask
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
        maskWS = mtd.unique_name(prefix="test_preprocess_reduction")

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
        createCompatibleMask(maskWS, sampleWS)

        return {
            "inputWorkspace": sampleWS,
            "diffcalWorkspace": calibWS,
            "maskWorkspace": maskWS,
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

        groceries = {"inputWorkspace": "sample", "diffcalWorkspace": "diffcal", "maskWorkspace": "mask"}
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert recipe.maskWs == groceries["maskWorkspace"]

        groceries = {"inputWorkspace": "sample"}
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == ""
        assert recipe.maskWs == ""

    def test_queueAlgos(self):
        recipe = PreprocessReductionRecipe()
        ingredients = Ingredients()
        groceries = self._make_groceries()
        recipe.prep(ingredients, groceries)
        recipe.queueAlgos()

        queuedAlgos = recipe.mantidSnapper._algorithmQueue
        maskDetectorFlagsTuple = queuedAlgos[0]
        applyDiffCalTuple = queuedAlgos[1]

        assert maskDetectorFlagsTuple[0] == "MaskDetectorFlags"
        assert applyDiffCalTuple[0] == "ApplyDiffCal"

        # Excessive testing maybe?
        assert maskDetectorFlagsTuple[2]["MaskWorkspace"] == groceries["maskWorkspace"]
        assert maskDetectorFlagsTuple[2]["OutputWorkspace"] == groceries["inputWorkspace"]
        assert applyDiffCalTuple[2]["InstrumentWorkspace"] == groceries["inputWorkspace"]
        assert applyDiffCalTuple[2]["CalibrationWorkspace"] == groceries["diffcalWorkspace"]

    def test_cook(self):
        utensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        utensils.mantidSnapper = mockSnapper
        recipe = PreprocessReductionRecipe(utensils=utensils)
        ingredients = Ingredients()
        groceries = self._make_groceries()
        del groceries["maskWorkspace"]

        output = recipe.cook(ingredients, groceries)

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert recipe.maskWs == ""
        assert output == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called
        assert not mockSnapper.MaskDetectorFlags.called

        groceries = self._make_groceries()
        output = recipe.cook(ingredients, groceries)

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert recipe.maskWs == groceries["maskWorkspace"]
        assert output == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called
        assert mockSnapper.MaskDetectorFlags.called

        groceries["outputWorkspace"] = "output"
        output = recipe.cook(ingredients, groceries)

        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert recipe.maskWs == groceries["maskWorkspace"]
        assert recipe.outputWs == groceries["outputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called
        assert mockSnapper.MaskDetectorFlags.called
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
        assert recipe.maskWs == groceries["maskWorkspace"]
        assert output[0] == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        assert mockSnapper.ApplyDiffCal.called
        assert mockSnapper.MaskDetectorFlags.called
