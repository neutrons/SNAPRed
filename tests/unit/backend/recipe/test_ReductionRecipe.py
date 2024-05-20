from unittest import TestCase, mock

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from snapred.backend.recipe.ReductionRecipe import (
    GenerateFocussedVanadiumRecipe,
    PreprocessReductionRecipe,
    ReductionGroupProcessingRecipe,
    ReductionRecipe,
)
from util.SculleryBoy import SculleryBoy


class ReductionRecipeTest(TestCase):
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

    def test_chopIngredients(self):
        recipe = ReductionRecipe()
        ingredients = mock.Mock()
        recipe.chopIngredients(ingredients)
        assert ingredients == recipe.ingredients

    def test_unbagGroceries(self):
        recipe = ReductionRecipe()

        groceries = {
            "inputWorkspace": "sample",
            "normalizationWorkspace": "norm",
            "groupingWorkspaces": ["group1", "group2"],
        }
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.normalizationWs == groceries["normalizationWorkspace"]
        assert recipe.groupWorkspaces == groceries["groupingWorkspaces"]

        groceries = {
            "inputWorkspace": "sample",
        }
        with pytest.raises(KeyError):
            recipe.unbagGroceries(groceries)

    def test_cloneWorkspace(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()
        inputWS = "input"
        outputWS = "out"
        resultWS = recipe._cloneWorkspace(inputWS, outputWS)

        assert recipe.mantidSnapper.CloneWorkspace.called_once_with(
            mock.ANY, InputWorkspace=inputWS, OutputWorkspace=outputWS
        )
        assert recipe.mantidSnapper.executeQueue.called
        assert resultWS == outputWS

    def test_deleteWorkspace(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()
        workspace = "input"
        recipe._deleteWorkspace(workspace)

        assert recipe.mantidSnapper.DeleteWorkspace.called_once_with(mock.ANY, Workspace=workspace)
        assert recipe.mantidSnapper.executeQueue.called

    def test_applyRecipe(self):
        recipe = ReductionRecipe()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()

        mockRecipe = mock.Mock()
        inputWS = "input"
        recipe._applyRecipe(mockRecipe, inputWS)

        assert recipe.groceries["inputWorkspace"] == inputWS
        assert mockRecipe.cook.called_once_with(recipe.ingredients, recipe.groceries)

    def test_prepGroupWorkspaces(self):
        recipe = ReductionRecipe()
        recipe.groupWorkspaces = ["group1", "group2"]
        recipe.normalizationWs = "norm"
        recipe.mantidSnapper = mock.Mock()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()
        recipe.ingredients.pixelGroups = [mock.Mock(), mock.Mock()]
        recipe.ingredients.detectorPeaksMany = [["peaks"], ["peaks2"]]

        recipe.sampleWs = "sample"
        recipe._cloneWorkspace = mock.Mock()
        recipe._cloneWorkspace.return_value = "cloned"

        sampleClone, normClone = recipe._prepGroupWorkspaces(0)

        assert recipe.groceries["inputWorkspace"] == "cloned"
        assert recipe.groceries["normalizationWorkspace"] == "cloned"
        assert recipe.ingredients.pixelGroup == recipe.ingredients.pixelGroups[0]
        assert recipe.ingredients.detectorPeaks == ["peaks"]
        assert sampleClone == "cloned"
        assert normClone == "cloned"

    def test_stubs(self):
        recipe = ReductionRecipe()
        recipe.validateInputs(None, None)
        recipe.queueAlgos()

    @mock.patch("snapred.backend.recipe.ReductionRecipe.ApplyNormalizationRecipe")
    def test_applyNormalization(self, mockApplyNormalizationRecipe):
        recipe = ReductionRecipe()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()
        recipe.ingredients.pixelGroup = "group"
        recipe.ingredients.detectorPeaks = ["peaks"]

        recipe._applyNormalization("sample", "norm")

        assert mockApplyNormalizationRecipe.cook.called_once_with(recipe.ingredients, recipe.groceries)
        assert recipe.groceries["inputWorkspace"] == "sample"
        assert recipe.groceries["normalizationWorkspace"] == "norm"

    def test_cloneIntermediateWorkspace(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()
        recipe.mantidSnapper.MakeDirtyDish = mock.Mock()
        recipe._cloneIntermediateWorkspace("input", "output")
        assert recipe.mantidSnapper.MakeDirtyDish.called_once_with(
            mock.ANY, InputWorkspace="input", OutputWorkspace="output"
        )

    def test_execute(self):
        recipe = ReductionRecipe()
        recipe.groceries = {}
        recipe._applyRecipe = mock.Mock()
        recipe._cloneIntermediateWorkspace = mock.Mock()
        recipe._applyNormalization = mock.Mock()
        recipe._deleteWorkspace = mock.Mock()
        recipe._prepGroupWorkspaces = mock.Mock()
        recipe._prepGroupWorkspaces.return_value = ("sample_grouped", "norm_grouped")
        recipe.sampleWs = "sample"
        recipe.normalizationWs = "norm"
        recipe.groupWorkspaces = ["group1", "group2"]

        output = recipe.execute()

        assert recipe._applyRecipe.called_once_with(PreprocessReductionRecipe, recipe.sampleWs)
        assert recipe._applyRecipe.called_once_with(PreprocessReductionRecipe, recipe.normalizationWs)

        assert recipe._applyRecipe.called_once_with(ReductionGroupProcessingRecipe, "sample_grouped")
        assert recipe._applyRecipe.called_once_with(ReductionGroupProcessingRecipe, "norm_grouped")

        assert recipe._applyRecipe.called_once_with(GenerateFocussedVanadiumRecipe, "norm_grouped")

        assert recipe._applyNormalization.called_once_with("sample_grouped", "norm_grouped")

        assert recipe._deleteWorkspace.called_once_with("norm_grouped")

        assert output[0] == "sample_grouped"

    def test_cook(self):
        recipe = ReductionRecipe()
        recipe.prep = mock.Mock()
        recipe.execute = mock.Mock()
        recipe.cook(None, None)
        assert recipe.prep.called
        assert recipe.execute.called

    def test_cater(self):
        recipe = ReductionRecipe()
        recipe.cook = mock.Mock()
        mockIngredients = mock.Mock()
        mockGroceries = mock.Mock()
        pallet = (mockIngredients, mockGroceries)
        shipment = [pallet]
        output = recipe.cater(shipment)
        assert recipe.cook.called_once_with(mockIngredients, mockGroceries)
        assert output[0] == recipe.cook.return_value
