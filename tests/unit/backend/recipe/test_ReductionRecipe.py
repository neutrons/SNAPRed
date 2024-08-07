from unittest import TestCase, mock

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from snapred.backend.recipe.ReductionRecipe import (
    ApplyNormalizationRecipe,
    GenerateFocussedVanadiumRecipe,
    PreprocessReductionRecipe,
    ReductionGroupProcessingRecipe,
    ReductionRecipe,
)
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
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
        assert ingredients.copy() == recipe.ingredients

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
        assert recipe.groupingWorkspaces == groceries["groupingWorkspaces"]

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

    def test_cloneAndConvertWorkspace(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()
        workspace = wng.run().runNumber("555").lite(True).build()
        units = "dSpacing"
        recipe._cloneAndConvertWorkspace(workspace, units)
        units = "MomentumTransfer"
        recipe._cloneAndConvertWorkspace(workspace, units)
        units = "Wavelength"
        recipe._cloneAndConvertWorkspace(workspace, units)

        assert recipe.mantidSnapper.ConvertUnits.called_once_with(mock.ANY, Workspace=workspace)
        assert recipe.mantidSnapper.executeQueue.called

    def test_keepUnfocusedData(self):
        recipe = ReductionRecipe()
        recipe.groceries = {}

        recipe.ingredients = mock.Mock()
        recipe.ingredients.groupProcessing = mock.Mock(
            return_value=lambda groupingIndex: f"groupProcessing_{groupingIndex}"
        )
        recipe.ingredients.generateFocussedVanadium = mock.Mock(
            return_value=lambda groupingIndex: f"generateFocussedVanadium_{groupingIndex}"
        )
        recipe.ingredients.applyNormalization = mock.Mock(
            return_value=lambda groupingIndex: f"applyNormalization_{groupingIndex}"
        )

        recipe._applyRecipe = mock.Mock()
        recipe._cloneIntermediateWorkspace = mock.Mock()
        recipe._deleteWorkspace = mock.Mock()
        recipe._cloneAndConvertWorkspace = mock.Mock()
        recipe._prepGroupingWorkspaces = mock.Mock()
        recipe._prepGroupingWorkspaces.return_value = ("sample_grouped", "norm_grouped")
        recipe.sampleWs = "sample"
        recipe.maskWs = "mask"
        recipe.normalizationWs = "norm"
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "dSpacing"

        result = recipe.execute()

        assert recipe._cloneAndConvertWorkspace.called_once_with("sample", "dSpacing")
        assert recipe._deleteWorkspace.called_once_with("norm_grouped")
        assert result["outputs"][0] == "sample_grouped"

    def test_applyRecipe(self):
        recipe = ReductionRecipe()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()
        recipe.mantidSnapper = mock.Mock()
        recipe.mantidSnapper.mtd = mock.Mock()
        recipe.mantidSnapper.mtd.doesExist = mock.Mock()
        recipe.mantidSnapper.mtd.doesExist.return_value = True

        mockRecipe = mock.Mock()
        inputWS = "input"
        recipe._applyRecipe(mockRecipe, recipe.ingredients, inputWorkspace=inputWS)

        assert recipe.groceries["inputWorkspace"] == inputWS
        assert mockRecipe.cook.called_once_with(recipe.ingredients, recipe.groceries)

    def test_applyRecipe_no_input_workspace(self):
        recipe = ReductionRecipe()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()
        recipe.mantidSnapper = mock.Mock()
        recipe.mantidSnapper.mtd = mock.Mock()
        recipe.mantidSnapper.mtd.doesExist = mock.Mock()
        recipe.mantidSnapper.mtd.doesExist.return_value = False
        recipe.logger = mock.Mock()
        recipe.logger.return_value = mock.Mock()
        recipe.logger().warning = mock.Mock()

        recipe._applyRecipe(mock.Mock(), recipe.ingredients, inputWorkspace="input")

        recipe.logger().warning.assert_called_once()

    def test_prepGroupingWorkspaces(self):
        recipe = ReductionRecipe()
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.normalizationWs = "norm"
        recipe.mantidSnapper = mock.Mock()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()
        recipe.ingredients.pixelGroups = [mock.Mock(), mock.Mock()]
        recipe.ingredients.detectorPeaksMany = [["peaks"], ["peaks2"]]

        recipe.sampleWs = "sample"
        recipe._cloneWorkspace = mock.Mock()
        recipe._cloneWorkspace.return_value = "cloned"

        sampleClone, normClone = recipe._prepGroupingWorkspaces(0)

        assert recipe.groceries["inputWorkspace"] == "cloned"
        assert recipe.groceries["normalizationWorkspace"] == "cloned"
        assert sampleClone == "cloned"
        assert normClone == "cloned"

    def test_prepGroupingWorkspaces_no_normalization(self):
        recipe = ReductionRecipe()
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.normalizationWs = None
        recipe.mantidSnapper = mock.Mock()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()
        recipe.ingredients.pixelGroups = [mock.Mock(), mock.Mock()]
        recipe.ingredients.detectorPeaksMany = [["peaks"], ["peaks2"]]

        recipe.sampleWs = "sample"
        recipe._cloneWorkspace = mock.Mock()
        recipe._cloneWorkspace.return_value = "cloned"

        recipe._deleteWorkspace = mock.Mock()

        sampleClone, normClone = recipe._prepGroupingWorkspaces(0)

        assert recipe.groceries["inputWorkspace"] == "cloned"
        assert "normalizationWorkspace" not in recipe.groceries
        assert sampleClone == "cloned"
        assert normClone is None
        recipe._deleteWorkspace.assert_not_called()

    def test_stubs(self):
        recipe = ReductionRecipe()
        recipe.validateInputs(None, None)
        recipe.queueAlgos()

    @mock.patch("snapred.backend.recipe.ReductionRecipe.ApplyNormalizationRecipe")
    def test_applyNormalization(self, mockApplyNormalizationRecipe):
        recipe = ReductionRecipe()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()
        recipe.ingredients.pixelGroups = ["group"]
        recipe.ingredients.detectorPeaksMany = [["peaks"]]
        recipe.mantidSnapper = mock.Mock()
        recipe.mantidSnapper.mtd = mock.Mock()
        recipe.mantidSnapper.mtd.doesExist = mock.Mock()
        recipe.mantidSnapper.mtd.doesExist.return_value = True

        groupingIndex = 0
        recipe._applyRecipe(
            mockApplyNormalizationRecipe,
            recipe.ingredients.applyNormalization(groupingIndex),
            inputWorkspace="sample",
            normalizationWorkspace="norm",
        )

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

        recipe.ingredients = mock.Mock()
        recipe.ingredients.groupProcessing = mock.Mock(
            return_value=lambda groupingIndex: f"groupProcessing_{groupingIndex}"
        )
        recipe.ingredients.generateFocussedVanadium = mock.Mock(
            return_value=lambda groupingIndex: f"generateFocussedVanadium_{groupingIndex}"
        )
        recipe.ingredients.applyNormalization = mock.Mock(
            return_value=lambda groupingIndex: f"applyNormalization_{groupingIndex}"
        )

        recipe._applyRecipe = mock.Mock()
        recipe._cloneIntermediateWorkspace = mock.Mock()
        recipe._deleteWorkspace = mock.Mock()
        recipe._cloneAndConvertWorkspace = mock.Mock()
        recipe._prepGroupingWorkspaces = mock.Mock()
        recipe._prepGroupingWorkspaces.return_value = ("sample_grouped", "norm_grouped")
        recipe.sampleWs = "sample"
        recipe.maskWs = "mask"
        recipe.normalizationWs = "norm"
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "TOF"

        result = recipe.execute()

        ingredients = recipe.ingredients()
        assert recipe._applyRecipe.called_once_with(PreprocessReductionRecipe, recipe.sampleWs)
        assert recipe._applyRecipe.called_once_with(PreprocessReductionRecipe, recipe.normalizationWs)

        assert recipe._applyRecipe.called_once_with(
            ReductionGroupProcessingRecipe, ingredients.groupProcessing(0), "sample_grouped"
        )
        assert recipe._applyRecipe.called_once_with(
            ReductionGroupProcessingRecipe, ingredients.groupProcessing(1), "norm_grouped"
        )

        assert recipe._applyRecipe.called_once_with(
            GenerateFocussedVanadiumRecipe, ingredients.generateFocussedVanadium(0), "norm_grouped"
        )
        assert recipe._applyRecipe.called_once_with(
            GenerateFocussedVanadiumRecipe, ingredients.generateFocussedVanadium(1), "norm_grouped"
        )

        assert recipe._applyRecipe.called_once_with(
            ApplyNormalizationRecipe, ingredients.applyNormalization(0), "sample_grouped", "norm_grouped"
        )
        assert recipe._applyRecipe.called_once_with(
            ApplyNormalizationRecipe, ingredients.applyNormalization(1), "sample_grouped", "norm_grouped"
        )

        assert recipe._deleteWorkspace.called_once_with("norm_grouped")
        assert result["outputs"][0] == "sample_grouped"

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
