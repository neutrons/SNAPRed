import time
from unittest import TestCase, mock

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from snapred.backend.dao.ingredients import ReductionIngredients
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

    def test_mandatory_inputs(self):
        inputs = ReductionRecipe().mandatoryInputWorkspaces()
        assert inputs == {"inputWorkspace", "groupingWorkspaces"}
        ReductionRecipe().logger()

    def test_cloneWorkspace(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()
        inputWS = "input"
        outputWS = "out"
        resultWS = recipe._cloneWorkspace(inputWS, outputWS)

        recipe.mantidSnapper.CloneWorkspace.assert_called_once_with(
            mock.ANY, InputWorkspace=inputWS, OutputWorkspace=outputWS
        )
        recipe.mantidSnapper.executeQueue.assert_called()
        assert resultWS == outputWS

    def test_deleteWorkspace(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()
        workspace = "input"
        recipe._deleteWorkspace(workspace)

        recipe.mantidSnapper.DeleteWorkspace.assert_called_once_with(mock.ANY, Workspace=workspace)
        recipe.mantidSnapper.executeQueue.assert_called()

    def test_prepareUnfocusedData(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()
        workspace = wng.run().runNumber("555").lite(True).build()

        # TODO: All units are tested here for now, this will be changed when EWM 6615 is completed
        units = "dSpacing"
        msg = f"Converting unfocused data to {units} units"
        recipe._prepareUnfocusedData(workspace, None, units)
        outputWs = wng.run().runNumber("555").lite(True).unit(wng.Units.DSP).group(wng.Groups.UNFOC).build()
        recipe.mantidSnapper.ConvertUnits.assert_called_once_with(
            msg, InputWorkspace=outputWs, OutputWorkspace=outputWs, Target=units
        )
        recipe.mantidSnapper.executeQueue.assert_called()
        recipe.mantidSnapper.reset_mock()

        units = "MomentumTransfer"
        msg = f"Converting unfocused data to {units} units"
        recipe._prepareUnfocusedData(workspace, None, units)
        outputWs = wng.run().runNumber("555").lite(True).unit(wng.Units.QSP).group(wng.Groups.UNFOC).build()
        recipe.mantidSnapper.ConvertUnits.assert_called_once_with(
            msg, InputWorkspace=outputWs, OutputWorkspace=outputWs, Target=units
        )
        recipe.mantidSnapper.executeQueue.assert_called()
        recipe.mantidSnapper.reset_mock()

        units = "Wavelength"
        msg = f"Converting unfocused data to {units} units"
        recipe._prepareUnfocusedData(workspace, None, units)
        outputWs = wng.run().runNumber("555").lite(True).unit(wng.Units.LAM).group(wng.Groups.UNFOC).build()
        recipe.mantidSnapper.ConvertUnits.assert_called_once_with(
            msg, InputWorkspace=outputWs, OutputWorkspace=outputWs, Target=units
        )
        recipe.mantidSnapper.executeQueue.assert_called()
        recipe.mantidSnapper.reset_mock()

        units = "TOF"
        msg = f"Converting unfocused data to {units} units"
        recipe._prepareUnfocusedData(workspace, None, units)
        outputWs = wng.run().runNumber("555").lite(True).unit(wng.Units.TOF).group(wng.Groups.UNFOC).build()
        recipe.mantidSnapper.ConvertUnits.assert_called_once_with(
            msg, InputWorkspace=outputWs, OutputWorkspace=outputWs, Target=units
        )
        recipe.mantidSnapper.executeQueue.assert_called()
        recipe.mantidSnapper.reset_mock()

        units = "NOT_A_UNIT"
        with pytest.raises(ValueError, match=r"cannot convert to unit.*"):
            recipe._prepareUnfocusedData(workspace, None, units)

    def test_prepareUnfocusedData_masking(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()

        units = "dSpacing"
        workspace = wng.run().runNumber("555").lite(True).build()
        maskWs = wng.reductionUserPixelMask().numberTag(2).build()
        outputWs = wng.run().runNumber("555").lite(True).unit(wng.Units.DSP).group(wng.Groups.UNFOC).build()

        recipe._prepareUnfocusedData(workspace, None, units)
        recipe.mantidSnapper.MaskDetectorFlags.assert_not_called()
        recipe.mantidSnapper.reset_mock()

        recipe._prepareUnfocusedData(workspace, maskWs, units)

        recipe.mantidSnapper.MaskDetectorFlags.assert_called_once_with(
            "Applying pixel mask to unfocused data", MaskWorkspace=maskWs, OutputWorkspace=outputWs
        )
        recipe.mantidSnapper.ConvertUnits.assert_called_once_with(
            f"Converting unfocused data to {units} units",
            InputWorkspace=outputWs,
            OutputWorkspace=outputWs,
            Target=units,
        )
        recipe.mantidSnapper.executeQueue.assert_called()
        recipe.mantidSnapper.reset_mock()

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_keepUnfocusedData(self, mockMtd):
        mockMantidSnapper = mock.Mock()

        mockMaskWorkspace = mock.Mock()
        mockGroupWorkspace = mock.Mock()

        mockGroupWorkspace.getNumberHistograms.return_value = 10
        mockGroupWorkspace.readY.return_value = [0] * 10
        mockMaskWorkspace.readY.return_value = [0] * 10

        # Mock mtd to return mask and group workspaces
        mockMtd.__getitem__.side_effect = lambda ws_name: mockMaskWorkspace if ws_name == "mask" else mockGroupWorkspace
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mockMantidSnapper
        recipe.mantidSnapper.mtd = mockMtd

        # Set up ingredients and other variables for the recipe
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

        # Mock internal methods of recipe
        recipe._applyRecipe = mock.Mock()
        recipe._cloneIntermediateWorkspace = mock.Mock()
        recipe._deleteWorkspace = mock.Mock()
        recipe._prepareUnfocusedData = mock.Mock()
        recipe._prepGroupingWorkspaces = mock.Mock()
        recipe._prepGroupingWorkspaces.return_value = ("sample_grouped", "norm_grouped")

        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.maskWs = "mask"
        recipe.normalizationWs = "norm"
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "dSpacing"

        # Execute the recipe
        result = recipe.execute()

        # Assertions
        recipe._prepareUnfocusedData.assert_called_once_with("sample", "mask", "dSpacing")
        assert recipe._deleteWorkspace.call_count == len(recipe._prepGroupingWorkspaces.return_value)
        recipe._deleteWorkspace.assert_called_with("norm_grouped")
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
        mockRecipe().cook.assert_called_once_with(recipe.ingredients, recipe.groceries)

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

        with pytest.raises(RuntimeError, match=r".*Missing non-default input workspace.*"):
            recipe._applyRecipe(mock.MagicMock(__name__="SomeRecipe"), recipe.ingredients, inputWorkspace="input")

    def test_applyRecipe_default_empty_input_workspace(self):
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

        recipe._applyRecipe(mock.MagicMock(__name__="SomeRecipe"), recipe.ingredients, inputWorkspace="")

        recipe.logger().debug.assert_called_once()

    def test_prepGroupingWorkspaces(self):
        recipe = ReductionRecipe()
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.normalizationWs = "norm"
        recipe.mantidSnapper = mock.Mock()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[mock.Mock(), mock.Mock()],
            detectorPeaksMany=[["peaks"], ["peaks2"]],
        )

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
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[mock.Mock(), mock.Mock()],
            detectorPeaksMany=[["peaks"], ["peaks2"]],
        )

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

        mockApplyNormalizationRecipe().cook.assert_called_once_with(
            recipe.ingredients.applyNormalization(groupingIndex), recipe.groceries
        )
        assert recipe.groceries["inputWorkspace"] == "sample"
        assert recipe.groceries["normalizationWorkspace"] == "norm"

    def test_cloneIntermediateWorkspace(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()
        recipe.mantidSnapper.MakeDirtyDish = mock.Mock()
        recipe._cloneIntermediateWorkspace("input", "output")
        recipe.mantidSnapper.MakeDirtyDish.assert_called_once_with(
            mock.ANY, InputWorkspace="input", OutputWorkspace="output"
        )

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_execute(self, mockMtd):
        mockMantidSnapper = mock.Mock()

        mockMaskworkspace = mock.Mock()
        mockGroupWorkspace = mock.Mock()

        mockGroupWorkspace.getNumberHistograms.return_value = 10
        mockGroupWorkspace.readY.return_value = [0] * 10
        mockMaskworkspace.readY.return_value = [0] * 10

        mockMtd.__getitem__.side_effect = lambda ws_name: mockMaskworkspace if ws_name == "mask" else mockGroupWorkspace

        recipe = ReductionRecipe()
        recipe.mantidSnapper = mockMantidSnapper
        recipe.mantidSnapper.mtd = mockMtd

        # Set up ingredients and other variables for the recipe
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

        # Mock internal methods of recipe
        recipe._applyRecipe = mock.Mock()
        recipe._cloneIntermediateWorkspace = mock.Mock()
        recipe._deleteWorkspace = mock.Mock()
        recipe._prepareUnfocusedData = mock.Mock()
        recipe._prepGroupingWorkspaces = mock.Mock()
        recipe._prepGroupingWorkspaces.return_value = ("sample_grouped", "norm_grouped")

        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.maskWs = "mask"
        recipe.normalizationWs = "norm"
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "TOF"

        # Execute the recipe
        result = recipe.execute()

        # Perform assertions
        recipe._applyRecipe.assert_any_call(
            PreprocessReductionRecipe,
            recipe.ingredients.preprocess(),
            inputWorkspace=recipe.sampleWs,
            maskWorkspace=recipe.maskWs,
        )
        recipe._applyRecipe.assert_any_call(
            PreprocessReductionRecipe,
            recipe.ingredients.preprocess(),
            inputWorkspace=recipe.normalizationWs,
            maskWorkspace=recipe.maskWs,
        )

        recipe._applyRecipe.assert_any_call(
            ReductionGroupProcessingRecipe, recipe.ingredients.groupProcessing(0), inputWorkspace="sample_grouped"
        )
        recipe._applyRecipe.assert_any_call(
            ReductionGroupProcessingRecipe, recipe.ingredients.groupProcessing(1), inputWorkspace="norm_grouped"
        )

        recipe._applyRecipe.assert_any_call(
            GenerateFocussedVanadiumRecipe,
            recipe.ingredients.generateFocussedVanadium(0),
            inputWorkspace="norm_grouped",
        )
        recipe._applyRecipe.assert_any_call(
            GenerateFocussedVanadiumRecipe,
            recipe.ingredients.generateFocussedVanadium(1),
            inputWorkspace="norm_grouped",
        )

        recipe._applyRecipe.assert_any_call(
            ApplyNormalizationRecipe,
            recipe.ingredients.applyNormalization(0),
            inputWorkspace="sample_grouped",
            normalizationWorkspace="norm_grouped",
        )
        recipe._applyRecipe.assert_any_call(
            ApplyNormalizationRecipe,
            recipe.ingredients.applyNormalization(1),
            inputWorkspace="sample_grouped",
            normalizationWorkspace="norm_grouped",
        )

        recipe._deleteWorkspace.assert_called_with("norm_grouped")
        assert recipe._deleteWorkspace.call_count == len(recipe._prepGroupingWorkspaces.return_value)
        assert result["outputs"][0] == "sample_grouped"

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_isGroupFullyMasked(self, mockMtd):
        mockMantidSnapper = mock.Mock()

        # Mock the group and mask workspaces
        mockMaskWorkspace = mock.Mock()
        mockGroupWorkspace = mock.Mock()

        # Case 1: All pixels are masked
        mockGroupWorkspace.getNumberHistograms.return_value = 10
        mockGroupWorkspace.readY.side_effect = lambda i: [i]  # Assume each group has a single index per spectrum
        mockMaskWorkspace.readY.side_effect = lambda i: [1]  # Assume every pixel is masked # noqa: ARG005

        # Mock mtd to return the group and mask workspaces
        mockMtd.__getitem__.side_effect = lambda ws_name: mockMaskWorkspace if ws_name == "mask" else mockGroupWorkspace

        # Attach mocked mantidSnapper to recipe and assign mocked mtd
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mockMantidSnapper
        recipe.mantidSnapper.mtd = mockMtd
        recipe.maskWs = "mask"

        # Test when all pixels are masked
        result = recipe._isGroupFullyMasked("groupWorkspace")
        assert result is True, "Expected _isGroupFullyMasked to return True when all pixels are masked."

        # Case 2: Not all pixels are masked
        mockMaskWorkspace.readY.side_effect = lambda i: [0] if i % 2 == 0 else [1]  # Only half the pixels are masked

        # Test when not all pixels are masked
        result = recipe._isGroupFullyMasked("groupWorkspace")
        assert result is False, "Expected _isGroupFullyMasked to return False when not all pixels are masked."

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_execute_with_fully_masked_group(self, mockMtd):
        mock_mantid_snapper = mock.Mock()

        # Mock the mask and group workspaces
        mockMaskWorkspace = mock.Mock()
        mockGroupWorkspace = mock.Mock()

        # Mock groupWorkspace to have all pixels masked
        mockGroupWorkspace.getNumberHistograms.return_value = 10
        mockGroupWorkspace.readY.side_effect = lambda i: [i]  # Spectrum index per spectrum
        mockMaskWorkspace.readY.side_effect = lambda i: [1]  # All pixels are masked # noqa: ARG005

        # Mock mtd to return the group and mask workspaces
        mockMtd.__getitem__.side_effect = lambda ws_name: mockMaskWorkspace if ws_name == "mask" else mockGroupWorkspace

        # Attach mocked mantidSnapper to recipe and assign mocked mtd
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock_mantid_snapper
        recipe.mantidSnapper.mtd = mockMtd
        recipe.maskWs = "mask"

        # Set up logger to capture warnings
        recipe.logger = mock.Mock()

        # Set up ingredients and other variables for the recipe
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

        # Mock internal methods of recipe
        recipe._applyRecipe = mock.Mock()
        recipe._cloneIntermediateWorkspace = mock.Mock()
        recipe._deleteWorkspace = mock.Mock()
        recipe._prepareUnfocusedData = mock.Mock()
        recipe._prepGroupingWorkspaces = mock.Mock()
        recipe._prepGroupingWorkspaces.return_value = ("sample_grouped", "norm_grouped")

        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.normalizationWs = "norm"
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "TOF"

        # Execute the recipe
        result = recipe.execute()

        # Assertions for both groups being fully masked
        expected_warning_message_group1 = (
            "\nAll pixels masked within group1 schema.\n"
            "Skipping all algorithm execution for this group.\n"
            "This will affect future reductions."
        )

        expected_warning_message_group2 = (
            "\nAll pixels masked within group2 schema.\n"
            "Skipping all algorithm execution for this group.\n"
            "This will affect future reductions."
        )

        # Check that the warnings were logged for both groups
        recipe.logger().warning.assert_any_call(expected_warning_message_group1)
        recipe.logger().warning.assert_any_call(expected_warning_message_group2)

        # Ensure the warning was called twice (once per group)
        assert (
            recipe.logger().warning.call_count == 2
        ), "Expected warning to be logged twice for the fully masked groups."

        # Ensure no algorithms were applied for the fully masked groups
        assert (
            recipe._applyRecipe.call_count == 2
        ), "Expected _applyRecipe to not be called for the fully masked groups."

        # Check the output result contains the mask workspace
        assert result["outputs"][0] == "mask", "Expected the mask workspace to be included in the outputs."

    def test_cook(self):
        recipe = ReductionRecipe()
        recipe.prep = mock.Mock()
        recipe.execute = mock.Mock()
        recipe.cook(None, None)
        recipe.prep.assert_called()
        recipe.execute.assert_called()

    def test_cater(self):
        recipe = ReductionRecipe()
        recipe.cook = mock.Mock()
        mockIngredients = mock.Mock()
        mockGroceries = mock.Mock()
        pallet = (mockIngredients, mockGroceries)
        shipment = [pallet]
        output = recipe.cater(shipment)
        recipe.cook.assert_called_once_with(mockIngredients, mockGroceries)
        assert output[0] == recipe.cook.return_value
