import time

## Put test-related imports at the end, so that the normal non-test import sequence is unmodified.
from unittest import TestCase, mock

import pytest
from mantid.simpleapi import CreateEmptyTableWorkspace, CreateSingleValuedWorkspace, mtd
from util.Config_helpers import Config_override
from util.SculleryBoy import SculleryBoy

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.state import FocusGroup, PixelGroup, PixelGroupingParameters
from snapred.backend.recipe.ReductionRecipe import (
    ApplyNormalizationRecipe,
    EffectiveInstrumentRecipe,
    GenerateFocussedVanadiumRecipe,
    PreprocessReductionRecipe,
    ReductionGroupProcessingRecipe,
    ReductionRecipe,
)
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


class ReductionRecipeTest(TestCase):
    sculleryBoy = SculleryBoy()

    def _make_groceries(self):
        sampleWS = mtd.unique_name(prefix="test_applynorm")
        normWS = mtd.unique_name(prefix="test_applynorm")
        difcWS = mtd.unique_name(prefix="test_applynorm")
        CreateSingleValuedWorkspace(OutputWorkspace=sampleWS)
        CreateSingleValuedWorkspace(OutputWorkspace=normWS)
        CreateEmptyTableWorkspace(OutputWorkspace=difcWS)
        return {
            "inputWorkspace": sampleWS,
            "normalizationWorkspace": normWS,
            "diffcalWorkspace": difcWS,
        }

    def mockPixelGroup(self, name: str, isFullyMasked: bool, N_gid: int):
        # Create a mock `PixelGroup` named `name` with `N_gid` mock subgroup PGPs:
        #   `isFullyMasked` => all subgroups will have their `isMasked` flags set,
        #   otherwise, only some will have the `isMasked` flag set.
        return mock.Mock(
            spec=PixelGroup,
            pixelGroupingParameters={
                gid: mock.Mock(
                    spec=PixelGroupingParameters,
                    # Unless it's fully masked,
                    #   generate sometimes masked subgroups when N_gid > 1.
                    isMasked=True if isFullyMasked else (gid % 2 == 0),
                )
                for gid in range(N_gid)
            },
            focusGroup=FocusGroup(name=name, definition=f"path_for_focus_group_{name}"),
        )

    def test_validateInputs_bad_workspaces(self):
        groceries = {
            "inputWorkspace": mock.sentinel.input,
            "groupingWorkspaces": mock.sentinel.groupws,
            "diffcalWorkspace": mock.sentinel.difc,
            "normalizationWorkspace": mock.sentinel.norm,
            "combinedPixelMask": mock.sentinel.mask,
            "notInTheList": mock.sentinel.bad,
        }
        with pytest.raises(ValueError, match=r".*input groceries: \{'notInTheList'\}"):
            ReductionRecipe().validateInputs(None, groceries)

    def test_chopIngredients(self):
        recipe = ReductionRecipe()
        ingredients = mock.Mock()
        recipe.chopIngredients(ingredients)
        assert ingredients.copy() == recipe.ingredients

    def test_unbagGroceries(self):
        recipe = ReductionRecipe()

        groceries = {
            "inputWorkspace": "sample",
            "diffcalWorkspace": "diffcal_table",
            "normalizationWorkspace": "norm",
            "combinedPixelMask": "mask",
            "groupingWorkspaces": ["group1", "group2"],
        }
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
        assert recipe.diffcalWs == groceries["diffcalWorkspace"]
        assert recipe.normalizationWs == groceries["normalizationWorkspace"]
        assert recipe.maskWs == groceries["combinedPixelMask"]
        assert recipe.groupingWorkspaces == groceries["groupingWorkspaces"]

    def test_unbagGroceries_required_keys(self):
        # Verify that all of the keys are required.

        recipe = ReductionRecipe()
        requiredKeys = ["inputWorkspace", "groupingWorkspaces"]
        for key in requiredKeys:
            groceries = {k: k for k in requiredKeys if k != key}
            with pytest.raises(KeyError, match=key):
                recipe.unbagGroceries(groceries)

    def test_unbagGroceries_optional_keys(self):
        # Verify that optional keys are initialized with default values.

        recipe = ReductionRecipe()
        requiredKeys = ["inputWorkspace", "groupingWorkspaces"]
        optionalKeys = ["normalizationWorkspace", "combinedPixelMask"]
        for key in optionalKeys:
            groceries = {k: k for k in requiredKeys}
            groceries.update({k: k for k in optionalKeys if k != key})
            recipe.unbagGroceries(groceries)
            match key:
                case "normalizationWorkspace":
                    assert recipe.normalizationWs == ""
                case "combinedMask":
                    assert recipe.maskWs == ""

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
        msg = f"Converting unfocused data to {units}"
        recipe._prepareUnfocusedData(workspace, None, units)
        outputWs = (
            wng.run().runNumber("555").lite(True).unit(wng.Units.DSP).group(wng.Groups.UNFOC).hidden(True).build()
        )
        recipe.mantidSnapper.ConvertUnits.assert_called_once_with(
            msg, InputWorkspace=outputWs, OutputWorkspace=outputWs, Target=units
        )
        recipe.mantidSnapper.executeQueue.assert_called()
        recipe.mantidSnapper.reset_mock()

        units = "MomentumTransfer"
        msg = f"Converting unfocused data to {units}"
        recipe._prepareUnfocusedData(workspace, None, units)
        outputWs = (
            wng.run().runNumber("555").lite(True).unit(wng.Units.QSP).group(wng.Groups.UNFOC).hidden(True).build()
        )
        recipe.mantidSnapper.ConvertUnits.assert_called_once_with(
            msg, InputWorkspace=outputWs, OutputWorkspace=outputWs, Target=units
        )
        recipe.mantidSnapper.executeQueue.assert_called()
        recipe.mantidSnapper.reset_mock()

        units = "Wavelength"
        msg = f"Converting unfocused data to {units}"
        recipe._prepareUnfocusedData(workspace, None, units)
        outputWs = (
            wng.run().runNumber("555").lite(True).unit(wng.Units.LAM).group(wng.Groups.UNFOC).hidden(True).build()
        )
        recipe.mantidSnapper.ConvertUnits.assert_called_once_with(
            msg, InputWorkspace=outputWs, OutputWorkspace=outputWs, Target=units
        )
        recipe.mantidSnapper.executeQueue.assert_called()
        recipe.mantidSnapper.reset_mock()

        units = "TOF"
        msg = f"Converting unfocused data to {units}"
        recipe._prepareUnfocusedData(workspace, None, units)
        outputWs = (
            wng.run().runNumber("555").lite(True).unit(wng.Units.TOF).group(wng.Groups.UNFOC).hidden(True).build()
        )
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

        # Generate the pre-output, and final output workspace names.
        preOutputWs = (
            wng.run().runNumber("555").lite(True).unit(wng.Units.DSP).group(wng.Groups.UNFOC).hidden(True).build()
        )
        outputWs = preOutputWs.builder.hidden(False).build()

        # Test that `MaskDetectorFlags` is only called when a mask is used.
        recipe._prepareUnfocusedData(workspace, None, units)
        recipe.mantidSnapper.MaskDetectorFlags.assert_not_called()
        recipe.mantidSnapper.reset_mock()

        # Test that the mask is applied to the unfocused data.
        recipe._prepareUnfocusedData(workspace, maskWs, units)

        recipe.mantidSnapper.MaskDetectorFlags.assert_called_once_with(
            "Applying pixel mask to unfocused data", MaskWorkspace=maskWs, OutputWorkspace=preOutputWs
        )
        recipe.mantidSnapper.ConvertUnits.assert_called_once_with(
            f"Converting unfocused data to {units}",
            InputWorkspace=preOutputWs,
            OutputWorkspace=preOutputWs,
            Target=units,
        )

        # Test the output add-or-replace sequence.
        recipe.mantidSnapper.RenameWorkspace.assert_called_once_with(
            "Add-or-replace unfocused data",
            OutputWorkspace=outputWs,
            InputWorkspace=preOutputWs,
            OverwriteExisting=True,
        )

        recipe.mantidSnapper.executeQueue.assert_called_once()
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
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                self.mockPixelGroup(name="Column", isFullyMasked=False, N_gid=6),
                self.mockPixelGroup(name="Bank", isFullyMasked=False, N_gid=2),
            ],
            detectorPeaksMany=[["peaks"], ["peaks2"]],
        )
        recipe.ingredients.artificialNormalizationIngredients = None
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
        # Output workspace name must actually be a `WorkspaceName` instance.
        preReducedOutputWs = (
            wng.reductionOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group("grouped")
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )
        recipe._prepGroupingWorkspaces.return_value = (preReducedOutputWs, "norm_grouped")
        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.diffcalWs = "diffcal_table"
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

        finalReducedOutputWs = preReducedOutputWs.builder.hidden(False).build()
        assert result["outputs"][0] == finalReducedOutputWs

    def test_applyRecipe(self):
        recipe = ReductionRecipe()
        recipe.ingredients = mock.Mock()
        recipe.mantidSnapper = mock.Mock()
        recipe.mantidSnapper.mtd = mock.Mock()
        recipe.mantidSnapper.mtd.doesExist = mock.Mock()
        recipe.mantidSnapper.mtd.doesExist.return_value = True

        mockRecipe = mock.Mock()
        inputWs = "input"
        recipe._applyRecipe(mockRecipe, recipe.ingredients, inputWorkspace=inputWs)

        mockRecipe().cook.assert_called_once_with(recipe.ingredients, groceries={"inputWorkspace": inputWs})

    def test_prepGroupingWorkspaces(self):
        recipe = ReductionRecipe()
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.normalizationWs = "norm"
        recipe.mantidSnapper = mock.Mock()
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                mock.Mock(
                    spec=PixelGroup, focusGroup=FocusGroup(name="first_grouping_name", definition="first_grouping_path")
                ),
                mock.Mock(
                    spec=PixelGroup,
                    focusGroup=FocusGroup(name="second_grouping_name", definition="second_grouping_path"),
                ),
            ],
            detectorPeaksMany=[["peaks"], ["peaks2"]],
        )

        # Output workspace name must actually be a `WorkspaceName` instance.
        preReducedOutputWsName = (
            wng.reductionOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group(recipe.ingredients.pixelGroups[0].focusGroup.name.lower())
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )

        normWsName = f"reduced_normalization_0_{wnvf.formatTimestamp(recipe.ingredients.timestamp)}"

        recipe.sampleWs = "sample"
        recipe._cloneWorkspace = mock.Mock(side_effect=lambda inputWs, outputWs: outputWs)  # noqa: ARG005

        sampleClone, normClone = recipe._prepGroupingWorkspaces(0)
        assert sampleClone == preReducedOutputWsName
        assert normClone == normWsName

    def test_prepGroupingWorkspaces_no_normalization(self):
        recipe = ReductionRecipe()
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.normalizationWs = None
        recipe.mantidSnapper = mock.Mock()
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                mock.Mock(
                    spec=PixelGroup, focusGroup=FocusGroup(name="first_grouping_name", definition="first_grouping_path")
                ),
                mock.Mock(
                    spec=PixelGroup,
                    focusGroup=FocusGroup(name="second_grouping_name", definition="second_grouping_path"),
                ),
            ],
            detectorPeaksMany=[["peaks"], ["peaks2"]],
        )

        # Output workspace name must actually be a `WorkspaceName` instance.
        preReducedOutputWsName = (
            wng.reductionOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group(recipe.ingredients.pixelGroups[0].focusGroup.name.lower())
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )

        recipe.sampleWs = "sample"
        recipe._cloneWorkspace = mock.Mock(side_effect=lambda inputWs, outputWs: outputWs)  # noqa: ARG005

        sampleClone, normClone = recipe._prepGroupingWorkspaces(0)
        assert sampleClone == preReducedOutputWsName
        assert normClone is None

    @mock.patch("snapred.backend.recipe.ReductionRecipe.ArtificialNormalizationRecipe")
    def test_prepareArtificialNormalization(self, mockArtificialNormalizationRecipe):
        recipe = ReductionRecipe()
        recipe.ingredients = mock.Mock()
        recipe.ingredients.timestamp = time.time()
        artificialNormalizationIngredients = mock.Mock()
        recipe.ingredients.artificialNormalizationIngredients = artificialNormalizationIngredients
        executeRecipeMock = mock.Mock(return_value="norm")
        mockArtificialNormalizationRecipe().executeRecipe = executeRecipeMock
        artNormWorkspace = recipe._prepareArtificialNormalization("sample", 1)
        assert artNormWorkspace == "norm"
        executeRecipeMock.assert_called_once()

    @mock.patch("snapred.backend.recipe.ReductionRecipe.ApplyNormalizationRecipe")
    def test_applyNormalization(self, mockApplyNormalizationRecipe):
        recipe = ReductionRecipe()
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
            recipe.ingredients.applyNormalization(groupingIndex),
            groceries={"inputWorkspace": "sample", "normalizationWorkspace": "norm"},
        )

    def test_cloneIntermediateWorkspace(self):
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock.Mock()
        recipe.mantidSnapper.MakeDirtyDish = mock.Mock()
        recipe._cloneIntermediateWorkspace("input", "output")
        recipe.mantidSnapper.MakeDirtyDish.assert_called_once_with(
            mock.ANY, InputWorkspace="input", OutputWorkspace="output"
        )

    def test_queueAlgos(self):
        # Mostly this is to make code-coverage happy.
        mockMantidSnapper = mock.Mock()
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mockMantidSnapper
        recipe.queueAlgos()
        assert mockMantidSnapper.mock_calls == []

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
        recipe._getNormalizationWorkspaceName = mock.Mock()
        recipe._getNormalizationWorkspaceName.return_value = "norm_grouped"

        # Set up ingredients and other variables for the recipe
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                self.mockPixelGroup(name="Column", isFullyMasked=False, N_gid=6),
                self.mockPixelGroup(name="Bank", isFullyMasked=False, N_gid=2),
            ],
            detectorPeaksMany=[["peaks"], ["peaks2"]],
        )

        recipe.ingredients.artificialNormalizationIngredients = "test"
        recipe.ingredients.groupProcessing = mock.Mock(
            return_value=lambda groupingIndex: f"groupProcessing_{groupingIndex}"
        )
        recipe.ingredients.generateFocussedVanadium = mock.Mock(
            return_value=lambda groupingIndex: f"generateFocussedVanadium_{groupingIndex}"
        )
        recipe.ingredients.applyNormalization = mock.Mock(
            return_value=lambda groupingIndex: f"applyNormalization_{groupingIndex}"
        )
        recipe.ingredients.effectiveInstrument = mock.Mock(
            return_value=lambda groupingIndex: f"unmaskedPixelGroup_{groupingIndex}"
        )

        # Mock internal methods of recipe
        recipe._applyRecipe = mock.Mock()
        recipe._cloneIntermediateWorkspace = mock.Mock()
        recipe._deleteWorkspace = mock.Mock()
        recipe._prepareUnfocusedData = mock.Mock()

        recipe._prepGroupingWorkspaces = mock.Mock()
        # Output workspace name must actually be a `WorkspaceName` instance.
        preReducedOutputWs = (
            wng.reductionOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group("grouped")
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )
        recipe._prepGroupingWorkspaces.return_value = (preReducedOutputWs, "norm_grouped")

        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.diffcalWs = "diffcal_table"
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
            diffcalWorkspace=recipe.diffcalWs,
            maskWorkspace=recipe.maskWs,
        )
        recipe._applyRecipe.assert_any_call(
            PreprocessReductionRecipe,
            recipe.ingredients.preprocess(),
            inputWorkspace=recipe.normalizationWs,
            diffcalWorkspace=recipe.diffcalWs,
            maskWorkspace=recipe.maskWs,
        )

        for groupIndex in (0, 1):
            recipe._applyRecipe.assert_any_call(
                ReductionGroupProcessingRecipe,
                recipe.ingredients.groupProcessing(groupIndex),
                inputWorkspace=recipe._prepGroupingWorkspaces.return_value[0],
                groupingWorkspace=recipe.groupingWorkspaces[groupIndex],
            )
            recipe._applyRecipe.assert_any_call(
                ReductionGroupProcessingRecipe,
                recipe.ingredients.groupProcessing(groupIndex),
                inputWorkspace=recipe._prepGroupingWorkspaces.return_value[1],
                groupingWorkspace=recipe.groupingWorkspaces[groupIndex],
            )

            recipe._applyRecipe.assert_any_call(
                GenerateFocussedVanadiumRecipe,
                recipe.ingredients.generateFocussedVanadium(groupIndex),
                inputWorkspace=recipe._prepGroupingWorkspaces.return_value[0],
                outputWorkspace=recipe._getNormalizationWorkspaceName.return_value,
            )

            recipe._applyRecipe.assert_any_call(
                ApplyNormalizationRecipe,
                recipe.ingredients.applyNormalization(groupIndex),
                inputWorkspace=recipe._prepGroupingWorkspaces.return_value[0],
                normalizationWorkspace="norm_grouped",
            )

        recipe._getNormalizationWorkspaceName.call_count == 2
        recipe._getNormalizationWorkspaceName.assert_any_call(0)
        recipe._getNormalizationWorkspaceName.assert_any_call(1)

        recipe.ingredients.effectiveInstrument.assert_not_called()

        recipe._deleteWorkspace.assert_called_with("norm_grouped")
        assert recipe._deleteWorkspace.call_count == len(recipe._prepGroupingWorkspaces.return_value)

        finalReducedOutputWs = preReducedOutputWs.builder.hidden(False).build()
        assert result["outputs"][0] == finalReducedOutputWs

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_execute_useEffectiveInstrument(self, mockMtd):
        with Config_override("reduction.output.useEffectiveInstrument", True):
            mockMantidSnapper = mock.Mock()

            mockMaskworkspace = mock.Mock()
            mockGroupWorkspace = mock.Mock()

            mockGroupWorkspace.getNumberHistograms.return_value = 10
            mockGroupWorkspace.readY.return_value = [0] * 10
            mockMaskworkspace.readY.return_value = [0] * 10

            mockMtd.__getitem__.side_effect = (
                lambda ws_name: mockMaskworkspace if ws_name == "mask" else mockGroupWorkspace
            )

            recipe = ReductionRecipe()
            recipe.mantidSnapper = mockMantidSnapper
            recipe.mantidSnapper.mtd = mockMtd
            recipe._getNormalizationWorkspaceName = mock.Mock()
            recipe._getNormalizationWorkspaceName.return_value = "norm_grouped"

            # Set up ingredients and other variables for the recipe
            recipe.ingredients = mock.Mock(
                spec=ReductionIngredients,
                runNumber="12345",
                useLiteMode=True,
                timestamp=time.time(),
                pixelGroups=[
                    self.mockPixelGroup(name="Column", isFullyMasked=False, N_gid=6),
                    self.mockPixelGroup(name="Bank", isFullyMasked=False, N_gid=2),
                ],
                detectorPeaksMany=[["peaks"], ["peaks2"]],
            )

            recipe.ingredients.artificialNormalizationIngredients = "test"
            recipe.ingredients.groupProcessing = mock.Mock(
                return_value=lambda groupingIndex: f"groupProcessing_{groupingIndex}"
            )
            recipe.ingredients.generateFocussedVanadium = mock.Mock(
                return_value=lambda groupingIndex: f"generateFocussedVanadium_{groupingIndex}"
            )
            recipe.ingredients.applyNormalization = mock.Mock(
                return_value=lambda groupingIndex: f"applyNormalization_{groupingIndex}"
            )
            recipe.ingredients.effectiveInstrument = mock.Mock(
                return_value=lambda groupingIndex: f"unmaskedPixelGroup_{groupingIndex}"
            )

            # Mock internal methods of recipe
            recipe._applyRecipe = mock.Mock()
            recipe._cloneIntermediateWorkspace = mock.Mock()
            recipe._deleteWorkspace = mock.Mock()
            recipe._prepareUnfocusedData = mock.Mock()

            recipe._prepGroupingWorkspaces = mock.Mock()
            # Output workspace name must actually be a `WorkspaceName` instance.
            preReducedOutputWs = (
                wng.reductionOutput()
                .runNumber(recipe.ingredients.runNumber)
                .group("grouped")
                .timestamp(recipe.ingredients.timestamp)
                .hidden(True)
                .build()
            )
            recipe._prepGroupingWorkspaces.return_value = (preReducedOutputWs, "norm_grouped")

            # Set up other recipe variables
            recipe.sampleWs = "sample"
            recipe.diffcalWs = "diffcal_table"
            recipe.maskWs = "mask"
            recipe.normalizationWs = "norm"
            recipe.groupingWorkspaces = ["group1", "group2"]
            recipe.keepUnfocused = True
            recipe.convertUnitsTo = "TOF"

            # Execute the recipe
            result = recipe.execute()

            # Perform assertions:
            #   here we only assert the calls that _change_ when the effective-instrument is substituted.

            for groupIndex in (0, 1):
                recipe._applyRecipe.assert_any_call(
                    EffectiveInstrumentRecipe,
                    recipe.ingredients.effectiveInstrument(groupIndex),
                    inputWorkspace=recipe._prepGroupingWorkspaces.return_value[0],
                )

            finalReducedOutputWs = preReducedOutputWs.builder.hidden(False).build()
            assert result["outputs"][0] == finalReducedOutputWs

    def test__isGroupFullyMasked(self):
        recipe = ReductionRecipe()

        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                self.mockPixelGroup(name="Column", isFullyMasked=False, N_gid=6),
                self.mockPixelGroup(name="Bank", isFullyMasked=False, N_gid=2),
                self.mockPixelGroup(name="Column2", isFullyMasked=True, N_gid=6),
                self.mockPixelGroup(name="Bank2", isFullyMasked=True, N_gid=2),
            ],
        )

        for n, flag in enumerate((False, False, True, True)):
            result = recipe._isGroupFullyMasked(n)
            assert result == flag

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_execute_with_fully_masked_group(self, mockMtd):
        mock_mantid_snapper = mock.Mock()

        # Mock the mask and group workspaces:
        #   for this test, these don't actually determine which groups are fully masked;
        #   that's determined by the `recipe.ingredients.pixelGroups[n].pixelGroupingParameters.isMasked` flags.
        mockMaskWorkspace = mock.sentinel.mask
        mockGroupWorkspace = mock.sentinel.grouping

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
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                self.mockPixelGroup(name="The_column_grouping", isFullyMasked=True, N_gid=6),
                self.mockPixelGroup(name="The_bank_grouping", isFullyMasked=True, N_gid=2),
            ],
        )
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
        recipe.diffcalWs = "diffcal_table"
        recipe.maskWs = "mask"
        recipe.normalizationWs = "norm"
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "TOF"

        # Execute the recipe
        result = recipe.execute()

        # Assertions for both groups being fully masked
        groupNames = (
            recipe.ingredients.pixelGroups[0].focusGroup.name,
            recipe.ingredients.pixelGroups[1].focusGroup.name,
        )
        expected_warning_message_group1 = (
            f"\nAll pixels within the '{groupNames[0]}' grouping are masked.\n"
            "Skipping all algorithm execution for this grouping."
        )

        expected_warning_message_group2 = (
            f"\nAll pixels within the '{groupNames[1]}' grouping are masked.\n"
            "Skipping all algorithm execution for this grouping."
        )

        # Check that the warnings were logged for both groups
        recipe.logger().warning.assert_any_call(expected_warning_message_group1)
        recipe.logger().warning.assert_any_call(expected_warning_message_group2)

        # Ensure the warning was called twice (once per group)
        assert recipe.logger().warning.call_count == 2, (
            "Expected warning to be logged twice for the fully masked groups."
        )

        # Ensure no algorithms were applied for the fully masked groups
        assert recipe._applyRecipe.call_count == 2, (
            "Expected _applyRecipe to not be called for the fully masked groups."
        )

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
