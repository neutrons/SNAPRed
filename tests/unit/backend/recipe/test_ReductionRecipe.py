import time
from typing import Tuple

## Put test-related imports at the end, so that the normal non-test import sequence is unmodified.
from unittest import TestCase, mock

import numpy as np
import pytest
from mantid.simpleapi import CreateEmptyTableWorkspace, CreateSingleValuedWorkspace, mtd
from util.Config_helpers import Config_override
from util.SculleryBoy import SculleryBoy

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.state import FocusGroup, PixelGroup, PixelGroupingParameters
from snapred.backend.recipe.PreprocessReductionRecipe import PreprocessReductionRecipe
from snapred.backend.recipe.ReductionRecipe import (
    ApplyNormalizationRecipe,
    EffectiveInstrumentRecipe,
    GenerateFocussedVanadiumRecipe,
    ReductionGroupProcessingRecipe,
    ReductionRecipe,
)
from snapred.meta.Config import Config
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

    def mockPixelGroup(self, name: str, N_gid: int, *, maskedSubgroups: Tuple[int] = ()):
        # Create a mock `PixelGroup` named `name` with `N_gid` mock subgroup PGPs:
        #   `isFullyMasked` => all subgroups will have their `isMasked` flags set,
        #   otherwise, only some will have the `isMasked` flag set.
        return mock.Mock(
            spec=PixelGroup,
            pixelGroupingParameters={
                gid: mock.Mock(
                    spec=PixelGroupingParameters,
                )
                # Unless it's fully masked,
                #   generate sometimes masked subgroups when N_gid > 1:
                for gid in range(1, N_gid + 1)
                if gid not in maskedSubgroups
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
            "normalizationWorkspace": "norm",
            "combinedPixelMask": "mask",
            "groupingWorkspaces": ["group1", "group2"],
        }
        recipe.unbagGroceries(groceries)
        assert recipe.sampleWs == groceries["inputWorkspace"]
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
                self.mockPixelGroup(name="Column", N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
            ],
            unmaskedPixelGroups=[
                self.mockPixelGroup(name="Column", N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
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

        recipe._generateWorkspaceNamesForGroup = mock.Mock()
        # Output workspace name must actually be a `WorkspaceName` instance.
        preReducedOutputWs = (
            wng.reductionOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group("grouped")
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )
        recipe._generateWorkspaceNamesForGroup.return_value = (preReducedOutputWs, "norm_grouped")
        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.diffcalWs = "diffcal_table"
        recipe.maskWs = "mask"
        recipe.normalizationWs = wng.rawVanadium().runNumber(recipe.ingredients.runNumber).build()
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "dSpacing"

        # Execute the recipe
        result = recipe.execute()

        # Assertions
        recipe._prepareUnfocusedData.assert_called_once_with("sample", "mask", "dSpacing")
        # Delete the group clones (sample + norm), and the masked normalization clone.
        assert recipe._deleteWorkspace.call_count == 3
        recipe._deleteWorkspace.assert_called_with(recipe.normalizationWsMasked)

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
            isDiagnostic=False,
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
        recipe.diffcalWs = "diffcal_table"

        sampleClone, normClone = recipe._generateWorkspaceNamesForGroup(0)
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
            isDiagnostic=True,
            pixelGroups=[
                mock.Mock(
                    spec=PixelGroup, focusGroup=FocusGroup(name="first_grouping_name", definition="first_grouping_path")
                ),
                mock.Mock(
                    spec=PixelGroup,
                    focusGroup=FocusGroup(name="second_grouping_name", definition="second_grouping_path"),
                ),
            ],
            unmaskedPixelGroups=[
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
            wng.reductionDiagnosticOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group(recipe.ingredients.pixelGroups[0].focusGroup.name.lower())
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )

        recipe.sampleWs = "sample"
        recipe._cloneWorkspace = mock.Mock(side_effect=lambda inputWs, outputWs: outputWs)  # noqa: ARG005
        recipe.diffcalWs = "diffcal_table"

        sampleClone, normClone = recipe._generateWorkspaceNamesForGroup(0)
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
                self.mockPixelGroup(name="Column", N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
            ],
            unmaskedPixelGroups=[
                self.mockPixelGroup(name="Column", N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
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

        recipe._generateWorkspaceNamesForGroup = mock.Mock()
        # Output workspace name must actually be a `WorkspaceName` instance.
        preReducedOutputWs = (
            wng.reductionOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group("grouped")
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )
        prepGroupingWorkspacesReturns = [
            (preReducedOutputWs, f"reduced_normalization_{0}_{wnvf.formatTimestamp(recipe.ingredients.timestamp)}"),
            (preReducedOutputWs, f"reduced_normalization_{1}_{wnvf.formatTimestamp(recipe.ingredients.timestamp)}"),
        ]
        recipe._generateWorkspaceNamesForGroup.side_effect = prepGroupingWorkspacesReturns

        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.diffcalWs = "diffcal_table"
        recipe.maskWs = "mask"
        recipe.normalizationWs = wng.rawVanadium().runNumber(recipe.ingredients.runNumber).build()
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
            inputWorkspace=recipe.normalizationWs.builder.masked(False).build(),
            maskWorkspace=recipe.maskWs,
            outputWorkspace=recipe.normalizationWs.builder.masked(True).build(),
        )

        for groupIndex in (0, 1):
            recipe._applyRecipe.assert_any_call(
                ReductionGroupProcessingRecipe,
                recipe.ingredients.groupProcessing(groupIndex),
                inputWorkspace=recipe.sampleWs,
                groupingWorkspace=recipe.groupingWorkspaces[groupIndex],
                outputWorkspace=prepGroupingWorkspacesReturns[groupIndex][0],
            )
            recipe._applyRecipe.assert_any_call(
                ReductionGroupProcessingRecipe,
                recipe.ingredients.groupProcessing(groupIndex),
                inputWorkspace=recipe.normalizationWs,
                groupingWorkspace=recipe.groupingWorkspaces[groupIndex],
                outputWorkspace=prepGroupingWorkspacesReturns[groupIndex][1],
            )

            recipe._applyRecipe.assert_any_call(
                GenerateFocussedVanadiumRecipe,
                recipe.ingredients.generateFocussedVanadium(groupIndex),
                inputWorkspace=prepGroupingWorkspacesReturns[groupIndex][0],
                outputWorkspace=recipe._getNormalizationWorkspaceName.return_value,
            )

            recipe._applyRecipe.assert_any_call(
                ApplyNormalizationRecipe,
                recipe.ingredients.applyNormalization(groupIndex),
                inputWorkspace=prepGroupingWorkspacesReturns[groupIndex][0],
                normalizationWorkspace="norm_grouped",
            )

        recipe._getNormalizationWorkspaceName.call_count == 2
        recipe._getNormalizationWorkspaceName.assert_any_call(0)
        recipe._getNormalizationWorkspaceName.assert_any_call(1)

        assert recipe.ingredients.effectiveInstrument.call_count == (
            2 if Config["reduction.output.useEffectiveInstrument"] else 0
        )

        recipe._deleteWorkspace.assert_called_with(recipe.normalizationWs.builder.masked(True).build())
        # Delete the group clones (sample + norm), and the masked normalization clone.
        assert recipe._deleteWorkspace.call_count == 3

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
                    self.mockPixelGroup(name="Column", N_gid=6),
                    self.mockPixelGroup(name="Bank", N_gid=2),
                ],
                unmaskedPixelGroups=[
                    self.mockPixelGroup(name="Column", N_gid=6),
                    self.mockPixelGroup(name="Bank", N_gid=2),
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

            recipe._generateWorkspaceNamesForGroup = mock.Mock()
            # Output workspace name must actually be a `WorkspaceName` instance.
            preReducedOutputWs = (
                wng.reductionOutput()
                .runNumber(recipe.ingredients.runNumber)
                .group("grouped")
                .timestamp(recipe.ingredients.timestamp)
                .hidden(True)
                .build()
            )
            recipe._generateWorkspaceNamesForGroup.return_value = (preReducedOutputWs, "norm_grouped")

            # Set up other recipe variables
            recipe.sampleWs = "sample"
            recipe.diffcalWs = "diffcal_table"
            recipe.maskWs = "mask"
            recipe.normalizationWs = wng.rawVanadium().runNumber(recipe.ingredients.runNumber).build()
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
                    inputWorkspace=recipe._generateWorkspaceNamesForGroup.return_value[0],
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
                self.mockPixelGroup(name="Column", N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
                self.mockPixelGroup(name="Column2", maskedSubgroups=(1, 2, 3, 4, 5, 6), N_gid=6),
                self.mockPixelGroup(name="Bank2", maskedSubgroups=(1, 2), N_gid=2),
            ],
        )

        for n, flag in enumerate((False, False, True, True)):
            result = recipe._isGroupFullyMasked(n)
            assert result == flag

    def test__maskedSubgroups(self):
        recipe = ReductionRecipe()

        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                mock.Mock(
                    spec=PixelGroup,
                    # Fully-masked subgroups are excluded:
                    pixelGroupingParameters={
                        n: mock.Mock(spec=PixelGroupingParameters)
                        for n in range(1, 51)
                        if bool(np.random.randint(0, 2))
                    },
                )
            ],
            unmaskedPixelGroups=[
                mock.Mock(
                    spec=PixelGroup,
                    # All subgroups are included:
                    pixelGroupingParameters={n: mock.Mock(spec=PixelGroupingParameters) for n in range(1, 51)},
                )
            ],
        )

        maskedSubgroups = recipe._maskedSubgroups(0)
        assert set(maskedSubgroups) == set(
            recipe.ingredients.unmaskedPixelGroups[0].pixelGroupingParameters.keys()
        ) - set(recipe.ingredients.pixelGroups[0].pixelGroupingParameters.keys())

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_execute_with_fully_masked_group(self, mockMtd):
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

        # Set up logger to capture warnings
        recipe.logger = mock.Mock()

        recipe._getNormalizationWorkspaceName = mock.Mock()
        recipe._getNormalizationWorkspaceName.return_value = "norm_grouped"

        # Set up ingredients and other variables for the recipe
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                self.mockPixelGroup(name="Column", maskedSubgroups=(1, 2, 3, 4, 5, 6), N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
            ],
            unmaskedPixelGroups=[
                self.mockPixelGroup(name="Column", N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
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

        recipe._generateWorkspaceNamesForGroup = mock.Mock()
        # Output workspace name must actually be a `WorkspaceName` instance.
        preReducedOutputWs = (
            wng.reductionOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group("grouped")
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )
        recipe._generateWorkspaceNamesForGroup.return_value = (preReducedOutputWs, "norm_grouped")

        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.diffcalWs = "diffcal_table"
        recipe.maskWs = "mask"
        recipe.normalizationWs = wng.rawVanadium().runNumber(recipe.ingredients.runNumber).build()
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "TOF"

        # Execute the recipe
        result = recipe.execute()

        # Assertions for the group being fully masked
        groupNames = (recipe.ingredients.pixelGroups[0].focusGroup.name,)
        expected_warning_message = (
            f"\nAll pixels within the '{groupNames[0]}' grouping are masked.\nThis grouping will be skipped!"
        )

        # Check that the expected warnings were logged.
        recipe.logger().warning.assert_any_call(expected_warning_message)

        # Ensure the warning was called.
        assert recipe.logger().warning.call_count == 1, "Expected warning to be called for the fully masked group."

        # Ensure no algorithms were applied for the fully masked group.
        assert recipe._applyRecipe.call_count == 6, "Expected _applyRecipe to not be called for the fully masked group."

        assert "mask" in result["outputs"], "Expected the mask workspace to be included in the outputs."

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_execute_with_masked_subgroups(self, mockMtd):
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

        # Set up logger to capture warnings
        recipe.logger = mock.Mock()

        recipe._getNormalizationWorkspaceName = mock.Mock()
        recipe._getNormalizationWorkspaceName.return_value = "norm_grouped"

        # Set up ingredients and other variables for the recipe
        maskedSubgroups = (1, 3, 4, 6)
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                self.mockPixelGroup(name="Column", maskedSubgroups=maskedSubgroups, N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
            ],
            unmaskedPixelGroups=[
                self.mockPixelGroup(name="Column", N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
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

        recipe._generateWorkspaceNamesForGroup = mock.Mock()
        # Output workspace name must actually be a `WorkspaceName` instance.
        preReducedOutputWs = (
            wng.reductionOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group("grouped")
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )
        recipe._generateWorkspaceNamesForGroup.return_value = (preReducedOutputWs, "norm_grouped")

        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.diffcalWs = "diffcal_table"
        recipe.maskWs = "mask"
        recipe.normalizationWs = wng.rawVanadium().runNumber(recipe.ingredients.runNumber).build()
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "TOF"

        # Execute the recipe
        result = recipe.execute()  # noqa: F841

        groupNames = (recipe.ingredients.pixelGroups[0].focusGroup.name,)
        expected_warning_message = (
            f"\nWithin the '{groupNames[0]}' " + f"grouping:\n    subgroups {list(maskedSubgroups)} are fully masked."
        )

        # Check that the expected warnings were logged.
        recipe.logger().warning.assert_any_call(expected_warning_message)

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_execute_with_all_groups_masked(self, mockMtd):
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

        # Set up logger to capture warnings
        recipe.logger = mock.Mock()

        recipe._getNormalizationWorkspaceName = mock.Mock()
        recipe._getNormalizationWorkspaceName.return_value = "norm_grouped"

        # Set up ingredients and other variables for the recipe
        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[
                self.mockPixelGroup(name="Column", maskedSubgroups=(1, 2, 3, 4, 5, 6), N_gid=6),
                self.mockPixelGroup(name="Bank", maskedSubgroups=(1, 2), N_gid=2),
            ],
            unmaskedPixelGroups=[
                self.mockPixelGroup(name="Column", N_gid=6),
                self.mockPixelGroup(name="Bank", N_gid=2),
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

        recipe._generateWorkspaceNamesForGroup = mock.Mock()
        # Output workspace name must actually be a `WorkspaceName` instance.
        preReducedOutputWs = (
            wng.reductionOutput()
            .runNumber(recipe.ingredients.runNumber)
            .group("grouped")
            .timestamp(recipe.ingredients.timestamp)
            .hidden(True)
            .build()
        )
        recipe._generateWorkspaceNamesForGroup.return_value = (preReducedOutputWs, "norm_grouped")

        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.diffcalWs = "diffcal_table"
        recipe.maskWs = "mask"
        recipe.normalizationWs = "norm"
        recipe.groupingWorkspaces = ["group1", "group2"]
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "TOF"

        # Execute the recipe
        with pytest.raises(
            RuntimeError,
            match="There are no unmasked pixels in any of the groupings.  Please check your mask workspace!",
        ):
            result = recipe.execute()  # noqa: F841

        # Ensure no algorithms were applied for the fully masked groups.
        assert recipe._applyRecipe.call_count == 0, (
            "Expected _applyRecipe to not be called for the fully masked groups."
        )

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
