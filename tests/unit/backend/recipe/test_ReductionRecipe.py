import time
from unittest import TestCase, mock

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from util.Config_helpers import Config_override
from util.SculleryBoy import SculleryBoy

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.recipe.ReductionRecipe import (
    ApplyNormalizationRecipe,
    EffectiveInstrumentRecipe,
    GenerateFocussedVanadiumRecipe,
    PreprocessReductionRecipe,
    ReductionGroupProcessingRecipe,
    ReductionRecipe,
)
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


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
        # Test various units
        for units, abbr in [
            ("dSpacing", wng.Units.DSP),
            ("MomentumTransfer", wng.Units.QSP),
            ("Wavelength", wng.Units.LAM),
            ("TOF", wng.Units.TOF),
        ]:
            msg = f"Converting unfocused data to {units} units"
            recipe._prepareUnfocusedData(workspace, None, units)
            outputWs = wng.run().runNumber("555").lite(True).unit(abbr).group(wng.Groups.UNFOC).build()
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

        # No mask
        recipe._prepareUnfocusedData(workspace, None, units)
        recipe.mantidSnapper.MaskDetectorFlags.assert_not_called()
        recipe.mantidSnapper.reset_mock()

        # With mask
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

        # Create pixel groups that match the needed format
        recipe.groupingWorkspaces = ["prefix__focusgroup1_0", "prefix__focusgroup2_0"]

        pg1 = mock.Mock()
        pg1.focusGroup.name = "focusgroup1"
        pg1.pixelGroupingParameters = {"subgroup": mock.Mock(isMasked=False)}

        pg2 = mock.Mock()
        pg2.focusGroup.name = "focusgroup2"
        pg2.pixelGroupingParameters = {"subgroup": mock.Mock(isMasked=False)}

        # Set up ingredients
        recipe.ingredients = mock.Mock()
        recipe.ingredients.pixelGroups = [pg1, pg2]
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

        recipe.groceries = {}
        recipe.sampleWs = "sample"
        recipe.maskWs = "mask"
        recipe.normalizationWs = "norm"
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "dSpacing"

        # Mock internal methods of recipe
        recipe._applyRecipe = mock.Mock()
        recipe._cloneIntermediateWorkspace = mock.Mock()
        recipe._deleteWorkspace = mock.Mock()
        recipe._prepareUnfocusedData = mock.Mock()
        recipe._prepGroupingWorkspaces = mock.Mock()
        recipe._prepGroupingWorkspaces.side_effect = [
            ("sample_grouped", "norm_grouped"),
            ("sample_grouped2", "norm_grouped2"),
        ]

        # Execute the recipe
        result = recipe.execute()

        recipe._prepareUnfocusedData.assert_called_once_with("sample", "mask", "dSpacing")
        assert recipe._deleteWorkspace.call_count == 2
        recipe._deleteWorkspace.assert_any_call("norm_grouped")
        recipe._deleteWorkspace.assert_any_call("norm_grouped2")

        assert result["outputs"][0] in ["sample_grouped", "sample_grouped2"]

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
        recipe.groupingWorkspaces = ["prefix__myfocus_1234", "prefix__anotherfocus_5678"]
        recipe.normalizationWs = "norm"
        recipe.mantidSnapper = mock.Mock()
        recipe.groceries = {}
        mock_pg1 = mock.Mock()
        mock_pg1.focusGroup.name = "myfocus"
        mock_pg1.pixelGroupingParameters = {"subgroup": mock.Mock(isMasked=False)}

        mock_pg2 = mock.Mock()
        mock_pg2.focusGroup.name = "anotherfocus"
        mock_pg2.pixelGroupingParameters = {"subgroup": mock.Mock(isMasked=False)}

        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[mock_pg1, mock_pg2],
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

    @mock.patch("snapred.backend.recipe.ReductionRecipe.ArtificialNormalizationRecipe")
    def test_prepareArtificialNormalization(self, mockArtificialNormalizationRecipe):
        recipe = ReductionRecipe()
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()
        recipe.ingredients.timestamp = time.time()
        artificialNormalizationIngredients = mock.Mock()
        recipe.ingredients.artificialNormalizationIngredients = artificialNormalizationIngredients
        executeRecipeMock = mock.Mock(return_value="norm")
        mockArtificialNormalizationRecipe().executeRecipe = executeRecipeMock
        artNormWorkspace = recipe._prepareArtificialNormalization("sample", 1)
        assert artNormWorkspace == "norm"
        executeRecipeMock.assert_called_once()
        assert recipe.groceries["normalizationWorkspace"] == "norm"

    def test_prepGroupingWorkspaces_no_normalization(self):
        recipe = ReductionRecipe()
        recipe.groupingWorkspaces = ["prefix__somefocus_9999"]
        recipe.normalizationWs = None
        recipe.mantidSnapper = mock.Mock()
        recipe.groceries = {}

        mock_pg = mock.Mock()
        mock_pg.focusGroup.name = "somefocus"
        mock_pg.pixelGroupingParameters = {"subgroup": mock.Mock(isMasked=False)}

        recipe.ingredients = mock.Mock(
            spec=ReductionIngredients,
            runNumber="12345",
            useLiteMode=True,
            timestamp=time.time(),
            pixelGroups=[mock_pg],
            detectorPeaksMany=[["peaks"]],
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

        recipe._prepareArtificialNormalization = mock.Mock(side_effect=["norm_grouped", "norm_grouped_2"])

        recipe.groupingWorkspaces = ["prefix__focusgroup1_0", "prefix__focusgroup2_0"]

        pg1 = mock.Mock()
        pg1.focusGroup.name = "focusgroup1"
        pg1.pixelGroupingParameters = {"subgroup": mock.Mock(isMasked=False)}

        pg2 = mock.Mock()
        pg2.focusGroup.name = "focusgroup2"
        pg2.pixelGroupingParameters = {"subgroup": mock.Mock(isMasked=False)}

        # Set up ingredients and other variables for the recipe
        recipe.groceries = {}
        recipe.ingredients = mock.Mock()
        recipe.ingredients.pixelGroups = [pg1, pg2]
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
        recipe._prepGroupingWorkspaces.side_effect = [
            ("sample_grouped", "norm_grouped"),
            ("sample_grouped_2", "norm_grouped_2"),
        ]

        # Set up other recipe variables
        recipe.sampleWs = "sample"
        recipe.maskWs = "mask"
        recipe.normalizationWs = "norm"
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
            inputWorkspace="norm_grouped_2",
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
            inputWorkspace="sample_grouped_2",
            normalizationWorkspace="norm_grouped_2",
        )

        recipe._prepareArtificialNormalization.assert_any_call("sample_grouped", 0)
        recipe._prepareArtificialNormalization.assert_any_call("sample_grouped_2", 1)

        recipe.ingredients.effectiveInstrument.assert_not_called()

        recipe._deleteWorkspace.assert_any_call("norm_grouped")
        recipe._deleteWorkspace.assert_any_call("norm_grouped_2")

        assert len(result["outputs"]) > 0
        assert result["outputs"][-1] in ["sample_grouped", "sample_grouped_2", "mask"]

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

            recipe._prepareArtificialNormalization = mock.Mock(side_effect=["norm_grouped", "norm_grouped_2"])

            recipe.groupingWorkspaces = ["prefix__focusgroup1_0", "prefix__focusgroup2_0"]

            pg1 = mock.Mock()
            pg1.focusGroup.name = "focusgroup1"
            pg1.pixelGroupingParameters = {"subgroup": mock.Mock(isMasked=False)}

            pg2 = mock.Mock()
            pg2.focusGroup.name = "focusgroup2"
            pg2.pixelGroupingParameters = {"subgroup": mock.Mock(isMasked=False)}

            # Set up ingredients and other variables for the recipe
            recipe.groceries = {}
            recipe.ingredients = mock.Mock()
            recipe.ingredients.pixelGroups = [pg1, pg2]
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
            recipe._prepGroupingWorkspaces.side_effect = [
                ("sample_grouped", "norm_grouped"),
                ("sample_grouped_2", "norm_grouped_2"),
            ]

            # Set up other recipe variables
            recipe.sampleWs = "sample"
            recipe.maskWs = "mask"
            recipe.normalizationWs = "norm"
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
                inputWorkspace="norm_grouped_2",
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
                inputWorkspace="sample_grouped_2",
                normalizationWorkspace="norm_grouped_2",
            )

            recipe._prepareArtificialNormalization.assert_any_call("sample_grouped", 0)
            recipe._prepareArtificialNormalization.assert_any_call("sample_grouped_2", 1)

            recipe._applyRecipe.assert_any_call(
                EffectiveInstrumentRecipe,
                recipe.ingredients.effectiveInstrument(0),
                inputWorkspace="sample_grouped",
            )
            recipe._applyRecipe.assert_any_call(
                EffectiveInstrumentRecipe,
                recipe.ingredients.effectiveInstrument(1),
                inputWorkspace="sample_grouped_2",
            )

            recipe._deleteWorkspace.assert_any_call("norm_grouped")
            recipe._deleteWorkspace.assert_any_call("norm_grouped_2")
            assert len(result["outputs"]) > 0
            assert result["outputs"][-1] in ["sample_grouped", "sample_grouped_2", "mask"]

    @mock.patch("mantid.simpleapi.mtd", create=True)
    def test_execute_with_fully_masked_group(self, mockMtd):
        mock_mantid_snapper = mock.Mock()

        # Mock the mask and group workspaces
        mockMaskWorkspace = mock.Mock()
        mockGroupWorkspace = mock.Mock()

        # Mock groupWorkspace to have all pixels masked
        mockGroupWorkspace.getNumberHistograms.return_value = 10
        mockGroupWorkspace.readY.side_effect = lambda i: [i]
        mockMaskWorkspace.readY.side_effect = lambda i: [1]  # noqa: ARG005

        # Mock mtd to return the group and mask workspaces
        mockMtd.__getitem__.side_effect = lambda ws_name: mockMaskWorkspace if ws_name == "mask" else mockGroupWorkspace

        # Attach mocked mantidSnapper to recipe and assign mocked mtd
        recipe = ReductionRecipe()
        recipe.mantidSnapper = mock_mantid_snapper
        recipe.mantidSnapper.mtd = mockMtd
        recipe.maskWs = "mask"

        # Set up pixel groups all masked
        recipe.groupingWorkspaces = ["prefix__focus1_0", "prefix__focus2_0"]

        pg1 = mock.Mock()
        pg1.focusGroup.name = "focus1"
        pg1.pixelGroupingParameters = {
            "subgroupA": mock.Mock(isMasked=True),
            "subgroupB": mock.Mock(isMasked=True),
        }

        pg2 = mock.Mock()
        pg2.focusGroup.name = "focus2"
        pg2.pixelGroupingParameters = {
            "subgroupX": mock.Mock(isMasked=True),
            "subgroupY": mock.Mock(isMasked=True),
        }

        # Set up logger to capture warnings
        recipe.logger = mock.Mock()

        # Set up ingredients and other variables for the recipe
        recipe.ingredients = mock.Mock()
        recipe.ingredients.pixelGroups = [pg1, pg2]
        recipe.ingredients.groupProcessing = mock.Mock(return_value=lambda gi: f"groupProcessing_{gi}")
        recipe.ingredients.generateFocussedVanadium = mock.Mock(
            return_value=lambda gi: f"generateFocussedVanadium_{gi}"
        )
        recipe.ingredients.applyNormalization = mock.Mock(return_value=lambda gi: f"applyNormalization_{gi}")
        recipe.ingredients.artificialNormalizationIngredients = None

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
        recipe.keepUnfocused = True
        recipe.convertUnitsTo = "TOF"

        # Ensure groceries is defined
        recipe.groceries = {}

        # Execute the recipe
        result = recipe.execute()

        # The code likely logs only the first masked subgroup per group.
        expected_warning_subgroupA = (
            "Subgroup 'subgroupA' in group 'focus1' is fully masked. "
            "Skipping execution for prefix__focus1_0 workspace.\n"
            "This will affect future reductions.\n\n"
        )
        expected_warning_subgroupX = (
            "Subgroup 'subgroupX' in group 'focus2' is fully masked. "
            "Skipping execution for prefix__focus2_0 workspace.\n"
            "This will affect future reductions.\n\n"
        )

        # We only check for the first masked subgroup warning in each group:
        recipe.logger().warning.assert_any_call(expected_warning_subgroupA)
        recipe.logger().warning.assert_any_call(expected_warning_subgroupX)

        # No group-related recipes should run
        calls = [
            c
            for c in recipe._applyRecipe.call_args_list
            if c[0][0] in (ReductionGroupProcessingRecipe, GenerateFocussedVanadiumRecipe, ApplyNormalizationRecipe)
        ]
        assert len(calls) == 0, "No group-related recipes should run for fully masked groups."

        assert result["outputs"][0] == "mask"

    def test_checkMaskedPixels(self):
        recipe = ReductionRecipe()
        recipe.logger = mock.Mock()

        # Scenario 1: Invalid groupingWorkspace format (no "__")
        invalid_format_ws = "group1"
        result = recipe._checkMaskedPixels(invalid_format_ws)
        assert result is True, "Expected True when groupingWorkspace format is invalid."
        recipe.logger().error.assert_any_call(
            f"Unexpected groupingWorkspace format: '{invalid_format_ws}'. Skipping this workspace."
        )

        recipe.logger().error.reset_mock()

        # Scenario 2: No matching PixelGroup found
        recipe.ingredients = mock.Mock()
        recipe.ingredients.pixelGroups = []
        valid_format_ws = "someprefix__focus1_1234"
        result = recipe._checkMaskedPixels(valid_format_ws)
        assert result is True, "Expected True when no matching PixelGroup is found."
        recipe.logger().error.assert_any_call(f"No matching PixelGroup found for {valid_format_ws}")

        recipe.logger().error.reset_mock()
        recipe.logger().warning.reset_mock()

        # Prepare a mock pixel group for the next scenarios
        mock_pixel_group = mock.Mock()
        mock_pixel_group.focusGroup.name = "focus1"
        mock_pixel_group.pixelGroupingParameters = {
            "subgroupA": mock.Mock(isMasked=False),
            "subgroupB": mock.Mock(isMasked=False),
        }
        recipe.ingredients.pixelGroups = [mock_pixel_group]

        # Scenario 3: No subgroups masked -> should return False
        result = recipe._checkMaskedPixels(valid_format_ws)
        assert result is False, "Expected False when no subgroups are masked."
        recipe.logger().warning.assert_not_called()
        recipe.logger().error.assert_not_called()

        recipe.logger().warning.reset_mock()
        recipe.logger().error.reset_mock()

        # Scenario 4: One subgroup masked -> should return True
        mock_pixel_group.pixelGroupingParameters["subgroupA"].isMasked = True
        result = recipe._checkMaskedPixels(valid_format_ws)
        assert result is True, "Expected True when a subgroup is masked."
        recipe.logger().warning.assert_called_once_with(
            "Subgroup 'subgroupA' in group 'focus1' is fully masked. "
            f"Skipping execution for {valid_format_ws} workspace.\n"
            "This will affect future reductions.\n\n"
        )

        recipe.logger().warning.reset_mock()

        # Scenario 5: All subgroups masked -> should also return True and log a different message
        mock_pixel_group.pixelGroupingParameters["subgroupB"].isMasked = True
        result = recipe._checkMaskedPixels(valid_format_ws)

        mock_pixel_group.pixelGroupingParameters["subgroupA"].isMasked = True
        mock_pixel_group.pixelGroupingParameters["subgroupB"].isMasked = True

        mock_pixel_group.pixelGroupingParameters["subgroupA"].isMasked = True
        mock_pixel_group.pixelGroupingParameters["subgroupB"].isMasked = True

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
