# ruff: noqa: E402, ARG002
import unittest
import unittest.mock as mock
from unittest.mock import MagicMock, patch

import pytest
from mantid.simpleapi import (
    mtd,
)

thisService = "snapred.backend.service.NormalizationService"
localMock = mock.Mock()

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.data.DataExportService": mock.Mock(),
        "snapred.backend.data.DataFactoryService": mock.Mock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.ingredients import (
        ReductionIngredients,
    )
    from snapred.backend.dao.normalization import NormalizationIndexEntry
    from snapred.backend.dao.request import (
        FocusSpectraRequest,
        NormalizationCalibrationRequest,
        SmoothDataExcludingPeaksRequest,
        VanadiumCorrectionRequest,
    )
    from snapred.backend.dao.state import FocusGroup
    from snapred.backend.service.NormalizationService import NormalizationService
    from snapred.meta.Config import Resource
    from util.SculleryBoy import SculleryBoy

    thisService = "snapred.backend.service.NormalizationService."

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/normalization/ReductionIngredients.json", "r") as f:
            return ReductionIngredients.parse_raw(f.read())

    def test_saveNormalization():
        normalizationService = NormalizationService()
        normalizationService.dataExportService.exportNormalizationRecord = mock.Mock()
        normalizationService.dataExportService.exportNormalizationRecord.return_value = MagicMock(version="1.0.0")
        normalizationService.dataExportService.exportNormalizationWorkspaces = mock.Mock()
        normalizationService.dataExportService.exportNormalizationWorkspaces.return_value = MagicMock(version="1.0.0")
        normalizationService.dataExportService.exportNormalizationIndexEntry = mock.Mock()
        normalizationService.dataExportService.exportNormalizationIndexEntry.return_value = MagicMock("expected")
        normalizationService.saveNormalization(mock.Mock())
        assert normalizationService.dataExportService.exportNormalizationRecord.called
        savedEntry = normalizationService.dataExportService.exportNormalizationRecord.call_args.args[0]
        assert savedEntry.parameters is not None
        assert normalizationService.dataExportService.exportNormalizationWorkspaces.called

    def test_exportNormalizationIndexEntry():
        normalizationService = NormalizationService()
        normalizationService.dataExportService.exportNormalizationIndexEntry = MagicMock()
        normalizationService.dataExportService.exportNormalizationIndexEntry.return_value = "expected"
        normalizationService.saveNormalizationToIndex(
            NormalizationIndexEntry(runNumber="1", useLiteMode=True, backgroundRunNumber="2")
        )
        assert normalizationService.dataExportService.exportNormalizationIndexEntry.called
        savedEntry = normalizationService.dataExportService.exportNormalizationIndexEntry.call_args.args[0]
        assert savedEntry.appliesTo == ">1"
        assert savedEntry.timestamp is not None


from snapred.backend.service.NormalizationService import NormalizationService  # noqa: F811


class TestNormalizationService(unittest.TestCase):
    def setUp(self):
        self.instance = NormalizationService()
        self.request = NormalizationCalibrationRequest(
            runNumber="12345",
            backgroundRunNumber="67890",
            useLiteMode=True,
            calibrantSamplePath="path/to/sample",
            focusGroup=FocusGroup(name="apple", definition="path/to/grouping"),
            smoothingParameter=0.5,
            dMin=0.4,
        )

    def clearoutWorkspaces(self) -> None:
        for ws in mtd.getObjectNames():
            self.instance.groceryService.deleteWorkspace(ws)

    def tearDown(self) -> None:
        self.clearoutWorkspaces()
        return super().tearDown()

    @patch(thisService + "FarmFreshIngredients")
    @patch(thisService + "FocusSpectraRecipe")
    def test_focusSpectra(
        self,
        mockRecipe,
        FarmFreshIngredients,
    ):
        mockRequest = FocusSpectraRequest(
            runNumber="12345",
            focusGroup=self.request.focusGroup,
            useLiteMode=True,
            inputWorkspace="input_ws",
            groupingWorkspace="grouping_ws",
            outputWorkspace="output_ws",
        )

        self.instance = NormalizationService()
        self.instance.sousChef = SculleryBoy()
        mockRecipeInst = mockRecipe.return_value
        mockRecipeInst.executeRecipe.return_value = "expected"

        res = self.instance.focusSpectra(mockRequest)

        # assert self.instance.sousChef.prepPixelGroup.called_once_with(FarmFreshIngredients.return_value)
        assert mockRecipeInst.executeRecipe.called_once_with(
            InputWorkspace=mockRequest.inputWorkspace,
            GroupingWorkspace=mockRequest.groupingWorkspace,
            Ingredients=self.instance.sousChef.prepPixelGroup(FarmFreshIngredients()),
            OutputWorkspace=mockRequest.outputWorkspace,
        )
        assert res == mockRecipeInst.executeRecipe.return_value

    @patch(thisService + "FarmFreshIngredients")
    @patch(thisService + "RawVanadiumCorrectionRecipe")
    def test_vanadiumCorrection(self, RawVanadiumCorrectionRecipe, FarmFreshIngredients):
        mockRequest = VanadiumCorrectionRequest(
            runNumber="12345",
            useLiteMode=True,
            focusGroup=self.request.focusGroup,
            calibrantSamplePath="path/to/sample",
            inputWorkspace="input_ws",
            backgroundWorkspace="background_ws",
            outputWorkspace="output_ws",
            crystalDMin=0.4,
        )
        self.instance = NormalizationService()
        self.instance._sameStates = MagicMock(return_value=True)
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService = MagicMock()

        res = self.instance.vanadiumCorrection(mockRequest)

        mockRecipeInst = RawVanadiumCorrectionRecipe.return_value
        # assert self.instance.sousChef.prepNormalizationIngredients.called_once_with(FarmFreshIngredients.return_value)
        assert mockRecipeInst.executeRecipe.called_once_with(
            InputWorkspace=mockRequest.inputWorkspace,
            BackgroundWorkspace=mockRequest.backgroundWorkspace,
            Ingredients=self.instance.sousChef.prepNormalizationIngredients({}),
            OutputWorkspace=mockRequest.outputWorkspace,
        )
        assert res == mockRecipeInst.executeRecipe.return_value

    @patch(thisService + "FarmFreshIngredients")
    @patch(thisService + "SmoothDataExcludingPeaksRecipe")
    def test_smoothDataExcludingPeaks(
        self,
        mockRecipe,
        FarmFreshIngredients,
    ):
        FarmFreshIngredients.return_value = {"good": ""}
        mockRequest = SmoothDataExcludingPeaksRequest(
            runNumber="12345",
            useLiteMode=True,
            focusGroup=self.request.focusGroup,
            calibrantSamplePath="path/to/sample",
            inputWorkspace="input_ws",
            outputWorkspace="output_ws",
            smoothingParameter=0.5,
            crystalDMin=0.4,
        )

        self.instance = NormalizationService()
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService.getCifFilePath = MagicMock(return_value="path/to/cif")

        mockRecipeInst = mockRecipe.return_value

        self.instance.smoothDataExcludingPeaks(mockRequest)

        # assert self.instance.sousChef.prepDetectorPeaks.called_once_with(FarmFreshIngredients.return_value)
        assert mockRecipeInst.executeRecipe.called_once_with(
            InputWorkspace=mockRequest.inputWorkspace,
            OutputWorkspace=mockRequest.outputWorkspace,
            Ingredients=self.instance.sousChef.prepDetectorPeaks({}),
        )

    @patch(
        thisService + "NormalizationRecord",
        return_value=MagicMock(mockId="mock_normalization_record"),
    )
    def test_normalizationAssessment(
        self,
        NormalizationRecord,
    ):
        self.instance = NormalizationService()
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService.getNormalizationRecord = MagicMock(return_value=MagicMock())

        result = self.instance.normalizationAssessment(self.request)

        assert result.mockId == NormalizationRecord.return_value.mockId

    @patch(thisService + "FarmFreshIngredients")
    @patch(thisService + "RawVanadiumCorrectionRecipe")
    @patch(thisService + "FocusSpectraRecipe")
    @patch(thisService + "SmoothDataExcludingPeaksRecipe")
    @patch(thisService + "GroceryService")
    def test_normalization(
        self,
        mockGroceryService,
        mockSmoothDataExcludingPeaks,
        mockFocusSpectra,
        mockVanadiumCorrection,
        FarmFreshIngredients,
    ):
        FarmFreshIngredients.return_value = {"purge": True}
        mockGroceryService = mockGroceryService.return_value
        mockGroceryService.fetchGroceryDict.return_value = {
            "backgroundWorkspace": "bg_ws",
            "inputWorkspace": "input_ws",
            "groupingWorkspace": "grouping_ws",
            "outputWorkspace": "output_ws",
        }
        mockGroceryService.workspaceDoesExist.return_value = False
        mockVanadiumCorrection.executeRecipe.return_value = "corrected_vanadium_ws"
        mockFocusSpectra.executeRecipe.return_value = "focussed_vanadium_ws"
        mockSmoothDataExcludingPeaks.executeRecipe.return_value = "smoothed_output_ws"

        self.instance = NormalizationService()
        self.instance._sameStates = MagicMock(return_value=True)
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService.getCifFilePath = MagicMock(return_value="path/to/cif")
        result = self.instance.normalization(self.request)

        self.assertIsInstance(result, dict)  # noqa: PT009
        self.assertIn("correctedVanadium", result)  # noqa: PT009
        self.assertIn("focusedVanadium", result)  # noqa: PT009
        self.assertIn("smoothedVanadium", result)  # noqa: PT009
        mockGroceryService.fetchGroceryDict.assert_called_once()
        mockVanadiumCorrection.assert_called_once()
        mockFocusSpectra.assert_called_once()
        mockSmoothDataExcludingPeaks.assert_called_once()

    @patch(thisService + "FarmFreshIngredients")
    @patch(thisService + "GroceryService")
    def test_cachedNormalization(self, mockGroceryService, FarmFreshIngredients):
        FarmFreshIngredients.return_value = {"purge": True}
        mockGroceryService.workSpaceDoesExist = mock.Mock(return_value=True)
        self.instance = NormalizationService()
        self.instance._sameStates = MagicMock(return_value=True)
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService.getCifFilePath = MagicMock(return_value="path/to/cif")
        result = self.instance.normalization(self.request)
        assert result == {
            "correctedVanadium": "tof_unfoc_raw_van_corr_012345",
            "focusedVanadium": f"tof_{self.request.focusGroup.name}_s+f-vanadium_012345",
            "smoothedVanadium": "dsp_apple_fitted_van_cor_012345",
            "detectorPeaks": self.instance.sousChef.prepNormalizationIngredients(FarmFreshIngredients()).detectorPeaks,
        }

    def test_nomaliztionStatesDontMatch(self):
        self.instance = NormalizationService()
        self.instance._sameStates = MagicMock(return_value=False)
        with pytest.raises(ValueError, match="must be of the same Instrument State."):
            self.instance.normalization(self.request)

    def test_sameStates(self):
        self.instance = NormalizationService()
        self.instance.dataFactoryService.constructStateId = MagicMock()
        self.instance.dataFactoryService.constructStateId.return_value = "state"
        assert self.instance._sameStates("12345", "state")

    def test_differentStates(self):
        self.instance = NormalizationService()
        # constructStateId must return different each call
        self.instance.dataFactoryService.constructStateId = MagicMock()
        self.instance.dataFactoryService.constructStateId.side_effect = ["state", "different_state"]
        assert not self.instance._sameStates("12345", "different_state")
