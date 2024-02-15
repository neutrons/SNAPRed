# ruff: noqa: E402, ARG002
import unittest
import unittest.mock as mock
from typing import Any, Dict
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from mantid.simpleapi import (
    CreateWorkspace,
    mtd,
)

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

    thisService = "snapred.backend.service.NormalizationService."

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/normalization/ReductionIngredients.json", "r") as f:
            return ReductionIngredients.parse_raw(f.read())

    def test_saveNormalization():
        normalizationService = NormalizationService()
        normalizationService.dataExportService.exportNormalizationRecord = mock.Mock()
        normalizationService.dataExportService.exportNormalizationRecord.return_value = MagicMock(version="1.0.0")
        normalizationService.dataExportService.exportNormalizationIndexEntry = mock.Mock()
        normalizationService.dataExportService.exportNormalizationIndexEntry.return_value = MagicMock("expected")
        normalizationService.sousChef.prepReductionIngredients = mock.Mock()
        normalizationService.sousChef.prepReductionIngredients.return_value = readReductionIngredientsFromFile()  # noqa: E501
        normalizationService.saveNormalization(mock.Mock())
        assert normalizationService.dataExportService.exportNormalizationRecord.called
        savedEntry = normalizationService.dataExportService.exportNormalizationRecord.call_args.args[0]
        assert savedEntry.parameters is not None

    def test_exportNormalizationIndexEntry():
        normalizationService = NormalizationService()
        normalizationService.dataExportService.exportNormalizationIndexEntry = MagicMock()
        normalizationService.dataExportService.exportNormalizationIndexEntry.return_value = "expected"
        normalizationService.saveNormalizationToIndex(NormalizationIndexEntry(runNumber="1", backgroundRunNumber="2"))
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
            self.instance.dataFactoryService.deleteWorkspace(ws)

    def tearDown(self) -> None:
        self.clearoutWorkspaces()
        return super().tearDown()

    @patch("snapred.backend.service.NormalizationService.FarmFreshIngredients")
    @patch("snapred.backend.service.NormalizationService.FocusSpectraRecipe")
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
        self.instance.sousChef.prepPixelGroup = MagicMock()
        mockRecipeInst = mockRecipe.return_value
        mockRecipeInst.executeRecipe.return_value = "expected"

        res = self.instance.focusSpectra(mockRequest)

        assert self.instance.sousChef.prepPixelGroup.called_once_with(FarmFreshIngredients.return_value)
        assert mockRecipeInst.executeRecipe.called_once_with(
            InputWorkspace=mockRequest.inputWorkspace,
            GroupingWorkspace=mockRequest.groupingWorkspace,
            Ingredients=self.instance.sousChef.prepPixelGroup.return_value,
            OutputWorkspace=mockRequest.outputWorkspace,
        )
        assert res == mockRecipeInst.executeRecipe.return_value

    @patch("snapred.backend.service.NormalizationService.FarmFreshIngredients")
    @patch("snapred.backend.service.NormalizationService.RawVanadiumCorrectionRecipe")
    def test_vanadiumCorrection(self, RawVanadiumCorrectionRecipe, FarmFreshIngredients):
        mockRequest = VanadiumCorrectionRequest(
            runNumber="12345",
            useLiteMode=True,
            focusGroup=self.request.focusGroup,
            calibrantSamplePath="path/to/sample",
            inputWorkspace="input_ws",
            backgroundWorkspace="background_ws",
            outputWorkspace="output_ws",
        )
        self.instance = NormalizationService()
        self.instance.sousChef.prepNormalizationIngredients = MagicMock()
        self.instance.dataFactoryService = MagicMock()

        res = self.instance.vanadiumCorrection(mockRequest)

        mockRecipeInst = RawVanadiumCorrectionRecipe.return_value
        assert self.instance.sousChef.prepNormalizationIngredients.called_once_with(FarmFreshIngredients.return_value)
        assert mockRecipeInst.executeRecipe.called_once_with(
            InputWorkspace=mockRequest.inputWorkspace,
            BackgroundWorkspace=mockRequest.backgroundWorkspace,
            Ingredients=self.instance.sousChef.prepNormalizationIngredients.return_value,
            OutputWorkspace=mockRequest.outputWorkspace,
        )
        assert res == mockRecipeInst.executeRecipe.return_value

    @patch("snapred.backend.service.NormalizationService.FarmFreshIngredients")
    @patch("snapred.backend.service.NormalizationService.SmoothDataExcludingPeaksRecipe")
    def test_smoothDataExcludingPeaks(
        self,
        mockRecipe,
        FarmFreshIngredients,
    ):
        mockRequest = SmoothDataExcludingPeaksRequest(
            runNumber="12345",
            useLiteMode=True,
            focusGroup=self.request.focusGroup,
            calibrantSamplePath="path/to/sample",
            inputWorkspace="input_ws",
            outputWorkspace="output_ws",
            smoothingParameter=0.5,
            dMin=0.4,
        )

        self.instance = NormalizationService()
        self.instance.sousChef.prepPeakIngredients = MagicMock()
        self.instance.dataFactoryService.getCifFilePath = MagicMock(return_value="path/to/cif")

        mockRecipeInst = mockRecipe.return_value

        self.instance.smoothDataExcludingPeaks(mockRequest)

        assert self.instance.sousChef.prepPeakIngredients.called_once_with(FarmFreshIngredients.return_value)
        assert mockRecipeInst.executeRecipe.called_once_with(
            InputWorkspace=mockRequest.inputWorkspace,
            OutputWorkspace=mockRequest.outputWorkspace,
            Ingredients=self.instance.sousChef.prepPeakIngredients.return_value,
        )

    @patch("snapred.backend.service.NormalizationService.DataFactoryService")
    @patch(
        "snapred.backend.service.NormalizationService.NormalizationRecord",
        return_value=MagicMock(mockId="mock_normalization_record"),
    )
    def test_normalizationAssessment(
        self,
        mockNormalizationRecord,
        mockDataFactoryService,
    ):
        mockNormalization = MagicMock()
        mockDataFactoryService = mockDataFactoryService.return_value
        mockDataFactoryService.getCalibrationState.return_value = mockNormalization

        self.instance = NormalizationService()
        self.instance.dataFactoryService.getNormalizationRecord = MagicMock(return_value=mockNormalizationRecord)

        result = self.instance.normalizationAssessment(self.request)

        mockDataFactoryService.getCalibrationState.assert_called_once_with(self.request.runNumber)
        expected_record_mockId = "mock_normalization_record"
        assert result.mockId == expected_record_mockId

    @patch("snapred.backend.service.NormalizationService.NormalizationService.vanadiumCorrection")
    @patch("snapred.backend.service.NormalizationService.NormalizationService.focusSpectra")
    @patch("snapred.backend.service.NormalizationService.NormalizationService.smoothDataExcludingPeaks")
    @patch("snapred.backend.service.NormalizationService.GroceryService")
    def test_normalization(
        self,
        mockGroceryService,
        mockSmoothDataExcludingPeaks,
        mockFocusSpectra,
        mockVanadiumCorrection,
    ):
        mockGroceryService = mockGroceryService.return_value
        mockGroceryService.fetchGroceryDict.return_value = {
            "backgroundWorkspace": "bg_ws",
            "inputWorkspace": "input_ws",
            "groupingWorkspace": "grouping_ws",
            "outputWorkspace": "output_ws",
        }
        mockGroceryService.workspaceDoesExist.return_value = False
        mockVanadiumCorrection.return_value = "corrected_vanadium_ws"
        mockFocusSpectra.return_value = "focussed_vanadium_ws"
        mockSmoothDataExcludingPeaks.return_value = "smoothed_output_ws"

        self.instance = NormalizationService()
        result = self.instance.normalization(self.request)

        self.assertIsInstance(result, dict)  # noqa: PT009
        self.assertIn("correctedVanadium", result)  # noqa: PT009
        self.assertIn("outputWorkspace", result)  # noqa: PT009
        self.assertIn("smoothedOutput", result)  # noqa: PT009
        mockGroceryService.fetchGroceryDict.assert_called_once()
        mockVanadiumCorrection.assert_called_once()
        mockFocusSpectra.assert_called_once()
        mockSmoothDataExcludingPeaks.assert_called_once()

    @patch("snapred.backend.service.NormalizationService.GroceryService")
    def test_cachedNormalization(self, mockGroceryService):
        self.instance = NormalizationService()
        result = self.instance.normalization(self.request)
        assert result == {
            "correctedVanadium": "tof_unfoc_12345_raw_van_corr",
            "outputWorkspace": f"tof_{self.request.focusGroup.name}_12345_s+f-vanadium",
            "smoothedOutput": "dsp_apple_12345_fitted_van_cor",
        }
