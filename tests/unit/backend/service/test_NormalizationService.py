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
        GroceryListItem,
        ReductionIngredients,
        SmoothDataExcludingPeaksIngredients,
    )
    from snapred.backend.dao.normalization import Normalization, NormalizationIndexEntry, NormalizationRecord
    from snapred.backend.dao.request import (
        FocusSpectraRequest,
        NormalizationCalibrationRequest,
        NormalizationExportRequest,
        SmoothDataExcludingPeaksRequest,
        VanadiumCorrectionRequest,
    )
    from snapred.backend.service.NormalizationService import NormalizationService
    from snapred.meta.Config import Resource

    thisService = "snapred.backend.service.NormalizationService."

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/normalization/input.json", "r") as f:
            return ReductionIngredients.parse_raw(f.read())

    def test_saveNormalization():
        normalizationService = NormalizationService()
        normalizationService.dataExportService.exportNormalizationRecord = mock.Mock()
        normalizationService.dataExportService.exportNormalizationRecord.return_value = MagicMock(version="1.0.0")
        normalizationService.dataExportService.exportNormalizationIndexEntry = mock.Mock()
        normalizationService.dataExportService.exportNormalizationIndexEntry.return_value = MagicMock("expected")
        normalizationService.dataFactoryService.getReductionIngredients = mock.Mock()
        normalizationService.dataFactoryService.getReductionIngredients.return_value = (
            readReductionIngredientsFromFile()
        )  # noqa: E501
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

    def clearoutWorkspaces(self) -> None:
        for ws in mtd.getObjectNames():
            self.instance.dataFactoryService.deleteWorkspace(ws)

    def tearDown(self) -> None:
        self.clearoutWorkspaces()
        return super().tearDown()

    @patch("snapred.backend.service.NormalizationService.CalibrationService")
    @patch("snapred.backend.service.NormalizationService.DataFactoryService")
    @patch("snapred.backend.service.NormalizationService.FocusSpectraRecipe")
    def test_focusSpectra(
        self,
        mockRecipe,
        mockDataFactoryService,
        mockCalibrationService,
    ):
        mockRequest = FocusSpectraRequest(
            runNumber="12345",
            groupingPath="path/to/grouping",
            useLiteMode=True,
            inputWorkspace="input_ws",
            groupingWorkspace="grouping_ws",
            outputWorkspace="output_ws",
        )
        self.instance = NormalizationService()
        mockedCalibration = MagicMock()
        mockCalibrationService = mockCalibrationService.return_value
        mockCalibrationService.getCalibration.return_value = mockedCalibration
        mockedReductionIngredients = MagicMock()
        mockDataFactoryService = mockDataFactoryService.return_value
        mockDataFactoryService.getReductionIngredients.return_value = mockedReductionIngredients
        self.instance.focusSpectra(mockRequest)
        mockRecipeInst = mockRecipe.return_value
        mockRecipeInst.executeRecipe.return_value = "expected"

        mockRecipeInst.executeRecipe.assert_called_once_with(
            InputWorkspace=mockRequest.inputWorkspace,
            GroupingWorkspace=mockRequest.groupingWorkspace,
            Ingredients=mockedReductionIngredients,
            OutputWorkspace=mockRequest.outputWorkspace,
        )
        mockCalibrationService.getCalibration.assert_called_once_with(
            mockRequest.runNumber, mockRequest.groupingPath, mockRequest.useLiteMode
        )
        mockDataFactoryService.getReductionIngredients.assert_called_once_with(
            mockRequest.runNumber, mockedCalibration.instrumentState.pixelGroup
        )

    @patch("snapred.backend.service.NormalizationService.DataFactoryService")
    @patch("snapred.backend.service.NormalizationService.CalibrationService")
    @patch("snapred.backend.service.NormalizationService.RawVanadiumCorrectionRecipe")
    def test_vanadiumCorrection(
        self,
        mockRecipe,
        mockCalibrationService,
        mockDataFactoryService,
    ):
        mockRequest = VanadiumCorrectionRequest(
            samplePath="path/to/sample",
            runNumber="12345",
            groupingPath="path/to/grouping",
            useLiteMode=True,
            inputWorkspace="input_ws",
            backgroundWorkspace="background_ws",
            outputWorkspace="output_ws",
        )
        self.instance = NormalizationService()
        mockedCalibrantSample = MagicMock()
        mockDataFactoryService = mockDataFactoryService.return_value
        mockDataFactoryService.getCalibrantSample.return_value = mockedCalibrantSample
        mockedCalibration = MagicMock()
        mockCalibrationService = mockCalibrationService.return_value
        mockCalibrationService.getCalibration.return_value = mockedCalibration
        mockedReductionIngredients = MagicMock()
        mockDataFactoryService.getReductionIngredients.return_value = mockedReductionIngredients
        self.instance.vanadiumCorrection(mockRequest)
        mockRecipeInst = mockRecipe.return_value
        mockRecipeInst.executeRecipe.return_value = "expected"

        mockRecipeInst.executeRecipe.assert_called_once_with(
            InputWorkspace=mockRequest.inputWorkspace,
            BackgroundWorkspace=mockRequest.backgroundWorkspace,
            Ingredients=mockedReductionIngredients,
            CalibrantSample=mockedCalibrantSample,
            OutputWorkspace=mockRequest.outputWorkspace,
        )
        mockDataFactoryService.getCalibrantSample.assert_called_once_with(mockRequest.samplePath)
        mockCalibrationService.getCalibration.assert_called_once_with(
            mockRequest.runNumber, mockRequest.groupingPath, mockRequest.useLiteMode
        )
        mockDataFactoryService.getReductionIngredients.assert_called_once_with(
            mockRequest.runNumber, mockedCalibration.instrumentState.pixelGroup
        )

    @patch("snapred.backend.service.NormalizationService.CalibrationService")
    @patch("snapred.backend.service.NormalizationService.CrystallographicInfoService")
    @patch("snapred.backend.service.NormalizationService.DataFactoryService")
    @patch("snapred.backend.service.NormalizationService.SmoothDataExcludingPeaksRecipe")
    def test_smoothDataExcludingPeaks(
        self,
        mockRecipe,
        mockDataFactoryService,
        mockCrystallographicInfoService,
        mockCalibrationService,
    ):
        mockRequest = SmoothDataExcludingPeaksRequest(
            inputWorkspace="input_ws",
            outputWorkspace="output_ws",
            useLiteMode=True,
            groupingPath="path/to/grouping",
            samplePath="path/to/sample",
            runNumber="12345",
            smoothingParameter=0.5,
            dMin=0.4,
        )

        self.instance = NormalizationService()
        mockSamplePath = MagicMock()
        mockDataFactoryService = mockDataFactoryService.return_value
        mockDataFactoryService.getCifFilePath.return_value = mockSamplePath
        mockedCrystalInfo = MagicMock()
        mockCrystallographicInfoService = mockCrystallographicInfoService.return_value
        mockCrystallographicInfoService.ingest.return_value = mockedCrystalInfo
        mockedCalibration = MagicMock()
        mockCalibrationService = mockCalibrationService.return_value
        mockCalibrationService.getCalibration.return_value = mockedCalibration

        self.instance.smoothDataExcludingPeaks(mockRequest)

        mockDataFactoryService.getCifFilePath.assert_called_once_with(
            mockRequest.samplePath.split("/")[-1].split(".")[0]
        )
        mockCrystallographicInfoService.ingest.assert_called_once_with(mockSamplePath, mockRequest.dMin)
        mockCalibrationService.getCalibration.assert_called_once_with(
            mockRequest.runNumber, mockRequest.groupingPath, mockRequest.useLiteMode
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
        mockRequest = NormalizationCalibrationRequest(
            runNumber="12345",
            backgroundRunNumber="67890",
            useLiteMode=True,
            samplePath="path/to/sample",
            groupingPath="path/to/grouping",
            smoothingParameter=0.5,
            dMin=0.4,
        )

        self.instance = NormalizationService()
        mockNormalization = MagicMock()
        mockDataFactoryService = mockDataFactoryService.return_value
        mockDataFactoryService.getNormalizationState.return_value = mockNormalization

        self.instance.dataFactoryService.getNormalizationRecord = MagicMock(return_value=mockNormalizationRecord)

        result = self.instance.normalizationAssessment(mockRequest)

        mockDataFactoryService.getNormalizationState.assert_called_once_with(mockRequest.runNumber)
        expected_record_mockId = "mock_normalization_record"
        assert result.mockId == expected_record_mockId

    @patch("snapred.backend.service.NormalizationService.vanadiumCorrection")
    @patch("snapred.backend.service.NormalizationService.focusSpectra")
    @patch("snapred.backend.service.NormalizationService.smoothDataExcludingPeaks")
    @patch("snapred.backend.service.NormalizationService.GroceryService")
    def test_normalization(
        self,
        mockGroceryService,
        mockSmoothDataExcludingPeaks,
        mockFocusSpectra,
        mockVanadiumCorrection,
    ):
        mockRequest = NormalizationCalibrationRequest(
            runNumber="12345",
            backgroundRunNumber="67890",
            samplePath="path/to/sample",
            groupingPath="path/to/grouping",
            useLiteMode=True,
            smoothingParameter=0.5,
            dMin=0.1,
        )

        self.instance = NormalizationService()
        mockGroceryService = mockGroceryService.return_value
        mockGroceryService.fetchGroceryDict.return_value = {
            "backgroundWorkspace": "bg_ws",
            "inputWorkspace": "input_ws",
            "groupingWorkspace": "grouping_ws",
            "outputWorkspace": "output_ws",
        }
        mockVanadiumCorrection.return_value = "corrected_vanadium_ws"
        mockFocusSpectra.return_value = "focussed_vanadium_ws"
        mockSmoothDataExcludingPeaks.return_value = "smoothed_output_ws"

        result = self.instance.normalization(mockRequest)

        self.assertIsInstance(result, dict)  # noqa: PT009
        self.assertIn("correctedVanadium", result)  # noqa: PT009
        self.assertIn("outputWorkspace", result)  # noqa: PT009
        self.assertIn("smoothedOutput", result)  # noqa: PT009
        mockGroceryService.fetchGroceryDict.assert_called_once()
        mockVanadiumCorrection.assert_called_once()
        mockFocusSpectra.assert_called_once()
        mockSmoothDataExcludingPeaks.assert_called_once()
