# ruff: noqa: E402, ARG002
import unittest
import unittest.mock as mock
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from mantid.simpleapi import (
    CreateSingleValuedWorkspace,
    mtd,
)
from snapred.backend.dao.request import CalibrationWritePermissionsRequest
from snapred.backend.dao.response.NormalizationResponse import NormalizationResponse
from snapred.backend.error.ContinueWarning import ContinueWarning

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
    from snapred.backend.dao.indexing.IndexEntry import IndexEntry
    from snapred.backend.dao.request import (
        FocusSpectraRequest,
        NormalizationRequest,
        SmoothDataExcludingPeaksRequest,
        VanadiumCorrectionRequest,
    )
    from snapred.backend.dao.state import FocusGroup
    from snapred.backend.service.NormalizationService import NormalizationService
    from util.SculleryBoy import SculleryBoy

    thisService = "snapred.backend.service.NormalizationService."

    def test_saveNormalization():
        normalizationService = NormalizationService()
        normalizationService.dataExportService.exportNormalizationRecord = mock.Mock()
        normalizationService.dataExportService.exportNormalizationRecord.return_value = MagicMock(version=1)
        normalizationService.dataExportService.exportNormalizationWorkspaces = mock.Mock()
        normalizationService.dataExportService.exportNormalizationWorkspaces.return_value = MagicMock(version=1)
        normalizationService.dataExportService.exportNormalizationIndexEntry = mock.Mock()
        normalizationService.dataExportService.exportNormalizationIndexEntry.return_value = MagicMock("expected")
        normalizationService.dataFactoryService.createNormalizationIndexEntry = mock.Mock()
        normalizationService.dataFactoryService.createNormalizationRecord = mock.Mock(
            return_value=mock.Mock(workspaceNames=[])
        )
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
            IndexEntry(runNumber="1", useLiteMode=True, backgroundRunNumber="2")
        )
        assert normalizationService.dataExportService.exportNormalizationIndexEntry.called
        savedEntry = normalizationService.dataExportService.exportNormalizationIndexEntry.call_args.args[0]
        assert savedEntry.appliesTo == ">=1"
        assert savedEntry.timestamp is not None


from snapred.backend.service.NormalizationService import NormalizationService  # noqa: F811


class TestNormalizationService(unittest.TestCase):
    def setUp(self):
        self.instance = NormalizationService()
        self.request = NormalizationRequest(
            runNumber="12345",
            backgroundRunNumber="67890",
            useLiteMode=True,
            calibrantSamplePath="path/to/sample",
            focusGroup=FocusGroup(name="apple", definition="path/to/grouping"),
            smoothingParameter=0.5,
            crystalDBounds={"minimum": 0.4, "maximum": 100.0},
            continueFlags=ContinueWarning.Type.UNSET,
        )

    def clearoutWorkspaces(self) -> None:
        for ws in mtd.getObjectNames():
            self.instance.groceryService.deleteWorkspace(ws)

    def tearDown(self) -> None:
        self.clearoutWorkspaces()
        return super().tearDown()

    def test_saveNormalization_workspaces_renamed(self):
        version = 10
        wsname = "test"
        CreateSingleValuedWorkspace(OutputWorkspace=wsname)
        self.instance.dataFactoryService.createNormalizationIndexEntry = mock.Mock(
            return_value=mock.Mock(version=version)
        )
        self.instance.dataFactoryService.createNormalizationRecord = mock.Mock(
            return_value=mock.Mock(version=version, workspaceNames=[wsname])
        )
        self.instance.dataExportService.exportNormalizationRecord = mock.Mock()
        self.instance.dataExportService.exportNormalizationWorkspaces = mock.Mock()
        self.instance.dataExportService.exportNormalizationIndexEntry = mock.Mock()
        self.instance.saveNormalization(mock.Mock())
        savedRecord = self.instance.dataExportService.exportNormalizationRecord.call_args[0]
        assert savedRecord[0].workspaceNames == [f"{wsname}_v0010"]

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

        mockRecipeInst.executeRecipe.assert_called_once_with(
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

        FarmFreshIngredients.return_value.get.return_value = True

        res = self.instance.vanadiumCorrection(mockRequest)

        mockRecipeInst = RawVanadiumCorrectionRecipe.return_value
        mockRecipeInst.executeRecipe.assert_called_once_with(
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
        FarmFreshIngredients.return_value = mock.Mock(spec=FarmFreshIngredients)
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
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="path/to/cif")

        mockRecipeInst = mockRecipe.return_value

        self.instance.smoothDataExcludingPeaks(mockRequest)

        mockRecipeInst.executeRecipe.assert_called_once_with(
            InputWorkspace=mockRequest.inputWorkspace,
            OutputWorkspace=mockRequest.outputWorkspace,
            DetectorPeaks=self.instance.sousChef.prepDetectorPeaks(FarmFreshIngredients.return_value),
            SmoothingParameter=mockRequest.smoothingParameter,
        )

    def test_matchRuns(self):
        self.instance.dataFactoryService.getThisOrLatestNormalizationVersion = mock.Mock(
            side_effect=[mock.sentinel.version1, mock.sentinel.version2],
        )
        request = mock.Mock(runNumbers=[mock.sentinel.run1, mock.sentinel.run2], useLiteMode=True)
        response = self.instance.matchRunsToNormalizationVersions(request)
        assert response == {
            mock.sentinel.run1: mock.sentinel.version1,
            mock.sentinel.run2: mock.sentinel.version2,
        }

    def test_fetchRuns(self):
        mockCalibrations = {
            mock.sentinel.run1: mock.sentinel.version1,
            mock.sentinel.run2: mock.sentinel.version2,
            mock.sentinel.run3: mock.sentinel.version2,
        }
        mockGroceries = [mock.sentinel.grocery1, mock.sentinel.grocery2, mock.sentinel.grocery2]
        self.instance.matchRunsToNormalizationVersions = mock.Mock(return_value=mockCalibrations)
        self.instance.groceryService.fetchGroceryList = mock.Mock(return_value=mockGroceries)
        self.instance.groceryClerk = mock.Mock()

        request = mock.Mock(runNumbers=[mock.sentinel.run1, mock.sentinel.run2], useLiteMode=True)
        groceries, cal = self.instance.fetchMatchingNormalizations(request)
        assert groceries == {mock.sentinel.grocery1, mock.sentinel.grocery2}
        assert cal == mockCalibrations

    def test_normalizationAssessment(self):
        self.instance = NormalizationService()
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService.createNormalizationRecord = MagicMock()

        result = self.instance.normalizationAssessment(self.request)

        assert result == self.instance.dataFactoryService.createNormalizationRecord.return_value

    @patch(thisService + "FarmFreshIngredients")
    @patch(thisService + "RawVanadiumCorrectionRecipe")
    @patch(thisService + "PreprocessReductionRecipe")
    @patch(thisService + "ReductionGroupProcessingRecipe")
    @patch(thisService + "SmoothDataExcludingPeaksRecipe")
    def test_normalization(
        self,
        mockSmoothDataExcludingPeaks,
        mockReductionGroupProcessing,  # noqa: ARG002
        mockPreprocessReduction,
        mockVanadiumCorrection,
        FarmFreshIngredients,
    ):
        FarmFreshIngredients.return_value = {"purge": True}
        mockGroceryService = mock.Mock()
        mockGroceryService.fetchGroceryDict.return_value = {
            "backgroundWorkspace": "bg_ws",
            "inputWorkspace": "input_ws",
            "groupingWorkspace": "grouping_ws",
            "outputWorkspace": "output_ws",
        }
        mockGroceryService.workspaceDoesExist.return_value = False
        mockVanadiumCorrection.executeRecipe.return_value = "corrected_vanadium_ws"
        mockPreprocessReduction.executeRecipe.return_value = "focussed_vanadium_ws"
        mockSmoothDataExcludingPeaks.executeRecipe.return_value = "smoothed_output_ws"

        self.instance = NormalizationService()
        self.instance._sameStates = mock.Mock(return_value=True)
        self.instance.sousChef = SculleryBoy()
        self.instance.groceryService = mockGroceryService
        self.instance.dataFactoryService.getCifFilePath = MagicMock(return_value="path/to/cif")
        self.instance.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=1)
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value="lah/dee/dah")
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=mock.Mock(runNumber="12345"))
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=True)

        result = self.instance.normalization(self.request)

        self.assertIsInstance(result, dict)  # noqa: PT009
        self.assertIn("correctedVanadium", result)  # noqa: PT009
        self.assertIn("focusedVanadium", result)  # noqa: PT009
        self.assertIn("smoothedVanadium", result)  # noqa: PT009
        mockGroceryService.fetchGroceryDict.assert_called_once()
        mockVanadiumCorrection.assert_called_once()
        mockPreprocessReduction.assert_called_once()
        mockSmoothDataExcludingPeaks.assert_called_once()

    def test_validateRequest(self):
        # test `validateRequest` internal calls
        self.instance._sameStates = mock.Mock(return_value=True)
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)
        self.instance.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=1)
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=self.request.runNumber, continueFlags=self.request.continueFlags
        )
        with mock.patch.object(self.instance, "validateWritePermissions") as mockValidateWritePermissions:
            self.instance.validateRequest(self.request)
            self.instance._sameStates.assert_called_once_with(self.request.runNumber, self.request.backgroundRunNumber)
            mockValidateWritePermissions.assert_called_once_with(permissionsRequest)

    def test_validateDiffractionCalibrationExists_failure(self):
        request = mock.Mock(runNumber="12345", backgroundRunNumber="67890", continueFlags=ContinueWarning.Type.UNSET)
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=-1)

        with pytest.raises(
            ContinueWarning,
            match=r".*Continue anyway*",
        ):
            self.instance._validateDiffractionCalibrationExists(request)

    def test_validateDiffractionCalibrationExists_success_contineuAnyway(self):
        request = mock.Mock(
            runNumber="12345",
            backgroundRunNumber="67890",
            continueFlags=ContinueWarning.Type.DEFAULT_DIFFRACTION_CALIBRATION,
        )
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=-1)
        self.instance._validateDiffractionCalibrationExists(request)

    def test_validateRequest_different_states(self):
        # test `validateRequest` raises `ValueError`
        self.instance._sameStates = mock.Mock(return_value=False)
        self.instance.validateWritePermissions = mock.Mock()
        with pytest.raises(
            ValueError, match=r"Run number and background run number must be of the same Instrument State."
        ):
            self.instance.validateRequest(self.request)

    def test_validateWritePermissions(self):
        # test that `validateWritePermissions` throws no exceptions
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value=Path("lah/dee/dah"))
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=True)
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=self.request.runNumber, continueFlags=self.request.continueFlags
        )
        self.instance.validateWritePermissions(permissionsRequest)
        self.instance.dataExportService.getCalibrationStateRoot.assert_called_once_with(self.request.runNumber)
        self.instance.dataExportService.checkWritePermissions.assert_called_once_with(
            self.instance.dataExportService.getCalibrationStateRoot.return_value
        )

    def test_validateWritePermissions_no_permissions(self):
        # test that `validateWritePermissions` raises `ContinueWarning`
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value=Path("lah/dee/dah"))
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=False)
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=self.request.runNumber, continueFlags=self.request.continueFlags
        )
        with pytest.raises(RuntimeError, match=r".*you don't have permissions to write.*"):
            self.instance.validateWritePermissions(permissionsRequest)

    @patch(thisService + "FarmFreshIngredients")
    def test_cachedNormalization(self, mockFarmFreshIngredients):
        mockFarmFreshIngredients.return_value = {"purge": True}
        self.instance = NormalizationService()
        self.instance._sameStates = mock.Mock(return_value=True)
        self.instance.sousChef = SculleryBoy()
        self.instance.groceryService = mock.Mock()
        self.instance.groceryService.workSpaceDoesExist = mock.Mock(return_value=True)
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="path/to/cif")
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value="lah/dee/dah")
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)
        self.instance.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=1)
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=True)
        result = self.instance.normalization(self.request)

        assert (
            result
            == NormalizationResponse(
                correctedVanadium="_tof_unfoc_raw_van_corr_012345",
                focusedVanadium=f"_tof_{self.request.focusGroup.name}_s+f-vanadium_012345",
                smoothedVanadium="_dsp_apple_fitted_van_corr_012345",
                detectorPeaks=self.instance.sousChef.prepNormalizationIngredients(
                    mockFarmFreshIngredients()
                ).detectorPeaks,
            ).dict()
        )

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
