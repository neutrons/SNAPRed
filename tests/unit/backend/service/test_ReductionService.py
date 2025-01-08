import unittest
import unittest.mock as mock
from pathlib import Path
from typing import List

import numpy as np
import pydantic
import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    mtd,
)
from mantid.testing import assert_almost_equal as wksp_almost_equal
from util.helpers import (
    arrayFromMask,
    createCompatibleMask,
    maskFromArray,
)
from util.InstaEats import InstaEats
from util.SculleryBoy import SculleryBoy
from util.state_helpers import reduction_root_redirect

from snapred.backend.api.RequestScheduler import RequestScheduler
from snapred.backend.dao import WorkspaceMetadata
from snapred.backend.dao.ingredients import ArtificialNormalizationIngredients
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.request import (
    CreateArtificialNormalizationRequest,
    FarmFreshIngredients,
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.request.ReductionRequest import Versions
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.state import DetectorState
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.service.ReductionService import ReductionService
from snapred.meta.Config import Resource
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

localMock = mock.Mock()

thisService = "snapred.backend.service.ReductionService."


class TestReductionService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sculleryBoy = SculleryBoy()
        cls.instaEats = InstaEats()
        cls.localDataService = cls.instaEats.dataService

    def clearoutWorkspaces(self) -> None:
        # Delete the workspaces created by loading
        for ws in mtd.getObjectNames():
            DeleteWorkspace(ws)

    def tearDown(self) -> None:
        # At the end of each test, clear out the workspaces
        self.clearoutWorkspaces()
        return super().tearDown()

    def setUp(self):
        self.instance = ReductionService()

        ## Mock out the assistant services
        self.instance.sousChef = self.sculleryBoy
        self.instance.groceryService = self.instaEats
        self.instance.dataFactoryService.lookupService = self.localDataService
        self.instance.dataExportService.dataService = self.localDataService

        self.request = ReductionRequest(
            runNumber="123",
            useLiteMode=False,
            timestamp=self.instance.getUniqueTimestamp(),
            versions=(1, 2),
            pixelMasks=[],
            keepUnfocused=True,
            convertUnitsTo="TOF",
            focusGroups=[FocusGroup(name="apple", definition="path/to/grouping")],
            artificialNormalizationIngredients=mock.Mock(spec=ArtificialNormalizationIngredients),
        )

    def test_name(self):
        ## this makes codecov happy
        assert "reduction" == self.instance.name()

    def test_loadAllGroupings(self):
        data = self.instance.loadAllGroupings(self.request.runNumber, self.request.useLiteMode)
        assert pydantic.TypeAdapter(List[FocusGroup]).validate_python(data["focusGroups"])
        assert pydantic.TypeAdapter(List[str]).validate_python(data["groupingWorkspaces"])

    def test_loadAllGroupings_default(self):
        self.instance.dataFactoryService.getGroupingMap = mock.Mock(
            side_effect=StateValidationException(Exception("test"))
        )
        self.instance.dataFactoryService.getDefaultGroupingMap = mock.MagicMock()
        self.instance.loadAllGroupings(self.request.runNumber, self.request.useLiteMode)

        self.instance.dataFactoryService.getDefaultGroupingMap.assert_called_once()

    def test_fetchReductionGroupings(self):
        data = self.instance.fetchReductionGroupings(self.request)
        assert data == self.instance.loadAllGroupings(self.request.runNumber, self.request.useLiteMode)

    def test_prepReductionIngredients(self):
        # Call the method with the provided parameters
        result = self.instance.prepReductionIngredients(self.request)

        farmFresh = FarmFreshIngredients(
            runNumber=self.request.runNumber,
            useLiteMode=self.request.useLiteMode,
            timestamp=self.request.timestamp,
            focusGroups=self.request.focusGroups,
            keepUnfocused=self.request.keepUnfocused,
            convertUnitsTo=self.request.convertUnitsTo,
            versions=self.request.versions,
        )
        expected = self.instance.sousChef.prepReductionIngredients(farmFresh)
        expected.artificialNormalizationIngredients = self.request.artificialNormalizationIngredients

        assert ReductionIngredients.model_validate(result)
        assert result == expected

    def test_fetchReductionGroceries(self):
        self.instance.dataFactoryService.getLatestApplicableCalibrationVersion = mock.Mock(return_value=1)
        self.instance.dataFactoryService.getLatestApplicableNormalizationVersion = mock.Mock(return_value=1)
        self.instance._markWorkspaceMetadata = mock.Mock()
        self.request.continueFlags = ContinueWarning.Type.UNSET
        res = self.instance.fetchReductionGroceries(self.request)
        assert "inputWorkspace" in res
        assert "diffcalWorkspace" in res
        assert "normalizationWorkspace" in res

    def test_fetchReductionGroceries_use_mask(self):
        """
        Check that this properly handles using the reduction mask.
        This actually sets if the mask workspace inside the RECIPE is set.
        - when a mask is created by prepCombineMask and is non-empty, then should be sent to recipe
        - otherwise, there should be no mask sent to the recipe
        """
        from snapred.backend.recipe.ReductionRecipe import ReductionRecipe

        self.instance.dataFactoryService.getLatestApplicableCalibrationVersion = mock.Mock(return_value=1)
        self.instance.dataFactoryService.getLatestApplicableNormalizationVersion = mock.Mock(return_value=1)
        self.instance._markWorkspaceMetadata = mock.Mock()
        self.instance.prepCombinedMask = mock.Mock(return_value=mock.sentinel.mask)
        self.request.continueFlags = ContinueWarning.Type.UNSET

        rx = ReductionRecipe()

        # situation where mask is true -- ensure mask is set
        self.instance.groceryService.checkPixelMask = mock.Mock(return_value=True)
        res = self.instance.fetchReductionGroceries(self.request)
        res["groupingWorkspaces"] = [mock.sentinel.groupingWS]
        rx.unbagGroceries(res)
        assert rx.maskWs == mock.sentinel.mask

        # change mask to be false -- make sure unused
        self.instance.groceryService.checkPixelMask.return_value = False
        res = self.instance.fetchReductionGroceries(self.request)
        res["groupingWorkspaces"] = [mock.sentinel.groupingWS]
        rx.unbagGroceries(res)
        assert rx.maskWs == ""

    @mock.patch(thisService + "ReductionRecipe")
    def test_reduction(self, mockReductionRecipe):
        mockReductionRecipe.return_value = mock.Mock()
        mockResult = {
            "result": True,
            "outputs": ["one", "two", "three"],
        }
        mockReductionRecipe.return_value.cook = mock.Mock(return_value=mockResult)
        self.instance.dataFactoryService.getLatestApplicableCalibrationVersion = mock.Mock(return_value=1)
        self.instance.dataFactoryService.stateExists = mock.Mock(return_value=True)
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)
        self.instance.dataFactoryService.getLatestApplicableNormalizationVersion = mock.Mock(return_value=1)
        self.instance.dataFactoryService.normalizationExists = mock.Mock(return_value=True)
        self.instance._markWorkspaceMetadata = mock.Mock()

        result = self.instance.reduction(self.request)
        groupings = self.instance.fetchReductionGroupings(self.request)
        ingredients = self.instance.prepReductionIngredients(self.request)
        groceries = self.instance.fetchReductionGroceries(self.request)
        groceries["groupingWorkspaces"] = groupings["groupingWorkspaces"]
        mockReductionRecipe.assert_called()
        mockReductionRecipe.return_value.cook.assert_called_once_with(ingredients, groceries)
        assert result.record.workspaceNames == mockReductionRecipe.return_value.cook.return_value["outputs"]

    @mock.patch(thisService + "ReductionResponse")
    @mock.patch(thisService + "ReductionRecipe")
    def test_reduction_full_sequence(self, mockReductionRecipe, mockReductionResponse):
        mockReductionRecipe.return_value = mock.Mock()
        mockResult = {"result": True, "outputs": ["one", "two", "three"], "unfocusedWS": mock.Mock()}
        mockReductionRecipe.return_value.cook = mock.Mock(return_value=mockResult)
        self.instance.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=1)
        self.instance.dataFactoryService.stateExists = mock.Mock(return_value=True)
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)
        self.instance.dataFactoryService.getThisOrLatestNormalizationVersion = mock.Mock(return_value=1)
        self.instance.dataFactoryService.normalizationExists = mock.Mock(return_value=True)
        self.instance._markWorkspaceMetadata = mock.Mock()

        self.instance.fetchReductionGroupings = mock.Mock(
            return_value={"focusGroups": mock.Mock(), "groupingWorkspaces": mock.Mock()}
        )
        self.instance.fetchReductionGroceries = mock.Mock(return_value={"combinedPixelMask": mock.Mock()})
        self.instance.prepReductionIngredients = mock.Mock(return_value=mock.Mock())
        self.instance._createReductionRecord = mock.Mock(return_value=mock.Mock())

        request_ = self.request.model_copy()
        self.instance.reduction(request_)

        self.instance.fetchReductionGroupings.assert_called_once_with(request_)
        assert request_.focusGroups == self.instance.fetchReductionGroupings.return_value["focusGroups"]
        self.instance.fetchReductionGroceries.assert_called_once_with(request_)
        self.instance.prepReductionIngredients.assert_called_once_with(
            request_, self.instance.fetchReductionGroceries.return_value["combinedPixelMask"]
        )
        assert (
            self.instance.fetchReductionGroceries.return_value["groupingWorkspaces"]
            == self.instance.fetchReductionGroupings.return_value["groupingWorkspaces"]
        )

        self.instance._createReductionRecord.assert_called_once_with(
            request_,
            self.instance.prepReductionIngredients.return_value,
            mockReductionRecipe.return_value.cook.return_value["outputs"],
        )
        mockReductionResponse.assert_called_once_with(
            record=self.instance._createReductionRecord.return_value,
            unfocusedData=mockReductionRecipe.return_value.cook.return_value["unfocusedWS"],
        )

    def test_reduction_noState_withWritePerms(self):
        mockRequest = mock.Mock()
        self.instance.dataFactoryService.stateExists = mock.Mock(return_value=False)
        self.instance.checkReductionWritePermissions = mock.Mock(return_value=True)
        with pytest.raises(RecoverableException, match="State uninitialized"):
            self.instance.validateReduction(mockRequest)

    def test_reduction_noState_noWritePerms(self):
        mockRequest = mock.Mock()
        self.instance.dataFactoryService.stateExists = mock.Mock(return_value=False)
        self.instance.checkCalibrationWritePermissions = mock.Mock(return_value=False)
        self.instance.getSavePath = mock.Mock(return_value="path")
        with pytest.raises(
            RuntimeError,
            match=r".*This run has not been initialized for reduction and you lack the necessary permissions*",
        ):
            self.instance.validateReduction(mockRequest)

    def test_markWorkspaceMetadata(self):
        request = mock.Mock(continueFlags=ContinueWarning.Type.UNSET)
        metadata = WorkspaceMetadata(diffcalState="exists", normalizationState="exists")
        wsName = "test"
        self.instance.groceryService = mock.Mock()
        self.instance._markWorkspaceMetadata(request, wsName)
        self.instance.groceryService.writeWorkspaceMetadataAsTags.assert_called_once_with(wsName, metadata)

    def test_markWorkspaceMetadata_continue(self):
        request = mock.Mock(continueFlags=ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION)
        metadata = WorkspaceMetadata(diffcalState="none", normalizationState="exists")
        wsName = "test"
        self.instance.groceryService = mock.Mock()
        self.instance._markWorkspaceMetadata(request, wsName)
        self.instance.groceryService.writeWorkspaceMetadataAsTags.assert_called_once_with(wsName, metadata)

    def test_markWorkspaceMetadata_continueNormalization(self):
        request = mock.Mock(continueFlags=ContinueWarning.Type.MISSING_NORMALIZATION)
        metadata = WorkspaceMetadata(diffcalState="exists", normalizationState="fake")
        wsName = "test"
        self.instance.groceryService = mock.Mock()
        self.instance._markWorkspaceMetadata(request, wsName)
        self.instance.groceryService.writeWorkspaceMetadataAsTags.assert_called_once_with(wsName, metadata)

    def test_saveReduction(self):
        with (
            mock.patch.object(self.instance.dataExportService, "exportReductionRecord") as mockExportRecord,
            mock.patch.object(self.instance.dataExportService, "exportReductionData") as mockExportData,
        ):
            runNumber = "123456"
            useLiteMode = True
            timestamp = self.instance.getUniqueTimestamp()
            record = ReductionRecord.model_construct(
                runNumber=runNumber,
                useLiteMode=useLiteMode,
                timestamp=timestamp,
            )
            request = ReductionExportRequest(record=record)
            with reduction_root_redirect(self.localDataService):
                self.instance.saveReduction(request)
                mockExportRecord.assert_called_once_with(record)
                mockExportData.assert_called_once_with(record)

    def test_loadReduction(self):
        ## this makes codecov happy
        with pytest.raises(NotImplementedError):
            self.instance.loadReduction(
                stateId="babeeeee",
                timestamp=self.instance.getUniqueTimestamp(),
            )

    def test_hasState(self):
        assert self.instance.hasState("123456")
        assert not self.instance.hasState("not a state")
        assert not self.instance.hasState("1")

    def test_uniqueTimestamp(self):
        with mock.patch.object(self.instance.dataExportService, "getUniqueTimestamp") as mockTimestamp:
            mockTimestamp.return_value = 123.456
            assert self.instance.getUniqueTimestamp() == mockTimestamp.return_value

    def test_checkReductionWritePermissions(self):
        with (
            mock.patch.object(self.instance.dataExportService, "checkWritePermissions") as mockcheckWritePermissions,
            mock.patch.object(self.instance.dataExportService, "getReductionStateRoot") as mockGetReductionStateRoot,
        ):
            runNumber = "12345"
            mockcheckWritePermissions.return_value = True
            mockGetReductionStateRoot.return_value = Path("/reduction/state/root")
            assert self.instance.checkReductionWritePermissions(runNumber)
            mockcheckWritePermissions.assert_called_once_with(mockGetReductionStateRoot.return_value)
            mockGetReductionStateRoot.assert_called_once_with(runNumber)

    def test_getSavePath(self):
        with mock.patch.object(self.instance.dataExportService, "getReductionStateRoot") as mockGetReductionStateRoot:
            runNumber = "12345"
            expected = Path("/reduction/state/root")
            mockGetReductionStateRoot.return_value = expected
            actual = self.instance.getSavePath(runNumber)
            assert actual == expected
            mockGetReductionStateRoot.assert_called_once_with(runNumber)

    def test_getStateIds(self):
        expectedStateIds = ["0" * 16, "1" * 16, "2" * 16, "3" * 16]
        with mock.patch.object(self.instance.dataFactoryService, "constructStateId") as mockConstructStateId:
            runNumbers = ["4" * 6, "5" * 6, "6" * 6, "7" * 6]
            mockConstructStateId.side_effect = lambda runNumber: (
                dict(zip(runNumbers, expectedStateIds))[runNumber],
                None,
            )
            actualStateIds = self.instance.getStateIds(runNumbers)
            assert actualStateIds == expectedStateIds
            mockConstructStateId.call_count == len(runNumbers)

    def test_groupRequests(self):
        payload = self.request.json()
        request = SNAPRequest(path="test", payload=payload)

        scheduler = RequestScheduler()
        self.instance.registerGrouping("", self.instance._groupByStateId)
        self.instance.registerGrouping("", self.instance._groupByVanadiumVersion)
        groupings = self.instance.getGroupings("")
        result = scheduler.handle([request], groupings)

        # Verify the request is sorted by state id then normalization version
        mockDataFactory = mock.Mock()
        mockDataFactory.getLatestApplicableNormalizationVersion.side_effect = [0, 1]
        mockDataFactory.constructStateId.return_value = ("state1", "_")
        self.instance.dataFactoryService = mockDataFactory

        # now sort
        result = scheduler.handle(
            [request, request], [self.instance._groupByStateId, self.instance._groupByVanadiumVersion]
        )

        assert result["root"]["state1"]["normalization_1"][0] == request
        assert result["root"]["state1"]["normalization_0"][0] == request

    def test_validateReduction(self):
        fakeDataService = mock.Mock()
        fakeDataService.calibrationExists.return_value = True
        fakeDataService.normalizationExists.return_value = True
        self.instance.dataFactoryService = fakeDataService
        self.instance.validateReduction(self.request)

    def test_validateReduction_noCalibration(self):
        # assert ContinueWarning is raised

        # copilot come on, we have tested for exceptions before, please pick up on this
        # its even in the same file
        fakeDataService = mock.Mock()
        fakeDataService.calibrationExists.return_value = False
        fakeDataService.normalizationExists.return_value = True
        self.instance.dataFactoryService = fakeDataService
        with pytest.raises(ContinueWarning) as excInfo:
            self.instance.validateReduction(self.request)
        assert excInfo.value.model.flags == ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION

    def test_validateReduction_noNormalization(self):
        # assert ContinueWarning is raised
        fakeDataService = mock.Mock()
        fakeDataService.calibrationExists.return_value = True
        fakeDataService.normalizationExists.return_value = False
        self.instance.dataFactoryService = fakeDataService
        with pytest.raises(ContinueWarning) as excInfo:
            self.instance.validateReduction(self.request)
        assert excInfo.value.model.flags == ContinueWarning.Type.MISSING_NORMALIZATION

    def test_validateReduction_reentry(self):
        # assert ContinueWarning is NOT raised multiple times
        fakeDataService = mock.Mock()
        fakeDataService.calibrationExists.return_value = False
        fakeDataService.normalizationExists.return_value = False
        self.instance.dataFactoryService = fakeDataService
        self.request.continueFlags = (
            ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION | ContinueWarning.Type.MISSING_NORMALIZATION
        )
        self.instance.validateReduction(self.request)

    def test_validateReduction_no_permissions(self):
        # assert ContinueWarning is raised
        fakeDataService = mock.Mock()
        fakeDataService.calibrationExists.return_value = True
        fakeDataService.normalizationExists.return_value = True
        self.instance.dataFactoryService = fakeDataService
        fakeExportService = mock.Mock()
        fakeExportService.checkWritePermissions.return_value = False
        self.instance.dataExportService = fakeExportService
        with pytest.raises(ContinueWarning) as excInfo:
            self.instance.validateReduction(self.request)
        assert excInfo.value.model.flags == ContinueWarning.Type.NO_WRITE_PERMISSIONS

    def test_validateReduction_no_permissions_reentry(self):
        # assert ContinueWarning is NOT raised multiple times
        fakeDataService = mock.Mock()
        fakeDataService.calibrationExists.return_value = True
        fakeDataService.normalizationExists.return_value = True
        self.instance.dataFactoryService = fakeDataService
        fakeExportService = mock.Mock()
        fakeExportService.checkReductionWritePermissions.return_value = False
        self.instance.dataExportService = fakeExportService
        self.request.continueFlags = ContinueWarning.Type.NO_WRITE_PERMISSIONS
        self.instance.validateReduction(self.request)

    def test_validateReduction_no_permissions_and_no_calibrations(self):
        # assert ContinueWarning is raised
        fakeDataService = mock.Mock()
        fakeDataService.calibrationExists.return_value = False
        fakeDataService.normalizationExists.return_value = False
        self.instance.dataFactoryService = fakeDataService

        fakeExportService = mock.Mock()
        fakeExportService.checkReductionWritePermissions.return_value = False
        self.instance.dataExportService = fakeExportService

        with pytest.raises(ContinueWarning) as excInfo:
            self.instance.validateReduction(self.request)

        assert excInfo.value.model.flags == (
            ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION | ContinueWarning.Type.MISSING_NORMALIZATION
        )

    def test_validateReduction_no_permissions_and_no_calibrations_first_reentry(self):
        # assert ContinueWarning is raised
        fakeDataService = mock.Mock()
        fakeDataService.calibrationExists.return_value = False
        fakeDataService.normalizationExists.return_value = False
        self.instance.dataFactoryService = fakeDataService
        fakeExportService = mock.Mock()
        fakeExportService.checkWritePermissions.return_value = False
        self.instance.dataExportService = fakeExportService
        self.request.continueFlags = (
            ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION | ContinueWarning.Type.MISSING_NORMALIZATION
        )
        with pytest.raises(ContinueWarning) as excInfo:
            self.instance.validateReduction(self.request)

        assert excInfo.value.model.flags == ContinueWarning.Type.NO_WRITE_PERMISSIONS

    def test_validateReduction_no_permissions_and_no_calibrations_second_reentry(self):
        # assert ContinueWarning is raised
        fakeDataService = mock.Mock()
        fakeDataService.calibrationExists.return_value = False
        fakeDataService.normalizationExists.return_value = False
        self.instance.dataFactoryService = fakeDataService
        fakeExportService = mock.Mock()
        fakeExportService.checkReductionWritePermissions.return_value = False
        self.instance.dataExportService = fakeExportService
        self.request.continueFlags = (
            ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION
            | ContinueWarning.Type.MISSING_NORMALIZATION
            | ContinueWarning.Type.NO_WRITE_PERMISSIONS
        )
        # Note: this tests re-entry for the _first_ continue-anyway check,
        #   and in addition, re-entry for the second continue-anyway check.
        self.instance.validateReduction(self.request)

    @mock.patch(thisService + "ArtificialNormalizationRecipe")
    def test_artificialNormalization(self, mockArtificialNormalizationRecipe):
        mockArtificialNormalizationRecipe.return_value = mock.Mock()
        mockResult = mock.Mock()
        mockArtificialNormalizationRecipe.return_value.executeRecipe.return_value = mockResult

        request = CreateArtificialNormalizationRequest(
            runNumber="123",
            useLiteMode=False,
            peakWindowClippingSize=5,
            smoothingParameter=0.1,
            decreaseParameter=True,
            lss=True,
            diffractionWorkspace="mock_diffraction_workspace",
            outputWorkspace="artificial_norm_dsp_column_000123_preview",
        )

        result = self.instance.artificialNormalization(request)

        mockArtificialNormalizationRecipe.return_value.executeRecipe.assert_called_once_with(
            InputWorkspace=request.diffractionWorkspace,
            peakWindowClippingSize=request.peakWindowClippingSize,
            smoothingParameter=request.smoothingParameter,
            decreaseParameter=request.decreaseParameter,
            lss=request.lss,
            OutputWorkspace=request.outputWorkspace,
        )
        assert result == mockResult

    @mock.patch(thisService + "RebinFocussedGroupDataRecipe")
    @mock.patch(thisService + "ReductionGroupProcessingRecipe")
    @mock.patch(thisService + "GroceryService")
    @mock.patch(thisService + "DataFactoryService")
    def test_grabWorkspaceforArtificialNorm(
        self,
        mockDataFactoryService,
        mockGroceryService,
        mockReductionGroupProcessingRecipe,
        mockRebinFocussedGroupDataRecipe,
    ):
        self.instance.groceryService = mockGroceryService
        self.instance.dataFactoryService = mockDataFactoryService
        self.instance.groceryClerk = mock.Mock()
        request = ReductionRequest(
            runNumber="123",
            useLiteMode=False,
            timestamp=self.instance.getUniqueTimestamp(),
            versions=(1, 2),
            pixelMasks=[],
            focusGroups=[FocusGroup(name="apple", definition="path/to/grouping")],
        )

        mockIngredients = mock.Mock()
        mockIngredients.pixelGroups = [mock.Mock()]

        runWorkspaceName = "runworkspace"
        columnGroupingWS = "columnGroupingWS"
        self.instance.groceryService.fetchGroceryList.return_value = [runWorkspaceName]
        self.instance.loadAllGroupings = mock.Mock(
            return_value={
                "groupingWorkspaces": [columnGroupingWS],
                "focusGroups": [mock.MagicMock(name="columnFocusGroup")],
            }
        )
        self.instance.prepReductionIngredients = mock.Mock(return_value=mockIngredients)

        self.instance.grabWorkspaceforArtificialNorm(request)

        groceries = {
            "inputWorkspace": runWorkspaceName,
            "groupingWorkspace": columnGroupingWS,
            "outputWorkspace": "artificial_norm_dsp_column_000123_source",
        }

        mockReductionGroupProcessingRecipe().cook.assert_called_once_with(mockIngredients.groupProcessing(0), groceries)
        groceries = {"inputWorkspace": groceries["outputWorkspace"]}
        rebinIngredients = mockRebinFocussedGroupDataRecipe.Ingredients()
        mockRebinFocussedGroupDataRecipe().cook.assert_called_once_with(rebinIngredients, groceries)


class TestReductionServiceMasks:
    @pytest.fixture(autouse=True, scope="class")
    @classmethod
    def _setup_test_data(
        cls,
        create_sample_workspace,
        create_sample_pixel_mask,
    ):
        # Warning: the order of class `__init__` vs. autouse-fixture setup calls is ambiguous;
        #   for this reason, the `service` attribute, and anything that is initialized using it,
        #   is initialized _here_ in this fixture.

        cls.service = ReductionService()
        cls.dataExportService = cls.service.dataExportService
        cls.localDataService = cls.service.dataExportService.dataService

        cls.runNumber1 = "123456"
        cls.runNumber2 = "123457"
        cls.runNumber3 = "123458"
        cls.runNumber4 = "123459"
        cls.useLiteMode = True

        # Arbitrary `DetectorState`s used for realistic instrument initialization
        cls.detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        cls.detectorState2 = DetectorState(arc=(7.0, 8.0), wav=9.0, freq=10.0, guideStat=2, lin=(11.0, 12.0))

        # The corresponding stateId:
        cls.stateId1 = cls.localDataService._stateIdFromDetectorState(cls.detectorState1).hex
        cls.stateId2 = cls.localDataService._stateIdFromDetectorState(cls.detectorState2).hex

        cls.instrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
        cls.instrumentLiteFilePath = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
        instrumentFilePath = cls.instrumentLiteFilePath if cls.useLiteMode else cls.instrumentFilePath

        # create instrument workspaces for each state
        cls.sampleWS1 = mtd.unique_hidden_name()
        create_sample_workspace(cls.sampleWS1, cls.detectorState1, instrumentFilePath)
        cls.sampleWS2 = mtd.unique_hidden_name()
        create_sample_workspace(cls.sampleWS2, cls.detectorState2, instrumentFilePath)

        # random fraction used for mask initialization
        cls.randomFraction = 0.2

        # Create a pair of mask workspaces for each state
        cls.maskWS1 = (
            wng.reductionPixelMask().runNumber(cls.runNumber1).timestamp(cls.service.getUniqueTimestamp()).build()
        )
        cls.maskWS2 = (
            wng.reductionPixelMask().runNumber(cls.runNumber2).timestamp(cls.service.getUniqueTimestamp()).build()
        )
        cls.maskWS3 = (
            wng.reductionPixelMask().runNumber(cls.runNumber3).timestamp(cls.service.getUniqueTimestamp()).build()
        )
        cls.maskWS4 = (
            wng.reductionPixelMask().runNumber(cls.runNumber4).timestamp(cls.service.getUniqueTimestamp()).build()
        )
        cls.maskWS5 = wng.reductionUserPixelMask().numberTag(1).build()
        cls.maskWS6 = wng.reductionUserPixelMask().numberTag(2).build()
        create_sample_pixel_mask(cls.maskWS1, cls.detectorState1, instrumentFilePath, cls.randomFraction)
        create_sample_pixel_mask(cls.maskWS2, cls.detectorState1, instrumentFilePath, cls.randomFraction)
        create_sample_pixel_mask(cls.maskWS3, cls.detectorState2, instrumentFilePath, cls.randomFraction)
        create_sample_pixel_mask(cls.maskWS4, cls.detectorState2, instrumentFilePath, cls.randomFraction)
        create_sample_pixel_mask(cls.maskWS5, cls.detectorState1, instrumentFilePath, cls.randomFraction)
        create_sample_pixel_mask(cls.maskWS6, cls.detectorState2, instrumentFilePath, cls.randomFraction)
        yield

        # teardown...
        pass

    @pytest.fixture(autouse=True)
    def _setup_test_mocks(self, monkeypatch, cleanup_workspace_at_exit):
        monkeypatch.setattr(
            self.service.dataFactoryService.lookupService,
            "generateStateId",
            lambda runNumber: {
                self.runNumber1: (self.stateId1, None),
                self.runNumber2: (self.stateId1, None),
                self.runNumber3: (self.stateId2, None),
                self.runNumber4: (self.stateId2, None),
            }[runNumber],
        )
        monkeypatch.setattr(
            self.service.dataFactoryService.lookupService,
            "getIPTS",
            lambda runNumber: {
                self.runNumber1: "/SNS/SNAP/IPTS-1",
                self.runNumber2: "/SNS/SNAP/IPTS-1",
                self.runNumber3: "/SNS/SNAP/IPTS-2",
                self.runNumber4: "/SNS/SNAP/IPTS-2",
            }[runNumber],
        )

        def _instrumentDonorFromRun(runNumber):
            match runNumber:
                case self.runNumber1 | self.runNumber2:
                    return self.sampleWS1
                case self.runNumber3 | self.runNumber4:
                    return self.sampleWS2
                case _:
                    raise RuntimeError(f"run number {runNumber} is unknown to this test")

        def _createCompatibleMask(maskWSName, runNumber):
            createCompatibleMask(maskWSName, _instrumentDonorFromRun(runNumber))
            cleanup_workspace_at_exit(maskWSName)

        monkeypatch.setattr(
            self.service.groceryService,
            "fetchCompatiblePixelMask",
            lambda combinedMask, runNumber, useLiteMode: _createCompatibleMask(combinedMask, runNumber),  # noqa: ARG005
        )
        yield

        # teardown...
        pass

    def test_prepCombinedMask_correct(self):
        """
        Check that prepCombinedMask correctly combines pixel masks
        """
        masks = [self.maskWS1, self.maskWS2]
        maskArrays = [arrayFromMask(mask) for mask in masks]

        # WARNING: the timestamp used here must be unique,
        #   otherwise `prepCombinedMask` might overwrite one of the
        #   sample mask workspaces!
        timestamp = self.service.getUniqueTimestamp()
        request = ReductionRequest(
            runNumber=self.runNumber1,
            useLiteMode=self.useLiteMode,
            timestamp=timestamp + 1,
            pixelMasks=masks,
        )

        # call code and check result
        with mock.patch.object(
            self.service.dataFactoryService,
            "getLatestApplicableCalibrationVersion",
            return_value=None,
        ):
            combinedMask = self.service.prepCombinedMask(request)
        actual = arrayFromMask(combinedMask)
        expected = np.zeros(maskArrays[0].shape, dtype=bool)
        for mask in maskArrays:
            expected |= mask
        failmsg = (
            "The expected combined mask doesn't match the calculated mask.\n"
            + f"  Masking values are incorrect for {np.count_nonzero(expected != actual)} pixels."
        )
        assert np.all(expected == actual), failmsg

    def test_prepCombinedMask_load(self):
        """
        Check that prepCombinedMask correctly loads all things it should
        """
        with (
            mock.patch.object(self.service.groceryService, "fetchGroceryDict") as mockFetchGroceryDict,
            mock.patch.object(self.service.dataFactoryService, "getLatestApplicableCalibrationVersion", return_value=1),
        ):
            fetchGroceryCallArgs = []

            def trackFetchGroceryDict(*args, **kwargs):
                fetchGroceryCallArgs.append((args, kwargs))
                return mock.MagicMock()

            mockFetchGroceryDict.side_effect = trackFetchGroceryDict

            # timestamp must be unique: see comment at `test_prepCombinedMask`.
            timestamp = self.service.getUniqueTimestamp()
            request = ReductionRequest(
                runNumber=self.runNumber1,
                useLiteMode=self.useLiteMode,
                timestamp=timestamp,
                versions=Versions(1, 2),
                pixelMasks=[self.maskWS1, self.maskWS2, self.maskWS5],
            )

            # prepare the expected grocery dicionary
            groceryClerk = self.service.groceryClerk
            groceryClerk.name("diffcalMaskWorkspace").diffcal_mask(request.runNumber, 1).useLiteMode(
                request.useLiteMode
            ).add()
            for mask in (self.maskWS1, self.maskWS2):
                runNumber, timestamp = mask.tokens("runNumber", "timestamp")
                groceryClerk.name(mask).reduction_pixel_mask(runNumber, timestamp).useLiteMode(
                    request.useLiteMode
                ).add()

            loadableMaskGroceryItems = groceryClerk.buildDict()
            residentMaskGroceryKwargs = {self.maskWS5.toString(): self.maskWS5}

            self.service.prepCombinedMask(request)

            realArgs = fetchGroceryCallArgs[0][0][0]
            realKwargs = fetchGroceryCallArgs[0][1]
            assert realArgs == loadableMaskGroceryItems
            assert realKwargs == residentMaskGroceryKwargs
            mockFetchGroceryDict.assert_called_with(loadableMaskGroceryItems, **residentMaskGroceryKwargs)

    def test_prepCombinedMask_only_diffcal(self):
        """
        Check that prepCombinedMask will still work if only a diffcal file is present
        Logic:
            - the grocery service is mocked to create a MaskWorkspace based on the item
            - if only the diffcal mask is loaded, it will compare equal to itself at the end
            - if some other mask is loaded, either an error will occur, or it will be unequal
        """

        def mock_compatible_mask(wsname, runNumber, useLiteMode):  # noqa ARG001
            return maskFromArray(wsname, [0, 0, 0, 0, 0])

        def mock_fetch_grocery_list(groceryList):
            import hashlib
            import json

            groceries = []
            for item in groceryList:
                runNumber, version, useLiteMode = item.runNumber, item.version, item.useLiteMode
                workspaceName = f"{runNumber}_{useLiteMode}_v{version}"
                hasher = hashlib.shake_256()
                hasher.update(json.dumps(item.__dict__).encode("utf-8"))
                x = int.from_bytes(hasher.digest(1), "big")
                mask = [int(x) for x in list("{0:0b}".format(x))]
                workspaceName = maskFromArray(workspaceName, mask)
                groceries.append(workspaceName)
            return groceries

        with (
            mock.patch.object(
                self.service.dataFactoryService,
                "getLatestApplicableCalibrationVersion",
                return_value=1,
            ),
            mock.patch.object(
                self.service.groceryService,
                "fetchGroceryList",
                mock_fetch_grocery_list,
            ),
            mock.patch.object(
                self.service.groceryService,
                "fetchCompatiblePixelMask",
                mock_compatible_mask,
            ),
        ):
            # timestamp must be unique: see comment at `test_prepCombinedMask`.
            timestamp = self.service.getUniqueTimestamp()
            request = ReductionRequest(
                runNumber=self.runNumber1,
                useLiteMode=self.useLiteMode,
                timestamp=timestamp,
                versions=Versions(1, 2),
                pixelMasks=[],
            )

            # prepare the expected grocery dicionary
            groceryClerk = self.service.groceryClerk
            groceryClerk.name("diffcalMaskWorkspace").diffcal_mask(request.runNumber, 1).useLiteMode(
                request.useLiteMode
            ).add()
            exp = self.service.groceryService.fetchGroceryDict(groceryClerk.buildDict())

            res = self.service.prepCombinedMask(request)

            wksp_almost_equal(exp["diffcalMaskWorkspace"], res, atol=0.0)

    def test_fetchReductionGroceries_load(self):
        """
        Check that fetchReductionGroceries constructs the correct grocery dictionary
        NOTE this probably belongs more properly to the other test class.
        However, it was already here, for simplicity of review I am not moving it.
        """

        # timestamp must be unique: see comment at `test_prepCombinedMask`.
        timestamp = self.service.getUniqueTimestamp()
        request = ReductionRequest(
            runNumber=self.runNumber1,
            useLiteMode=False,
            timestamp=timestamp,
            versions=Versions(1, 2),
            pixelMasks=[self.maskWS1, self.maskWS2, self.maskWS5],
        )

        # prepare mocks
        self.service._markWorkspaceMetadata = mock.Mock()
        fetchGroceryCallArgs = []

        def trackFetchGroceryDict(*args, **kwargs):
            fetchGroceryCallArgs.append((args, kwargs))
            return mock.MagicMock()

        combinedMaskName = wng.reductionPixelMask().runNumber(request.runNumber).build()

        # construct the expected grocery dictionaries
        groceryClerk = self.service.groceryClerk
        groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        groceryClerk.name("diffcalWorkspace").diffcal_table(
            request.runNumber, request.versions.calibration
        ).useLiteMode(request.useLiteMode).add()
        groceryClerk.name("normalizationWorkspace").normalization(
            request.runNumber, request.versions.normalization
        ).useLiteMode(request.useLiteMode).add()
        loadableOtherGroceryItems = groceryClerk.buildDict()
        residentOtherGroceryKwargs = {"combinedPixelMask": combinedMaskName}

        with (
            mock.patch.object(self.service.groceryService, "fetchGroceryDict", side_effect=trackFetchGroceryDict),
            mock.patch.object(self.service, "prepCombinedMask", return_value=combinedMaskName),
            mock.patch.object(
                self.service.dataFactoryService,
                "getLatestApplicableCalibrationVersion",
                return_value=1,
            ),
            mock.patch.object(
                self.service.dataFactoryService,
                "getLatestApplicableNormalizationVersion",
                return_value=2,
            ),
            mock.patch.object(self.service.groceryService, "checkPixelMask") as mockCheckPixelMask,
        ):
            # check -- with valid combinedPixelMask, it is used as keyword arg to fetchGroceryDict
            mockCheckPixelMask.return_value = True
            self.service.fetchReductionGroceries(request)
            self.service.groceryService.fetchGroceryDict.assert_called_with(
                loadableOtherGroceryItems, **residentOtherGroceryKwargs
            )

            # check -- with invalid combinedPixelMask, no mask is added
            mockCheckPixelMask.return_value = False
            self.service.fetchReductionGroceries(request)
            self.service.groceryService.fetchGroceryDict.assert_called_with(
                loadableOtherGroceryItems,
            )

    def test_prepCombinedMask_not_a_mask(self):
        not_a_mask = (
            wng.reductionOutput()
            .unit(wng.Units.DSP)
            .group("bank")
            .runNumber(self.runNumber1)
            .timestamp(self.service.getUniqueTimestamp())
            .build()
        )

        # timestamp must be unique: see comment at `test_prepCombinedMask`.
        timestamp = self.service.getUniqueTimestamp()
        request = ReductionRequest(
            runNumber=self.runNumber1,
            useLiteMode=self.useLiteMode,
            timestamp=timestamp,
            pixelMasks=[not_a_mask],
        )

        with (
            mock.patch.object(
                self.service.dataFactoryService,
                "getLatestApplicableCalibrationVersion",
                return_value=None,
            ),
            pytest.raises(RuntimeError, match=r".*unexpected workspace-type.*"),
        ):
            self.service.prepCombinedMask(request)

    def test_getCompatibleMasks(self):
        timestamp = self.service.getUniqueTimestamp()
        request = ReductionRequest.model_construct(
            runNumber=self.runNumber1,
            useLiteMode=self.useLiteMode,
            timestamp=timestamp,
            versions=Versions(1, 2),
            pixelMasks=[self.maskWS1, self.maskWS2, self.maskWS5],
        )
        with mock.patch.object(
            self.service.dataFactoryService, "getCompatibleReductionMasks"
        ) as mockGetCompatibleReductionMasks:
            self.service.getCompatibleMasks(request)
            mockGetCompatibleReductionMasks.assert_called_with(request.runNumber, request.useLiteMode)
