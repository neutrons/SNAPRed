import unittest
import unittest.mock as mock
from pathlib import Path
from typing import List

import pydantic
import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    mtd,
)
from snapred.backend.api.RequestScheduler import RequestScheduler
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.request import (
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.service.ReductionService import ReductionService
from util.InstaEats import InstaEats
from util.SculleryBoy import SculleryBoy
from util.state_helpers import reduction_root_redirect

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
            focusGroups=[FocusGroup(name="apple", definition="path/to/grouping")],
        )

    def test_name(self):
        ## this makes codecov happy
        assert "reduction" == self.instance.name()

    def test_loadAllGroupings(self):
        data = self.instance.loadAllGroupings(self.request.runNumber, self.request.useLiteMode)
        assert pydantic.TypeAdapter(List[FocusGroup]).validate_python(data["focusGroups"])
        assert pydantic.TypeAdapter(List[str]).validate_python(data["groupingWorkspaces"])

    def test_fetchReductionGroupings(self):
        data = self.instance.fetchReductionGroupings(self.request)
        assert data == self.instance.loadAllGroupings(self.request.runNumber, self.request.useLiteMode)

    def test_prepReductionIngredients(self):
        # Call the method with the provided parameters
        res = self.instance.prepReductionIngredients(self.request)

        assert ReductionIngredients.model_validate(res)
        assert res == self.instance.sousChef.prepReductionIngredients(self.request)

    def test_fetchReductionGroceries(self):
        res = self.instance.fetchReductionGroceries(self.request)
        assert "inputWorkspace" in res
        assert "diffcalWorkspace" in res
        assert "normalizationWorkspace" in res

    @mock.patch(thisService + "ReductionRecipe")
    def test_reduction(self, mockReductionRecipe):
        mockReductionRecipe.return_value = mock.Mock()
        mockResult = {
            "result": True,
            "outputs": ["one", "two", "three"],
        }
        mockReductionRecipe.return_value.cook = mock.Mock(return_value=mockResult)
        self.instance.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=1)
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)
        self.instance.dataFactoryService.getThisOrLatestNormalizationVersion = mock.Mock(return_value=1)
        self.instance.dataFactoryService.normalizationExists = mock.Mock(return_value=True)

        result = self.instance.reduction(self.request)
        groupings = self.instance.fetchReductionGroupings(self.request)
        ingredients = self.instance.prepReductionIngredients(self.request)
        groceries = self.instance.fetchReductionGroceries(self.request)
        groceries["groupingWorkspaces"] = groupings["groupingWorkspaces"]
        mockReductionRecipe.assert_called()
        mockReductionRecipe.return_value.cook.assert_called_once_with(ingredients, groceries)
        assert result.record.workspaceNames == mockReductionRecipe.return_value.cook.return_value["outputs"]

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

    def test_checkWritePermissions(self):
        with (
            mock.patch.object(self.instance.dataExportService, "checkWritePermissions") as mockCheckWritePermissions,
            mock.patch.object(self.instance.dataExportService, "getReductionStateRoot") as mockGetReductionStateRoot,
        ):
            runNumber = "12345"
            mockCheckWritePermissions.return_value = True
            mockGetReductionStateRoot.return_value = Path("/reduction/state/root")
            assert self.instance.checkWritePermissions(runNumber)
            mockCheckWritePermissions.assert_called_once_with(mockGetReductionStateRoot.return_value)
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
        mockDataFactory.getNormalizationVersion.side_effect = [0, 1]
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
        # assert RuntimeError is raised
        fakeDataService = mock.Mock()
        fakeDataService.getCalibrationRecord.return_value = None
        fakeDataService.getNormalizationRecord.return_value = mock.Mock()
        self.instance.dataFactoryService = fakeDataService
        with pytest.raises(RuntimeError, match=r".*missing calibration data.*"):
            self.instance.validateReduction(self.request)

    def test_validateReduction_noNormalization(self):
        # assert RuntimeError is raised
        fakeDataService = mock.Mock()
        fakeDataService.getCalibrationRecord.return_value = mock.Mock()
        fakeDataService.getNormalizationRecord.return_value = None
        self.instance.dataFactoryService = fakeDataService
        with pytest.raises(RuntimeError, match=r".*missing calibration data.*"):
            self.instance.validateReduction(self.request)

    def test_validateReduction_no_permissions(self):
        # assert ContinueWarning is raised
        fakeDataService = mock.Mock()
        fakeDataService.getCalibrationRecord.return_value = mock.Mock()
        fakeDataService.getNormalizationRecord.return_value = mock.Mock()
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
        fakeDataService.getCalibrationRecord.return_value = mock.Mock()
        fakeDataService.getNormalizationRecord.return_value = mock.Mock()
        self.instance.dataFactoryService = fakeDataService
        fakeExportService = mock.Mock()
        fakeExportService.checkWritePermissions.return_value = False
        self.instance.dataExportService = fakeExportService
        self.request.continueFlags = ContinueWarning.Type.NO_WRITE_PERMISSIONS
        self.instance.validateReduction(self.request)
