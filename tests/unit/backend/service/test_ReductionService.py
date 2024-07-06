import time
from typing import List

import numpy as np
import pydantic

import unittest
import unittest.mock as mock
import pytest

from mantid.simpleapi import (
    DeleteWorkspace,
    mtd,
)
from snapred.backend.api.RequestScheduler import RequestScheduler
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.request import (
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.request.ReductionRequest import Versions
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.state import DetectorState
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.service.ReductionService import ReductionService
from snapred.meta.Config import Resource
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from util.helpers import (
    arrayFromMask,
    createCompatibleMask,
)
from util.InstaEats import InstaEats
from util.SculleryBoy import SculleryBoy
from util.state_helpers import reduction_root_redirect

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
        self.request = ReductionRequest(
            runNumber="123",
            useLiteMode=False,
            versions=(1, 2),
            pixelMasks=[],
            focusGroups=[FocusGroup(name="apple", definition="path/to/grouping")],
        )
        ## Mock out the assistant services
        self.instance.sousChef = self.sculleryBoy
        self.instance.groceryService = self.instaEats
        self.instance.dataFactoryService.lookupService = self.localDataService
        self.instance.dataExportService.dataService = self.localDataService

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
        self.instance.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=1)
        self.instance.dataFactoryService.getThisOrLatestNormalizationVersion = mock.Mock(return_value=1)

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
        self.instance.dataFactoryService.getThisOrLatestNormalizationVersion = mock.Mock(return_value=1)
        
        result = self.instance.reduction(self.request)
        groupings = self.instance.fetchReductionGroupings(self.request)
        ingredients = self.instance.prepReductionIngredients(self.request)
        groceries = self.instance.fetchReductionGroceries(self.request)
        groceries["groupingWorkspaces"] = groupings["groupingWorkspaces"]
        assert mockReductionRecipe.called
        assert mockReductionRecipe.return_value.cook.called_once_with(ingredients, groceries)
        assert result.workspaces == mockReductionRecipe.return_value.cook.return_value["outputs"]

    def test_saveReduction(self):
        # this test will ensure the two indicated files (record, data)
        # are saved into the appropriate directory when save is called.
        runNumber = "123456"
        useLiteMode = True
        timestamp = time.time()
        record = self.localDataService.readReductionRecord(runNumber, useLiteMode, timestamp)
        request = ReductionExportRequest(reductionRecord=record)
        with reduction_root_redirect(self.localDataService):
            # save the files
            self.instance.saveReduction(request)

            # now ensure the files exist
            assert self.localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp).exists()
            assert self.localDataService._constructReductionDataFilePath(runNumber, useLiteMode, timestamp).exists()

    def test_loadReduction(self):
        ## this makes codecov happy
        with pytest.raises(NotImplementedError):
            self.instance.loadReduction(stateId="babeeeee", timestamp=time.time())

    def test_hasState(self):
        assert self.instance.hasState("123456")
        assert not self.instance.hasState("not a state")
        assert not self.instance.hasState("1")

    def test_groupRequests(self):
        payload = self.request.json()
        request = SNAPRequest(path="test", payload=payload)

        scheduler = RequestScheduler()
        self.instance.registerGrouping("", self.instance._groupByStateId)
        self.instance.registerGrouping("", self.instance._groupByVanadiumVersion)
        groupings = self.instance.getGroupings("")
        result = scheduler.handle([request], groupings)

        # Verify the request is sorted by state id then normalization version
        lookupService = self.instance.dataFactoryService.lookupService
        stateId, _ = lookupService._generateStateId(self.request.runNumber)
        # need to add a normalization version to find
        lookupService.normalizationIndexer(self.request.runNumber, self.request.useLiteMode).index = {1: mock.Mock()}
        # now sort
        result = scheduler.handle([request], [self.instance._groupByStateId, self.instance._groupByVanadiumVersion])
        assert result["root"][stateId]["normalization_1"][0] == request


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
            wng.reductionPixelMask()
            .runNumber(cls.runNumber1)
            .timestamp(cls.dataExportService.getUniqueTimestamp())
            .build()
        )
        cls.maskWS2 = (
            wng.reductionPixelMask()
            .runNumber(cls.runNumber2)
            .timestamp(cls.dataExportService.getUniqueTimestamp())
            .build()
        )
        cls.maskWS3 = (
            wng.reductionPixelMask()
            .runNumber(cls.runNumber3)
            .timestamp(cls.dataExportService.getUniqueTimestamp())
            .build()
        )
        cls.maskWS4 = (
            wng.reductionPixelMask()
            .runNumber(cls.runNumber4)
            .timestamp(cls.dataExportService.getUniqueTimestamp())
            .build()
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
            "_generateStateId",
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

    def test_prepCombinedMask(self):
        masks = [self.maskWS1, self.maskWS2]
        maskArrays = [arrayFromMask(mask) for mask in masks]
        combinedMask = self.service.prepCombinedMask(self.runNumber1, self.useLiteMode, masks)
        actual = arrayFromMask(combinedMask)
        expected = np.zeros(maskArrays[0].shape, dtype=bool)
        for mask in maskArrays:
            expected |= mask
        assert np.all(expected == actual)

    def test_fetchReductionGroceries_pixelMasks(self):
        with (
            mock.patch.object(self.service.groceryService, "fetchGroceryDict") as mockFetchGroceryDict,
            mock.patch.object(self.service, "prepCombinedMask") as mockPrepCombinedMask,
        ):
            request = ReductionRequest(
                runNumber=self.runNumber1,
                useLiteMode=False,
                versions=Versions(1, 2),
                pixelMasks=[self.maskWS1, self.maskWS2, self.maskWS5],
                focusGroups=[FocusGroup(name="apple", definition="path/to/grouping")],
            )
            self.service.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=1)
            self.service.dataFactoryService.getThisOrLatestNormalizationVersion = mock.Mock(return_value=2)

            groceryClerk = GroceryListItem.builder()
            for mask in (self.maskWS1, self.maskWS2):
                runNumber, timestamp = mask.tokens("runNumber", "timestamp")
                groceryClerk.name(mask).reduction_pixel_mask(runNumber, timestamp).useLiteMode(
                    request.useLiteMode
                ).add()
            loadableMaskGroceryItems = groceryClerk.buildDict()
            residentMaskGroceryKwargs = {self.maskWS5.toString(): self.maskWS5}
            combinedMaskName = wng.reductionPixelMask().runNumber(request.runNumber).build()
            mockPrepCombinedMask.return_value = combinedMaskName

            groceryClerk.name("diffcalWorkspace").diffcal_table(
                request.runNumber, request.versions.calibration
            ).useLiteMode(request.useLiteMode).add()
            groceryClerk.name("normalizationWorkspace").normalization(
                request.runNumber, request.versions.normalization
            ).useLiteMode(request.useLiteMode).add()
            loadableOtherGroceryItems = groceryClerk.buildDict()
            residentOtherGroceryKwargs = {"maskWorkspace": combinedMaskName}

            self.service.fetchReductionGroceries(request)
            assert mockFetchGroceryDict.called_with(loadableMaskGroceryItems, **residentMaskGroceryKwargs)
            assert mockFetchGroceryDict.called_with(loadableOtherGroceryItems, **residentOtherGroceryKwargs)

    def test_fetchReductionGroceries_pixelMasks_not_a_mask(self):
        with (
            mock.patch.object(self.service.groceryService, "fetchGroceryDict"),
            mock.patch.object(self.service, "prepCombinedMask") as mockPrepCombinedMask,
        ):
            not_a_mask = (
                wng.reductionOutput()
                .unit(wng.Units.DSP)
                .group("bank")
                .runNumber(self.runNumber1)
                .timestamp(self.dataExportService.getUniqueTimestamp())
                .build()
            )

            request = ReductionRequest(
                runNumber=self.runNumber1,
                useLiteMode=False,
                versions=Versions(1, 2),
                pixelMasks=[self.maskWS1, self.maskWS2, self.maskWS5, not_a_mask],
                focusGroups=[FocusGroup(name="apple", definition="path/to/grouping")],
            )
            self.service.dataFactoryService.getThisOrLatestCalibrationVersion = mock.Mock(return_value=1)
            self.service.dataFactoryService.getThisOrLatestNormalizationVersion = mock.Mock(return_value=2)
            combinedMaskName = wng.reductionPixelMask().runNumber(request.runNumber).build()
            mockPrepCombinedMask.return_value = combinedMaskName

            with pytest.raises(RuntimeError, match=r".*unexpected workspace-type.*"):
                self.service.fetchReductionGroceries(request)

    def test_getCompatibleMasks(self):
        request = ReductionRequest.model_construct(
            runNumber=self.runNumber1,
            useLiteMode=False,
            versions=Versions(1, 2),
            pixelMasks=[self.maskWS1, self.maskWS2, self.maskWS5],
            focusGroups=[FocusGroup(name="apple", definition="path/to/grouping")],
        )
        with mock.patch.object(
            self.service.dataFactoryService, "getCompatibleReductionMasks"
        ) as mockGetCompatibleReductionMasks:
            self.service.getCompatibleMasks(request)
            assert mockGetCompatibleReductionMasks.called_with(request.runNumber, request.useLiteMode)
