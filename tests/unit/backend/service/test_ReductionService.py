# ruff: noqa: E402, ARG002
import unittest
import unittest.mock as mock
from typing import List

import pydantic
import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    mtd,
)

# Mock out of scope modules before importing DataExportService

localMock = mock.Mock()

from snapred.backend.api.RequestScheduler import RequestScheduler
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.request import (
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.service.ReductionService import ReductionService
from util.InstaEats import InstaEats
from util.SculleryBoy import SculleryBoy

thisService = "snapred.backend.service.ReductionService."


class TestReductionService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sculleryBoy = SculleryBoy()
        cls.instaEats = InstaEats()

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
            focusGroup=FocusGroup(name="apple", definition="path/to/grouping"),
        )
        ## Mock out the assistant services
        self.instance.sousChef = self.sculleryBoy
        self.instance.groceryService = self.instaEats
        self.instance.dataFactoryService.lookupService = self.instaEats.dataService
        self.instance.dataExportService.dataService = self.instaEats.dataService

    def test_name(self):
        ## this makes codecov happy
        assert "reduction" == self.instance.name()

    def test_fakeMethod(self):
        ## this makes codecov happy
        with pytest.raises(NotImplementedError):
            self.instance.fakeMethod()

    def test_loadAllGroupings(self):
        data = self.instance.loadAllGroupings(self.request.runNumber, self.request.useLiteMode)
        assert pydantic.TypeAdapter(List[FocusGroup]).validate_python(data["focusGroups"])
        assert pydantic.TypeAdapter(List[str]).validate_python(data["groupingWorkspaces"])

    def test_fetchReductionGroupings(self):
        data = self.instance.fetchReductionGroupings(self.request)
        assert self.request.focusGroup == data["focusGroups"]
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
    def test_reduction(self, ReductionRecipe):
        res = self.instance.reduction(self.request)
        groupings = self.instance.fetchReductionGroupings(self.request)
        ingredients = self.instance.prepReductionIngredients(self.request)
        groceries = self.instance.fetchReductionGroceries(self.request)
        groceries["groupingWorkspaces"] = groupings["groupingWorkspaces"]
        assert ReductionRecipe.called
        assert ReductionRecipe.return_value.cook.called_once_with(ingredients, groceries)
        assert res == ReductionRecipe.return_value.cook.return_value

    def test_saveReduction(self):
        # this method only needs to call the methods in the data service
        # the corresponding methods are setup to add themselves to the list of run numbers
        record = ReductionRecord.construct(runNumbers=["test"])
        request = ReductionExportRequest.construct(reductionRecord=record)
        self.instance.saveReduction(request)
        assert record.runNumbers == ["test", "writeReductionRecord", "writeReductionData"]

    def test_loadReduction(self):
        ## this makes codecov happy
        with pytest.raises(NotImplementedError):
            self.instance.loadReduction()

    def test_hasState(self):
        assert self.instance.hasState("123")
        assert not self.instance.hasState("not a state")

    def test_groupRequests(self):
        payload = self.request.json()
        request = SNAPRequest(path="test", payload=payload)
        scheduler = RequestScheduler()
        self.instance.registerGrouping("", self.instance._groupByStateId)
        self.instance.registerGrouping("", self.instance._groupByVanadiumVersion)
        groupings = self.instance.getGroupings("")
        result = scheduler.handle([request], groupings)

        # outpus/2kfxjiqm is the state id defined in WhateversInTheFridge util
        # Verify the request is sorted by state id then normalization version
        assert result["root"]["outpus/2kfxjiqm"]["normalization_0"][0] == request
