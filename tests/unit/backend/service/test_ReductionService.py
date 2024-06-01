# ruff: noqa: E402, ARG002
import unittest
import unittest.mock as mock
from typing import List

from mantid.simpleapi import (
    DeleteWorkspace,
    mtd,
)
from pydantic import parse_obj_as
from snapred.backend.dao.request.ReductionRequest import ReductionRequest

# Mock out of scope modules before importing DataExportService

localMock = mock.Mock()


from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
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
        self.instance.sousChef = self.sculleryBoy
        self.instance.groceryService = self.instaEats
        self.instance.dataFactoryService.lookupService = self.instaEats.dataService
        self.instance.dataExportService.dataService = self.instaEats.dataService

    def test_loadAllGroupings(self):
        data = self.instance.loadAllGroupings(self.request.runNumber, self.request.useLiteMode)
        assert parse_obj_as(List[FocusGroup], data["focusGroups"])
        assert parse_obj_as(List[str], data["groupingWorkspaces"])

    def test_fetchReductionGroupings(self):
        data = self.instance.fetchReductionGroupings(self.request)
        assert self.request.focusGroup == data["focusGroups"]
        assert data == self.instance.loadAllGroupings(self.request.runNumber, self.request.useLiteMode)

    def test_prepReductionIngredients(self):
        # Call the method with the provided parameters
        res = self.instance.prepReductionIngredients(self.request)

        assert ReductionIngredients.parse_obj(res)
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

    def test_hasState(self):
        assert self.instance.hasState("123")
        assert not self.instance.hasState("not a state")
