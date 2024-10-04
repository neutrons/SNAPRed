import unittest
from typing import Dict, List

from mantid.simpleapi import (
    CreateSingleValuedWorkspace,
    mtd,
)
from pydantic import TypeAdapter
from snapred.backend.recipe.algorithm.GroupedDetectorIDs import GroupedDetectorIDs as Algo
from util.diffraction_calibration_synthetic_data import SyntheticData


class TestGroupedDetectorIDs(unittest.TestCase):
    def setUp(self):
        inputs = SyntheticData()
        self.fakeIngredients = inputs.ingredients

        runNumber = self.fakeIngredients.runConfig.runNumber
        self.fakeData = f"_test_remove_event_background_{runNumber}"
        self.fakeGroupingWorkspace = f"_test_remove_event_background_{runNumber}_grouping"
        self.fakeMaskWorkspace = f"_test_remove_event_background_{runNumber}_mask"
        inputs.generateWorkspaces(self.fakeData, self.fakeGroupingWorkspace, self.fakeMaskWorkspace)

    def tearDown(self) -> None:
        mtd.clear()
        assert len(mtd.getObjectNames()) == 0
        return super().tearDown()

    def test_init(self):
        algo = Algo()
        algo.initialize()

    def test_validate(self):
        not_a_grouping_ws = mtd.unique_name(5, "grpid")
        CreateSingleValuedWorkspace(OutputWorkspace=not_a_grouping_ws)
        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", not_a_grouping_ws)
        err = algo.validateInputs()
        assert "GroupingWorkspace" in err

    def test_execute(self):
        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        assert algo.execute()
        data = TypeAdapter(Dict[int, List[int]]).validate_json(algo.getPropertyValue("GroupWorkspaceIndices"))
        assert data == {2: [2, 4, 11, 14], 3: [0, 5, 10, 15], 7: [1, 7, 8, 13], 11: [3, 6, 9, 12]}
