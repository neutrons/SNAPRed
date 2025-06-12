import unittest

from mantid.simpleapi import (
    ConvertToEventWorkspace,
    Rebin,
    mtd,
)
from util.diffraction_calibration_synthetic_data import SyntheticData

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.recipe.algorithm.RemoveSmoothedBackground import RemoveSmoothedBackground as Algo
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.pointer import create_pointer


class TestRemoveSmoothedBackground(unittest.TestCase):
    def setUp(self):
        inputs = SyntheticData()
        self.fakeIngredients = inputs.ingredients

        runNumber = self.fakeIngredients.runConfig.runNumber
        self.fakeData = f"_test_remove_event_background_{runNumber}"
        self.fakeGroupingWorkspace = f"_test_remove_event_background_{runNumber}_grouping"
        self.fakeMaskWorkspace = f"_test_remove_event_background_{runNumber}_mask"
        inputs.generateWorkspaces(self.fakeData, self.fakeGroupingWorkspace, self.fakeMaskWorkspace)
        # this algorithm requires event workspacws
        ConvertToEventWorkspace(
            InputWorkspace=self.fakeData,
            OutputWorkspace=self.fakeData,
        )
        Rebin(
            InputWorkspace=self.fakeData,
            OutputWorkspace=self.fakeData,
            Params=self.fakeIngredients.pixelGroup.timeOfFlight.params,
        )

    def tearDown(self) -> None:
        mtd.clear()
        assert len(mtd.getObjectNames()) == 0
        return super().tearDown()

    def create_test_peaks(self):
        peaks = [
            GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=2),
            GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=3),
            GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=7),
            GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=11),
        ]
        return peaks

    def test_validate_inputs(self):
        algo = Algo()
        algo.initialize()
        err = algo.validateInputs()
        assert "DetectorPeaks" in err

    def test_chop_ingredients(self):
        peaks = self.create_test_peaks()

        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("DetectorPeaks", create_pointer(peaks))

        algo.chopIngredients(peaks)

        for peakList in peaks:
            groupID = peakList.groupID
            assert groupID in algo.groupIDs, f"Group ID {groupID} not found in maskRegions"

        expected_group_ids = [peakList.groupID for peakList in peaks]
        assert algo.groupIDs == expected_group_ids, (
            f"Group IDs in workspace and peak list do not match: {algo.groupIDs} vs {expected_group_ids}"
        )

    def test_incorrect_group_ids(self):
        peaks = [
            GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=999),  # Incorrect group ID
        ]

        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("DetectorPeaks", create_pointer(peaks))

        with self.assertRaises(RuntimeError) as context:  # noqa: PT027
            algo.chopIngredients(peaks)

        assert "Groups IDs in workspace and peak list do not match" in str(context.exception)

    def test_missing_properties(self):
        algo = Algo()
        algo.initialize()

        noPeaks = []
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("DetectorPeaks", create_pointer(noPeaks))

        with self.assertRaises(RuntimeError):  # noqa: PT027
            algo.execute()

        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeData)
        algo.setProperty("DetectorPeaks", create_pointer(noPeaks))

        with self.assertRaises(RuntimeError):  # noqa: PT027
            algo.execute()

        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeData)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)

        with self.assertRaises(RuntimeError):  # noqa: PT027
            algo.execute()

    def test_smoothing_parameter_edge_cases(self):
        algo = Algo()
        algo.initialize()

        # negative values are excluded
        with self.assertRaises(ValueError):  # noqa: PT027
            algo.setProperty("SmoothingParameter", -1)

        # zero is valid, large numbers valid, floats valid
        valid_values = [0, 1000, 3.141592]
        for value in valid_values:
            algo.setProperty("SmoothingParameter", value)
            assert value == algo.getProperty("SmoothingParameter").value

    def test_execute(self):
        peaks = self.create_test_peaks()
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeData)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("DetectorPeaks", create_pointer(peaks))
        algo.setProperty("OutputWorkspace", "output_test_ws")
        assert algo.execute()
        assert "output_test_ws" in mtd, "Output workspace not found in the Mantid workspace dictionary"

    def test_execute_from_mantidSnapper(self):
        peaks = self.create_test_peaks()
        utensils = Utensils()
        utensils.PyInit()
        utensils.mantidSnapper.RemoveSmoothedBackground(
            "Run in mantid snapper",
            InputWorkspace=self.fakeData,
            GroupingWorkspace=self.fakeGroupingWorkspace,
            DetectorPeaks=peaks,  # NOTE passed as object, not pointer
            OutputWorkspace="output_test_ws",
        )
        utensils.mantidSnapper.executeQueue()
        assert "output_test_ws" in mtd
