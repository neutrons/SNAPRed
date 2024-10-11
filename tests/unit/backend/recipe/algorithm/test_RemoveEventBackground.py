import unittest

from mantid.simpleapi import (
    ConvertToEventWorkspace,
    mtd,
)
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.recipe.algorithm.RemoveEventBackground import RemoveEventBackground as Algo
from snapred.meta.pointer import create_pointer
from util.diffraction_calibration_synthetic_data import SyntheticData


class TestRemoveEventBackground(unittest.TestCase):
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

    def create_test_peaks(self):
        peaks = [
            GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=2),
            GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=3),
            GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=7),
            GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=11),
        ]
        return peaks

    def test_chop_ingredients(self):
        peaks = self.create_test_peaks()

        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("DetectorPeaks", create_pointer(peaks))

        algo.chopIngredients(peaks)

        for peakList in peaks:
            groupID = peakList.groupID
            assert groupID in algo.maskRegions, f"Group ID {groupID} not found in maskRegions"

            expected_peak_count = len(peakList.peaks)
            actual_peak_count = len(algo.maskRegions[groupID])
            assert (
                actual_peak_count == expected_peak_count
            ), f"Mismatch in number of peaks for group {groupID}: expected {expected_peak_count}, found {actual_peak_count}"  # noqa: E501

            for peak, mask in zip(peakList.peaks, algo.maskRegions[groupID]):
                assert (
                    mask == (peak.minimum, peak.maximum)
                ), f"Mask region mismatch for group {groupID}, peak {peak}: expected {(peak.minimum, peak.maximum)}, found {mask}"  # noqa: E501

        expected_group_ids = [peakList.groupID for peakList in peaks]
        assert (
            algo.groupIDs == expected_group_ids
        ), f"Group IDs in workspace and peak list do not match: {algo.groupIDs} vs {expected_group_ids}"

    def test_execute(self):
        peaks = self.create_test_peaks()
        ConvertToEventWorkspace(
            InputWorkspace=self.fakeData,
            OutputWorkspace=self.fakeData,
        )
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeData)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("DetectorPeaks", create_pointer(peaks))
        algo.setProperty("OutputWorkspace", "output_test_ws")
        assert algo.execute()

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

        ConvertToEventWorkspace(
            InputWorkspace=self.fakeData,
            OutputWorkspace=self.fakeData,
        )

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
        peaks = self.create_test_peaks()
        ConvertToEventWorkspace(
            InputWorkspace=self.fakeData,
            OutputWorkspace=self.fakeData,
        )
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeData)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("DetectorPeaks", create_pointer(peaks))
        algo.setProperty("SmoothingParameter", 0)
        algo.setProperty("OutputWorkspace", "output_test_ws_no_smoothing")

        assert algo.execute()

        algo.setProperty("SmoothingParameter", -1)
        algo.setProperty("OutputWorkspace", "output_test_ws_negative_smoothing")

        with self.assertRaises(RuntimeError):  # noqa: PT027
            algo.execute()

        algo.setProperty("SmoothingParameter", 1000)
        algo.setProperty("OutputWorkspace", "output_test_ws_large_smoothing")

        assert algo.execute()

    def test_output_workspace_creation(self):
        peaks = self.create_test_peaks()

        ConvertToEventWorkspace(
            InputWorkspace=self.fakeData,
            OutputWorkspace=self.fakeData,
        )

        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeData)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("DetectorPeaks", create_pointer(peaks))
        algo.setProperty("OutputWorkspace", "output_test_ws")

        assert algo.execute()

        assert "output_test_ws" in mtd, "Output workspace not found in the Mantid workspace dictionary"
        output_ws = mtd["output_test_ws"]  # noqa: F841
