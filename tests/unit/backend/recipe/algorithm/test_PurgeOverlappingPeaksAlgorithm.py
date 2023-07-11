import unittest.mock as mock

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    import json

    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
    from snapred.backend.dao.DetectorPeak import DetectorPeak
    from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import (
        PurgeOverlappingPeaksAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    def test_init():
        """Test ability to initialize purge overlapping peaks algo"""
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/purge_peaks/input_parameters.json")
        ).instrumentState
        crystalInfo = CrystallographicInfo.parse_raw(Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("InstrumentState", instrumentState.json())
        purgeAlgo.setProperty("CrystalInfo", crystalInfo.json())
        assert purgeAlgo.getProperty("InstrumentState").value == instrumentState.json()
        assert CrystallographicInfo.parse_raw(purgeAlgo.getProperty("CrystalInfo").value) == crystalInfo

    def test_execute():
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/purge_peaks/input_parameters.json")
        ).instrumentState
        crystalInfo = CrystallographicInfo.parse_raw(Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("InstrumentState", instrumentState.json())
        purgeAlgo.setProperty("CrystalInfo", crystalInfo.json())
        purgeAlgo.execute()

        expected_pos = json.loads(Resource.read("/outputs/purge_peaks/output.json"))

        # retrieve a list of json-serialized DetectorPeak lists. Extract a list of peak position lists,
        actual_pos_json = json.loads(purgeAlgo.getProperty("OutputPeakMap").value)
        actual_pos = [
            [DetectorPeak.parse_raw(peak_json).position for peak_json in peak_group_json]
            for peak_group_json in actual_pos_json
        ]

        print(actual_pos)
        assert expected_pos == actual_pos
