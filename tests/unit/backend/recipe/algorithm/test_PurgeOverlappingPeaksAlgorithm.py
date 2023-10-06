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
        purgeAlgo.setProperty("PeakIntensityThreshold", 0.045)
        assert purgeAlgo.getProperty("InstrumentState").value == instrumentState.json()
        assert CrystallographicInfo.parse_raw(purgeAlgo.getProperty("CrystalInfo").value) == crystalInfo

    # TODO this sample data has no overlapping peaks, so that the result of DetectorPeakPredictor
    # and PurgeOverlappingPeaks are the same.  Needs data with overlapping peaks to check if purged.
    def test_execute():
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/purge_peaks/input_parameters.json")
        ).instrumentState
        crystalInfo = CrystallographicInfo.parse_raw(Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("InstrumentState", instrumentState.json())
        purgeAlgo.setProperty("CrystalInfo", crystalInfo.json())
        purgeAlgo.setProperty("PeakIntensityThreshold", 0.05)
        purgeAlgo.execute()

        actual_pos_json = json.loads(purgeAlgo.getProperty("OutputPeakMap").value)
        # TODO edit the purge_peaks version of output file to correct format
        expected_pos_json = json.loads(Resource.read("/outputs/predict_peaks/peaks.json"))

        assert expected_pos_json == actual_pos_json

        # test the threshold -- set to over-1 value and verify no peaks are found
        purgeAlgo.setProperty("PeakIntensityThreshold", 1.2)
        purgeAlgo.execute()
        no_pos_json = json.loads(purgeAlgo.getProperty("OutputPeakMap").value)
        for x in no_pos_json:
            assert len(x["peaks"]) == 0
