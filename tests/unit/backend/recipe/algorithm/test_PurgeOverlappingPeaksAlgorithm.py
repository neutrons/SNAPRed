import unittest.mock as mock

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    import json

    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
    from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
    from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import (
        PurgeOverlappingPeaksAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource
    from snapred.meta.redantic import list_to_raw
    from util.ScullionBoy import ScullionBoy

    def test_validate():
        """Test ability to initialize purge overlapping peaks algo"""
        peaks = ScullionBoy().prepDetectorPeaks({})
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("DetectorPeaks", list_to_raw(peaks))
        res = purgeAlgo.validateInputs()
        assert res == {}

    # TODO this sample data has no overlapping peaks, so that the result of DetectorPeakPredictor
    # and PurgeOverlappingPeaks are the same.  Needs data with overlapping peaks to check if purged.
    def test_execute():
        peaks = ScullionBoy().prepDetectorPeaks({"good": "", "purge": False})
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("DetectorPeaks", list_to_raw(peaks))
        purgeAlgo.execute()

        actual_pos_json = json.loads(purgeAlgo.getProperty("OutputPeakMap").value)
        # TODO edit the purge_peaks version of output file to correct format
        expected_pos_json = json.loads(Resource.read("/outputs/predict_peaks/peaks.json"))

        assert expected_pos_json == actual_pos_json
