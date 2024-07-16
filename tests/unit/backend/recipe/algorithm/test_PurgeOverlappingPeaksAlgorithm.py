import unittest.mock as mock

from snapred.backend.dao import GroupPeakList
from util.diffraction_calibration_synthetic_data import SyntheticData

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    import json

    from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import (
        PurgeOverlappingPeaksAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource
    from snapred.meta.redantic import list_to_raw

    def test_validate():
        """Test ability to initialize purge overlapping peaks algo"""
        peaks = SyntheticData.fakeDetectorPeaks()
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("DetectorPeaks", list_to_raw(peaks))
        res = purgeAlgo.validateInputs()
        assert res == {}

    # TODO this sample data has no overlapping peaks, so that the result of DetectorPeakPredictor
    # and PurgeOverlappingPeaks are the same.  Needs data with overlapping peaks to check if purged.
    def test_execute():
        ingredientsFile = "/inputs/predict_peaks/input_good_ingredients.json"
        peaks = [GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=1)]
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("Ingredients", Resource.read(ingredientsFile))
        purgeAlgo.setProperty("DetectorPeaks", list_to_raw(peaks))
        purgeAlgo.execute()

        actual_pos_json = json.loads(purgeAlgo.getProperty("OutputPeakMap").value)

        expected_pos_json = json.loads(Resource.read("/outputs/purge_peaks/peaks.json"))
        assert expected_pos_json == actual_pos_json
