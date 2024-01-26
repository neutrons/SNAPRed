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

    def test_validate():
        """Test ability to initialize purge overlapping peaks algo"""
        ingredients = Ingredients.parse_raw(Resource.read("inputs/predict_peaks/input_fake_ingredients.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("DetectorPeakIngredients", ingredients.json())
        res = purgeAlgo.validateInputs()
        assert res == {}

    # TODO this sample data has no overlapping peaks, so that the result of DetectorPeakPredictor
    # and PurgeOverlappingPeaks are the same.  Needs data with overlapping peaks to check if purged.
    def test_execute():
        ingredients = Ingredients.parse_raw(Resource.read("inputs/predict_peaks/input_good_ingredients.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("DetectorPeakIngredients", ingredients.json())
        purgeAlgo.execute()

        actual_pos_json = json.loads(purgeAlgo.getProperty("OutputPeakMap").value)
        # TODO edit the purge_peaks version of output file to correct format
        expected_pos_json = json.loads(Resource.read("/outputs/predict_peaks/peaks.json"))

        assert expected_pos_json == actual_pos_json

        # test the threshold -- set to over-1 value and verify no peaks are found
        ingredients.peakIntensityThreshold = 1.2
        purgeAlgo.setProperty("DetectorPeakIngredients", ingredients.json())
        purgeAlgo.execute()
        no_pos_json = json.loads(purgeAlgo.getProperty("OutputPeakMap").value)
        for x in no_pos_json:
            assert len(x["peaks"]) == 0
