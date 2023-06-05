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
    from snapred.backend.dao.ReductionIngredients import ReductionIngredients
    from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import (
        PurgeOverlappingPeaksAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    def test_init():
        """Test ability to initialize purge overlapping peaks algo"""
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/purge_peaks/input_parameters.json")
        ).instrumentState
        focusGroups = ReductionIngredients.parse_raw(
            Resource.read("/inputs/purge_peaks/input_ingredients.json")
        ).reductionState.stateConfig.focusGroups
        crystalInfo = CrystallographicInfo.parse_raw(Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("InstrumentState", instrumentState.json())
        purgeAlgo.setProperty("FocusGroups", json.dumps([focusGroup.json() for focusGroup in focusGroups]))
        purgeAlgo.setProperty("CrystalInfo", crystalInfo.json())
        assert purgeAlgo.getProperty("InstrumentState").value == instrumentState.json()
        assert purgeAlgo.getProperty("FocusGroups").value == json.dumps(
            [focusGroup.json() for focusGroup in focusGroups]
        )
        assert CrystallographicInfo.parse_raw(purgeAlgo.getProperty("CrystalInfo").value) == crystalInfo

    def test_execute():
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/purge_peaks/input_parameters.json")
        ).instrumentState
        focusGroups = ReductionIngredients.parse_raw(
            Resource.read("/inputs/purge_peaks/input_ingredients.json")
        ).reductionState.stateConfig.focusGroups
        crystalInfo = CrystallographicInfo.parse_raw(Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("InstrumentState", instrumentState.json())
        purgeAlgo.setProperty("FocusGroups", json.dumps([focusGroup.dict() for focusGroup in focusGroups]))
        purgeAlgo.setProperty("CrystalInfo", crystalInfo.json())
        purgeAlgo.execute()
        expected = json.loads(Resource.read("/outputs/purge_peaks/output.json"))
        actual = json.loads(purgeAlgo.getProperty("OutputPeakMap").value)
        assert expected["Column"] == actual["Column"]
