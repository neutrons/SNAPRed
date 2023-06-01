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
            Resource.read("/inputs/calibration/CalibrationParameters.json")
        ).instrumentState
        focusGroups = ReductionIngredients.parse_raw(
            Resource.read("/inputs/calibration/input.json")
        ).reductionState.stateConfig.focusGroups
        peakList = CrystallographicInfo.parse_raw(Resource.read("/outputs/crystalinfo/output.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("InstrumentState", instrumentState.json())
        purgeAlgo.setProperty("FocusGroups", json.dumps([focusGroup.json() for focusGroup in focusGroups]))
        purgeAlgo.setProperty("PeakList", json.dumps(peakList.d))
        assert purgeAlgo.getProperty("InstrumentState").value == instrumentState.json()
        assert purgeAlgo.getProperty("FocusGroups").value == json.dumps(
            [focusGroup.json() for focusGroup in focusGroups]
        )
        assert purgeAlgo.getProperty("PeakList").value == json.dumps(peakList.d)

    def test_execute():
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/calibration/CalibrationParameters.json")
        ).instrumentState
        focusGroups = ReductionIngredients.parse_raw(
            Resource.read("/inputs/calibration/input.json")
        ).reductionState.stateConfig.focusGroups
        peakList = CrystallographicInfo.parse_raw(Resource.read("/outputs/crystalinfo/output.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("InstrumentState", instrumentState.json())
        purgeAlgo.setProperty("FocusGroups", json.dumps([focusGroup.dict() for focusGroup in focusGroups]))
        purgeAlgo.setProperty("PeakList", json.dumps(peakList.d))
        purgeAlgo.execute()
