import unittest.mock as mock

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
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
        focusGroups = ReductionIngredients.parse_raw(Resource.read("/inputs/calibration/input.json")).focusGroups
        peakList = CrystallographicInfo.parse_raw(Resource.read("/outputs/crystalinfo/output.json"))
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.setProperty("instrumentState", instrumentState.json())
        purgeAlgo.setProperty("focusGroups", focusGroups.json())
        purgeAlgo.setProperty("peakList", peakList.d)
        purgeAlgo.initialize()
        assert purgeAlgo.getProperty("instrumentState").value == instrumentState.json()
        assert purgeAlgo.getProperty("focusGroups").value == focusGroups.json()
        assert purgeAlgo.getProperty("peakList").value == peakList.d
