import unittest
import unittest.mock as mock
from unittest.mock import call

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
    from snapred.backend.dao.ingredients import (
        ReductionIngredients,
        SmoothDataExcludingPeaksIngredients,
    )
    from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import (
        CalibrationNormalization,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    class TestVanadiumFocussedReductionAlgorithm(unittest.TestCase):
        def setUp(self):
            self.reductionIngredients = ReductionIngredients.parse_raw(
                Resource.read("/inputs/reduction/input_ingredients.json")
            )

            crystalInfo = CrystallographicInfo.parse_raw(Resource.read("inputs/purge_peaks/input_crystalInfo.json"))
            instrumentState = Calibration.parse_raw(
                Resource.read("inputs/purge_peaks/input_parameters.json")
            ).instrumentState
            self.smoothIngredients = SmoothDataExcludingPeaksIngredients(
                crystalInfo=crystalInfo, instrumentState=instrumentState
            )

        def test_init(self):
            """Test ability to initialize vanadium focussed reduction algo"""
            normalAlgo = CalibrationNormalization()
            normalAlgo.initialize()
            normalAlgo.setProperty("ReductionIngredients", self.reductionIngredients.json())
            normalAlgo.setProperty("SmoothDataIngredients", self.smoothIngredients.json())
            assert normalAlgo.getProperty("ReductionIngredients").value == self.reductionIngredients.json()
            assert normalAlgo.getProperty("SmoothDataIngredients").value == self.smoothIngredients.json()

        @mock.patch("snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo.mtd")
        @mock.patch("snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo.MantidSnapper")
        def test_execute(self, mock_MantidSnapper, mock_mtd):
            normalAlgo = CalibrationNormalization()
            mock_mtd.side_effect = {"diffraction_focused_vanadium": ["ws1", "ws2"]}
            normalAlgo.initialize()
            normalAlgo.setProperty("ReductionIngredients", self.reductionIngredients.json())
            normalAlgo.setProperty("SmoothDataIngredients", self.smoothIngredients.json())
            normalAlgo.execute()

            wsGroupName = normalAlgo.getProperty("FocusWorkspace").value
            assert wsGroupName == "ws"
            expected_calls = [
                call().loadEventNexus,
                call().CustomGroupWorkspace,
                call().RebinRagged,
                call().RebinRagged,
                call().RebinRagged,
                call().DiffractionFocussing,
                call().CloneWorkspace,
                call().SmoothDataExcludingPeaks,
                call().executeQueue,
            ]

            actual_calls = [call[0] for call in mock_MantidSnapper.mock_calls if call[0]]
            print(actual_calls)

            # Assertions
            assert actual_calls == [call[0] for call in expected_calls]
