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
    from snapred.backend.dao.ReductionIngredients import ReductionIngredients
    from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
    from snapred.backend.recipe.algorithm.VanadiumFocussedReductionAlgorithm import (
        VanadiumFocussedReductionAlgorithm,  # noqa: E402
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
            vanAlgo = VanadiumFocussedReductionAlgorithm()
            vanAlgo.initialize()
            vanAlgo.setProperty("ReductionIngredients", self.reductionIngredients.json())
            vanAlgo.setProperty("SmoothDataIngredients", self.smoothIngredients.json())
            assert vanAlgo.getProperty("ReductionIngredients").value == self.reductionIngredients.json()
            assert vanAlgo.getProperty("SmoothDataIngredients").value == self.smoothIngredients.json()

        @mock.patch("snapred.backend.recipe.algorithm.VanadiumFocussedReductionAlgorithm.mtd")
        @mock.patch("snapred.backend.recipe.algorithm.VanadiumFocussedReductionAlgorithm.MantidSnapper.createAlgorithm")
        def test_execute(self, mock_MantidSnapper, mock_mtd):
            vanAlgo = VanadiumFocussedReductionAlgorithm()
            mock_mtd.side_effect = {"diffraction_focused_vanadium": ["ws1", "ws2"]}
            vanAlgo.initialize()
            vanAlgo.setProperty("ReductionIngredients", self.reductionIngredients.json())
            vanAlgo.setProperty("SmoothDataIngredients", self.smoothIngredients.json())
            vanAlgo.execute()

            wsGroupName = vanAlgo.getProperty("OutputWorkspaceGroup").value
            assert wsGroupName == "diffraction_focused_vanadium"
            calls = [
                call("LoadNexus"),
                call("CustomGroupWorkspace"),
                call("ConvertUnits"),
                call("DiffractionFocussing"),
                call("LoadNexus"),
                call("CustomGroupWorkspace"),
                call("ConvertUnits"),
                call("DiffractionFocussing"),
            ]
            mock_MantidSnapper.assert_has_calls(calls, any_order=True)

        @mock.patch("snapred.backend.recipe.algorithm.VanadiumFocussedReductionAlgorithm.mtd")
        @mock.patch("snapred.backend.recipe.algorithm.VanadiumFocussedReductionAlgorithm.MantidSnapper.createAlgorithm")
        def test_fullCoverage(self, mock_snapper, mock_mtd):  # noqa ARG002
            vanAlgo = VanadiumFocussedReductionAlgorithm()
            vanAlgo.PyInit()
            vanAlgo.setProperty("ReductionIngredients", self.reductionIngredients.json())
            vanAlgo.setProperty("SmoothDataIngredients", self.smoothIngredients.json())
            vanAlgo.PyExec()
            wsGroupName = vanAlgo.getProperty("OutputWorkspaceGroup").value
            assert wsGroupName == "diffraction_focused_vanadium"
