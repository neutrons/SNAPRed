import os
import unittest
import unittest.mock as mock

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.api import mtd
    from snapred.backend.dao.ReductionIngredients import ReductionIngredients
    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
    from snapred.backend.recipe.algorithm.VanadiumFocussedReductionAlgorithm import (
        VanadiumFocussedReductionAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource
    class TestVanadiumFocussedReductionAlgorithm(unittest.TestCase):
        def setUp(self):
            self.reductionIngredients = ReductionIngredients.parse_raw(Resource.read("/inputs/reduction/input_ingredients.json"))

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

        def test_execute(self):
            vanAlgo = VanadiumFocussedReductionAlgorithm()
            vanAlgo.initialize()
            vanAlgo.setProperty("ReductionIngredients", self.reductionIngredients.json())
            vanAlgo.setProperty("SmoothDataIngredients", self.smoothIngredients.json())
            vanAlgo.execute()
            wsGroupName = vanAlgo.getProperty("OutputWorkspaceGroup").value
            assert wsGroupName == "vanadiumFocussedWSGroup"
            wsGroup = list(mtd[wsGroupName].getNames())
            expected = [
                'diffraction_focused_vanadium',
                'smooth_vanadium'
            ]
            assert wsGroup == expected
