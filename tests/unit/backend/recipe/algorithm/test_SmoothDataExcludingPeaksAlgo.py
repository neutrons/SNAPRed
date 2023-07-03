import json
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import DeleteWorkspace, mtd
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
        PixelGroupingParametersCalculationAlgorithm,
    )
    from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import (
        CrystallographicInfo,
    )
    from snapred.backend.dao.SmoothDataPeaksIngredients import(
        SmoothDataPeaksIngredients,
    )
    from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import (
        SmoothDataExcludingPeaks,
    )
    from snapred.meta.Config import Resource

    def test_init_path():
        # Test functionality to initialize crystal ingestion algo from path name
        fakeCIF = Resource.getPath("/inputs/crystalInfo/fake_file.cif")
        try:
            smoothAlgo = SmoothDataExcludingPeaks()
            smoothAlgo.initialize()
            smoothAlgo.setProperty("cifPath", fakeCIF)
            assert fakeCIF == smoothAlgo.getProperty("cifPath").value
        except Exception:
            pytest.fail("Failed to open.")

    def test_load_crystalInfo():
        CIFpath = Resource.getPath("/inputs/crystalInfo/example.cif")
        CrystalInfo = CrystallographicInfo.parse_raw(Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        CalState = Calibration.parse_raw(Resource.read("/inputs/purge_peaks/input_parameters.json")).instrumentState
        Ws = "testWorkSpace"

        smoothAlgoIngredients = SmoothDataPeaksIngredients(crystalInfo = CrystalInfo, instrumentState = CalState, inputWorkspace = Ws)
        try:
            smoothAlgo = SmoothDataExcludingPeaks()
            smoothAlgo.initialize()
            smoothAlgo.setProperty("SmoothDataExcludingPeaksIngredients", smoothAlgoIngredients.json())
            assert smoothAlgo.getProperty("SmoothDataExcludingPeaksIngredients").value == smoothAlgoIngredients.json()
        except Exception:
            pytest.fail("Failed to open.")