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
        try:
            smoothAlgo = SmoothDataExcludingPeaks()
            smoothAlgo.initialize()
            smoothAlgo.setProperty("cifPath", CIFpath)
            smoothAlgo.execute()
            xtalinfo = json.loads(smoothAlgo.getProperty("crystalInfo").value)
        except Exception:
            pytest.fail("Failed to open.")
        else:
            assert xtalinfo["peaks"][0]["hkl"] == [1, 1, 1]
            assert xtalinfo["peaks"][5]["hkl"] == [4, 0, 0]
            assert xtalinfo["peaks"][0]["dSpacing"] == 3.13592994862768
            assert xtalinfo["peaks"][4]["dSpacing"] == 1.0453099828758932