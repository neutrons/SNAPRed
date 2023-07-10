import unittest.mock as mock
import json

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from pydantic import parse_file_as
    from mantid.simpleapi import (
        DeleteWorkspace,
        LoadNexusProcessed,
        mtd,
    )
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import PixelGroupingParametersCalculationAlgorithm
    from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import CrystallographicInfo
    from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaks
    from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
    from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import IngestCrystallographicInfoAlgorithm
    from snapred.meta.Config import Resource

    def setup():
        pass

    def teardown():
        workspaces = mtd.getObjectNames()

        for workspace in workspaces:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")

    @pytest.fixture(autouse = True)
    def _setup_teardown():
        setup()
        yield
        teardown()