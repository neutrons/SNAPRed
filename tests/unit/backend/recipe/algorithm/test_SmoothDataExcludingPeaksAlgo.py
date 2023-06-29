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
        IngestCrystallographicInfoAlgorithm,
        CrystallographicInfo,
    )
    from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import (
        SmoothDataExcludingPeaks,
    )
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
    
    @pytest.fixture(autouse=True)
    def _setup_teardown():
        setup()
        yield
        teardown()

    def test_smooth_data_excluding_peaks():
        # Initialize and set up test
        smooth_data_alg = SmoothDataExcludingPeaks()

        fakeCIF = Resource.getPath("/inputs/crystalInfo/fake_file.cif")

        # Set input
        smooth_data_alg.setProperty("Input", fakeCIF)  # replace with your actual input

        # Execute algorithm
        smooth_data_alg.execute()

        # Perform assertion
        assert smooth_data_alg.isExecuted()

        # Retrieve the output and perform assertions on it
        # The exact assertions will depend on specific algorithm and expected outputs
        # Replace actual assertions
        output = smooth_data_alg.getProperty("Output").value
        assert output is not None  # replace with your actual assertions
