import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import (
        DeleteWorkspace,
        LoadNexusProcessed,
        mtd,
    )
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
    from snapred.backend.dao.ingredients import SmoothDataExcludingPeaksIngredients
    from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaks
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

    def test_SmoothDataExcludingPeaksAlgo():
        # input data
        testWorkspaceFile = "inputs/strip_peaks/DSP_58882_cal_CC_Column_spectra.nxs"

        # loading test workspace
        test_ws_name = "test_ws"
        LoadNexusProcessed(Filename=Resource.getPath(testWorkspaceFile), OutputWorkspace=test_ws_name)

        # load crystal info for testing
        crystalInfo = CrystallographicInfo.parse_raw(Resource.read("inputs/purge_peaks/input_crystalInfo.json"))

        # load instrument state for testing
        instrumentState = Calibration.parse_raw(
            Resource.read("inputs/purge_peaks/input_parameters.json")
        ).instrumentState

        # populate ingredients
        smoothDataIngredients = SmoothDataExcludingPeaksIngredients(
            crystalInfo=crystalInfo,
            instrumentState=instrumentState,
            smoothingParameter=0.5,
        )

        # initialize and run smoothdata algo
        smoothDataAlgo = SmoothDataExcludingPeaks()
        smoothDataAlgo.initialize()
        smoothDataAlgo.setProperty("InputWorkspace", test_ws_name)
        smoothDataAlgo.setProperty("Ingredients", smoothDataIngredients.json())
        smoothDataAlgo.execute()

        assert smoothDataAlgo.getProperty("InputWorkspace").value == "test_ws"
        assert smoothDataAlgo.getProperty("OutputWorkspace").value == "SmoothPeaks_out"
