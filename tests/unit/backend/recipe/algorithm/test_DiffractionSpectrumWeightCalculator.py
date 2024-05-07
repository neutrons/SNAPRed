import json
import socket
import unittest.mock as mock

import pytest
from snapred.backend.dao import GroupPeakList
from snapred.meta.redantic import list_to_raw
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import workspacesEqual

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
    from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
    from snapred.meta.Config import Resource

    IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

    def setup():
        """Common setup before each test"""
        pass

    def teardown():
        """Common teardown after each test"""
        if not IS_ON_ANALYSIS_MACHINE:  # noqa: F821
            return
        # collect list of all workspaces
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for workspace in workspaces:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")

    @pytest.fixture(autouse=True)
    def _setup_teardown():
        """Setup before each test, teardown after each test"""
        setup()
        yield
        teardown()

    def test_with_predicted_peaks():
        """Test the weight calculator given predicted peaks"""
        inputWorkspaceFile = "/inputs/strip_peaks/DSP_58882_cal_CC_Column_spectra.nxs"
        referenceWeightFile = "/outputs/weight_spectra/weights.nxs"

        # load test workspace
        input_ws_name = "input_ws"
        LoadNexusProcessed(
            Filename=Resource.getPath(inputWorkspaceFile),
            OutputWorkspace=input_ws_name,
        )

        # load predicted peaks
        peaks_json = list_to_raw([GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=0)])

        # initialize and run the weight algo
        weight_ws_name = "weight_ws"
        weightCalculatorAlgo = DiffractionSpectrumWeightCalculator()
        weightCalculatorAlgo.initialize()
        weightCalculatorAlgo.setProperty("InputWorkspace", input_ws_name)
        weightCalculatorAlgo.setProperty("DetectorPeaks", peaks_json)
        weightCalculatorAlgo.setProperty("WeightWorkspace", weight_ws_name)

        assert weightCalculatorAlgo.execute()

        # match results with reference
        # weight_ws = mtd[weight_ws_name]
        ref_weight_ws_name = mtd.unique_name(prefix="_ref_weight_")
        LoadNexusProcessed(
            Filename=Resource.getPath(referenceWeightFile),
            OutputWorkspace=ref_weight_ws_name,
        )
        # TODO FIX THIS TEST
        assert workspacesEqual(
            Workspace1=weight_ws_name,
            Workspace2=ref_weight_ws_name,
            CheckInstrument=False,
            BAD=True,
        )
