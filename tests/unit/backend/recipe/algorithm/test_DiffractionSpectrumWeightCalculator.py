import json
import socket
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
        inputPeaksFile = "/inputs/strip_peaks/peaks.json"
        referenceWeightFile = "/inputs/strip_peaks/weights.nxs"

        # load test workspace
        input_ws_name = "input_ws"
        LoadNexusProcessed(Filename=Resource.getPath(inputWorkspaceFile), OutputWorkspace=input_ws_name)

        # load predicted peaks
        peaks_json = json.loads(Resource.read(inputPeaksFile))

        # initialize and run the weight algo
        weight_ws_name = "weight_ws"
        weightCalculatorAlgo = DiffractionSpectrumWeightCalculator()
        weightCalculatorAlgo.initialize()
        weightCalculatorAlgo.setProperty("InputWorkspace", input_ws_name)
        weightCalculatorAlgo.setProperty("DetectorPeaks", json.dumps(peaks_json))
        weightCalculatorAlgo.setProperty("WeightWorkspace", weight_ws_name)

        assert weightCalculatorAlgo.execute()

        # match results with reference
        weight_ws = mtd[weight_ws_name]
        ref_weight_ws = LoadNexusProcessed(Filename=Resource.getPath(referenceWeightFile))
        assert weight_ws.getNumberHistograms() == ref_weight_ws.getNumberHistograms()
        for index in range(weight_ws.getNumberHistograms()):
            x_calc = weight_ws.readX(index)
            y_calc = weight_ws.readY(index)
            x_ref = ref_weight_ws.readX(index)
            y_ref = ref_weight_ws.readY(index)
            assert (abs(x_calc - x_ref) < 1.0e-14).all()
            assert (y_calc == y_ref).all()
