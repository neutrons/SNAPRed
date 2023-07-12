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
    from mantid.simpleapi import DeleteWorkspace, mtd
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
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

    def test_execute():
        calibrationFile = "/inputs/purge_peaks/input_parameters.json"
        crystalInfoFile = "/inputs/purge_peaks/input_crystalInfo.json"
        peaksRefFile = "/outputs/predict_peaks/peaks.json"

        instrumentState = Calibration.parse_raw(Resource.read(calibrationFile)).instrumentState

        peakPredictorAlgo = DetectorPeakPredictor()
        peakPredictorAlgo.initialize()
        peakPredictorAlgo.setProperty("InstrumentState", instrumentState.json())
        peakPredictorAlgo.setProperty("CrystalInfo", Resource.read(crystalInfoFile))

        assert peakPredictorAlgo.execute()

        peaks_calc_json = json.loads(peakPredictorAlgo.getProperty("DetectorPeaks").value)
        peaks_ref_json = json.loads(Resource.read(peaksRefFile))

        assert peaks_calc_json == peaks_ref_json

    def test_wrong_instrument_state():
        crystalInfoFile = "/inputs/purge_peaks/input_crystalInfo.json"

        peakPredictorAlgo = DetectorPeakPredictor()
        peakPredictorAlgo.initialize()
        peakPredictorAlgo.setProperty("InstrumentState", "junk")
        peakPredictorAlgo.setProperty("CrystalInfo", Resource.read(crystalInfoFile))

        with pytest.raises(RuntimeError) as excinfo:
            peakPredictorAlgo.execute()
        assert "InstrumentState" in str(excinfo.value)

    def test_wrong_crystal_info():
        calibrationFile = "/inputs/purge_peaks/input_parameters.json"
        instrumentState = Calibration.parse_raw(Resource.read(calibrationFile)).instrumentState

        peakPredictorAlgo = DetectorPeakPredictor()
        peakPredictorAlgo.initialize()
        peakPredictorAlgo.setProperty("InstrumentState", instrumentState.json())
        peakPredictorAlgo.setProperty("CrystalInfo", "junk")

        with pytest.raises(RuntimeError) as excinfo:
            peakPredictorAlgo.execute()
        assert "CrystallographicInfo" in str(excinfo.value)
