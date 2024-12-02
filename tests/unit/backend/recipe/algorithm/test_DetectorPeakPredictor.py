import socket
import unittest.mock as mock
from typing import List

import pydantic
import pytest
from util.dao import DAOFactory

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import DeleteWorkspace, mtd

    from snapred.backend.dao.GroupPeakList import GroupPeakList
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

    def test_chopIngredients():
        ingredients = DAOFactory.fake_peak_ingredients.copy()
        algo = DetectorPeakPredictor()
        algo.initialize()
        algo.chopIngredients(ingredients)

        # check various properties copied over
        assert algo.beta_0 == ingredients.instrumentState.gsasParameters.beta[0]
        assert algo.beta_1 == ingredients.instrumentState.gsasParameters.beta[1]
        assert algo.FWHMMultiplierLeft == ingredients.instrumentState.fwhmMultipliers.left
        assert algo.FWHMMultiplierRight == ingredients.instrumentState.fwhmMultipliers.right
        assert algo.peakTailCoefficient == ingredients.instrumentState.peakTailCoefficient
        assert (
            algo.L == ingredients.instrumentState.instrumentConfig.L1 + ingredients.instrumentState.instrumentConfig.L2
        )

        # check the peaks threshold
        # NOTE all peaks in test file have dspacing = multiplicity = 1, so ordered by fSquared
        peaks = ingredients.crystalInfo.peaks
        goodPeaks = [peak for peak in peaks if peak.fSquared >= 0]
        assert algo.goodPeaks == goodPeaks
        assert algo.allGroupIDs == ingredients.pixelGroup.groupIDs

    def test_execute():
        peaksRefFile = "/outputs/predict_peaks/peaks.json"

        ingredients = DAOFactory.good_peak_ingredients.copy()

        peakPredictorAlgo = DetectorPeakPredictor()
        peakPredictorAlgo.initialize()
        peakPredictorAlgo.setProperty("Ingredients", ingredients.model_dump_json())
        peakPredictorAlgo.setProperty("PurgeDuplicates", False)
        assert peakPredictorAlgo.execute()

        peaks_cal = pydantic.TypeAdapter(List[GroupPeakList]).validate_json(
            peakPredictorAlgo.getProperty("DetectorPeaks").value
        )
        peaks_ref = pydantic.TypeAdapter(List[GroupPeakList]).validate_json(Resource.read(peaksRefFile))
        assert peaks_cal == peaks_ref

    def test_execute_purge_duplicates():
        peaksRefFile = "/outputs/predict_peaks/peaks_purged.json"

        ingredients = DAOFactory.good_peak_ingredients.copy()

        peakPredictorAlgo = DetectorPeakPredictor()
        peakPredictorAlgo.initialize()
        peakPredictorAlgo.setProperty("Ingredients", ingredients.model_dump_json())
        peakPredictorAlgo.setProperty("PurgeDuplicates", True)
        assert peakPredictorAlgo.execute()

        peaks_cal = pydantic.TypeAdapter(List[GroupPeakList]).validate_json(
            peakPredictorAlgo.getProperty("DetectorPeaks").value
        )
        peaks_ref = pydantic.TypeAdapter(List[GroupPeakList]).validate_json(Resource.read(peaksRefFile))
        for peakList in peaks_ref:
            for peak in peakList.peaks:
                peak.position.value = round(peak.value, 5)
        assert peaks_cal == peaks_ref

    def test_bad_ingredients():
        peakPredictorAlgo = DetectorPeakPredictor()
        peakPredictorAlgo.initialize()
        peakPredictorAlgo.setProperty("Ingredients", "junk")
        with pytest.raises(RuntimeError) as excinfo:
            peakPredictorAlgo.execute()
        assert "Ingredients" in str(excinfo.value)
