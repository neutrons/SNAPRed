import socket
import unittest.mock as mock

import pytest
from mantid.testing import assert_almost_equal
from snapred.backend.dao import CrystallographicPeak, DetectorPeak, GroupPeakList
from snapred.meta.redantic import list_to_raw
from util.diffraction_calibration_synthetic_data import SyntheticData

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import (
        ConvertToEventWorkspace,
        CreateWorkspace,
        DeleteWorkspace,
        LoadNexusProcessed,
        mtd,
    )

    # NOTE testing chop ingredients requires interacting w/ algo as object
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

    def test_chop_ingredients():
        """
        Test that chopIngredients creates the needed data structures from the list of detector peaks.
        """
        # create input detector peaks
        len_wksp = 6
        peaks = [GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=i) for i in range(len_wksp)]

        # initialize the algo and chop ingredients
        weightCalculatorAlgo = DiffractionSpectrumWeightCalculator()
        weightCalculatorAlgo.initialize()
        weightCalculatorAlgo.setProperty("DetectorPeaks", list_to_raw(peaks))
        weightCalculatorAlgo.chopIngredients(peaks)

        # verify result
        assert len(weightCalculatorAlgo.groupIDs) == len_wksp
        assert weightCalculatorAlgo.groupIDs == list(range(len_wksp))
        assert len(weightCalculatorAlgo.predictedPeaks) == len_wksp
        assert list(weightCalculatorAlgo.predictedPeaks.keys()) == list(range(len_wksp))

    def test_unbag_ingredients():
        """Test that unbag groceries clones the input into the weight workspace output"""
        len_wksp = 6
        input_ws_name = mtd.unique_name(prefix="input_ws_")
        weight_ws_name = mtd.unique_name(prefix="weight_ws_")

        # create the input workspace
        CreateWorkspace(
            OutputWorkspace=input_ws_name,
            DataX=[1] * len_wksp,
            DataY=[1] * len_wksp,
            NSpec=len_wksp,
        )

        # initialize the algo and unbag groceries
        algo = DiffractionSpectrumWeightCalculator()
        algo.initialize()
        algo.setProperty("InputWorkspace", input_ws_name)
        algo.setProperty("WeightWorkspace", weight_ws_name)
        algo.unbagGroceries()

        # verify result
        assert_almost_equal(
            Workspace1=input_ws_name,
            Workspace2=weight_ws_name,
        )

    def test_unbag_ingredients_converts_events():
        """
        Test that unbag groceries clones the input into the weight workspace output
        AND converts the event workspace into a matrix workspace.
        """
        len_wksp = 6
        input_ws_name = mtd.unique_name(prefix="input_ws_")
        event_ws_name = mtd.unique_name(prefix="event_ws_")
        weight_ws_name = mtd.unique_name(prefix="weight_ws_")

        # create the original histogram workspace
        CreateWorkspace(
            OutputWorkspace=input_ws_name,
            DataX=[1, 2] * len_wksp,
            DataY=[1] * len_wksp,
            NSpec=len_wksp,
        )
        # convert it to an event workspace
        ConvertToEventWorkspace(
            InputWorkspace=input_ws_name,
            OutputWorkspace=event_ws_name,
        )

        # initialize algo and unbag groceries
        algo = DiffractionSpectrumWeightCalculator()
        algo.initialize()
        algo.setProperty("InputWorkspace", event_ws_name)
        algo.setProperty("WeightWorkspace", weight_ws_name)
        algo.unbagGroceries()

        # verify result: the weight ws should converted back to histogram data
        assert_almost_equal(
            Workspace1=input_ws_name,
            Workspace2=weight_ws_name,
        )

    def test_validate_fail_wrong_sizes():
        """
        Test that validation fails if the input workspace has an incompatible size with the detector peaks.
        """

        len_wksp = 2
        len_peaks = 1
        input_ws_name = mtd.unique_name(prefix="input_ws_")
        weight_ws_name = mtd.unique_name(prefix="weight_ws_")

        # create input workspace with TWO histograms
        CreateWorkspace(
            OutputWorkspace=input_ws_name,
            DataX=[1] * len_wksp,
            DataY=[1] * len_wksp,
            NSpec=len_wksp,
        )

        # create input detector peaks with ONE histogram
        peaks_json = list_to_raw(
            [GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=i) for i in range(len_peaks)]
        )

        # initialize the algo and try to run -- verify that it fails with error
        weightCalculatorAlgo = DiffractionSpectrumWeightCalculator()
        weightCalculatorAlgo.initialize()
        weightCalculatorAlgo.setProperty("InputWorkspace", input_ws_name)
        weightCalculatorAlgo.setProperty("DetectorPeaks", peaks_json)
        weightCalculatorAlgo.setProperty("WeightWorkspace", weight_ws_name)
        with pytest.raises(RuntimeError) as e:
            weightCalculatorAlgo.execute()
        assert str(len_wksp) in str(e.value)
        assert str(len_peaks) in str(e.value)

    def test_validate_pass_and_execute():
        """
        Test that validation will pass if sizes are compatible, and the algo will run.
        This does not guarantee any results are correct, only that the algo runs.
        """
        len_wksp = 1
        input_ws_name = mtd.unique_name(prefix="input_ws_")
        weight_ws_name = mtd.unique_name(prefix="weight_ws")

        # create input workspace
        CreateWorkspace(
            OutputWorkspace=input_ws_name,
            DataX=[1] * len_wksp,
            DataY=[1] * len_wksp,
            NSpec=len_wksp,
        )

        # create input detector peaks
        peaks_json = list_to_raw(
            [GroupPeakList(peaks=SyntheticData.fakeDetectorPeaks(), groupID=i) for i in range(len_wksp)]
        )

        # initialize and run the weight algo -- verify it runs
        weightCalculatorAlgo = DiffractionSpectrumWeightCalculator()
        weightCalculatorAlgo.initialize()
        weightCalculatorAlgo.setProperty("InputWorkspace", input_ws_name)
        weightCalculatorAlgo.setProperty("DetectorPeaks", peaks_json)
        weightCalculatorAlgo.setProperty("WeightWorkspace", weight_ws_name)
        assert weightCalculatorAlgo.execute()

    def test_execute_correct_weights():
        """
        Test that the correct weights are produced for a simple case
        where weights are exactly specified ahead of time.
        """

        expected_weights = [1, 1, 0, 1, 0, 1, 1, 0, 1, 1]

        input_ws_name = mtd.unique_name(prefix="input_ws_")
        output_ws_name = mtd.unique_name(prefix="output_ws_")
        weight_ws_name = mtd.unique_name(prefix="weight_ws_")

        # create input workspace based on expected weight output
        peak_lo = 2
        peak_hi = 7
        peaks = [peak_lo if x == 1 else peak_hi for x in expected_weights]
        CreateWorkspace(
            OutputWorkspace=input_ws_name,
            DataX=list(range(len(expected_weights))),
            DataY=peaks,
            NSpec=1,
        )

        # create the expected output workspace
        CreateWorkspace(
            OutputWorkspace=output_ws_name, DataX=list(range(len(expected_weights))), DataY=expected_weights, NSpec=1
        )

        # create the list of peaks where weight will be zeroed
        xtalPeak = CrystallographicPeak(
            # NOTE this is needed for pydantic validation, but not the algo
            hkl=(0, 0, 0),
            dSpacing=0.0,
            fSquared=0.0,
            multiplicity=0,
        )
        peakList = [
            DetectorPeak(
                position={"value": x, "minimum": x - 0.01, "maximum": x + 0.01},
                peak=xtalPeak,
            )
            for x in range(len(peaks))
            if peaks[x] == peak_hi
        ]
        peaks_json = list_to_raw([GroupPeakList(peaks=peakList, groupID=0)])

        # initialize and run the weight algo
        weightCalculatorAlgo = DiffractionSpectrumWeightCalculator()
        weightCalculatorAlgo.initialize()
        weightCalculatorAlgo.setProperty("InputWorkspace", input_ws_name)
        weightCalculatorAlgo.setProperty("DetectorPeaks", peaks_json)
        weightCalculatorAlgo.setProperty("WeightWorkspace", weight_ws_name)
        assert weightCalculatorAlgo.execute()

        # verify the weight workspace matches expectations
        assert_almost_equal(
            Workspace1=output_ws_name,
            Workspace2=weight_ws_name,
        )

    def test_with_predicted_peaks():
        """
        Test the weight calculator given predicted peaks
        This test is similar to above, but using realistic data, instead of constructed data.
        """

        inputWorkspaceFile = "/inputs/strip_peaks/DSP_58882_cal_CC_Column_spectra.nxs"
        referenceWeightFile = "/outputs/weight_spectra/weights.nxs"
        input_ws_name = mtd.unique_name(prefix="input_ws_")
        ref_weight_ws_name = mtd.unique_name(prefix="_ref_weight_")
        weight_ws_name = mtd.unique_name(prefix="weight_ws_")

        # load test workspace
        LoadNexusProcessed(
            Filename=Resource.getPath(inputWorkspaceFile),
            OutputWorkspace=input_ws_name,
        )

        # load expected workspaced
        LoadNexusProcessed(
            Filename=Resource.getPath(referenceWeightFile),
            OutputWorkspace=ref_weight_ws_name,
        )

        # load predicted peaks
        peaks_json = Resource.read("inputs/weight_spectra/peaks.json")

        # initialize and run the weight algo
        weightCalculatorAlgo = DiffractionSpectrumWeightCalculator()
        weightCalculatorAlgo.initialize()
        weightCalculatorAlgo.setProperty("InputWorkspace", input_ws_name)
        weightCalculatorAlgo.setProperty("DetectorPeaks", peaks_json)
        weightCalculatorAlgo.setProperty("WeightWorkspace", weight_ws_name)
        assert weightCalculatorAlgo.execute()

        # assert the weights are as expected
        assert_almost_equal(
            Workspace1=weight_ws_name,
            Workspace2=ref_weight_ws_name,
            CheckInstrument=False,
        )
