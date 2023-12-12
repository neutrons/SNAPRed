import json
import unittest

import pytest
from mantid.simpleapi import CreateSampleWorkspace, DeleteWorkspace
from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import NormalizationCalibrationIngredients
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.CalibrantSample import CalibrantSamples
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import CalibrationNormalizationAlgo
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.meta.Config import Resource


@pytest.fixture(scope="module")
def setup_workspaces():
    # Create mock input workspaces
    input_ws = CreateSampleWorkspace(OutputWorkspace="input_ws")
    background_ws = CreateSampleWorkspace(OutputWorkspace="background_ws")
    grouping_ws = CreateSampleWorkspace(OutputWorkspace="grouping_ws")

    yield input_ws, background_ws, grouping_ws

    # Clean up
    DeleteWorkspace(input_ws)
    DeleteWorkspace(background_ws)
    DeleteWorkspace(grouping_ws)


def buildIngredients():
    fakeRunConfig = RunConfig.parse_raw(Resource.read("inputs/purge_peaks/input_ingredients.json"))
    reductionState = ReductionState.parse_raw(Resource.read("inputs/purge_peaks/input_ingredients.json"))
    fakeCIF = Resource.getPath("/inputs/crystalInfo/example.cif")
    calibrantJSON = json.loads(Resource.read("/inputs/normalization/Silicon_NIST_640D_001.json"))
    calibrantJSON["crystallography"]["cifFile"] = fakeCIF
    del calibrantJSON["material"]["packingFraction"]
    calibrantSample = CalibrantSamples.parse_obj(calibrantJSON)
    calibrantSample.crystallography.cifFile = fakeCIF
    crystalInfo = CrystallographicInfoService().ingest(fakeCIF)["crystalInfo"]

    fakeInstrumentState = InstrumentState.parse_raw(Resource.read("inputs/diffcal/fakeInstrumentState.json"))

    reductionIngredients = ReductionIngredients(fakeRunConfig, reductionState, fakeInstrumentState.pixelGroup)
    backgroundReductionIngredients = ReductionIngredients(fakeRunConfig, reductionState, fakeInstrumentState.pixelGroup)

    smoothDataIngredients = SmoothDataExcludingPeaksIngredients(
        smoothingParameter=0.5,
        instrumentState=fakeInstrumentState,
        crystalInfo=crystalInfo,
    )

    return NormalizationCalibrationIngredients(
        reductionIngredients=reductionIngredients,
        backgroundReductionIngredients=backgroundReductionIngredients,
        calibrantSample=calibrantSample,
        smoothDataIngredients=smoothDataIngredients,
    )


def test_calibration_normalization_algo(setup_workspaces):
    input_ws, background_ws, grouping_ws = setup_workspaces

    ingredients = buildIngredients()

    # Create instance of your algorithm
    algo = CalibrationNormalizationAlgo()
    algo.initialize()

    # Set properties as required
    algo.setProperty("InputWorkspace", input_ws)
    algo.setProperty("BackgroundWorkspace", background_ws)
    algo.setProperty("GroupingWorkspace", grouping_ws)
    algo.setProperty("Ingredients", ingredients.json())
    algo.setProperty("OutputWorkspace", "output_ws")
    algo.setProperty("SmoothedOutput", "smoothed_output_ws")

    # Here you would set up the "Ingredients" property with the necessary JSON string
    # algo.setProperty("Ingredients", your_ingredients_json)

    # Execute the algorithm
    assert algo.execute()

    # # Verify the output
    # output_ws = algo.getPropertyValue("OutputWorkspace")
    # smoothed_output_ws = algo.getPropertyValue("SmoothedOutput")

    # assert output_ws, "Output workspace should not be None"
    # assert smoothed_output_ws, "Smoothed output workspace should not be None"
