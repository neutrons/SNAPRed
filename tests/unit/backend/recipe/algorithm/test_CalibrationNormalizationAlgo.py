import json
import socket
import unittest
from unittest.mock import MagicMock

import pytest
from mantid.api import AnalysisDataService as ADS
from mantid.simpleapi import AddSampleLog, CreateEmptyTableWorkspace, CreateSampleWorkspace, mtd
from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import (
    NormalizationCalibrationIngredients as Ingredients,
)
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import CalibrationNormalization
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.meta.Config import Resource

IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")


class TestCalibrationNormalizationAlgo(unittest.TestCase):
    @unittest.skipIf(not IS_ON_ANALYSIS_MACHINE, "requires analysis datafiles")
    def setUp(self):
        with open(Resource.getPath("inputs/calibration/input.json"), "r") as file:
            inputData = json.load(file)
        runConfigData = inputData["runConfig"]
        instrumentConfigData = inputData["reductionState"]["instrumentConfig"]
        stateConfigData = inputData["reductionState"]["stateConfig"]

        mockInstrumentConfig = InstrumentConfig.parse_obj(instrumentConfigData)

        mockStateConfig = StateConfig.parse_obj(stateConfigData)

        mockRunConfig = RunConfig.parse_obj(runConfigData)

        mockReductionState = ReductionState(
            instrumentConfig=mockInstrumentConfig, stateConfig=mockStateConfig, overrides=None
        )

        mockReductionIngredients = ReductionIngredients(
            runConfig=mockRunConfig,
            reductionState=mockReductionState,
        )

        randomSmoothingParameter = 0.01
        fakeCIF = Resource.getPath("/inputs/crystalInfo/example.cif")
        samplePath = Resource.getPath("/inputs/normalization/Silicon_NIST_640D_001.json")
        calibrantSample = self.getCalibrantSample(samplePath)
        crystalInfo = CrystallographicInfoService().ingest(fakeCIF)["crystalInfo"]
        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("inputs/diffcal/fakeInstrumentState.json"))
        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("inputs/diffcal/fakeFocusGroup.json"))
        fakeFocusGroup.definition = Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml")

        smoothDataIngredients = SmoothDataExcludingPeaksIngredients(
            smoothingParameter=randomSmoothingParameter,
            instrumentState=fakeInstrumentState,
            crystalInfo=crystalInfo,
        )

        self.mockIngredientsJson = Ingredients(
            reductionIngredients=mockReductionIngredients,
            backgroundReductionIngredients=mockReductionIngredients,
            calibrantSample=calibrantSample,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            smoothDataIngredients=smoothDataIngredients,
        )

        self.inputWorkspaceName = "mock_input_workspace"
        CreateSampleWorkspace(OutputWorkspace=self.inputWorkspaceName)

        self.backgroundInputWorkspaceName = "mock_background_input_workspace"
        CreateSampleWorkspace(OutputWorkspace=self.backgroundInputWorkspaceName)

        self.mockCalibrationTableName = "mock_calibration_table"
        self.mockCalibrationTable = CreateEmptyTableWorkspace(OutputWorkspace=self.mockCalibrationTableName)
        self.mockCalibrationTable.addColumn("int", "detid")
        self.mockCalibrationTable.addColumn("double", "difc")
        self.mockCalibrationTable.addColumn("double", "difa")
        self.mockCalibrationTable.addColumn("double", "tzero")

        self.algo = CalibrationNormalization()
        self.algo.initialize()
        self.algo.setProperty("InputWorkspace", self.inputWorkspaceName)
        self.algo.setProperty("BackgroundWorkspace", self.backgroundInputWorkspaceName)
        self.algo.setProperty("CalibrationWorkspace", self.mockCalibrationTableName)
        self.algo.setProperty("Ingredients", self.mockIngredientsJson.json())

    @unittest.skipIf(not IS_ON_ANALYSIS_MACHINE, "requires analysis datafiles")
    def testAlgorithmInitialization(self):
        assert self.algo.isInitialized()

    @unittest.skipIf(not IS_ON_ANALYSIS_MACHINE, "requires analysis datafiles")
    def testAlgorithmPropertySetup(self):
        assert "mock_input_workspace" in self.algo.getPropertyValue("InputWorkspace")
        assert "mock_background_input_workspace" in self.algo.getPropertyValue("BackgroundWorkspace")

    @unittest.skipIf(not IS_ON_ANALYSIS_MACHINE, "requires analysis datafiles")
    def getCalibrantSample(self, samplePath):
        with open(samplePath, "r") as file:
            sampleJson = json.load(file)
        del sampleJson["material"]["packingFraction"]
        for atom in sampleJson["crystallography"]["atoms"]:
            atom["symbol"] = atom.pop("atom_type")
            atom["coordinates"] = atom.pop("atom_coordinates")
            atom["siteOccupationFactor"] = atom.pop("site_occupation_factor")
        sample = CalibrantSamples.parse_raw(json.dumps(sampleJson))
        return sample


@pytest.fixture(autouse=True)
def clearLoggers():  # noqa: PT004
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
