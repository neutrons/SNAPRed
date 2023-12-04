import json
import unittest

import pytest
from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import (
    NormalizationCalibrationIngredients as Ingredients,
)

# to make ingredients
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.InstrumentState import InstrumentState

# the algorithm to test
from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import CalibrationNormalizationAlgo as Algo
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService

# for accessing test files
from snapred.meta.Config import Resource


class TestCalibrationNormalizationAlgo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from mantid.simpleapi import (
            AddSampleLog,
            CloneWorkspace,
            CreateSampleWorkspace,
            DeleteWorkspace,
            LoadDetectorsGroupingFile,
            LoadInstrument,
            Plus,
        )

        super().setUpClass()

        # create mock ingredients
        cls.mockIngredients: Ingredients = cls.createMockIngredients()
        tofMin = cls.mockIngredients.reductionIngredients.reductionState.stateConfig.tofMin
        tofMax = cls.mockIngredients.reductionIngredients.reductionState.stateConfig.tofMax
        cls.sample_proton_charge = 10.0

        # the test instrument uses 16 pixels, 4 banks of 4 pixels each
        # first create background data, then add a spectrum to it

        cls.rawInputWS: str = "_test_calibration_normalization_input_raw"
        cls.rawBackgroundWS: str = "_test_calibration_normalization_background_raw"
        CreateSampleWorkspace(
            OutputWorkspace=cls.rawBackgroundWS,
            WorkspaceType="Histogram",
            Function="Flat background",
            Xmin=tofMin,
            Xmax=tofMax,
            BinWidth=0.01,
            XUnit="TOF",
            NumBanks=4,
            BankPixelWidth=2,
        )
        AddSampleLog(
            Workspace=cls.rawBackgroundWS,
            LogName="gd_prtn_chrg",
            LogText=f"{cls.sample_proton_charge}",
            LogType="Number",
        )
        LoadInstrument(
            Workspace=cls.rawBackgroundWS,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
            RewriteSpectraMap=True,
        )

        # create the total data
        CloneWorkspace(
            InputWorkspace=cls.rawBackgroundWS,
            OutputWorkspace=cls.rawInputWS,
        )
        # now create just the signal and add it in
        CreateSampleWorkspace(
            OutputWorkspace="_tmp_sample",
            WorkspaceType="Histogram",
            Function="Powder Diffraction",
            Xmin=tofMin,
            Xmax=tofMax,
            BinWidth=0.01,
            XUnit="TOF",
            NumBanks=4,
            BankPixelWidth=2,
            InstrumentName="fakeSNAP.xml",
        )
        Plus(LHSWorkspace="_tmp_sample", RHSWorkspace=cls.rawInputWS, OutputWorkspace=cls.rawInputWS)
        DeleteWorkspace("_tmp_sample")
        # create the grouping workspace
        cls.focusWS = "_test_calibration_normalization_focus"
        LoadDetectorsGroupingFile(
            InputFile=Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml"),
            InputWorkspace=cls.rawInputWS,
            OutputWorkspace=cls.focusWS,
        )

    @classmethod
    def createMockIngredients(cls) -> Ingredients:
        ingredientsPath = "inputs/purge_peaks/input_ingredients.json"
        mockReductionIngredients = ReductionIngredients.parse_raw(Resource.read(ingredientsPath))

        randomSmoothingParameter = 0.01
        fakeCIF = Resource.getPath("/inputs/crystalInfo/example.cif")
        calibrantJSON = json.loads(Resource.read("/inputs/normalization/Silicon_NIST_640D_001.json"))
        calibrantJSON["crystallography"]["cifFile"] = fakeCIF
        del calibrantJSON["material"]["packingFraction"]
        calibrantSample = CalibrantSamples.parse_obj(calibrantJSON)
        calibrantSample.crystallography.cifFile = fakeCIF
        crystalInfo = CrystallographicInfoService().ingest(fakeCIF)["crystalInfo"]
        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("inputs/diffcal/fakeInstrumentState.json"))

        smoothDataIngredients = SmoothDataExcludingPeaksIngredients(
            smoothingParameter=randomSmoothingParameter,
            instrumentState=fakeInstrumentState,
            crystalInfo=crystalInfo,
        )

        return Ingredients(
            reductionIngredients=mockReductionIngredients,
            calibrantSample=calibrantSample,
            smoothDataIngredients=smoothDataIngredients,
        )

    def setUp(self):
        from mantid.simpleapi import CloneWorkspace

        self.inputWS = "_test_calibration_normalization_input"
        self.backgroundWS = "_test_calibration_normalization_background"
        CloneWorkspace(
            InputWorkspace=self.rawInputWS,
            OutputWorkspace=self.inputWS,
        )
        CloneWorkspace(
            InputWorkspace=self.rawBackgroundWS,
            OutputWorkspace=self.backgroundWS,
        )

    def test_init(self):
        algo = Algo()
        algo.initialize()
        try:
            algo.setPropertyValue("InputWorkspace", self.inputWS)
            algo.setPropertyValue("BackgroundWorkspace", self.backgroundWS)
            algo.setPropertyValue("GroupingWorkspace", self.focusWS)
            algo.setPropertyValue("Ingredients", self.mockIngredients.json())
        except RuntimeError:
            pytest.fail("Failed to initialize the properties")

    def test_fail_no_output(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("InputWorkspace", self.inputWS)
        algo.setPropertyValue("BackgroundWorkspace", self.backgroundWS)
        algo.setPropertyValue("GroupingWorkspace", self.focusWS)
        algo.setPropertyValue("Ingredients", self.mockIngredients.json())
        with pytest.raises(RuntimeError) as e:
            algo.execute()
        assert "OutputWorkspace" in str(e.value)

    def test_exec(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("InputWorkspace", self.inputWS)
        algo.setPropertyValue("BackgroundWorkspace", self.backgroundWS)
        algo.setPropertyValue("GroupingWorkspace", self.focusWS)
        algo.setPropertyValue("Ingredients", self.mockIngredients.json())
        algo.setPropertyValue("OutputWorkspace", "_test_normalization_calibration_output")
        assert algo.execute()


@pytest.fixture(autouse=True)
def clearLoggers():  # noqa: PT004
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
