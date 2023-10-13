import json
import random
import unittest
import unittest.mock as mock
from typing import Dict, List

import pytest
from mantid.api import PythonAlgorithm
from mantid.kernel import Direction
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.CalibrantSample.Atom import Atom
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.algorithm.ConvertDiffCalLog import ConvertDiffCalLog  # noqa
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

# the algorithm to test
from snapred.backend.recipe.algorithm.RawVanadiumCorrection import RawVanadiumCorrection as Algo  # noqa: E402
from snapred.meta.Config import Resource

TheAlgorithmManager: str = "snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager"


class TestRawVanadiumCorrection(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        self.fakeIngredients = Ingredients.parse_raw(Resource.read("/inputs/reduction/fake_file.json"))
        self.fakeIngredients.runConfig = fakeRunConfig
        self.fakeIngredients.reductionState.stateConfig.tofMin = 1
        self.fakeIngredients.reductionState.stateConfig.tofBin = 1
        self.fakeIngredients.reductionState.stateConfig.tofMax = 100
        pass

    def mockRaidPantry(algo, wsName, filename):  # noqa
        """Will cause algorithm to execute with sample data, instead of loading from file"""
        # prepare with test data
        algo.mantidSnapper.CreateSampleWorkspace(
            "Make fake data for testing",
            OutputWorkspace=wsName,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=algo.TOFPars[0],
            Xmax=algo.TOFPars[2],
            BinWidth=algo.TOFPars[1],
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        algo.mantidSnapper.Rebin(
            "Rebin in log TOF",
            InputWorkspace=wsName,
            Params=algo.TOFPars,
            PreserveEvents=False,
            OutputWorkspace=wsName,
            BinningMode="Logarithmic",
        )
        algo.mantidSnapper.executeQueue()
        pass

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        algo = Algo()
        algo.initialize()
        algo.chopIngredients(self.fakeIngredients)
        assert algo.liteMode == self.fakeIngredients.reductionState.stateConfig.isLiteMode
        assert algo.vanadiumRunNumber == self.fakeIngredients.runConfig.runNumber
        assert (
            algo.vanadiumBackgroundRunNumber == self.fakeIngredients.reductionState.stateConfig.emptyInstrumentRunNumber
        )
        assert algo.TOFPars[0] == self.fakeIngredients.reductionState.stateConfig.tofMin
        assert algo.TOFPars[1] == self.fakeIngredients.reductionState.stateConfig.tofBin
        assert algo.TOFPars[2] == self.fakeIngredients.reductionState.stateConfig.tofMax
        assert algo.geomCalibFile == self.fakeIngredients.reductionState.stateConfig.geometryCalibrationFileName
        assert algo.rawVFile == self.fakeIngredients.reductionState.stateConfig.rawVanadiumCorrectionFileName

    def test_chop_calibrant_sample(self):
        # create some nonsense material and crystallography
        fakeMaterial = Material(
            packingFraction=0.3,
            massDensity=1.0,
            chemicalFormula="V B",
        )
        vanadiumAtom = Atom(
            symbol="V",
            coordinates=[0, 0, 0],
            siteOccupationFactor=0.5,
        )
        boronAtom = Atom(
            symbol="B",
            coordinates=[0, 1, 0],
            siteOccupationFactor=1.0,
        )
        fakeXtal = Crystallography(
            cifFile=Resource.getPath("inputs/crystalInfo/example.cif"),
            spaceGroup="I m -3 m",
            latticeParameters=[1, 2, 3, 4, 5, 6],
            atoms=[vanadiumAtom, boronAtom],
        )
        # create two mock geometries for calibrant samples
        sphere = Geometry(
            shape="Sphere",
            radius=1.0,
        )
        cylinder = Geometry(
            shape="Cylinder",
            radius=1.5,
            height=5.0,
        )
        # not create two differently shaped calibrant sample entries
        sphereSample = CalibrantSamples(
            name="fake sphere sample",
            unique_id="123fakest",
            geometry=sphere,
            material=fakeMaterial,
            crystallography=fakeXtal,
        )
        cylinderSample = CalibrantSamples(
            name="fake cylinder sample",
            unique_id="435elmst",
            geometry=cylinder,
            material=fakeMaterial,
            crystallography=fakeXtal,
        )

        # start the algorithm
        algo = Algo()
        algo.initialize()

        # chop and verify the spherical sample
        sample = algo.chopCalibrantSample(sphereSample)
        assert sample["geometry"]["Shape"] == "Sphere"
        assert sample["geometry"]["Radius"] == sphere.radius
        assert sample["geometry"]["Center"] == [0, 0, 0]
        assert sample["material"]["ChemicalFormula"] == fakeMaterial.chemicalFormula

        # chop and verify the cylindrical sample
        sample = {}  # clear the sample
        sample = algo.chopCalibrantSample(cylinderSample)
        assert sample["geometry"]["Shape"] == "Cylinder"
        assert sample["geometry"]["Radius"] == cylinder.radius
        assert sample["geometry"]["Height"] == cylinder.height
        assert sample["geometry"]["Center"] == [0, 0, 0]
        assert sample["geometry"]["Axis"] == [0, 1, 0]
        assert sample["material"]["ChemicalFormula"] == fakeMaterial.chemicalFormula

        self.calibrantSample = cylinderSample

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        goodOutputWSName = "_test_raw_vanadium_corr"

        algo = Algo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()
        if not hasattr(self, "calibrantSample"):
            self.test_chop_calibrant_sample()
        algo.setProperty("CalibrantSample", self.calibrantSample.json())
        assert algo.getProperty("CalibrantSample").value == self.calibrantSample.json()
        assert algo.getProperty("OutputWorkspace").value == "vanadiumrawcorr_out"
        algo.setProperty("OutputWorkspace", goodOutputWSName)
        assert algo.getProperty("OutputWorkspace").value == goodOutputWSName

    @mock.patch.object(Algo, "raidPantry", mockRaidPantry)
    @mock.patch.object(Algo, "restockPantry", mock.Mock(return_value=None))
    @mock.patch(TheAlgorithmManager)
    def test_execute(self, mockAlgorithmManager):
        """Test that the algorithm executes"""

        class mockGetIPTS(PythonAlgorithm):
            def PyInit(self):
                self.declareProperty("RunNumber", defaultValue="", direction=Direction.Input)
                self.declareProperty("Instrument", defaultValue="", direction=Direction.Input)
                self.declareProperty("Directory", defaultValue="nope!", direction=Direction.Output)
                self.setRethrows(True)

            def PyExec(self):
                self.setProperty("Directory", "nope!")

        def mockAlgorithmCreate(algoName: str):
            from mantid.api import AlgorithmManager

            if algoName == "GetIPTS":
                algo = mockGetIPTS()
                algo.initialize()
                return algo
            else:
                return AlgorithmManager.create(algoName)

        mockAlgorithmManager.create = mockAlgorithmCreate

        algo = Algo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        if not hasattr(self, "calibrantSample"):
            self.test_chop_calibrant_sample()
        algo.setProperty("CalibrantSample", self.calibrantSample.json())
        algo.setProperty("OutputWorkspace", "_test_workspace_rar_vanadium")
        assert algo.execute()


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
