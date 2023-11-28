# TODO: figure out how to run snapred algos like python functions

import unittest

import pytest
from mantid.simpleapi import (
    AddSampleLog,
    CalculateDIFC,
    CloneWorkspace,
    CreateEmptyTableWorkspace,
    CreateSampleWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    DeleteWorkspaces,
    LoadInstrument,
    Plus,
    Rebin,
    mtd,
)
from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.CalibrantSample.Atom import Atom
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material

# the algorithm to test
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import (
    RawVanadiumCorrectionAlgorithm as Algo,  # noqa: E402
)
from snapred.meta.Config import Resource

TheAlgorithmManager: str = "snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager"


class TestRawVanadiumCorrection(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        self.fakeIngredients = Ingredients.parse_raw(Resource.read("/inputs/reduction/fake_file.json"))
        self.fakeIngredients.runConfig = fakeRunConfig
        TOFBinParams = (1, 1, 100)
        self.fakeIngredients.reductionState.stateConfig.tofMin = TOFBinParams[0]
        self.fakeIngredients.reductionState.stateConfig.tofBin = TOFBinParams[1]
        self.fakeIngredients.reductionState.stateConfig.tofMax = TOFBinParams[2]

        # create some nonsense material and crystallography
        fakeMaterial = Material(
            packingFraction=0.3,
            massDensity=1.0,
            chemicalFormula="V-B",
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
        cylinder = Geometry(
            shape="Cylinder",
            radius=1.5,
            height=5.0,
        )
        self.calibrantSample = CalibrantSamples(
            name="fake cylinder sample",
            unique_id="435elmst",
            geometry=cylinder,
            material=fakeMaterial,
            crystallography=fakeXtal,
        )

        self.sample_proton_charge = 10.0

        # create some sample data
        self.backgroundWS = "_test_data_raw_vanadium_background"
        self.sampleWS = "_test_data_raw_vanadium"
        CreateSampleWorkspace(
            OutputWorkspace=self.backgroundWS,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=TOFBinParams[0],
            Xmax=TOFBinParams[2],
            BinWidth=TOFBinParams[1],
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        # add proton charge for current normalization
        AddSampleLog(
            Workspace=self.backgroundWS,
            LogName="gd_prtn_chrg",
            LogText=f"{self.sample_proton_charge}",
            LogType="Number",
        )
        # load an instrument into sample data
        LoadInstrument(
            Workspace=self.backgroundWS,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
            InstrumentName="fakeSNAPLite",
            RewriteSpectraMap=False,
        )

        CloneWorkspace(
            InputWorkspace=self.backgroundWS,
            OutputWorkspace=self.sampleWS,
        )
        CreateSampleWorkspace(
            OutputWorkspace="_tmp_raw_vanadium",
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=70,Sigma=1",
            Xmin=TOFBinParams[0],
            Xmax=TOFBinParams[2],
            BinWidth=TOFBinParams[1],
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        Plus(
            LHSWorkspace="_tmp_raw_vanadium",
            RHSWorkspace=self.sampleWS,
            OutputWorkspace=self.sampleWS,
        )
        DeleteWorkspace("_tmp_raw_vanadium")

        self.difcWS = "_difc_table_raw_vanadium"
        ws = CalculateDIFC(
            InputWorkspace=self.sampleWS,
            OutputWorkspace=self.difcWS,
            OffsetMode="Signed",
            BinWidth=TOFBinParams[1],
        )
        difc = CreateEmptyTableWorkspace()
        difc.addColumn("int", "detid")
        difc.addColumn("double", "difc")
        difc.addColumn("double", "tzero")
        difc.addColumn("double", "difa")
        for i in range(ws.getNumberHistograms()):
            difc.addRow([i + 1, ws.readY(i)[0], 0.0, 0.0])
        DeleteWorkspace(self.difcWS)
        self.difcWS = difc.name()

        Rebin(
            InputWorkspace=self.sampleWS,
            Params=TOFBinParams,
            PreserveEvents=False,
            OutputWorkspace=self.sampleWS,
            BinningMode="Logarithmic",
        )
        Rebin(
            InputWorkspace=self.backgroundWS,
            Params=TOFBinParams,
            PreserveEvents=False,
            OutputWorkspace=self.backgroundWS,
            BinningMode="Logarithmic",
        )

    def tearDown(self) -> None:
        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        return super().tearDown()

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        algo = Algo()
        algo.initialize()
        algo.chopIngredients(self.fakeIngredients)
        assert algo.TOFPars[0] == self.fakeIngredients.reductionState.stateConfig.tofMin
        assert algo.TOFPars[1] == self.fakeIngredients.reductionState.stateConfig.tofBin
        assert algo.TOFPars[2] == self.fakeIngredients.reductionState.stateConfig.tofMax

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        algo = Algo()
        algo.initialize()

        # set the input workspaces
        algo.setProperty("InputWorkspace", self.sampleWS)
        print(algo.getPropertyValue("InputWorkspace"), self.sampleWS)
        assert algo.getPropertyValue("InputWorkspace") == self.sampleWS
        algo.setPropertyValue("BackgroundWorkspace", self.backgroundWS)
        assert algo.getPropertyValue("BackgroundWorkspace") == self.backgroundWS

        # set the ingredients
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()

        # set the calibrant sample
        algo.setProperty("CalibrantSample", self.calibrantSample.json())
        assert algo.getProperty("CalibrantSample").value == self.calibrantSample.json()

        # set the output workspace
        goodOutputWSName = "_test_raw_vanadium_corr"
        algo.setProperty("OutputWorkspace", goodOutputWSName)
        assert algo.getPropertyValue("OutputWorkspace") == goodOutputWSName

    def test_chop_neutron_data(self):
        # make an incredibly simple workspace, with incredibly simple data
        dataX = [1, 2, 3, 4, 5]
        dataY = [10, 110, 200, 110, 10]
        testWS = "_test_chop_neutron_data_raw_vanadium"
        ws = CreateWorkspace(
            OutputWorkspace=testWS,
            DataX=dataX,
            DataY=dataY,
            UnitX="TOF",
        )
        AddSampleLog(
            Workspace=testWS,
            LogName="gd_prtn_chrg",
            LogText=f"{self.sample_proton_charge}",
            LogType="Number",
        )

        difc = CreateEmptyTableWorkspace()
        difc.addColumn("int", "detid")
        difc.addColumn("double", "difc")
        difc.addColumn("double", "tzero")
        difc.addColumn("double", "difa")
        difc.addRow([0, 7000, 0, 0])
        difc = difc.name()

        algo = Algo()
        algo.initialize()
        algo.setProperty("CalibrationWorkspace", difc)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.chopIngredients(self.fakeIngredients)
        algo.TOFPars = (2, 2, 4)
        algo.chopNeutronData(testWS)

        dataXnorm = []
        dataYnorm = []
        for x, y in zip(dataX, dataY):
            if x >= algo.TOFPars[0] and x <= algo.TOFPars[2]:
                dataXnorm.append(x)
                dataYnorm.append(y / self.sample_proton_charge)

        print(dataXnorm, dataYnorm)
        dataXrebin = [sum(dataXnorm) / len(dataXnorm)]
        dataYrebin = [sum(dataYnorm[:-1])]

        ws = mtd[testWS]
        assert ws.readX(0) == dataXrebin
        assert ws.readY(0) == dataYrebin

        DeleteWorkspaces([testWS, difc])

    def test_execute(self):
        """Test that the algorithm executes"""
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.sampleWS)
        algo.setProperty("BackgroundWorkspace", self.backgroundWS)
        algo.setProperty("CalibrationWorkspace", self.difcWS)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
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
