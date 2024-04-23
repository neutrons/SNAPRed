# TODO: figure out how to run snapred algos like python functions

import unittest

import pytest
from mantid.simpleapi import (
    AddSampleLog,
    CloneWorkspace,
    CreateSampleWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    Plus,
    Rebin,
    mtd,
)
from snapred.backend.dao.ingredients import NormalizationIngredients as Ingredients

# needed to make mocked ingredients
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
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
from util.SculleryBoy import SculleryBoy

TheAlgorithmManager: str = "snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager"


class TestRawVanadiumCorrection(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        # self.fakeRunNumber = "555"
        # fakeIngredients = ReductionIngredients.parse_raw(Resource.read("/inputs/reduction/fake_file.json"))

        self.ingredients = SculleryBoy().prepNormalizationIngredients({})
        tof = self.ingredients.pixelGroup.timeOfFlight

        self.sample_proton_charge = 10.0

        # create some sample data
        self.backgroundWS = "_test_data_raw_vanadium_background"
        self.sampleWS = "_test_data_raw_vanadium"
        CreateSampleWorkspace(
            OutputWorkspace=self.backgroundWS,
            WorkspaceType="Event",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=tof.minimum,
            Xmax=tof.maximum,
            BinWidth=1,
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
            WorkspaceType="Event",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=70,Sigma=1",
            Xmin=tof.minimum,
            Xmax=tof.maximum,
            BinWidth=1,
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

        Rebin(
            InputWorkspace=self.sampleWS,
            Params=tof.params,
            PreserveEvents=True,
            OutputWorkspace=self.sampleWS,
            BinningMode="Logarithmic",
        )
        Rebin(
            InputWorkspace=self.backgroundWS,
            Params=tof.params,
            PreserveEvents=True,
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
        algo.chopIngredients(self.ingredients)
        assert algo.TOFPars == self.ingredients.pixelGroup.timeOfFlight.params
        assert algo.geometry == self.ingredients.calibrantSample.geometry
        assert algo.material == self.ingredients.calibrantSample.material
        assert algo.sampleShape == self.ingredients.calibrantSample.geometry.shape

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        algo = Algo()
        algo.initialize()

        # set the input workspaces
        algo.setProperty("InputWorkspace", self.sampleWS)
        assert algo.getPropertyValue("InputWorkspace") == self.sampleWS
        algo.setPropertyValue("BackgroundWorkspace", self.backgroundWS)
        assert algo.getPropertyValue("BackgroundWorkspace") == self.backgroundWS

        # set the ingredients
        algo.setProperty("Ingredients", self.ingredients.json())
        assert algo.getProperty("Ingredients").value == self.ingredients.json()

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

        algo = Algo()
        algo.initialize()
        algo.setProperty("Ingredients", self.ingredients.json())
        algo.chopIngredients(self.ingredients)
        algo.TOFPars = (2, 2, 4)
        algo.chopNeutronData(testWS, testWS)

        dataXnorm = []
        dataYnorm = []
        for x, y in zip(dataX, dataY):
            if x >= algo.TOFPars[0] and x <= algo.TOFPars[2]:
                dataXnorm.append(x)
                dataYnorm.append(y)

        dataXrebin = [sum(dataXnorm) / len(dataXnorm)]
        dataYrebin = [sum(dataYnorm[:-1])]

        ws = mtd[testWS]
        assert ws.readX(0) == dataXrebin
        assert ws.readY(0) == dataYrebin

        DeleteWorkspace(testWS)

    def test_execute(self):
        """Test that the algorithm executes"""
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.sampleWS)
        algo.setProperty("BackgroundWorkspace", self.backgroundWS)
        algo.setProperty("Ingredients", self.ingredients.json())
        algo.setProperty("OutputWorkspace", "_test_workspace_rar_vanadium")
        assert algo.execute()


# # old test from VanadiumFocussedReductionAlgorithm
# # retained here for possible future refactor + inclusion to tests
#     def test_execute(self):
#         vanAlgo = VanadiumFocussedReductionAlgorithm()
#         vanAlgo.initialize()
#         vanAlgo.mantidSnapper = mock.MagicMock()
#         vanAlgo.mantidSnapper.mtd = mock.MagicMock(side_effect={"diffraction_focused_vanadium": ["ws1", "ws2"]})
#         vanAlgo.setProperty("ReductionIngredients", self.reductionIngredients.json())
#         vanAlgo.setProperty("SmoothDataIngredients", self.smoothIngredients.json())
#         vanAlgo.execute()
#         wsGroupName = vanAlgo.getProperty("OutputWorkspaceGroup").value
#         assert wsGroupName == "diffraction_focused_vanadium"
#         expected_calls = [
#             call.LoadNexus,
#             call.CustomGroupWorkspace,
#             call.ConvertUnits,
#             call.DiffractionFocussing,
#             call.executeQueue,
#             call.mtd.__getitem__(),
#             call.mtd.__getitem__().getNames,
#             call.mtd.__getitem__().getNames().__iter__,
#             call.mtd.__getitem__().getNames().__len__,
#             call.WashDishes,
#             call.executeQueue,
#         ]
#         actual_calls = [call[0] for call in vanAlgo.mantidSnapper.mock_calls if call[0]]
#         # Assertions
#         assert actual_calls == [call[0] for call in expected_calls]


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
