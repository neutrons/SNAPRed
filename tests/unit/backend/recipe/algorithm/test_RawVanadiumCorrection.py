# TODO: figure out how to run snapred algos like python functions

import unittest

from mantid.simpleapi import (
    AddSampleLog,
    CreateSampleWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    Plus,
    Rebin,
    mtd,
)
from numpy import argmax

# needed to make mocked ingredients
# the algorithm to test
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import (
    RawVanadiumCorrectionAlgorithm as Algo,  # noqa: E402
)
from snapred.meta.Config import Resource
from util.SculleryBoy import SculleryBoy


class TestRawVanadiumCorrection(unittest.TestCase):
    def make_workspace_with_peak_at(self, ws, peak):
        # prepare the "background" data
        CreateSampleWorkspace(
            OutputWorkspace=ws,
            Function="User Defined",
            UserDefinedFunction=f"name=Gaussian,Height=10,PeakCentre={peak},Sigma=1",
            Xmin=self.tof.minimum,
            Xmax=self.tof.maximum,
            BinWidth=1,
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=False,
        )
        # load an instrument into sample data
        LoadInstrument(
            Workspace=ws,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
            InstrumentName="fakeSNAPLite",
            RewriteSpectraMap=True,
        )
        AddSampleLog(
            Workspace=ws,
            LogName="proton_charge",
            LogText=f"{self.sample_proton_charge}",
            LogType="Number Series",
        )
        # rebin!
        Rebin(
            InputWorkspace=ws,
            Params=self.tof.params,
            PreserveEvents=False,
            OutputWorkspace=ws,
            BinningMode="Logarithmic",
        )

    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""

        # Prepare the initial signal.
        # Add a "background" to it.
        # Run through the algo.
        # Output should be the "raw data", but scaled by proton charge
        self.signalWS = mtd.unique_name(prefix="_signal_")
        self.backgroundWS = mtd.unique_name(prefix="_bkgr_")
        self.sampleWS = mtd.unique_name(prefix="_sample_")

        self.ingredients = SculleryBoy().prepNormalizationIngredients({})
        TOFBinParams = (1, 0.01, 100)
        self.ingredients.pixelGroup.timeOfFlight.minimum = TOFBinParams[0]
        self.ingredients.pixelGroup.timeOfFlight.binWidth = TOFBinParams[1]
        self.ingredients.pixelGroup.timeOfFlight.maximum = TOFBinParams[2]
        self.tof = self.ingredients.pixelGroup.timeOfFlight
        self.sample_proton_charge = 10.0

        # prepare the "signal" data
        self.make_workspace_with_peak_at(self.signalWS, 70)

        # prepare the "background" data
        self.make_workspace_with_peak_at(self.backgroundWS, 30)

        # prepare the "sample" data, by combining both
        Plus(
            LHSWorkspace=self.signalWS,
            RHSWorkspace=self.backgroundWS,
            OutputWorkspace=self.sampleWS,
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

        outputWS = mtd.unique_name(prefix="_raw_out_")
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.sampleWS)
        algo.setProperty("BackgroundWorkspace", self.backgroundWS)
        algo.setProperty("Ingredients", self.ingredients.json())
        algo.setProperty("OutputWorkspace", outputWS)
        assert algo.execute()

        # the output workspace cannot be negative
        ws = mtd[outputWS]
        for n in range(ws.getNumberHistograms()):
            for y in ws.readY(n):
                assert y >= 0.0

        # the peak of the output is in same spot as signal
        for n in range(ws.getNumberHistograms()):
            signal = argmax(mtd[self.signalWS].readY(n))
            output = argmax(ws.readY(n))
            assert signal == output


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
