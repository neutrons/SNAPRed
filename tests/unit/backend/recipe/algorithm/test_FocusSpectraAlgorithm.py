import unittest

from mantid.simpleapi import (
    DeleteWorkspace,
    mtd,
)
from mantid.testing import assert_almost_equal
from util.dao import DAOFactory

# the algorithm to test
from snapred.backend.recipe.algorithm.FocusSpectraAlgorithm import (
    FocusSpectraAlgorithm as ThisAlgo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestFocusSpectra(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.maxOffset = 2
        self.fakeRunNumber = "555"

        self.pixelGroup = DAOFactory.synthetic_pixel_group.copy()

        self.fakeRawData = f"_test_focusSpectra_{self.fakeRunNumber}"
        self.fakeGroupingWorkspace = f"_test_focusSpectra_difc_{self.fakeRunNumber}"
        self.makeFakeNeutronData(self.fakeRawData, self.fakeGroupingWorkspace)

    def tearDown(self) -> None:
        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        return super().tearDown()

    def makeFakeNeutronData(self, rawWsName: str, focusWSname: str) -> None:
        """Create sample data for test runs"""
        from mantid.simpleapi import (
            ConvertUnits,
            CreateSampleWorkspace,
            LoadDetectorsGroupingFile,
            LoadInstrument,
            Rebin,
            RebinRagged,
        )

        TOFMin = self.pixelGroup.timeOfFlight.minimum
        TOFMax = self.pixelGroup.timeOfFlight.maximum

        dMin = self.pixelGroup.dMin()
        dMax = self.pixelGroup.dMax()
        DBin = self.pixelGroup.dBin()
        overallDMax = max(dMax)
        overallDMin = min(dMin)
        dBin = min([abs(d) for d in DBin])

        # prepare with test data
        midpoint = (overallDMin + overallDMax) / 2.0
        CreateSampleWorkspace(
            OutputWorkspace=rawWsName,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction=f"name=Gaussian,Height=10,PeakCentre={midpoint},Sigma={midpoint/10}",
            Xmin=overallDMin,
            Xmax=overallDMax,
            BinWidth=abs(dBin),
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )
        LoadInstrument(
            Workspace=rawWsName,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
            RewriteSpectraMap=True,
        )
        # also load the focus grouping workspace
        LoadDetectorsGroupingFile(
            InputFile=Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml"),
            InputWorkspace=rawWsName,
            OutputWorkspace=focusWSname,
        )
        # # rebin and convert for DSP, TOF
        allXmins, allXmaxs, allDelta = self.getRebinRaggedParams(focusWSname)

        RebinRagged(
            InputWorkspace=rawWsName,
            OutputWorkspace=rawWsName,
            XMin=allXmins,
            XMax=allXmaxs,
            Delta=allDelta,
        )
        ConvertUnits(
            InputWorkspace=rawWsName,
            OutputWorkspace=rawWsName,
            Target="TOF",
        )
        Rebin(
            InputWorkspace=rawWsName,
            OutputWorkspace=rawWsName,
            Params=(TOFMin, dBin, TOFMax),
            BinningMode="Logarithmic",
        )

    def getRebinRaggedParams(self, focusWSname: str):
        focWS = mtd[focusWSname]
        dMin = self.pixelGroup.dMin()
        dMax = self.pixelGroup.dMax()
        DBin = self.pixelGroup.dBin()
        allXmins = [0] * 16
        allXmaxs = [0] * 16
        allDelta = [0] * 16
        groupIDs = self.pixelGroup.groupIDs
        for i, gid in enumerate(groupIDs):
            for detid in focWS.getDetectorIDsOfGroup(int(gid)):
                allXmins[detid] = dMin[i]
                allXmaxs[detid] = dMax[i]
                allDelta[detid] = DBin[i]
        return allXmins, allXmaxs, allDelta

    def test_happy_path(self):
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("PixelGroup", self.pixelGroup.json())
        algo.setProperty("OutputWorkspace", "_test_focusSpectra_output")
        assert algo.execute()
        # assert outputworkspace is focussed correctly
        outputWs = algo.getProperty("OutputWorkspace").valueAsStr
        # write outputWs to file
        from mantid.simpleapi import Load, RebinRagged
        # assert that outputWs is focussed correctly

        RebinRagged(InputWorkspace=outputWs, OutputWorkspace=outputWs, XMin=2000, XMax=14500, Delta=1)
        Load(Resource.getPath(f"/outputs/focus_spectra/{outputWs}.nxs"), OutputWorkspace=outputWs + "_loaded")
        assert mtd[outputWs].getNumberHistograms() == mtd[outputWs + "_loaded"].getNumberHistograms()
        # check block size
        assert mtd[outputWs].blocksize() == mtd[outputWs + "_loaded"].blocksize()
        assert_almost_equal(
            Workspace1=outputWs,
            Workspace2=outputWs + "_loaded",
            CheckAllData=True,
            rtol=1.0e-10,
        )

    def test_chopIngredients(self):
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(self.pixelGroup)
        assert algo.dMin == self.pixelGroup.dMin()
        assert algo.dMax == self.pixelGroup.dMax()
        assert algo.dBin == self.pixelGroup.dBin()

    def test_unbagGroceries(self):
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("PixelGroup", self.pixelGroup.json())
        algo.setProperty("OutputWorkspace", "_test_focusSpectra_output")
        algo.unbagGroceries()
        assert algo.inputWSName == self.fakeRawData
        assert algo.groupingWSName == self.fakeGroupingWorkspace
        assert algo.outputWSName == "_test_focusSpectra_output"

    def test_validateInputs(self):
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("PixelGroup", self.pixelGroup.json())
        algo.setProperty("OutputWorkspace", "_test_focusSpectra_output")
        errors = algo.validateInputs()
        assert errors == {}

    def test_failedValidation_groupingWsIsNotGroupingWorkspace(self):
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Groupingworkspace", self.fakeRawData)
        algo.setProperty("PixelGroup", self.pixelGroup.json())
        algo.setProperty("OutputWorkspace", "_test_focusSpectra_output")
        errors = algo.validateInputs()
        assert errors.get("GroupingWorkspace") is not None

    def test_failedValidation_inputGroupingIdsMismatch(self):
        from mantid.simpleapi import (
            CreateSampleWorkspace,
        )

        dMin = self.pixelGroup.dMin()
        dMax = self.pixelGroup.dMax()
        DBin = self.pixelGroup.dBin()
        overallDMax = max(dMax)
        overallDMin = min(dMin)
        dBin = min([abs(d) for d in DBin])

        # prepare with test data
        midpoint = (overallDMin + overallDMax) / 2.0
        CreateSampleWorkspace(
            OutputWorkspace=self.fakeRawData,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction=f"name=Gaussian,Height=10,PeakCentre={midpoint},Sigma={midpoint/10}",
            Xmin=overallDMin,
            Xmax=overallDMax,
            BinWidth=abs(dBin),
            XUnit="TOF",
            NumBanks=5,  # must wrong for test
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("PixelGroup", self.pixelGroup.json())
        algo.setProperty("OutputWorkspace", "_test_focusSpectra_output")
        errors = algo.validateInputs()
        assert errors.get("InputWorkspace") is not None
