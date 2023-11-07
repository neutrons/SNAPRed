import unittest

import pytest
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.algorithm.CalculateDiffCalTable import CalculateDiffCalTable

# the algorithm to test
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import (
    GroupDiffractionCalibration as ThisAlgo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestGroupDiffractionCalibration(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.fakeDBin = abs(0.001)
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("inputs/diffcal/fakeInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 10
        fakeInstrumentState.particleBounds.tof.maximum = 1000

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("inputs/diffcal/fakeFocusGroup.json"))
        fakeFocusGroup.definition = Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml")

        peakList3 = [
            DetectorPeak.parse_obj({"position": {"value": 2, "minimum": 1, "maximum": 3}}),
            DetectorPeak.parse_obj({"position": {"value": 5, "minimum": 4, "maximum": 6}}),
        ]
        group3 = GroupPeakList(groupID=3, peaks=peakList3)
        peakList7 = [
            DetectorPeak.parse_obj({"position": {"value": 3, "minimum": 2, "maximum": 4}}),
            DetectorPeak.parse_obj({"position": {"value": 6, "minimum": 5, "maximum": 7}}),
        ]
        group7 = GroupPeakList(groupID=7, peaks=peakList7)
        peakList2 = [
            DetectorPeak.parse_obj({"position": {"value": 3, "minimum": 2, "maximum": 4}}),
            DetectorPeak.parse_obj({"position": {"value": 6, "minimum": 5, "maximum": 7}}),
        ]
        group2 = GroupPeakList(groupID=2, peaks=peakList2)
        peakList11 = [
            DetectorPeak.parse_obj({"position": {"value": 3, "minimum": 2, "maximum": 4}}),
            DetectorPeak.parse_obj({"position": {"value": 6, "minimum": 5, "maximum": 7}}),
        ]
        group11 = GroupPeakList(groupID=11, peaks=peakList11)

        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[group3, group7, group2, group11],
            calPath=Resource.getPath("outputs/calibration/"),
            convergenceThreshold=1.0,
        )

        self.fakeRawData = f"_test_groupcal_{self.fakeRunNumber}"
        self.fakeGroupingWorkspace = f"_test_groupcal_difc_{self.fakeRunNumber}"
        self.makeFakeNeutronData(self.fakeRawData, self.fakeGroupingWorkspace)

    def makeFakeNeutronData(self, rawWSname, focusWSname):
        """Will cause algorithm to execute with sample data, instead of loading from file"""
        from mantid.simpleapi import (
            CreateSampleWorkspace,
            LoadDetectorsGroupingFile,
            LoadInstrument,
            Rebin,
        )

        TOFMin = self.fakeIngredients.instrumentState.particleBounds.tof.minimum
        TOFMax = self.fakeIngredients.instrumentState.particleBounds.tof.maximum
        instrConfig = self.fakeIngredients.instrumentState.instrumentConfig
        TOFBin = abs(instrConfig.delTOverT / instrConfig.NBins)
        TOFParams = (TOFMin, TOFBin, TOFMax)

        # prepare with test data in TOF
        midpoint = (TOFMax + TOFMin) / 2.0
        CreateSampleWorkspace(
            OutputWorkspace=rawWSname,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction=f"name=Gaussian,Height=10,PeakCentre={midpoint},Sigma={3*TOFBin}",
            Xmin=TOFMin,
            Xmax=TOFMax,
            BinWidth=TOFBin,
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        Rebin(
            InputWorkspace=rawWSname,
            Params=TOFParams,
            BinningMode="Logarithmic",
            OutputWorkspace=rawWSname,
        )
        # load the instrument and focus group
        LoadInstrument(
            Workspace=rawWSname,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )
        # also load the focus grouping workspace
        LoadDetectorsGroupingFile(
            InputFile=self.fakeIngredients.focusGroup.definition,
            InputWorkspace=rawWSname,
            OutputWorkspace=focusWSname,
        )

    def initDIFCTable(self, difcws: str):
        from mantid.simpleapi import (
            CreateWorkspace,
            DeleteWorkspace,
            LoadInstrument,
        )

        # load an instrument, requires a workspace to load into
        CreateWorkspace(
            OutputWorkspace="idf",
            DataX=1,
            DataY=1,
        )
        LoadInstrument(
            Workspace="idf",
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=False,
        )
        cc = CalculateDiffCalTable()
        cc.initialize()
        cc.setProperty("InputWorkspace", "idf")
        cc.setProperty("CalibrationTable", difcws)
        cc.setProperty("OffsetMode", "Signed")
        cc.setProperty("BinWidth", self.fakeDBin)
        cc.execute()
        DeleteWorkspace("idf")

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(self.fakeIngredients)
        assert algo.runNumber == self.fakeRunNumber
        assert algo.TOFMin == self.fakeIngredients.instrumentState.particleBounds.tof.minimum
        assert algo.TOFMax == self.fakeIngredients.instrumentState.particleBounds.tof.maximum
        assert algo.TOFBin == min([abs(db) for db in algo.dBin])

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        difcWS = f"_{self.fakeRunNumber}_difcs_test"
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("PreviousCalibrationTable", difcWS)
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()
        assert algo.getPropertyValue("PreviousCalibrationTable") == difcWS

    def test_execute(self):
        """Test that the algorithm executes"""
        # we need to create a DIFC table before we can run
        difcWS: str = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcWS)

        # now run the algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("FinalCalibrationTable", "_final_DIFc_table")
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
        assert algo.execute()

    # TODO more and more better tests of behavior


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
