import unittest
from typing import Any, Dict, List
from unittest import mock

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

        fakeFocusGroup = FocusGroup(
            name="natural",
            nHst=4,
            FWHM=[5, 5, 5, 5],
            dMin=[0.02, 0.05, 0.02, 0.03],
            dMax=[0.36, 0.41, 0.65, 0.485],
            dBin=[0.00086, 0.00096, 0.00130, 0.00117],
            definition=Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml"),
        )
        peakLists: Dict[int, List[Any]] = {
            3: [
                DetectorPeak.parse_obj({"position": {"value": 0.37, "minimum": 0.35, "maximum": 0.39}}),
                DetectorPeak.parse_obj({"position": {"value": 0.33, "minimum": 0.32, "maximum": 0.34}}),
            ],
            7: [
                DetectorPeak.parse_obj({"position": {"value": 0.57, "minimum": 0.54, "maximum": 0.62}}),
                DetectorPeak.parse_obj({"position": {"value": 0.51, "minimum": 0.49, "maximum": 0.53}}),
            ],
            2: [
                DetectorPeak.parse_obj({"position": {"value": 0.33, "minimum": 0.31, "maximum": 0.35}}),
                DetectorPeak.parse_obj({"position": {"value": 0.29, "minimum": 0.275, "maximum": 0.305}}),
            ],
            11: [
                DetectorPeak.parse_obj({"position": {"value": 0.43, "minimum": 0.41, "maximum": 0.47}}),
                DetectorPeak.parse_obj({"position": {"value": 0.39, "minimum": 0.37, "maximum": 0.405}}),
            ],
        }

        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[GroupPeakList(groupID=key, peaks=peakLists[key], maxfwhm=5) for key in peakLists.keys()],
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

        # prepare with test data in TOF
        CreateSampleWorkspace(
            OutputWorkspace=rawWSname,
            # WorkspaceType="Histogram",
            Function="Powder Diffraction",
            Xmin=TOFMin,
            Xmax=TOFMax,
            BinWidth=0.001,
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        Rebin(
            InputWorkspace=rawWSname,
            Params=(TOFMin, -0.001, TOFMax),
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
