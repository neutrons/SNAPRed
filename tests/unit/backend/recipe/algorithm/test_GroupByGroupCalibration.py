import json
import random
import unittest
import unittest.mock as mock
from typing import Dict, List

import pytest
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.DiffractionCalibrationIngredients import DiffractionCalibrationIngredients
from snapred.backend.dao.GroupPeakList import GroupPeakList

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.algorithm.ConvertDiffCalLog import ConvertDiffCalLog  # noqa

# the algorithm to test
from snapred.backend.recipe.algorithm.GroupByGroupCalibration import (
    GroupByGroupCalibration as ThisAlgo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestGroupByGroupCalibration(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.fakeDBin = -abs(0.001)
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/calibration/sampleInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 10
        fakeInstrumentState.particleBounds.tof.maximum = 1000

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("/inputs/calibration/sampleFocusGroup.json"))
        ntest = fakeFocusGroup.nHst
        fakeFocusGroup.dBin = [-abs(self.fakeDBin)] * ntest
        fakeFocusGroup.dMax = [float(x) for x in range(100 * ntest, 101 * ntest)]
        fakeFocusGroup.dMin = [float(x) for x in range(ntest)]
        fakeFocusGroup.FWHM = [5 * random.random() for x in range(ntest)]
        fakeFocusGroup.definition = Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml")

        peakList = [
            DetectorPeak.parse_obj({"position": {"value": 2, "minimum": 1, "maximum": 3}}),
            DetectorPeak.parse_obj({"position": {"value": 5, "minimum": 4, "maximum": 6}}),
        ]

        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[GroupPeakList(groupID=3, peaks=peakList)],
        )

    def mockRetrieveFromPantry(algo):
        """Will cause algorithm to execute with sample data, instead of loading from file"""
        # prepare with test data
        algo.mantidSnapper.CreateSampleWorkspace(
            "Make fake data for testing",
            OutputWorkspace=algo.inputWStof,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=algo.TOFMin,
            Xmax=algo.TOFMax,
            BinWidth=0.1,
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )

        # manually setup the grouping workspace
        focusWSname = "_focusws_name_"
        algo.mantidSnapper.CreateWorkspace(
            "Create workspace to hold IDF",
            OutputWorkspace="idf",
            DataX=1,
            DataY=1,
        )
        algo.mantidSnapper.LoadInstrument(
            "Load a fake instrument for testing",
            Workspace="idf",
            Filename=Resource.getPath("inputs/calibration/fakeSNAPLite.xml"),
            MonitorList="-2--1",
            RewriteSpectraMap=False,
        )
        algo.mantidSnapper.LoadDetectorsGroupingFile(
            "Load a fake grouping  file for testing",
            InputFile=Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml"),
            InputWorkspace="idf",
            OutputWorkspace=focusWSname,
        )
        algo.mantidSnapper.executeQueue()
        focusWS = algo.mantidSnapper.mtd[focusWSname]
        assert "getGroupIDs" in dir(focusWS)
        algo.groupIDs = focusWS.getGroupIDs()
        algo.subgroupDetectorIDs: Dict[int, List[int]] = {}
        for groupID in algo.groupIDs:
            algo.subgroupDetectorIDs[groupID] = focusWS.getDetectorIDsOfGroup(int(groupID))
        algo.mantidSnapper.DeleteWorkspace("Clean up", Workspace=focusWSname)
        algo.mantidSnapper.executeQueue()

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(self.fakeIngredients)
        assert algo.runNumber == self.fakeRunNumber
        assert algo.TOFMin == self.fakeIngredients.instrumentState.particleBounds.tof.minimum
        assert algo.TOFMax == self.fakeIngredients.instrumentState.particleBounds.tof.maximum
        assert algo.overallDMin == max(self.fakeIngredients.focusGroup.dMin)
        assert algo.overallDMax == min(self.fakeIngredients.focusGroup.dMax)
        assert algo.dBin == -abs(self.fakeDBin)

    def test_init_properties(self):
        """Test that he properties of the algorithm can be initialized"""
        difcWS = f"_{self.fakeRunNumber}_difcs_test"
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("PreviousCalibrationTable", difcWS)
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()
        assert algo.getProperty("PreviousCalibrationTable").value == difcWS

    @mock.patch.object(ThisAlgo, "retrieveFromPantry", mockRetrieveFromPantry)
    def test_execute(self):
        """Test that the algorithm executes"""

        difcWS = f"_{self.fakeRunNumber}_difcs_test"

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
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
