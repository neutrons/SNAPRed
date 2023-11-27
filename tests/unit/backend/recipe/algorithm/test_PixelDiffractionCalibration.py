import json
import unittest

import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    mtd,
)
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState

# the algorithm to test
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import (
    PixelDiffractionCalibration as ThisAlgo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestPixelDiffractionCalibration(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.maxOffset = 2
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/diffcal/fakeInstrumentState.json"))

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("/inputs/diffcal/fakeFocusGroup.json"))
        fakeFocusGroup.definition = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")

        peakLists = {
            3: DetectorPeak.parse_obj({"position": {"value": 0.2, "minimum": 0.15, "maximum": 0.25}}),
            7: DetectorPeak.parse_obj({"position": {"value": 0.3, "minimum": 0.20, "maximum": 0.40}}),
            2: DetectorPeak.parse_obj({"position": {"value": 0.18, "minimum": 0.10, "maximum": 0.25}}),
            11: DetectorPeak.parse_obj({"position": {"value": 0.24, "minimum": 0.18, "maximum": 0.30}}),
        }
        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[
                GroupPeakList(groupID=gid, peaks=[peakLists[gid]], maxfwhm=5) for gid in peakLists.keys()
            ],
            calPath=Resource.getPath("outputs/calibration/"),
            convergenceThreshold=1.0,
            maxOffset=self.maxOffset,
        )

        self.fakeRawData = f"_test_pixelcal_{self.fakeRunNumber}"
        self.fakeGroupingWorkspace = f"_test_pixelcal_difc_{self.fakeRunNumber}"
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

        TOFMin = self.fakeIngredients.instrumentState.particleBounds.tof.minimum
        TOFMax = self.fakeIngredients.instrumentState.particleBounds.tof.maximum
        overallDMax = min(self.fakeIngredients.focusGroup.dMax)
        overallDMin = max(self.fakeIngredients.focusGroup.dMin)
        dBin = min([abs(d) for d in self.fakeIngredients.focusGroup.dBin])

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
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
            RewriteSpectraMap=True,
        )
        # also load the focus grouping workspace
        LoadDetectorsGroupingFile(
            InputFile=Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml"),
            InputWorkspace=rawWsName,
            OutputWorkspace=focusWSname,
        )
        # # rebin and convert for DSP, TOF
        focWS = mtd[focusWSname]
        allXmins = [0] * 16
        allXmaxs = [0] * 16
        allDelta = [0] * 16
        for i, gid in enumerate(focWS.getGroupIDs()):
            for detid in focWS.getDetectorIDsOfGroup(int(gid)):
                allXmins[detid] = self.fakeIngredients.focusGroup.dMin[i]
                allXmaxs[detid] = self.fakeIngredients.focusGroup.dMax[i]
                allDelta[detid] = self.fakeIngredients.focusGroup.dBin[i]
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

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(self.fakeIngredients)
        assert algo.runNumber == self.fakeRunNumber
        assert algo.overallDMin == min(self.fakeIngredients.focusGroup.dMin)
        assert algo.overallDMax == max(self.fakeIngredients.focusGroup.dMax)
        assert algo.dBin == max([abs(db) for db in self.fakeIngredients.focusGroup.dBin])

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()

    def test_execute(self):
        """Test that the algorithm executes"""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.execute()

        data = json.loads(algo.getProperty("data").value)
        x = data["medianOffset"]
        assert x is not None
        assert x != 0.0
        assert x > 0.0
        assert x <= self.maxOffset

    # patch to make the offsets of sample data non-zero
    def test_reexecution_and_convergence(self):
        """Test that the algorithm can run, and that it will converge to an answer"""

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.execute()

        data = json.loads(algo.getProperty("data").value)
        x = data["medianOffset"]
        assert x is not None
        assert x != 0.0
        assert x > 0.0
        assert x <= self.maxOffset

        # check that value converges
        numIter = 5
        allOffsets = [data["medianOffset"]]
        for i in range(numIter):
            algo.execute()
            data = json.loads(algo.getProperty("data").value)
            allOffsets.append(data["medianOffset"])
            assert allOffsets[-1] <= max(1.0e-4, allOffsets[-2])

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
