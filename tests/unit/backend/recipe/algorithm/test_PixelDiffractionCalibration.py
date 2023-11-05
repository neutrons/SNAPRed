import json
import unittest
import unittest.mock as mock

import pytest
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
        self.maxOffset = 200
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/diffcal/fakeInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 10
        fakeInstrumentState.particleBounds.tof.maximum = 1000

        fakeFocusGroup = FocusGroup(
            name = "natural",
            nHst = 4,
            FWHM = {3:5, 7:5, 2:5, 11:5},
            dSpaceParams = {
                3: {"minimum": 0.05, "binWidth": 0.00086, "maximum": 0.35, "binMode": "Logarithmic"},
                7: {"minimum": 0.10, "binWidth": 0.00096, "maximum": 0.50, "binMode": "Logarithmic"},
                2: {"minimum": 0.05, "binWidth": 0.00130, "maximum": 0.30, "binMode": "Logarithmic"},
                11: {"minimum": 0.10, "binWidth": 0.00117, "maximum": 0.40, "binMode": "Logarithmic"},
            },
            definition = Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml"),
        )

        peakList3 = [
            DetectorPeak.parse_obj({"position": {"value": 0.2, "minimum": 0.15, "maximum": 0.25}}),
        ]
        group3 = GroupPeakList(groupID=3, peaks=peakList3, maxfwhm = 5)
        peakList7 = [
            DetectorPeak.parse_obj({"position": {"value": 0.3, "minimum": 0.20, "maximum": 0.40}}),
        ]
        group7 = GroupPeakList(groupID=7, peaks=peakList7, maxfwhm = 5)
        peakList2 = [
            DetectorPeak.parse_obj({"position": {"value": 0.18, "minimum": 0.10, "maximum": 0.25}}),
        ]
        group2 = GroupPeakList(groupID=2, peaks=peakList2, maxfwhm = 5)
        peakList11 = [
            DetectorPeak.parse_obj({"position": {"value": 0.24, "minimum": 0.18, "maximum": 0.30}}),
        ]
        group11 = GroupPeakList(groupID=11, peaks=peakList11, maxfwhm = 5)

        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[group3, group7, group2, group11],
            calPath=Resource.getPath("outputs/calibration/"),
            convergenceThreshold=1.0,
        )

    def makeFakeNeutronData(self, algo):
        """Will cause algorithm to execute with sample data, instead of loading from file"""
        from mantid.simpleapi import (
            CreateSampleWorkspace,
            LoadInstrument,
        )

        # prepare with test data
        CreateSampleWorkspace(
            OutputWorkspace=algo.inputWSdsp,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=1.2,Sigma=0.2",
            Xmin=algo.overallDMin,
            Xmax=algo.overallDMax,
            BinWidth=algo.dBin,
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )
        LoadInstrument(
            Workspace=algo.inputWSdsp,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )
        # # rebin and convert for DSP, TOF
        algo.convertUnitsAndRebin(algo.inputWSdsp, algo.inputWSdsp, "dSpacing")
        algo.convertUnitsAndRebin(algo.inputWSdsp, algo.inputWStof, "TOF")

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
        assert algo.dBin == min([abs(db) for db in self.fakeIngredients.focusGroup.dBin])
        assert algo.TOFBin == algo.dBin

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
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("MaxOFfset", self.maxOffset)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
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
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("MaxOFfset", self.maxOffset)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
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

    def test_retrieve_from_pantry(self):
        import os

        from mantid.simpleapi import (
            CompareWorkspaces,
            CreateSampleWorkspace,
            LoadInstrument,
            SaveNexus,
        )

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.chopIngredients(self.fakeIngredients)

        # create a fake nexus file to load
        fakeDataWorkspace = "_fake_sample_data"
        fakeNexusFile = Resource.getPath("outputs/calibration/testInputData.nxs")
        CreateSampleWorkspace(
            OutputWorkspace=fakeDataWorkspace,
            WorkspaceType="Event",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=algo.TOFMin,
            Xmax=algo.TOFMax,
            BinWidth=0.1,
            XUnit="TOF",
            NumMonitors=1,
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        LoadInstrument(
            Workspace=fakeDataWorkspace,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            InstrumentName="fakeSNAPLite",
            RewriteSpectraMap=False,
        )
        SaveNexus(
            InputWorkspace=fakeDataWorkspace,
            Filename=fakeNexusFile,
        )
        algo.rawDataPath = fakeNexusFile
        algo.raidPantry()
        os.remove(fakeNexusFile)
        assert CompareWorkspaces(
            Workspace1=algo.inputWStof,
            Workspace2=fakeDataWorkspace,
        )
        assert len(algo.subgroupIDs) > 0
        assert algo.subgroupIDs == list(algo.subgroupWorkspaceIndices.keys())


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
