import json
import random
import unittest
import unittest.mock as mock
from typing import Dict, List

import pytest
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState

# the algorithm to test
from snapred.backend.recipe.algorithm.CalculateOffsetDIFC import (
    CalculateOffsetDIFC as ThisAlgo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestCalculateOffsetDIFC(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.fakeDBin = 0.1
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/calibration/sampleInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 10
        fakeInstrumentState.particleBounds.tof.maximum = 1000

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("/inputs/calibration/sampleFocusGroup.json"))
        ntest = fakeFocusGroup.nHst
        fakeFocusGroup.dBin = [self.fakeDBin] * ntest
        fakeFocusGroup.dMax = [float(x / 100) + 5 for x in range(ntest)]
        fakeFocusGroup.dMin = [float(x / 100) for x in range(ntest)]
        fakeFocusGroup.FWHM = [5 * random.random() for x in range(ntest)]
        fakeFocusGroup.definition = Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml")

        peakList = [
            DetectorPeak.parse_obj({"position": {"value": 2, "minimum": 2, "maximum": 3}}),
            DetectorPeak.parse_obj({"position": {"value": 5, "minimum": 4, "maximum": 6}}),
        ]

        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[GroupPeakList(groupID=3, peaks=peakList)],
            calPath=Resource.getPath("outputs/calibration/"),
            threshold=1.0,
        )

    def mockRetrieveFromPantry(algo):
        """Will cause algorithm to execute with sample data, instead of loading from file"""
        # prepare with test data
        algo.mantidSnapper.CreateSampleWorkspace(
            "Make fake data for testing",
            OutputWorkspace=algo.inputWSdsp,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=1.2,Sigma=0.2",
            Xmin=algo.overallDMin,
            Xmax=algo.overallDMax,
            BinWidth=0.001,
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        algo.mantidSnapper.LoadInstrument(
            "Load a fake instrument for testing",
            Workspace=algo.inputWSdsp,
            Filename=Resource.getPath("inputs/calibration/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )
        algo.mantidSnapper.ChangeBinOffset(
            "Change bin offsets",
            InputWorkspace=algo.inputWSdsp,
            OutputWorkspace=algo.inputWSdsp,
            Offset=-0.7 * algo.overallDMin,
        )
        algo.mantidSnapper.RotateInstrumentComponent(
            "",
            Workspace=algo.inputWSdsp,
            ComponentName="bank1",
            Y=1.0,
            Angle=90.0,
        )
        algo.mantidSnapper.MoveInstrumentComponent(
            "",
            Workspace=algo.inputWSdsp,
            ComponentName="bank1",
            X=5.0,
            Y=-0.1,
            Z=0.1,
            RelativePosition=False,
        )

        # rebin and convert for DSP, TOF
        algo.convertUnitsAndRebin(algo.inputWSdsp, algo.inputWSdsp, "dSpacing")
        algo.convertUnitsAndRebin(algo.inputWSdsp, algo.inputWStof, "TOF")
        # manually setup the grouping workspace
        focusWSname = "_focusws_name_"
        inputFilePath = Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml")
        algo.mantidSnapper.LoadGroupingDefinition(
            f"Loading grouping file {inputFilePath}...",
            GroupingFilename=inputFilePath,
            InstrumentDonor=algo.inputWSdsp,
            OutputWorkspace=focusWSname,
        )
        algo.mantidSnapper.executeQueue()

        focusWS = algo.mantidSnapper.mtd[focusWSname]
        algo.groupIDs: List[int] = [int(x) for x in focusWS.getGroupIDs()]
        algo.subgroupWorkspaceIndices: Dict[int, List[int]] = {}
        algo.allDetectorIDs: List[int] = []
        for groupID in algo.groupIDs:
            groupDetectorIDs: List[int] = [int(x) for x in focusWS.getDetectorIDsOfGroup(groupID)]
            algo.allDetectorIDs.extend(groupDetectorIDs)
            algo.subgroupWorkspaceIndices[groupID] = focusWS.getIndicesFromDetectorIDs(groupDetectorIDs)
        algo.mantidSnapper.DeleteWorkspace(
            "Delete temp",
            Workspace=focusWSname,
        )
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
        assert algo.dBin == self.fakeDBin

    def test_init_properties(self):
        """Test that he properties of the algorithm can be initialized"""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()

    @mock.patch.object(ThisAlgo, "retrieveFromPantry", mockRetrieveFromPantry)
    def test_execute(self):
        """Test that the algorithm executes"""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.execute()

        data = json.loads(algo.getProperty("data").value)
        assert data["meanOffset"] is not None
        assert data["meanOffset"] != 0
        assert data["meanOffset"] > 0
        assert data["meanOffset"] <= 2

    # patch to make the offsets of sample data non-zero
    @mock.patch.object(ThisAlgo, "retrieveFromPantry", mockRetrieveFromPantry)
    # @mock.patch.object(ThisAlgo, "getRefID", lambda self, x: int(min(x)))  # noqa
    def test_reexecution_and_convergence(self):
        """Test that the algorithm can run, and that it will converge to an answer"""

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.execute()
        assert algo.reexecute()

        data = json.loads(algo.getProperty("data").value)
        assert data["medianOffset"] is not None
        assert data["medianOffset"] != 0
        assert data["medianOffset"] <= 2

        # check that value converges
        numIter = 5
        allOffsets = [data["medianOffset"]]
        for i in range(numIter):
            algo.reexecute()
            data = json.loads(algo.getProperty("data").value)
            allOffsets.append(data["medianOffset"])
            assert allOffsets[-1] <= max(1.0e-14, allOffsets[-2])

    @mock.patch.object(ThisAlgo, "retrieveFromPantry", mockRetrieveFromPantry)
    def test_init_difc_table(self):
        from mantid.simpleapi import mtd

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.chopIngredients(self.fakeIngredients)
        algo.retrieveFromPantry()
        algo.initDIFCTable()
        difcTable = mtd[algo.difcWS]
        for i, row in enumerate(difcTable.column("detid")):
            assert row == i
        for difc in difcTable.column("difc"):
            print(f"{difc},")
        # difc_refs = [ # these come from past runs of tests
        #     3277.315974986059,
        #     3277.3053335708523,
        #     3277.5727141552215,
        #     3277.562072615724,
        #     752.1782796557441,
        #     752.178649745504,
        #     752.9184419740511,
        #     752.9188116767137,
        #     752.1782796557441,
        #     752.9184419740511,
        #     752.178649745504,
        #     752.9188116767137,
        #     1055.4666684141987,
        #     1055.9783785851703,
        #     1055.9783785851703,
        #     1056.4898104685458,
        # ]
        # for difc, difc_ref in zip(difcTable.column("difc"), difc_refs):
        #     assert abs(difc - difc_ref) < 1.0e-3

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
            Filename=Resource.getPath("inputs/calibration/fakeSNAPLite.xml"),
            InstrumentName="fakeSNAPLite",
            RewriteSpectraMap=False,
        )
        SaveNexus(
            InputWorkspace=fakeDataWorkspace,
            Filename=fakeNexusFile,
        )
        algo.rawDataPath = fakeNexusFile
        algo.retrieveFromPantry()
        os.remove(fakeNexusFile)
        assert CompareWorkspaces(
            Workspace1=algo.inputWStof,
            Workspace2=fakeDataWorkspace,
        )
        assert len(algo.groupIDs) > 0
        assert algo.groupIDs == list(algo.subgroupWorkspaceIndices.keys())


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
