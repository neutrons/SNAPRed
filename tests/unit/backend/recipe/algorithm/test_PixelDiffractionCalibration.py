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
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/diffcal/fakeInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 10
        fakeInstrumentState.particleBounds.tof.maximum = 1000

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("/inputs/diffcal/fakeFocusGroup.json"))
        fakeFocusGroup.definition = Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml")

        peakList = [
            DetectorPeak.parse_obj({"position": {"value": 1.5, "minimum": 1, "maximum": 2}}),
            DetectorPeak.parse_obj({"position": {"value": 3.5, "minimum": 3, "maximum": 4}}),
        ]

        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[
                GroupPeakList(groupID=3, peaks=peakList, maxfwhm=0.01),
                GroupPeakList(groupID=7, peaks=peakList, maxfwhm=0.02),
                GroupPeakList(groupID=2, peaks=peakList, maxfwhm=0.03),
                GroupPeakList(groupID=11, peaks=peakList, maxfwhm=0.04),
            ],
            calPath=Resource.getPath("outputs/calibration/"),
            convergenceThreshold=1.0,
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
            ChangeBinOffset,
            ConvertUnits,
            CreateSampleWorkspace,
            LoadDetectorsGroupingFile,
            LoadInstrument,
            MoveInstrumentComponent,
            Rebin,
            RotateInstrumentComponent,
        )

        TOFMin = self.fakeIngredients.instrumentState.particleBounds.tof.minimum
        TOFMax = self.fakeIngredients.instrumentState.particleBounds.tof.maximum
        dParams = [dp.params for dp in self.fakeIngredients.focusGroup.dSpaceParams.values()]
        overallDMax = max([d[2] for d in dParams])
        overallDMin = min([d[0] for d in dParams])
        dBin = min([d[1] for d in dParams])

        # prepare with test data
        CreateSampleWorkspace(
            OutputWorkspace=rawWsName,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=1.2,Sigma=0.2",
            Xmin=overallDMin,
            Xmax=overallDMax,
            BinWidth=dBin,
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )
        LoadInstrument(
            Workspace=rawWsName,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )
        # also load the focus grouping workspace
        LoadDetectorsGroupingFile(
            InputFile=Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml"),
            InputWorkspace=rawWsName,
            OutputWorkspace=focusWSname,
        )
        # the below are meant to de-align the pixels so an offset correction is needed
        ChangeBinOffset(
            InputWorkspace=rawWsName,
            OutputWorkspace=rawWsName,
            Offset=-0.7 * TOFMin,
        )
        RotateInstrumentComponent(
            Workspace=rawWsName,
            ComponentName="bank1",
            Y=1.0,
            Angle=90.0,
        )
        MoveInstrumentComponent(
            Workspace=rawWsName,
            ComponentName="bank1",  # TODO: changing to bank2 breaks Rebin
            X=5.0,
            Y=-0.1,
            Z=0.1,
            RelativePosition=False,
        )
        Rebin(
            Inputworkspace=rawWsName,
            OutputWorkspace=rawWsName,
            Params=(overallDMin, dBin, overallDMax),
            BinningMode="Logarithmic",
        )
        ConvertUnits(
            InputWorkspace=rawWsName,
            OutputWorkspace=rawWsName,
            Target="TOF",
        )
        Rebin(
            Inputworkspace=rawWsName,
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
        assert algo.TOFMin == self.fakeIngredients.instrumentState.particleBounds.tof.minimum
        assert algo.TOFMax == self.fakeIngredients.instrumentState.particleBounds.tof.maximum
        # instrConfig = self.fakeIngredients.instrumentState.instrumentConfig
        # assert algo.TOFBin == instrConfig.delTOverT / instrConfig.NBins
        dsp = list(self.fakeIngredients.focusGroup.dSpaceParams.values())
        assert algo.overallDMin == max([d.minimum for d in dsp])
        assert algo.overallDMax == min([d.maximum for d in dsp])
        assert algo.dBin == min([abs(d.binWidth) for d in dsp])

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
        assert x <= 2.0

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
        assert data["medianOffset"] is not None
        assert data["medianOffset"] != 0
        assert data["medianOffset"] > 0
        assert data["medianOffset"] <= 2

        # check that value converges
        numIter = 5
        allOffsets = [data["medianOffset"]]
        for i in range(numIter):
            algo.execute()
            data = json.loads(algo.getProperty("data").value)
            allOffsets.append(data["medianOffset"])
            assert allOffsets[-1] <= max(1.0e-12, allOffsets[-2])

    def test_chop_neutron_data(self):
        # TODO: test it
        pass


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
