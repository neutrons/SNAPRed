import json
import random
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

    def makeFakeNeutronData(self, algo):
        """Will cause algorithm to execute with sample data, instead of loading from file"""
        from mantid.simpleapi import (
            ConvertUnits,
            CreateSampleWorkspace,
            LoadInstrument,
            Rebin,
        )

        # prepare with test data, made in d-spacing
        midpoint = (algo.TOFMax + algo.TOFMin) / 2.0
        CreateSampleWorkspace(
            OutputWorkspace=algo.inputWStof,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction=f"name=Gaussian,Height=10,PeakCentre={midpoint},Sigma={3*algo.TOFBin}",
            Xmin=algo.TOFMin,
            Xmax=algo.TOFMax,
            BinWidth=algo.TOFBin,
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        Rebin(
            InputWorkspace=algo.inputWStof,
            Params=algo.TOFParams,
            BinningMode="Logarithmic",
            OutputWorkspace=algo.inputWStof,
        )
        # load the instrument and focus group
        LoadInstrument(
            Workspace=algo.inputWStof,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
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

    @mock.patch.object(ThisAlgo, "restockPantry", mock.Mock(return_value=None))
    def test_execute(self):
        """Test that the algorithm executes"""
        # we need to create a DIFC table before we can run
        difcWS: str = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcWS)

        # now run the algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        assert algo.execute()

    def test_save_load(self):
        """Test that files are correctly saved and loaded"""
        import os

        from mantid.simpleapi import (
            CompareWorkspaces,
            LoadDiffCal,
        )

        # create a simple test calibration table
        difcws = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcws)

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcws)
        algo.setProperty("FinalCalibrationTable", difcws)
        algo.outputFilename: str = Resource.getPath("outputs/calibration/fakeCalibrationTable.h5")
        algo.restockPantry()
        assert CompareWorkspaces(
            Workspace1=difcws,
            Workspace2=algo.getProperty("FinalCalibrationTable").value,
        )

        LoadDiffCal(
            InstrumentFilename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            Filename=algo.outputFilename,
            WorkspaceName="ReloadedCalibrationTable",
        )
        assert CompareWorkspaces(
            Workspace1="ReloadedCalibrationTable_cal",
            Workspace2=algo.getProperty("FinalCalibrationTable").value,
        )
        os.remove(algo.outputFilename)

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
