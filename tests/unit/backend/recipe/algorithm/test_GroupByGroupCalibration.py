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

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("inputs/calibration/sampleInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 10
        fakeInstrumentState.particleBounds.tof.maximum = 1000

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("inputs/calibration/sampleFocusGroup.json"))
        ntest = fakeFocusGroup.nHst
        fakeFocusGroup.dBin = [-abs(self.fakeDBin)] * ntest
        fakeFocusGroup.dMax = [float(x) for x in range(100 * ntest, 101 * ntest)]
        fakeFocusGroup.dMin = [float(x) for x in range(ntest)]
        fakeFocusGroup.FWHM = [5 * random.random() for x in range(ntest)]
        fakeFocusGroup.definition = Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml")

        peakList1 = [
            DetectorPeak.parse_obj({"position": {"value": 2, "minimum": 1, "maximum": 3}}),
            DetectorPeak.parse_obj({"position": {"value": 5, "minimum": 4, "maximum": 6}}),
        ]
        group1 = GroupPeakList(groupID=1, peaks=peakList1)
        peakList2 = [
            DetectorPeak.parse_obj({"position": {"value": 3, "minimum": 2, "maximum": 4}}),
            DetectorPeak.parse_obj({"position": {"value": 6, "minimum": 5, "maximum": 7}}),
        ]
        group2 = GroupPeakList(groupID=2, peaks=peakList2)

        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[group1, group2],
            calPath=Resource.getPath("outputs/calibration/"),
            threshold=1.0,
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
        algo.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=algo.inputWStof,
            Params=f"{algo.TOFMin},{-abs(algo.TOFBin)},{algo.TOFMax}",
            OutputWorkspace=algo.inputWStof,
        )
        # also find d-spacing data and rebin logarithmically
        inputWSdsp: str = f"_DSP_{algo.runNumber}"
        algo.mantidSnapper.ConvertUnits(
            "Convert units to d-spacing",
            InputWorkspace=algo.inputWStof,
            OutputWorkspace=inputWSdsp,
            Target="dSpacing",
        )
        algo.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=inputWSdsp,
            Params=f"{algo.overallDMin},{-abs(algo.dBin)},{algo.overallDMax}",
            OutputWorkspace=inputWSdsp,
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
            RewriteSpectraMap=False,
        )

        inputFilePath = Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml")
        algo.mantidSnapper.LoadGroupingDefinition(
            f"Loading a fake grouping  file {inputFilePath} for testing...",
            GroupingFilename=inputFilePath,
            InstrumentDonor="idf",
            OutputWorkspace=focusWSname,
        )

        # now diffraction focus
        diffractionfocusedWSdsp: str = f"_DSP_{algo.runNumber}_diffoc"
        algo.mantidSnapper.DiffractionFocussing(
            "Refocus with offset-corrections",
            InputWorkspace=inputWSdsp,
            GroupingWorkspace=focusWSname,
            OutputWorkspace=diffractionfocusedWSdsp,
        )
        algo.mantidSnapper.ConvertUnits(
            "Convert units to TOF",
            InputWorkspace=diffractionfocusedWSdsp,
            OutputWorkspace=algo.diffractionfocusedWStof,
            Target="TOF",
        )
        algo.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=algo.diffractionfocusedWStof,
            Params=f"{algo.TOFMin},{-abs(algo.TOFBin)},{algo.TOFMax}",
            OutputWorkspace=algo.diffractionfocusedWStof,
        )

        # clean up
        algo.mantidSnapper.DeleteWorkspace(
            "Clean up",
            Workspace="idf",
        )
        algo.mantidSnapper.DeleteWorkspace(
            "Clean up",
            Workspace=focusWSname,
        )
        algo.mantidSnapper.DeleteWorkspace(
            "Clean up d-spacing data",
            Workspace=inputWSdsp,
        )
        algo.mantidSnapper.DeleteWorkspace(
            "Clean up d-spaced diffraction focused data",
            Workspace=diffractionfocusedWSdsp,
        )
        algo.mantidSnapper.executeQueue()

    def initDIFCTable(self, difcws):
        from mantid.simpleapi import (
            CalculateDIFC,
            CreateEmptyTableWorkspace,
            CreateWorkspace,
            DeleteWorkspace,
            LoadInstrument,
            mtd,
        )

        # prepare initial diffraction calibration workspace
        CreateWorkspace(
            OutputWorkspace="idf",
            DataX=1,
            DataY=1,
        )
        LoadInstrument(
            Workspace="idf",
            Filename=Resource.getPath("inputs/calibration/fakeSNAPLite.xml"),
            RewriteSpectraMap=False,
        )
        CalculateDIFC(
            InputWorkspace="idf",
            OutputWorkspace="_tmp_difc_ws",
        )
        # convert the calibration workspace into a calibration table
        CreateEmptyTableWorkspace(
            OutputWorkspace=difcws,
        )

        tmpDifcWS = mtd["_tmp_difc_ws"]
        DIFCtable = mtd[difcws]
        DIFCtable.addColumn(type="int", name="detid", plottype=6)
        DIFCtable.addColumn(type="double", name="difc", plottype=6)
        DIFCtable.addColumn(type="double", name="difa", plottype=6)
        DIFCtable.addColumn(type="double", name="tzero", plottype=6)
        DIFCtable.addColumn(type="double", name="tofmin", plottype=6)
        detids = [int(x) for x in tmpDifcWS.extractX()]
        difcs = [float(x) for x in tmpDifcWS.extractY()]
        for detid, difc in zip(detids, difcs):
            DIFCtable.addRow(
                {
                    "detid": detid,
                    "difc": difc,
                    "difa": 0,
                    "tzero": 0,
                    "tofmin": 0,
                }
            )
        DeleteWorkspace("_tmp_difc_ws")
        DeleteWorkspace("idf")

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
    @mock.patch.object(ThisAlgo, "storeInPantry", mock.Mock(return_value=None))
    def test_execute(self):
        """Test that the algorithm executes"""
        difcWS = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcWS)

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
        assert algo.execute()

    def test_save_load(self):
        """Test that files are correctly saved and loaded"""
        import os

        from mantid.simpleapi import (
            CompareWorkspaces,
            CreateEmptyTableWorkspace,
            LoadDiffCal,
            mtd,
        )

        difcws = f"_{self.fakeRunNumber}_difcs_test"
        # create a simple test calibration table
        CreateEmptyTableWorkspace(
            OutputWorkspace=difcws,
        )
        DIFCtable = mtd[difcws]
        DIFCtable.addColumn(type="int", name="detid", plottype=6)
        DIFCtable.addColumn(type="double", name="difc", plottype=6)
        DIFCtable.addColumn(type="double", name="difa", plottype=6)
        DIFCtable.addColumn(type="double", name="tzero", plottype=6)
        DIFCtable.addColumn(type="double", name="tofmin", plottype=6)
        detids = range(10)
        difcs = [detid * 2.0 for detid in detids]
        for detid, difc in zip(detids, difcs):
            DIFCtable.addRow(
                {
                    "detid": detid,
                    "difc": difc,
                    "difa": 0,
                    "tzero": 0,
                    "tofmin": 0,
                }
            )

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcws)
        algo.setProperty("FinalCalibrationTable", difcws)
        algo.outputFilename: str = Resource.getPath("outputs/calibration/fakeCalibrationTable.h5")
        algo.storeInPantry()
        assert CompareWorkspaces(
            Workspace1=difcws,
            Workspace2=algo.getProperty("FinalCalibrationTable").value,
        )

        LoadDiffCal(
            InstrumentFilename=Resource.getPath("inputs/calibration/fakeSNAPLite.xml"),
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
