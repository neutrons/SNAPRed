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
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
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
            DetectorPeak.parse_obj({"position": {"value": 0.37, "minimum": 0.35, "maximum": 0.39}}),
            DetectorPeak.parse_obj({"position": {"value": 0.33, "minimum": 0.32, "maximum": 0.34}}),
        ]
        group3 = GroupPeakList(groupID=3, peaks=peakList3, maxfwhm = 5)
        peakList7 = [
            DetectorPeak.parse_obj({"position": {"value": 0.57, "minimum": 0.54, "maximum": 0.62}}),
            DetectorPeak.parse_obj({"position": {"value": 0.51, "minimum": 0.49, "maximum": 0.53}}),
        ]
        group7 = GroupPeakList(groupID=7, peaks=peakList7, maxfwhm = 5)
        peakList2 = [
            DetectorPeak.parse_obj({"position": {"value": 0.33, "minimum": 0.31, "maximum": 0.35}}),
            DetectorPeak.parse_obj({"position": {"value": 0.29, "minimum": 0.275, "maximum": 0.305}}),
        ]
        group2 = GroupPeakList(groupID=2, peaks=peakList2, maxfwhm = 5)
        peakList11 = [
            DetectorPeak.parse_obj({"position": {"value": 0.43, "minimum": 0.41, "maximum": 0.47}}),
            DetectorPeak.parse_obj({"position": {"value": 0.39, "minimum": 0.37, "maximum": 0.405}}),
        ]
        group11 = GroupPeakList(groupID=11, peaks=peakList11, maxfwhm = 5)

        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[group2, group3, group7, group11],
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
        
        TOFMin = 10
        TOFMax = 1000
        TOFBin = 0.001
        
        # prepare with test data, made in d-spacing
        midpoint = (TOFMax + TOFMin) / 2.0
        CreateSampleWorkspace(
            OutputWorkspace=algo.inputWStof,
            # WorkspaceType="Histogram",
            Function="Powder Diffraction",
            Xmin=TOFMin,
            Xmax=TOFMax,
            BinWidth=TOFBin,
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        LoadInstrument(
            Workspace=algo.inputWStof,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )
        """
        Rebin(
            InputWorkspace=algo.inputWStof,
            Params=(TOFMin, TOFBin, TOFMax),
            BinningMode="Logarithmic",
            OutputWorkspace=algo.inputWStof,
        )
        """

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
    
    def makeFakeInputMaskAndGrouping(self, algo, maskWSName, groupingWSName = None):
        """Create input mask compatible with the sample data:
           -- return group-to-detid map """
        # Assumes algo.diffractionfocusedWStof has already been created
        from mantid.dataobjects import MaskWorkspace
        from mantid.simpleapi import (
            mtd,
            WorkspaceFactory,
            LoadInstrument,
            ClearMaskFlag,
            ExtractMask,
        )
        
        maskWSName = algo.getPropertyValue("MaskWorkspace")
        
        grouping = None
        if groupingWSName is not None:
            groupingWS = None
            if not mtd.doesExist(groupingWSName):
                lg = LoadGroupingDefinition()
                lg.initialize()
                lg.setProperty('GroupingFilename', algo.groupingFile)
                lg.setProperty('InstrumentDonor', algo.inputWStof)
                lg.setProperty('OutputWorkspace', groupingWSName)
                lg.execute()
            groupingWS = mtd[groupingWSName]
            grouping: Dict(int, List(int)) = {}
            for nd in range(groupingWS.getNumberHistograms()):
                groupID = groupingWS.readY(nd)[0]
                if groupID in grouping:
                    grouping[groupID].append(nd)
                else:
                    grouping[groupID] = [nd]
        
        inputWS = mtd[algo.inputWStof]
        inst = inputWS.getInstrument()
        mask = WorkspaceFactory.create("SpecialWorkspace2D", NVectors=inst.getNumberDetectors(),
                                        XLength=1, YLength=1)
        mtd[maskWSName] = mask
        LoadInstrument(
            Workspace=maskWSName,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )
        # output_workspace is converted to a MaskWorkspace
        ExtractMask(InputWorkspace=maskWSName, OutputWorkspace=maskWSName)
        assert isinstance(mtd[maskWSName], MaskWorkspace)
        return grouping
       
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
        from mantid.simpleapi import mtd
        # we need to create a DIFC table before we can run
        difcWS: str = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcWS)

        # now run the algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("MaskWorkspace", f"_test_mask_{self.fakeRunNumber}")
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        self.makeFakeInputMaskAndGrouping(algo, algo.getPropertyValue('MaskWorkspace'))
        assert algo.execute()
        mask = mtd[f"_test_mask_{self.fakeRunNumber}"]
        assert mask.getNumberMasked() == 0

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

    def test_mask_is_created(self):
        """Test that a mask workspace is created if it doesn't already exist:
           -- this method also verifies that none of the spectra in the synthetic data will be masked.
        """
        from mantid.simpleapi import mtd
        # we need to create a DIFC table before we can run
        difcWS: str = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcWS)
        
        # Ensure that the mask workspace doesn't already exist
        maskWSName = f"_test_mask_{self.fakeRunNumber}"
        mtd.remove(maskWSName);
        assert maskWSName not in mtd
        
        # now run the algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        algo.execute()
        assert maskWSName in mtd
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == 0        
        
    def test_existing_mask_is_used(self):
        """Test that an existing mask workspace is not overwritten"""
        import uuid
        from mantid.simpleapi import mtd
        
        # we need to create a DIFC table before we can run
        difcWS: str = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcWS)
        
        maskWSName = f"_test_mask_{self.fakeRunNumber}"
        maskTitle = str(uuid.uuid1())
        
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        self.makeFakeInputMaskAndGrouping(algo, maskWSName)
        assert maskWSName in mtd
        mask = mtd[maskWSName]
        mask.setTitle(maskTitle);
        # algo.execute()
        # THE FOLLOWING ASSERTION FAILS:  TODO: track this down.
        # Mantid 'PDCalibrate' itself indicates that it is *not* creating a new mask workspace.
        # assert id(mtd[maskWSName]) == idIncomingMask
        # 
        algo.execute()
        assert maskWSName in mtd
        mask = mtd[maskWSName] # handle will have changed
        assert mask.getTitle() == maskTitle
        assert mask.getNumberMasked() == 0        
 
    def countDetectorsForGroups(self, grouping, gs): # grouping: Dict(<group id>, List(<detector id>), gs: tuple[<group id>]
        count = 0
        for g in gs:
            count += 0 if g not in grouping else len(grouping[g])
        return count
              
    def prepareGroupsToFail(self, ws, grouping, gs): # ws: MatrixWorkspace, grouping:Dict(<group id>, List(<detector id>), gs: tuple[<group id>]
        import numpy as np
        zs = np.zeros_like(ws.readY(0))
        detInfo = ws.detectorInfo()
        for g in gs:
            if g in grouping:
                dets = grouping[g]
                for det in dets:
                    ws.setY(detInfo.indexOf(det), zs)

    def test_failures_are_masked(self):
        """Test that failing spectra are masked"""
        from mantid.simpleapi import mtd
        
        # we need to create a DIFC table before we can run
        difcWS: str = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcWS)
        
        maskWSName = f"_test_mask_{self.fakeRunNumber}"
        
        # now run the algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)

        groupingWSName = f"_test_grouping_{self.fakeRunNumber}"
        grouping = self.makeFakeInputMaskAndGrouping(algo, maskWSName, groupingWSName)
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == 0
        inputWS = mtd[algo.inputWStof]
        groupsToFail = (3,)
        self.prepareGroupsToFail(inputWS, grouping, groupsToFail)
        tofWS = inputWS.clone() # Algorithm will delete its temporary workspaces after execution
        algo.execute()
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == \
          self.countDetectorsForGroups(grouping, groupsToFail)       
        for g in groupsToFail:
            if g in grouping:
                dets = grouping[g]
                for det in dets:
                    assert mask.isMasked(det)

    def maskGroups(self, maskWS, grouping, gs): # maskWS: MaskWorkspace, grouping:Dict(<group id>, List(<detector id>), gs: tuple[<group id>]
        for g in gs:
            if g in grouping:
                dets = grouping[g]
                for det in dets:
                    maskWS.setValue(det, True)
            
    def test_masks_stay_masked(self):
        """Test that incoming masked spectra are still masked at output"""
        from mantid.simpleapi import mtd
        
        # we need to create a DIFC table before we can run
        difcWS: str = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcWS)
        
        maskWSName = f"_test_mask_{self.fakeRunNumber}"
        
        # now run the algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
               
        groupingWSName = f"_test_grouping_{self.fakeRunNumber}"
        grouping = self.makeFakeInputMaskAndGrouping(algo, maskWSName, groupingWSName)
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == 0
        inputWS = mtd[algo.inputWStof]
        groupsToMask = (0,)
        self.maskGroups(mask, grouping, groupsToMask)
        assert mask.getNumberMasked() == \
          self.countDetectorsForGroups(grouping, groupsToMask)        
        algo.execute()
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == \
          self.countDetectorsForGroups(grouping, groupsToMask)        
        for g in groupsToMask:
            if g in grouping:
                dets = grouping[g]
                for det in dets:
                    assert mask.isMasked(det)
            
    def test_masks_are_combined(self):
        """Test that masks for failing spectra are combined with any input mask"""
        from mantid.simpleapi import mtd
        
        # we need to create a DIFC table before we can run
        difcWS: str = f"_{self.fakeRunNumber}_difcs_test"
        self.initDIFCTable(difcWS)
        
        maskWSName = f"_test_mask_{self.fakeRunNumber}"
        
        # now run the algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", f"_TOF_{self.fakeRunNumber}")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeRunNumber}")
        algo.setProperty("PreviousCalibrationTable", difcWS)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)


        groupingWSName = f"_test_grouping_{self.fakeRunNumber}"
        grouping = self.makeFakeInputMaskAndGrouping(algo, maskWSName, groupingWSName)
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == 0
        inputWS = mtd[algo.inputWStof]
        groupsToFail = (3,)
        self.prepareGroupsToFail(inputWS, grouping, groupsToFail)
        groupsToMask = (0,)
        self.maskGroups(mask, grouping, groupsToMask)
        assert mask.getNumberMasked() == \
          self.countDetectorsForGroups(grouping, groupsToMask)        
        algo.execute()
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == \
          self.countDetectorsForGroups(grouping, groupsToFail) + \
          self.countDetectorsForGroups(grouping, groupsToMask)        
        for g in groupsToFail:
            if g in grouping:
                dets = grouping[g]
                for det in dets:
                    assert mask.isMasked(det)
        for g in groupsToMask:
          if g in grouping:
              dets = grouping[g] 
              for det in dets:
                  assert mask.isMasked(det)

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
