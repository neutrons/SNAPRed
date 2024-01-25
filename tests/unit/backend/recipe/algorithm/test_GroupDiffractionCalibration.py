import unittest
from collections.abc import Sequence
from typing import Any, Dict, List, Tuple
from unittest import mock

import pytest
from mantid.api import ITableWorkspace, MatrixWorkspace
from mantid.dataobjects import GroupingWorkspace, MaskWorkspace
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.CalculateDiffCalTable import CalculateDiffCalTable

# the algorithm to test
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import (
    GroupDiffractionCalibration as ThisAlgo,  # noqa: E402
)
from snapred.meta.Config import Resource
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import deleteWorkspaceNoThrow, maskGroups, mutableWorkspaceClones, setGroupSpectraToZero

logger = snapredLogger.getLogger(__name__)


class TestGroupDiffractionCalibration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        syntheticInputs = SyntheticData()
        cls.fakeIngredients = syntheticInputs.ingredients
        fakeDBin = max([abs(d) for d in cls.fakeIngredients.pixelGroup.dBin()])

        runNumber = cls.fakeIngredients.runConfig.runNumber
        cls.fakeRawData = f"_test_groupcal_{runNumber}"
        cls.fakeGroupingWorkspace = f"_test_groupcal_difc_{runNumber}"
        cls.fakeMaskWorkspace = f"_test_groupcal_difc_{runNumber}_mask"
        cls.difcWS = f"_{runNumber}_difcs_test"
        syntheticInputs.generateWorkspaces(cls.fakeRawData, cls.fakeGroupingWorkspace, cls.fakeMaskWorkspace)

        # create the DIFCprev table
        cc = CalculateDiffCalTable()
        cc.initialize()
        cc.setProperty("InputWorkspace", cls.fakeRawData)
        cc.setProperty("CalibrationTable", cls.difcWS)
        cc.setProperty("OffsetMode", "Signed")
        cc.setProperty("BinWidth", fakeDBin)
        cc.execute()

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Delete all workspaces created by this test, and remove any created files.
        This is run once at the end of this test suite.
        """
        for ws in [cls.fakeRawData, cls.fakeGroupingWorkspace, cls.fakeMaskWorkspace, cls.difcWS]:
            deleteWorkspaceNoThrow(ws)
        return super().tearDownClass()

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(self.fakeIngredients)
        assert algo.runNumber == self.fakeIngredients.runConfig.runNumber
        assert algo.TOF.minimum == self.fakeIngredients.pixelGroup.timeOfFlight.minimum
        assert algo.TOF.maximum == self.fakeIngredients.pixelGroup.timeOfFlight.maximum
        assert algo.dBin == self.fakeIngredients.pixelGroup.dBin()

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        difcWS = f"_{self.fakeIngredients.runConfig.runNumber}_difcs_test"
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("PreviousCalibrationTable", difcWS)
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()
        assert algo.getPropertyValue("PreviousCalibrationTable") == difcWS

    def test_execute(self):
        """Test that the algorithm executes"""
        from mantid.simpleapi import mtd

        uniquePrefix = "test_e_"
        (maskWS,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix)
        (maskWSName,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix, name_only=True)

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("FinalCalibrationTable", "_final_DIFc_table")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeIngredients.runConfig.runNumber}")
        algo.setProperty("PreviousCalibrationTable", self.difcWS)
        assert algo.execute()
        assert maskWSName in mtd
        assert maskWS.getNumberMasked() == 0
        deleteWorkspaceNoThrow(maskWSName)

    def test_mask_is_created(self):
        """Test that a mask workspace is created if it doesn't already exist:
        -- this method also verifies that none of the spectra in the synthetic data will be masked.
        """
        from mantid.simpleapi import mtd

        uniquePrefix = "test_mic_"
        maskWSName = uniquePrefix + "_mask"

        # Ensure that the mask workspace doesn't already exist
        assert maskWSName not in mtd

        # now run the algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("FinalCalibrationTable", "_final_DIFc_table")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeIngredients.runConfig.runNumber}")
        algo.setProperty("PreviousCalibrationTable", self.difcWS)

        algo.execute()
        assert maskWSName in mtd
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == 0
        deleteWorkspaceNoThrow(maskWSName)

    def test_existing_mask_is_used(self):
        """Test that an existing mask workspace is not overwritten"""
        from mantid.simpleapi import mtd

        uniquePrefix = "test_miu_"
        (maskWS,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix)
        (maskWSName,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix, name_only=True)
        maskTitle = "d35b4aae-93fe-421b-95f4-5eec635a70d1"

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("FinalCalibrationTable", "_final_DIFc_table")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeIngredients.runConfig.runNumber}")
        algo.setProperty("PreviousCalibrationTable", self.difcWS)
        assert maskWSName in mtd

        # Note: for some reason using 'id' for this test does not work:
        #   the 'id' of the mask workspace handle changes.
        maskWS.setTitle(maskTitle)
        assert maskWS.getTitle() == maskTitle

        algo.execute()
        assert maskWSName in mtd
        maskWS = mtd[maskWSName]
        assert maskWS.getTitle() == maskTitle
        assert maskWS.getNumberMasked() == 0
        deleteWorkspaceNoThrow(maskWSName)

    def countDetectorsForGroups(self, groupingWS: GroupingWorkspace, gids: Sequence[int]) -> int:
        count = 0
        for gid in gids:
            count += len(groupingWS.getDetectorIDsOfGroup(gid))
        return count

    def prepareGroupsToFail(self, ws: MatrixWorkspace, groupingWS: GroupingWorkspace, gids: Sequence[int]):
        # Zero out all spectra contributing to each group
        setGroupSpectraToZero(ws, groupingWS, gids)

    def test_failures_are_masked(self):
        """Test that failing spectra are masked"""
        from mantid.simpleapi import mtd

        uniquePrefix = "test_fam_"
        inputWS, maskWS = mutableWorkspaceClones((self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix)
        inputWSName, maskWSName = mutableWorkspaceClones(
            (self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix, name_only=True
        )

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", inputWSName)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("FinalCalibrationTable", "_final_DIFc_table")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeIngredients.runConfig.runNumber}")
        algo.setProperty("PreviousCalibrationTable", self.difcWS)

        assert maskWS.getNumberMasked() == 0
        groupsToFail = (3,)
        groupingWS = mtd[self.fakeGroupingWorkspace]
        self.prepareGroupsToFail(inputWS, groupingWS, groupsToFail)
        # Verify that at least some spectra have been modified.
        assert self.countDetectorsForGroups(groupingWS, groupsToFail) > 0

        algo.execute()
        assert maskWS.getNumberMasked() == self.countDetectorsForGroups(groupingWS, groupsToFail)
        for gid in groupsToFail:
            dets = groupingWS.getDetectorIDsOfGroup(gid)
            for det in dets:
                assert maskWS.isMasked(int(det))
        deleteWorkspaceNoThrow(inputWSName)
        deleteWorkspaceNoThrow(maskWSName)

    def test_masks_stay_masked(self):
        """Test that incoming masked spectra are still masked at output"""
        from mantid.simpleapi import mtd

        uniquePrefix = "test_msm_"
        inputWS, maskWS = mutableWorkspaceClones((self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix)
        inputWSName, maskWSName = mutableWorkspaceClones(
            (self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix, name_only=True
        )

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", inputWSName)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("FinalCalibrationTable", "_final_DIFc_table")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeIngredients.runConfig.runNumber}")
        algo.setProperty("PreviousCalibrationTable", self.difcWS)

        assert maskWS.getNumberMasked() == 0
        groupsToMask = (11,)
        groupingWS = mtd[self.fakeGroupingWorkspace]
        maskGroups(maskWS, groupingWS, groupsToMask)
        assert maskWS.getNumberMasked() == self.countDetectorsForGroups(groupingWS, groupsToMask)
        # Verify that at least some detectors have been masked.
        assert self.countDetectorsForGroups(groupingWS, groupsToMask) > 0

        algo.execute()
        assert maskWS.getNumberMasked() == self.countDetectorsForGroups(groupingWS, groupsToMask)
        for gid in groupsToMask:
            dets = groupingWS.getDetectorIDsOfGroup(gid)
            for det in dets:
                assert maskWS.isMasked(int(det))
        deleteWorkspaceNoThrow(inputWSName)
        deleteWorkspaceNoThrow(maskWSName)

    def test_masks_are_combined(self):
        """Test that masks for failing spectra are combined with any input mask"""
        from mantid.simpleapi import mtd

        uniquePrefix = "test_mac_"
        inputWS, maskWS = mutableWorkspaceClones((self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix)
        inputWSName, maskWSName = mutableWorkspaceClones(
            (self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix, name_only=True
        )

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("InputWorkspace", inputWSName)
        algo.setProperty("GroupingWorkspace", self.fakeGroupingWorkspace)
        algo.setProperty("FinalCalibrationTable", "_final_DIFc_table")
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("OutputWorkspace", f"_test_out_{self.fakeIngredients.runConfig.runNumber}")
        algo.setProperty("PreviousCalibrationTable", self.difcWS)

        assert maskWS.getNumberMasked() == 0
        groupingWS = mtd[self.fakeGroupingWorkspace]
        groupsToFail = (3,)
        self.prepareGroupsToFail(inputWS, groupingWS, groupsToFail)
        # Verify that at least some spectra have been modified.
        assert self.countDetectorsForGroups(groupingWS, groupsToFail) > 0
        groupsToMask = (11,)
        maskGroups(maskWS, groupingWS, groupsToMask)
        assert maskWS.getNumberMasked() == self.countDetectorsForGroups(groupingWS, groupsToMask)
        # Verify that at least some detectors have been masked.
        assert self.countDetectorsForGroups(groupingWS, groupsToMask) > 0

        algo.execute()
        assert maskWS.getNumberMasked() == self.countDetectorsForGroups(
            groupingWS, groupsToFail
        ) + self.countDetectorsForGroups(groupingWS, groupsToMask)
        for gid in groupsToFail:
            dets = groupingWS.getDetectorIDsOfGroup(gid)
            for det in dets:
                assert maskWS.isMasked(int(det))
        for gid in groupsToMask:
            dets = groupingWS.getDetectorIDsOfGroup(gid)
            for det in dets:
                assert maskWS.isMasked(int(det))
        deleteWorkspaceNoThrow(inputWSName)
        deleteWorkspaceNoThrow(maskWSName)


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    yield  # ... teardown follows:
    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
