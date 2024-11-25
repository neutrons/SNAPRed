import unittest
from collections.abc import Sequence
from unittest import mock

import pytest
from mantid.api import MatrixWorkspace
from mantid.dataobjects import GroupingWorkspace
from mantid.simpleapi import CalculateDiffCalTable, mtd
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import deleteWorkspaceNoThrow, maskGroups, mutableWorkspaceClones, setGroupSpectraToZero

# needed to make mocked ingredients
from snapred.backend.log.logger import snapredLogger

# the algorithm to test
from snapred.backend.recipe.GroupDiffCalRecipe import (
    GroupDiffCalRecipe as Recipe,  # noqa: E402
)

logger = snapredLogger.getLogger(__name__)


class TestGroupDiffCalRecipe(unittest.TestCase):
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
        CalculateDiffCalTable(
            InputWorkspace=cls.fakeRawData,
            CalibrationTable=cls.difcWS,
            OffsetMode="Signed",
            BinWidth=fakeDBin,
        )

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
        """Test that ingredients for rx are properly processed"""
        rx = Recipe()
        rx.chopIngredients(self.fakeIngredients)
        assert rx.runNumber == self.fakeIngredients.runConfig.runNumber
        assert rx.TOF.minimum == self.fakeIngredients.pixelGroup.timeOfFlight.minimum
        assert rx.TOF.maximum == self.fakeIngredients.pixelGroup.timeOfFlight.maximum
        assert rx.dBin == self.fakeIngredients.pixelGroup.dBin()

    def test_execute(self):
        """Test that the recipe xecutes"""

        uniquePrefix = "test_e_"
        (maskWS,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix)
        (maskWSName,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix, name_only=True)

        groceries = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "calibrationTable": "_final_DIFC_table",
            "maskWorkspace": maskWSName,
            "outputWorkspace": f"_test_out_dsp_{self.fakeIngredients.runConfig.runNumber}",
            "previousCalibration": self.difcWS,
        }
        res = Recipe().cook(self.fakeIngredients, groceries)

        assert res.result
        assert maskWSName in mtd
        assert maskWS.getNumberMasked() == 0
        deleteWorkspaceNoThrow(maskWSName)

    def test_mask_is_created(self):
        """Test that a mask workspace is created if it doesn't already exist:
        -- this method also verifies that none of the spectra in the synthetic data will be masked.
        """

        uniquePrefix = "test_mic_"
        maskWSName = uniquePrefix + "_mask"

        # Ensure that the mask workspace doesn't already exist
        assert maskWSName not in mtd

        groceries = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "calibrationTable": "_final_DIFc_table",
            "maskWorkspace": maskWSName,
            "outputWorkspace": f"_test_out_dsp_{self.fakeIngredients.runConfig.runNumber}",
            "previousCalibration": self.difcWS,
        }
        res = Recipe().cook(self.fakeIngredients, groceries)

        assert res.result
        assert maskWSName in mtd
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == 0
        deleteWorkspaceNoThrow(maskWSName)

    def test_existing_mask_is_used(self):
        """Test that an existing mask workspace is not overwritten"""

        uniquePrefix = "test_miu_"
        (maskWS,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix)
        (maskWSName,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix, name_only=True)
        maskTitle = "d35b4aae-93fe-421b-95f4-5eec635a70d1"

        assert maskWSName in mtd
        # Note: for some reason using 'id' for this test does not work:
        #   the 'id' of the mask workspace handle changes.
        maskWS.setTitle(maskTitle)
        assert maskWS.getTitle() == maskTitle

        groceries = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "calibrationTable": "_final_DIFc_table",
            "maskWorkspace": maskWSName,
            "outputWorkspace": f"_test_out_dsp_{self.fakeIngredients.runConfig.runNumber}",
            "previousCalibration": self.difcWS,
        }
        res = Recipe().cook(self.fakeIngredients, groceries)

        assert res.result
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

        uniquePrefix = "test_fam_"
        inputWS, maskWS = mutableWorkspaceClones((self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix)
        inputWSName, maskWSName = mutableWorkspaceClones(
            (self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix, name_only=True
        )

        assert maskWS.getNumberMasked() == 0
        groupsToFail = (3,)
        groupingWS = mtd[self.fakeGroupingWorkspace]
        self.prepareGroupsToFail(inputWS, groupingWS, groupsToFail)
        # Verify that at least some spectra have been modified.
        assert self.countDetectorsForGroups(groupingWS, groupsToFail) > 0

        groceries = {
            "inputWorkspace": inputWSName,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "calibrationTable": "_final_DIFc_table",
            "maskWorkspace": maskWSName,
            "outputWorkspace": f"_test_out_dsp_{self.fakeIngredients.runConfig.runNumber}",
            "previousCalibration": self.difcWS,
        }
        res = Recipe().cook(self.fakeIngredients, groceries)

        assert res.result
        assert maskWS.getNumberMasked() == self.countDetectorsForGroups(groupingWS, groupsToFail)
        for gid in groupsToFail:
            dets = groupingWS.getDetectorIDsOfGroup(gid)
            for det in dets:
                assert maskWS.isMasked(int(det))
        deleteWorkspaceNoThrow(inputWSName)
        deleteWorkspaceNoThrow(maskWSName)

    def test_masks_stay_masked(self):
        """Test that incoming masked spectra are still masked at output"""

        uniquePrefix = "test_msm_"
        inputWS, maskWS = mutableWorkspaceClones((self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix)
        inputWSName, maskWSName = mutableWorkspaceClones(
            (self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix, name_only=True
        )

        assert maskWS.getNumberMasked() == 0
        groupsToMask = (11,)
        groupingWS = mtd[self.fakeGroupingWorkspace]
        maskGroups(maskWS, groupingWS, groupsToMask)
        assert maskWS.getNumberMasked() == self.countDetectorsForGroups(groupingWS, groupsToMask)
        # Verify that at least some detectors have been masked.
        assert self.countDetectorsForGroups(groupingWS, groupsToMask) > 0

        groceries = {
            "inputWorkspace": inputWSName,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "calibrationTable": "_final_DIFc_table",
            "maskWorkspace": maskWSName,
            "outputWorkspace": f"_test_out_dsp_{self.fakeIngredients.runConfig.runNumber}",
            "previousCalibration": self.difcWS,
        }
        res = Recipe().cook(self.fakeIngredients, groceries)

        assert res.result
        assert maskWS.getNumberMasked() == self.countDetectorsForGroups(groupingWS, groupsToMask)
        for gid in groupsToMask:
            dets = groupingWS.getDetectorIDsOfGroup(gid)
            for det in dets:
                assert maskWS.isMasked(int(det))
        deleteWorkspaceNoThrow(inputWSName)
        deleteWorkspaceNoThrow(maskWSName)

    def test_masks_are_combined(self):
        """Test that masks for failing spectra are combined with any input mask"""

        uniquePrefix = "test_mac_"
        inputWS, maskWS = mutableWorkspaceClones((self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix)
        inputWSName, maskWSName = mutableWorkspaceClones(
            (self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix, name_only=True
        )

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

        groceries = {
            "inputWorkspace": inputWSName,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "calibrationTable": "_final_DIFc_table",
            "maskWorkspace": maskWSName,
            "outputWorkspace": f"_test_out_dsp_{self.fakeIngredients.runConfig.runNumber}",
            "previousCalibration": self.difcWS,
        }
        res = Recipe().cook(self.fakeIngredients, groceries)

        assert res.result
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

    def test_final_calibration_table_set_correctly(self):
        """Test that the final calibration table is set correctly"""

        uniquePrefix = "test_fct_"
        inputWS, maskWS = mutableWorkspaceClones((self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix)
        inputWSName, maskWSName = mutableWorkspaceClones(
            (self.fakeRawData, self.fakeMaskWorkspace), uniquePrefix, name_only=True
        )

        groceries = {
            "inputWorkspace": inputWSName,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "calibrationTable": "_final_DIFc_table",
            "maskWorkspace": maskWSName,
            "outputWorkspace": f"_test_out_dsp_{self.fakeIngredients.runConfig.runNumber}",
            "previousCalibration": self.difcWS,
        }
        res = Recipe().cook(self.fakeIngredients, groceries)

        assert res.result
        assert res.calibrationTable == groceries["calibrationTable"]
        deleteWorkspaceNoThrow(inputWSName)
        deleteWorkspaceNoThrow(maskWSName)

    def test_validateInputs_bad_workspaces(self):
        groceries = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "notInTheList": str(mock.sentinel.bad),
        }

        with pytest.raises(RuntimeError, match=r".*input groceries: \{'notInTheList'\}"):
            Recipe().validateInputs(self.fakeIngredients, groceries)
