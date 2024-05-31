# ruff: noqa: E722, PT011, PT012, F811
import json
import os
import shutil
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import ANY

import pytest
import snapred.backend.recipe.algorithm  # noqa: F401
from mantid.kernel import V3D, Quat
from mantid.simpleapi import (
    CloneWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    GenerateTableWorkspaceFromListOfDict,
    LoadEmptyInstrument,
    SaveDiffCal,
    SaveNexusProcessed,
    mtd,
)
from mantid.testing import assert_almost_equal as assert_wksp_almost_equal
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.state import DetectorState
from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata, diffcal_metadata_state_list
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask
from util.instrument_helpers import mapFromSampleLogs
from util.kernel_helpers import tupleFromQuat, tupleFromV3D

ThisService = "snapred.backend.data.GroceryService."

DataService = mock.Mock(spec_set=LocalDataService)


class TestGroceryService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create a mock data file which will be loaded in tests.
        This is created at the start of this test suite, then deleted at the end.
        """
        cls.runNumber = "555"
        cls.version = "1"
        cls.runNumber1 = "556"
        cls.useLiteMode = False
        cls.diffCalOutputName = (
            wng.diffCalOutput().runNumber(cls.runNumber1).unit(wng.Units.TOF).group(wng.Groups.UNFOC).build()
        )
        cls.sampleWSFilePath = Resource.getPath(f"inputs/test_{cls.runNumber}_groceryservice.nxs")
        cls.sampleTarWsFilePath = Resource.getPath(f"inputs/{cls.diffCalOutputName}.tar")

        cls.instrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        Config["instrument"]["native"]["definition"]["file"] = cls.instrumentFilePath

        cls.instrumentLiteFilePath = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
        Config["instrument"]["lite"]["definition"]["file"] = cls.instrumentLiteFilePath

        cls.fetchedWSname = "_fetched_grocery"
        cls.groupingScheme = "Native"
        # create some sample data
        cls.sampleWS = "_grocery_to_fetch"

        LoadEmptyInstrument(
            Filename=cls.instrumentFilePath,
            OutputWorkspace=cls.sampleWS,
        )
        SaveNexusProcessed(
            InputWorkspace=cls.sampleWS,
            Filename=cls.sampleWSFilePath,
        )

        with tarfile.open(cls.sampleTarWsFilePath, "w") as tar:
            tar.add(cls.sampleWSFilePath, arcname="0.nxs")

        assert os.path.exists(cls.sampleWSFilePath)

        cls.detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        cls.detectorState2 = DetectorState(arc=(7.0, 8.0), wav=9.0, freq=10.0, guideStat=2, lin=(11.0, 12.0))

        cls.difc_name = "diffract_consts"
        cls.sampleDiffCalFilePath = Resource.getPath(f"inputs/test_diffcal_{cls.runNumber}_groceryservice.h5")
        cls.sampleTableWS = "_table_grocery_to_fetch"
        createCompatibleDiffCalTable(cls.sampleTableWS, cls.sampleWS)
        cls.sampleMaskWS = "_mask_grocery_to_fetch"
        createCompatibleMask(cls.sampleMaskWS, cls.sampleWS, cls.instrumentFilePath)

        SaveDiffCal(
            CalibrationWorkspace=cls.sampleTableWS,
            MaskWorkspace=cls.sampleMaskWS,
            Filename=cls.sampleDiffCalFilePath,
        )
        assert os.path.exists(cls.sampleDiffCalFilePath)

        DataService.getIPTS.return_value = "/testdir"
        DataService.readCalibrationRecord.side_effect = lambda x, *y: mock.Mock(runNumber=x, version=cls.version)  # noqa ARG005
        DataService._getVersionFromCalibrationIndex.return_value = cls.version
        DataService._constructCalibrationDataPath.return_value = "/does/not/exist"
        DataService.readDetectorState.return_value = cls.detectorState1

        # cleanup at per-test teardown
        cls.excludeAtTeardown = [cls.sampleWS, cls.sampleTableWS, cls.sampleMaskWS]

        # cleanup during running tests:
        cls.exclude = cls.excludeAtTeardown + [cls.fetchedWSname]

    def setUp(self):
        self.instance = GroceryService(dataService=DataService)
        self.groupingItem = (
            GroceryListItem.builder()
            .fromRun(self.runNumber)
            .grouping(self.groupingScheme)
            .native()
            .source(InstrumentDonor=self.sampleWS)
            .build()
        )
        return super().setUp()

    def clearoutWorkspaces(self) -> None:
        """Delete the workspaces created by loading"""
        for ws in mtd.getObjectNames():
            if ws not in self.excludeAtTeardown:
                DeleteWorkspace(ws)

    def tearDown(self):
        """At the end of each test, clear out the workspaces"""
        self.clearoutWorkspaces()
        del self.instance
        return super().tearDown()

    @classmethod
    def tearDownClass(cls):
        """
        At the end of the test suite, delete all workspaces
        and remove the test file.
        """
        for ws in mtd.getObjectNames():
            DeleteWorkspace(ws)
        os.remove(cls.sampleWSFilePath)
        os.remove(cls.sampleDiffCalFilePath)
        os.remove(cls.sampleTarWsFilePath)
        return super().tearDownClass()

    @classmethod
    def create_dumb_workspace(cls, wsName):
        CreateWorkspace(
            OutputWorkspace=wsName,
            DataX=[1],
            DataY=[1],
        )

    # Check that string wsname_1 has a new token in comparison to wsname_2.
    # Note, the exact order of tokens and the location of the extra token within the string
    # are up to WorkspaceNameGenerator, which isn't the objective of this check method.
    def check_workspace_name_has_extra_token(self, wsname_1: str, wsname_2: str, extra_token: str) -> bool:
        delimiter = "_"
        tokens_1 = wsname_1.split(delimiter)
        tokens_2 = wsname_2.split(delimiter)
        diff_12 = [x for x in tokens_1 if x not in tokens_2]
        diff_21 = [x for x in tokens_2 if x not in tokens_1]
        return diff_12 == [extra_token] and diff_21 == []

    ## TESTS OF FILENAME METHODS

    def test_getIPTS(self):
        res = self.instance.getIPTS(self.runNumber)
        assert res == self.instance.dataService.getIPTS.return_value
        assert self.instance.dataService.getIPTS.called_with(
            runNumber=self.runNumber,
            instrumentName=Config["instrument.name"],
        )
        res = self.instance.getIPTS(self.runNumber, "CRACKLE")
        assert res == self.instance.dataService.getIPTS.return_value
        assert self.instance.dataService.getIPTS.called_with(
            runNumber=self.runNumber,
            instrumentName="CRACKLE",
        )

    def test_key_neutron(self):
        """ensure the key is a unique identifier for run number and lite mode"""
        # This tests both <neutron data> and <instrument donor> key tuples.
        runId1 = "555"
        runId2 = "556"
        lite = True
        native = False
        # two keys with different run numbers are different
        assert self.instance._key(runId1, lite) != self.instance._key(runId2, lite)
        # two keys with same run number but different resolution are different
        assert self.instance._key(runId1, lite) != self.instance._key(runId1, native)
        # two keys with different run number and resolution are different
        assert self.instance._key(runId1, lite) != self.instance._key(runId2, native)
        assert self.instance._key(runId1, native) != self.instance._key(runId2, lite)
        # it is shaped like itself
        assert self.instance._key(runId1, lite) == self.instance._key(runId1, lite)
        assert self.instance._key(runId1, native) == self.instance._key(runId1, native)

    def test_key_grouping(self):
        """ensure the key is a unique identifier for run number and lite mode"""
        runId1 = "2462"
        grouping1 = "column"
        runId2 = "2463"
        grouping2 = "bank"

        lite = True
        native = False
        # two keys with different groupings are different
        assert self.instance._key(grouping1, runId1, lite) != self.instance._key(grouping2, runId1, lite)
        # two keys with same grouping but different resolution are different
        assert self.instance._key(grouping1, runId1, lite) != self.instance._key(grouping1, runId1, native)
        # two keys with same grouping but different runNumber are different
        assert self.instance._key(grouping1, runId1, lite) != self.instance._key(grouping1, runId2, native)
        # two keys with different grouping  and resolution are different
        assert self.instance._key(grouping1, runId1, lite) != self.instance._key(grouping2, runId1, native)
        assert self.instance._key(grouping1, runId1, native) != self.instance._key(grouping2, runId1, lite)
        # it is shaped like itself
        assert self.instance._key(grouping1, runId1, lite) == self.instance._key(grouping1, runId1, lite)
        assert self.instance._key(grouping1, runId1, native) == self.instance._key(grouping1, runId1, native)

    def test_nexus_filename(self):
        """Test the creation of the nexus filename"""
        res = self.instance._createNeutronFilename(self.runNumber, False)
        assert self.instance.dataService.getIPTS.return_value in res
        assert Config["nexus.native.prefix"] in res
        assert self.runNumber in res
        assert "lite" not in res.lower()

        # now use lite mode
        res = self.instance._createNeutronFilename(self.runNumber, True)
        assert self.instance.dataService.getIPTS.return_value in res
        assert Config["nexus.lite.prefix"] in res
        assert self.runNumber in res
        assert "lite" in res.lower()

    def test_grouping_filename(self):
        """Test the creation of the grouping filename"""
        runNumber = "123"
        uniqueGroupingScheme = "Fruitcake"
        uniqueGroupingDefinition = "/some/path/for/fruitcake"  # NOTE initial / to make "absolute"
        self.groupingItem.groupingScheme = uniqueGroupingScheme

        # construct a mocked grouping map
        mockFocusGroup = mock.Mock(name=uniqueGroupingScheme, definition=uniqueGroupingDefinition)
        mockGroupMap = {uniqueGroupingScheme: mockFocusGroup}
        self.instance.dataService.readGroupingMap = mock.Mock(
            return_value=mock.Mock(getMap=mock.Mock(return_value=mockGroupMap))
        )

        res = self.instance._createGroupingFilename(runNumber, uniqueGroupingScheme, False)
        assert res == uniqueGroupingDefinition

    def test_litedatamap_filename(self):
        """Test it will return name of Lite data map"""
        runNumber = GroceryListItem.RESERVED_NATIVE_RUNNUMBER
        res = self.instance._createGroupingFilename(runNumber, "Lite", False)
        assert res == str(Config["instrument.lite.map.file"])
        res2 = self.instance._createGroupingFilename(runNumber, "Lite", True)
        assert res2 == res

    def test_diffcal_output_filename(self):
        # Test name generation for diffraction-calibration table filename
        res = self.instance._createDiffcalOutputWorkspaceFilename(
            self.runNumber,
            self.useLiteMode,
            self.version,
            "TOF",
            "some",
        )
        assert self.runNumber in res
        assert self.version in res
        assert "tof" in res
        assert "some" in res
        assert ".tar" in res

    def test_diffcal_table_filename(self):
        # Test name generation for diffraction-calibration table filename
        res = self.instance._createDiffcalTableFilename(self.runNumber, self.useLiteMode, self.version)
        assert self.difc_name in res
        assert self.runNumber in res
        assert self.version in res
        assert ".h5" in res

    def test_normalization_workspace_filename(self):
        # Test name generation for diffraction-calibration table filename
        res = self.instance._createNormalizationWorkspaceFilename(self.runNumber, self.useLiteMode, self.version)
        assert self.runNumber in res
        assert self.version in res
        assert ".nxs" in res

    ## TESTS OF WORKSPACE NAME METHODS

    def test_neutron_workspacename_plain(self):
        """Test the creation of a plain nexus workspace name"""
        res = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        fRunNumber = wnvf.formatRunNumber(self.runNumber)
        assert res == f"tof_all_{fRunNumber}"
        # now use lite mode
        res = self.instance._createNeutronWorkspaceName(self.runNumber, True)
        assert res == f"tof_all_lite_{fRunNumber}"

    def test_neutron_workspacename_raw(self):
        """Test the creation of a raw nexus workspace name"""
        plainName = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        rawName = self.instance._createRawNeutronWorkspaceName(self.runNumber, False)
        assert self.check_workspace_name_has_extra_token(rawName, plainName, "raw")

    def test_neutron_workspacename_copy(self):
        """Test the creation of a copy nexus workspace name"""
        fakeCopies = 117
        plainName = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        copyName = self.instance._createCopyNeutronWorkspaceName(self.runNumber, False, fakeCopies)
        assert self.check_workspace_name_has_extra_token(copyName, plainName, f"copy{fakeCopies}")

    def test_grouping_workspacename(self):
        """Test the creation of a grouping workspace name"""
        uniqueGroupingScheme = "Fruitcake"
        self.groupingItem.groupingScheme = uniqueGroupingScheme
        res = self.instance._createGroupingWorkspaceName(uniqueGroupingScheme, self.runNumber, False)
        assert uniqueGroupingScheme in res
        assert "lite" not in res.lower()
        res = self.instance._createGroupingWorkspaceName(uniqueGroupingScheme, self.runNumber, True)
        assert uniqueGroupingScheme in res
        assert "lite" in res.lower()

    def test_litedatamap_workspacename(self):
        """Test the creation of name for Lite data map"""
        res = self.instance._createGroupingWorkspaceName("Lite", self.runNumber, False)
        assert res == "lite_grouping_map"

    def test_diffcal_input_workspacename(self):
        # Test name generation for diffraction-calibration input workspace
        res = self.instance._createDiffcalInputWorkspaceName(self.runNumber)
        assert "tof" in res
        assert self.runNumber in res
        assert "raw" in res

    def test_diffcal_output_tof_workspacename(self):
        # Test name generation for diffraction-calibration focussed-data workspace
        res = self.instance._createDiffcalOutputWorkspaceName(
            self.runNumber, self.useLiteMode, self.version, wng.Units.TOF, wng.Groups.UNFOC
        )
        assert "tof" in res
        assert self.runNumber in res
        assert self.version in res

    def test_diffcal_output_dsp_workspacename(self):
        # Test name generation for diffraction-calibration focussed-data workspace
        res = self.instance._createDiffcalOutputWorkspaceName(
            self.runNumber, self.useLiteMode, self.version, wng.Units.DSP, wng.Groups.UNFOC
        )
        assert "dsp" in res
        assert self.runNumber in res
        assert self.version in res

    def test_diffcal_table_workspacename(self):
        # Test name generation for diffraction-calibration output table
        res = self.instance._createDiffcalTableWorkspaceName(self.runNumber, self.useLiteMode, self.version)
        assert self.difc_name in res
        assert self.runNumber in res
        assert self.version in res

    def test_diffcal_mask_workspacename(self):
        # Test name generation for diffraction-calibration output mask
        res = self.instance._createDiffcalMaskWorkspaceName(self.runNumber, self.useLiteMode, self.version)
        assert self.difc_name in res
        assert self.runNumber in res
        assert self.version in res
        assert "mask" in res

    def test_normalization_workspacename(self):
        # Test name generation for normalization workspaces
        res = self.instance._createNormalizationWorkspaceName(self.runNumber, self.useLiteMode, self.version)
        assert self.runNumber in res
        assert self.version in res

    # NOTE if your branch merge puts test_diffcal_table_filename here, do not include it
    # that test is above, under the filename tests

    ## TESTS OF ACCESS METHODS

    def test_workspaceDoesExist_false(self):
        wsname = "_test_ws_"
        # if there is no workspace, return false
        assert self.instance.workspaceDoesExist(wsname) is False

    def test_workspaceDoesExist_true(self):
        wsname = "_test_ws_"
        # create the workspace
        self.create_dumb_workspace(wsname)
        # if there is a workspace, return true
        assert self.instance.workspaceDoesExist(wsname) is True

    def test_getWorkspaceForName_exists(self):
        wsname = "_test_ws_"
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        assert self.instance.getWorkspaceForName(wsname)

    def test_getWorkspaceForName_does_not_exist(self):
        wsname = "_test_ws_"
        assert not mtd.doesExist(wsname)
        assert not self.instance.getWorkspaceForName(wsname)

    def test_getCloneOfWorkspace_fails(self):
        wsname1 = "_test_ws_"
        wsname2 = "_cloned_ws_"
        assert not mtd.doesExist(wsname1)
        assert not mtd.doesExist(wsname2)
        assert not self.instance.getCloneOfWorkspace(wsname1, wsname2)

    def test_getCloneOfWorkspace_works(self):
        wsname1 = "_test_ws_"
        wsname2 = "_cloned_ws_"
        self.create_dumb_workspace(wsname1)
        assert mtd.doesExist(wsname1)
        assert not mtd.doesExist(wsname2)
        ws = self.instance.getCloneOfWorkspace(wsname1, wsname2)
        assert ws.name() == wsname2  # checks that the workspace pointer can be used
        assert mtd.doesExist(wsname2)
        assert_wksp_almost_equal(
            Workspace1=wsname1,
            Workspace2=wsname2,
        )

    def test_updateNeutronCacheFromADS_noop(self):
        """ws is not in cache and not in ADS"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        assert self.instance._loadedRuns == {}
        assert not mtd.doesExist(wsname)
        self.instance._updateNeutronCacheFromADS(runId, useLiteMode)
        assert self.instance._createRawNeutronWorkspaceName.called_once_with(runId, useLiteMode)
        assert self.instance._loadedRuns == {}
        assert not mtd.doesExist(wsname)

    def test_updateNeutronCacheFromADS_both(self):
        """ws is in cache and in ADS"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedRuns[(runId, useLiteMode)] = 1
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        self.instance._updateNeutronCacheFromADS(runId, useLiteMode)
        assert self.instance._createRawNeutronWorkspaceName.called_once_with(runId, useLiteMode)
        assert self.instance._loadedRuns == {(runId, useLiteMode): 1}

    def test_updateNeutronCacheFromADS_cache_no_ADS(self):
        """ws is in cache but not ADS"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedRuns[(runId, useLiteMode)] = 1
        assert not mtd.doesExist(wsname)
        self.instance._updateNeutronCacheFromADS(runId, useLiteMode)
        assert self.instance._createRawNeutronWorkspaceName.called_once_with(runId, useLiteMode)
        assert self.instance._loadedRuns == {}

    def test_updateNeutronCacheFromADS_ADS_no_cache(self):
        """ws is in ADS and not in cache"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.create_dumb_workspace(wsname)
        assert self.instance._loadedRuns == {}
        assert mtd.doesExist(wsname)
        self.instance._updateNeutronCacheFromADS(runId, useLiteMode)
        assert self.instance._createRawNeutronWorkspaceName.called_once_with(runId, useLiteMode)
        assert self.instance._loadedRuns == {(runId, useLiteMode): 0}

    def test_updateGroupingCacheFromADS_noop(self):
        """ws is not in cache and not in ADS"""
        groupingScheme = "column"
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, runId, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        assert self.instance._loadedGroupings == {}
        assert not mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        assert self.instance._createGroupingWorkspaceName.called_once_with(groupingScheme, wsname)
        assert self.instance._loadedGroupings == {}
        assert not mtd.doesExist(wsname)

    def test_updateGroupingCacheFromADS_both(self):
        """ws is in cache and in ADS"""
        groupingScheme = "column"
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, runId, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedGroupings[key] = wsname
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        assert self.instance._createGroupingWorkspaceName.called_once_with(groupingScheme, wsname)
        assert self.instance._loadedGroupings == {key: wsname}

    def test_updateGroupingCacheFromADS_cache_no_ADS(self):
        """ws is in cache but not ADS"""
        groupingScheme = "column"
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, runId, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedGroupings[key] = wsname
        assert not mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        assert self.instance._createGroupingWorkspaceName.called_once_with(groupingScheme, wsname)
        assert self.instance._loadedGroupings == {}

    def test_updateGroupingCacheFromADS_ADS_no_cache(self):
        """ws is in ADS and not in cache"""
        groupingScheme = "column"
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, runId, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        self.create_dumb_workspace(wsname)
        assert self.instance._loadedGroupings == {}
        assert mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        assert self.instance._createGroupingWorkspaceName.called_once_with(groupingScheme, wsname)
        assert self.instance._loadedGroupings == {key: wsname}

    def test_updateInstrumentCacheFromADS_noop(self):
        """ws is not in cache and not in ADS"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        assert self.instance._loadedInstruments == {}
        assert not mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode)
        assert self.instance._loadedInstruments == {}
        assert not mtd.doesExist(wsname)

    def test_updateInstrumentCacheFromADS_both(self):
        """ws is in cache and in ADS"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        key = self.instance._key(runId, useLiteMode)
        self.instance._loadedInstruments[key] = wsname
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode)
        assert self.instance._loadedInstruments == {key: wsname}

    def test_updateInstrumentCacheFromADS_cache_no_ADS(self):
        """ws is in cache but not ADS"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        key = self.instance._key(runId, useLiteMode)
        self.instance._loadedInstruments[key] = wsname
        assert not mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode)
        assert self.instance._loadedInstruments == {}

    def test_updateInstrumentCacheFromADS_ADS_no_cache(self):
        """ws is in ADS and not in cache"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        key = self.instance._key(runId, useLiteMode)
        self.create_dumb_workspace(wsname)
        assert self.instance._loadedGroupings == {}
        assert mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode)
        assert self.instance._loadedInstruments == {key: wsname}

    def test_rebuildNeutronCache(self):
        """Test the correct behavior of the rebuildNeutronCache method"""
        runId = "555"
        useLiteMode = True
        key = (runId, useLiteMode)
        self.instance._loadedRuns[key] = 1
        self.instance.rebuildNeutronCache()
        assert self.instance._loadedRuns == {}

    def test_rebuildGroupingCache(self):
        """Test the correct behavior of the rebuildGroupingCache method"""
        groupingScheme = "column"
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, runId, useLiteMode)
        self.instance._loadedGroupings[key] = wsname
        self.instance.rebuildGroupingCache()
        assert self.instance._loadedGroupings == {}

    def test_rebuildInstrumentCache(self):
        """Test the correct behavior of the rebuildInstrumentCache method"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        key = self.instance._key(runId, useLiteMode)
        self.instance._loadedInstruments[key] = wsname
        self.instance.rebuildInstrumentCache()
        assert self.instance._loadedInstruments == {}

    def test_rebuildCache(self):
        """Test the correct behavior of the rebuildCache method"""
        groupingScheme = "column"
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        key = (runId, useLiteMode)
        self.instance._loadedRuns[key] = 1
        self.instance._loadedGroupings[self.instance._key(groupingScheme, runId, useLiteMode)] = wsname
        self.instance._loadedInstruments[key] = wsname
        self.instance.rebuildCache()
        assert self.instance._loadedRuns == {}
        assert self.instance._loadedGroupings == {}
        assert self.instance._loadedInstruments == {}

    def test_workspaceTagFunctions(self):
        wsname = mtd.unique_name(prefix="testWorkspaceTags_")
        tagValues = diffcal_metadata_state_list
        properties = list(WorkspaceMetadata.schema()["properties"].keys())
        logName = properties[0]

        # Make sure correct error is thrown if workspace does not exist
        with pytest.raises(RuntimeError) as e1:
            tagResult = self.instance.getWorkspaceTag(workspaceName=wsname, logname=logName)
        assert f"Workspace {wsname} does not exist" in str(e1.value)

        with pytest.raises(RuntimeError) as e2:
            self.instance.setWorkspaceTag(workspaceName=wsname, logname=logName, logvalue=tagValues[0])
        assert f"Workspace {wsname} does not exist" in str(e2.value)

        # Make sure default tag value is unset
        self.create_dumb_workspace(wsname)
        tagResult = self.instance.getWorkspaceTag(workspaceName=wsname, logname=logName)
        assert tagResult == UNSET

        # Set and update the tag with all possible tag values
        for tag in tagValues:
            self.instance.setWorkspaceTag(workspaceName=wsname, logname=logName, logvalue=tag)
            tagResult = self.instance.getWorkspaceTag(workspaceName=wsname, logname=logName)
            assert tagResult == tag

    ## TEST CLEANUP METHODS

    def test_deleteWorkspace(self):
        wsname = mtd.unique_name(prefix="_test_ws_")
        mockAlgo = mock.Mock()
        self.instance.mantidSnapper.WashDishes = mockAlgo
        # wash the dish
        self.instance.deleteWorkspace(wsname)
        assert mockAlgo.called_once_with(ANY, Workspace=wsname)

    def test_deleteWorkspaceUnconditional(self):
        wsname = mtd.unique_name(prefix="_test_ws_")
        mockAlgo = mock.Mock()
        self.instance.mantidSnapper.DeleteWorkspace = mockAlgo
        # if the workspace does not exist, then don't do anything
        self.instance.deleteWorkspaceUnconditional(wsname)
        assert mockAlgo.not_called
        # make the workspace
        self.create_dumb_workspace(wsname)
        # if the workspace does exist, then delete it
        self.instance.deleteWorkspaceUnconditional(wsname)
        assert mockAlgo.called_once_with(ANY, Workspace=wsname)

    ## TEST FETCH METHODS

    def test_fetchWorkspace(self):
        """Test the correct behavior of the fetchWorkspace method"""
        self.clearoutWorkspaces()
        res = self.instance.fetchWorkspace(self.sampleWSFilePath, self.fetchedWSname, loader="")
        assert res == {
            "result": True,
            "loader": "LoadNexusProcessed",
            "workspace": self.fetchedWSname,
        }
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

        # make sure it won't load same workspace name again
        self.instance.grocer.executeRecipe = mock.Mock(return_value="bad")
        assert mtd.doesExist(self.fetchedWSname)
        res = self.instance.fetchWorkspace(self.sampleWSFilePath, self.fetchedWSname, loader="")
        assert res == {
            "result": True,
            "loader": "cached",
            "workspace": self.fetchedWSname,
        }
        assert self.instance.grocer.executeRecipe.not_called
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

    def test_fetch_failed(self):
        # this is some file that it can't load
        mockFilename = Resource.getPath("inputs/crystalInfo/fake_file.cif")
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchWorkspace(mockFilename, self.fetchedWSname, loader="")
        assert self.fetchedWSname in str(e.value)
        assert mockFilename in str(e.value)

    def test_fetch_false(self):
        # this is some file that it can't load
        self.instance.grocer.executeRecipe = mock.Mock(return_value={"result": False})
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchWorkspace(self.sampleWSFilePath, self.fetchedWSname, loader="")
        assert self.fetchedWSname in str(e.value)
        assert self.sampleWSFilePath in str(e.value)

    def test_fetch_dirty_nexus_native(self):
        """Test the correct behavior when fetching raw nexus data"""
        # mock out the filename function to point to the test file
        self.instance.convertToLiteMode = mock.Mock()
        self.instance._createNeutronFilename = mock.Mock(return_value=self.sampleWSFilePath)

        # easier calls
        testItem = (self.runNumber, False)

        # ensure a clean ADS
        workspaceName = self.instance._createNeutronWorkspaceName(*testItem)
        rawWorkspaceName = self.instance._createRawNeutronWorkspaceName(*testItem)
        assert len(self.instance._loadedRuns) == 0
        assert not mtd.doesExist(workspaceName)
        assert not mtd.doesExist(rawWorkspaceName)

        # test that a nexus workspace can be loaded
        res = self.instance.fetchNeutronDataSingleUse(*testItem)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == workspaceName
        assert len(self.instance._loadedRuns) == 0
        # assert the correct workspaces exist
        assert not mtd.doesExist(rawWorkspaceName)
        assert mtd.doesExist(workspaceName)
        # test the workspace is correct
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

        # test that it will use a raw workspace if one exists
        # first clear out the ADS
        self.clearoutWorkspaces()
        assert not mtd.doesExist(rawWorkspaceName)
        assert not mtd.doesExist(workspaceName)
        # create a clean version so a raw exists in cache
        res = self.instance.fetchNeutronDataCached(*testItem)
        assert mtd.doesExist(rawWorkspaceName)
        assert not mtd.doesExist(workspaceName)
        assert len(self.instance._loadedRuns) == 1
        testKey = self.instance._key(*testItem)
        assert self.instance._loadedRuns == {testKey: 1}
        # now load a dirty version, which should clone the raw
        res = self.instance.fetchNeutronDataSingleUse(*testItem)
        assert mtd.doesExist(workspaceName)
        assert res["workspace"] == workspaceName
        assert res["loader"] == "cached"  # this indicates it used a cached value
        assert self.instance._loadedRuns == {testKey: 1}  # the number of copies did not increment
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

        # test calling with Lite data, that it will call to lite service
        liteItem = (self.runNumber, True)
        testKeyLite = self.instance._key(*liteItem)
        res = self.instance.fetchNeutronDataSingleUse(*liteItem)
        assert self.instance._loadedRuns.get(testKeyLite) is None
        workspaceNameLite = self.instance._createNeutronWorkspaceName(*liteItem)
        self.instance.convertToLiteMode.assert_called_once_with(workspaceNameLite, self.runNumber)
        assert mtd.doesExist(workspaceNameLite)

    def test_fetch_cached_native(self):
        """Test the correct behavior when fetching nexus data"""
        self.instance._createNeutronFilename = mock.Mock()

        testItem = (self.runNumber, False)
        testKey = self.instance._key(*testItem)

        workspaceNameRaw = self.instance._createRawNeutronWorkspaceName(*testItem)
        workspaceNameCopy1 = self.instance._createCopyNeutronWorkspaceName(*testItem, 1)
        workspaceNameCopy2 = self.instance._createCopyNeutronWorkspaceName(*testItem, 2)

        # make sure the ADS is clean
        self.clearoutWorkspaces()
        for ws in [workspaceNameRaw, workspaceNameCopy1, workspaceNameCopy2]:
            assert not mtd.doesExist(ws)
        assert len(self.instance._loadedRuns) == 0

        # run with nothing in cache and bad filename -- it will fail
        self.instance._createNeutronFilename.return_value = "not/a/real/file.txt"
        assert not os.path.isfile(self.instance._createNeutronFilename.return_value)
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchNeutronDataCached(*testItem)
        assert self.runNumber in str(e.value)
        assert self.instance._createNeutronFilename.return_value in str(e.value)

        # mock filename to point at test file
        self.instance._createNeutronFilename.return_value = self.sampleWSFilePath

        # run with nothing loaded -- it will find the file and load it
        res = self.instance.fetchNeutronDataCached(*testItem)
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == workspaceNameCopy1
        assert self.instance._loadedRuns == {testKey: 1}
        # assert the correct workspaces exist: a raw and a copy
        assert mtd.doesExist(workspaceNameRaw)
        assert mtd.doesExist(workspaceNameCopy1)
        # test the workspace is correct
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=workspaceNameCopy1,
        )

        # run with a raw workspace in cache -- it will copy it
        res = self.instance.fetchNeutronDataCached(*testItem)
        assert res["result"]
        assert res["loader"] == "cached"  # indicates no loader called
        assert res["workspace"] == workspaceNameCopy2
        assert self.instance._loadedRuns == {testKey: 2}
        # assert the correct workspaces exist: a raw and two copies
        assert mtd.doesExist(workspaceNameRaw)
        assert mtd.doesExist(workspaceNameCopy1)
        assert mtd.doesExist(workspaceNameCopy2)
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=workspaceNameCopy2,
        )

    def test_fetch_cached_lite(self):
        """
        Test the correct behavior when fetching nexus data in Lite mode.
        The cases where the data is cached or the file exists are tested above.
        This tests cases of using native-resolution data and auto-reducing.
        """
        self.instance.convertToLiteMode = mock.Mock()
        self.instance._createNeutronFilename = mock.Mock()

        testItem = (self.runNumber, True)
        testKey = self.instance._key(*testItem)
        nativeItem = (self.runNumber, False)
        nativeKey = self.instance._key(*nativeItem)

        workspaceNameNativeRaw = self.instance._createRawNeutronWorkspaceName(*nativeItem)
        workspaceNameLiteRaw = self.instance._createRawNeutronWorkspaceName(*testItem)
        workspaceNameLiteCopy1 = self.instance._createCopyNeutronWorkspaceName(*testItem, 1)

        # make sure the ADS is clean
        self.clearoutWorkspaces()
        for ws in [workspaceNameNativeRaw, workspaceNameLiteRaw, workspaceNameLiteCopy1]:
            assert not mtd.doesExist(ws)
        assert len(self.instance._loadedRuns) == 0

        # test that trying to load data from a fake file fails
        # will reach the final "else" statement
        fakeFilename = "not/a/real/file.txt"
        self.instance._createNeutronFilename.return_value = fakeFilename
        assert not os.path.isfile(self.instance._createNeutronFilename.return_value)
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchNeutronDataCached(*testItem)
        assert self.runNumber in str(e.value)
        assert fakeFilename in str(e.value)

        # mock filename -- create situation where Lite file does not exist, native does
        self.instance._createNeutronFilename.side_effect = [
            fakeFilename,  # called in the assert below
            fakeFilename,  # called at beginning of function looking for Lite file
            self.sampleWSFilePath,  # called inside elif when looking for Native file
            self.sampleWSFilePath,  # called inside the block when making the filename variable
        ]
        assert not os.path.isfile(self.instance._createNeutronFilename(*testItem))

        # there is no lite file and nothing cached
        # load native resolution from file, then clone/reduce the native data
        res = self.instance.fetchNeutronDataCached(*testItem)
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == workspaceNameLiteCopy1
        for ws in [workspaceNameNativeRaw, workspaceNameLiteRaw, workspaceNameLiteCopy1]:
            assert mtd.doesExist(ws)
        assert self.instance._loadedRuns == {testKey: 1, nativeKey: 0}
        # make sure it calls to convert to lite mode
        assert self.instance.convertToLiteMode.called_once

        # clear out the Lite workspaces from ADS and the cache
        self.instance._createNeutronFilename.side_effect = [fakeFilename, self.sampleWSFilePath]
        for ws in [workspaceNameLiteRaw, workspaceNameLiteCopy1]:
            DeleteWorkspace(ws)
            assert not mtd.doesExist(ws)
        del self.instance._loadedRuns[testKey]

        # there is no lite file and no cached lite workspace
        # but there is a cached native workspace
        # then clone/reduce the native workspace
        assert mtd.doesExist(workspaceNameNativeRaw)
        assert self.instance._loadedRuns == {nativeKey: 0}
        res = self.instance.fetchNeutronDataCached(*testItem)
        assert res["result"]
        assert res["loader"] == "cached"
        assert res["workspace"] == workspaceNameLiteCopy1
        for ws in [workspaceNameNativeRaw, workspaceNameLiteRaw, workspaceNameLiteCopy1]:
            assert mtd.doesExist(ws)
        assert self.instance._loadedRuns == {testKey: 1, nativeKey: 0}
        # make sure it calls to convert to lite mode
        assert self.instance.convertToLiteMode.called_once

    def test_fetch_grouping(self):
        groupFilepath = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
        self.instance._createGroupingFilename = mock.Mock(return_value=groupFilepath)
        testItem = (self.groupingItem.groupingScheme, self.runNumber, self.groupingItem.useLiteMode)

        # call once and load
        groupingWorkspaceName = self.instance._createGroupingWorkspaceName(*testItem)
        groupKey = self.instance._key(*testItem)
        res = self.instance.fetchGroupingDefinition(self.groupingItem)
        assert res["result"]
        assert res["loader"] == "LoadGroupingDefinition"
        assert res["workspace"] == groupingWorkspaceName
        assert self.instance._loadedGroupings == {groupKey: groupingWorkspaceName}

        # call again and no load
        res = self.instance.fetchGroupingDefinition(self.groupingItem)
        assert res["result"]
        assert res["loader"] == "cached"
        assert res["workspace"] == groupingWorkspaceName
        assert self.instance._loadedGroupings == {groupKey: groupingWorkspaceName}

    def test_failed_fetch_grouping(self):
        # this is some file that it can't load
        fakeFilepath = Resource.getPath("inputs/crystalInfo/fake_file.cif")
        self.instance._createGroupingFilename = mock.Mock(return_value=fakeFilepath)
        with pytest.raises(RuntimeError):
            self.instance.fetchGroupingDefinition(self.groupingItem)

    @mock.patch(ThisService + "GroceryListItem")
    def test_fetch_lite_data_map(self, mockGroceryList):
        """
        This is a special case of the fetch grouping method.
        """
        groupMapFilepath = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value="lite_map")
        self.instance._createGroupingFilename = mock.Mock(return_value=groupMapFilepath)
        # have to subvert the validation methods in grocerylistitem
        mockLiteMapGroceryItem = GroceryListItem(
            workspaceType="grouping",
            runNumber=self.runNumber,
            groupingScheme="Lite",
        )
        mockLiteMapGroceryItem.instrumentSource = self.instrumentFilePath
        mockGroceryList.builder.return_value.grouping.return_value.build.return_value = mockLiteMapGroceryItem

        # call once and load
        testItem = ("Lite", GroceryListItem.RESERVED_NATIVE_RUNNUMBER, False)
        groupingWorkspaceName = self.instance._createGroupingWorkspaceName(*testItem)
        groupKey = self.instance._key(*testItem)

        res = self.instance.fetchLiteDataMap()
        assert res == groupingWorkspaceName
        assert self.instance._loadedGroupings == {groupKey: groupingWorkspaceName}

    def test_fetch_grocery_list(self):
        # expected workspaces
        cleanWorkspace = mock.Mock()
        dirtyWorkspace = mock.Mock()
        groupWorkspace = mock.Mock()

        self.instance.fetchNeutronDataCached = mock.Mock(return_value={"result": True, "workspace": cleanWorkspace})
        self.instance.fetchNeutronDataSingleUse = mock.Mock(return_value={"result": True, "workspace": dirtyWorkspace})
        self.instance.fetchGroupingDefinition = mock.Mock(return_value={"result": True, "workspace": groupWorkspace})

        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).add()
        clerk.native().neutron(self.runNumber).dirty().add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).source(InstrumentDonor=self.sampleWS).add()
        groceryList = clerk.buildList()

        res = self.instance.fetchGroceryList(groceryList)

        assert res == [cleanWorkspace, dirtyWorkspace, groupWorkspace]
        for i, fetchMethod in enumerate(
            [self.instance.fetchNeutronDataCached, self.instance.fetchNeutronDataSingleUse]
        ):
            assert fetchMethod.called_once_with(
                groceryList[i].runNumber,
                groceryList[i].useLiteMode,
                groceryList[i].loader,
            )
        assert self.instance.fetchGroupingDefinition.called_once_with(groceryList[2])

    def test_fetch_grocery_list_fails(self):
        self.instance.fetchNeutronDataSingleUse = mock.Mock(return_value={"result": False, "workspace": "unimportant"})
        groceryList = GroceryListItem.builder().native().neutron(self.runNumber).dirty().buildList()
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchGroceryList(groceryList)
        print(str(e.value))

    def test_fetch_grocery_list_diffcal_fails(self):
        groceryList = GroceryListItem.builder().native().diffcal(self.runNumber).buildList()
        with pytest.raises(
            RuntimeError,
            match="not implemented: no path available to fetch diffcal",
        ):
            self.instance.fetchGroceryList(groceryList)

    def test_fetch_grocery_list_specialOrder_diffcal_output(self):
        # Test of workspace type "diffcal_output" as `Output` (i.e. "specialOrder") argument in the `GroceryList`
        self.instance._getDetectorState = mock.Mock(return_value=self.detectorState1)
        groceryList = (
            GroceryListItem.builder()
            .specialOrder()
            .native()
            .diffcal_output(self.runNumber)
            .group(wng.Groups.UNFOC)
            .unit(wng.Units.TOF)
            .buildList()
        )
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == wng.diffCalOutput().unit(wng.Units.TOF).runNumber(self.runNumber).build()

    def test_fetch_grocery_list_specialOrder_diffcal_table(self):
        # Test of workspace type "diffcal_table" as `Output` (i.e. "specialOrder") argument in the `GroceryList`
        self.instance._getDetectorState = mock.Mock(return_value=self.detectorState1)
        groceryList = GroceryListItem.builder().specialOrder().native().diffcal_table(self.runNumber).buildList()
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == wng.diffCalTable().runNumber(self.runNumber).build() + f"_{wnvf.formatVersion(self.version)}"

    def test_fetch_grocery_list_specialOrder_diffcal_mask(self):
        # Test of workspace type "diffcal_mask" as `Output` (i.e. "specialOrder") argument in the `GroceryList`
        self.instance._getDetectorState = mock.Mock(return_value=self.detectorState1)
        groceryList = GroceryListItem.builder().specialOrder().native().diffcal_mask(self.runNumber).buildList()
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == wng.diffCalMask().runNumber(self.runNumber).build()

    def test_fetch_grocery_list_diffcal_output(self):
        # Test of workspace type "diffcal_output" as `Input` argument in the `GroceryList`
        path = Resource.getPath("outputs")
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            self.instance.dataService._constructCalibrationDataPath.return_value = tmpPath
            groceryList = (
                GroceryListItem.builder()
                .native()
                .diffcal_output(self.runNumber1)
                .unit(wng.Units.TOF)
                .group(wng.Groups.UNFOC)
                .buildList()
            )

            diffCalOutputFilename = self.diffCalOutputName + Config["calibration.diffraction.output.extension"]
            shutil.copy2(self.sampleTarWsFilePath, Path(tmpPath) / diffCalOutputFilename)
            assert (Path(tmpPath) / diffCalOutputFilename).exists()

            assert not mtd.doesExist(self.diffCalOutputName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == self.diffCalOutputName
            assert mtd.doesExist(self.diffCalOutputName)

    def test_fetch_grocery_list_diffcal_output_cached(self):
        # Test of workspace type "diffcal_output" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        groceryList = (
            GroceryListItem.builder()
            .native()
            .diffcal_output(self.runNumber1)
            .unit(wng.Units.TOF)
            .group(wng.Groups.UNFOC)
            .buildList()
        )
        diffCalOutputName = (
            wng.diffCalOutput().unit(wng.Units.TOF).group(wng.Groups.UNFOC).runNumber(self.runNumber1).build()
        )
        CloneWorkspace(
            InputWorkspace=self.sampleWS,
            OutputWorkspace=diffCalOutputName,
        )
        assert mtd.doesExist(diffCalOutputName)
        testTitle = "I'm a little teapot"
        mtd[diffCalOutputName].setTitle(testTitle)
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == diffCalOutputName
        assert mtd.doesExist(diffCalOutputName)
        assert mtd[diffCalOutputName].getTitle() == testTitle

    def test_fetch_grocery_list_diffcal_table(self):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`
        path = Resource.getPath("outputs")
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            self.instance.dataService._constructCalibrationDataPath.return_value = tmpPath
            groceryList = GroceryListItem.builder().native().diffcal_table(self.runNumber1).buildList()
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).build()
            record = mock.Mock(runNumber=self.runNumber1, version=1)
            diffCalTableName = f"{diffCalTableName}_{wnvf.formatVersion(record.version)}"
            diffCalTableFilename = diffCalTableName + ".h5"
            shutil.copy2(self.sampleDiffCalFilePath, Path(tmpPath) / diffCalTableFilename)
            assert (Path(tmpPath) / diffCalTableFilename).exists()

            assert not mtd.doesExist(diffCalTableName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalTableName
            assert mtd.doesExist(diffCalTableName)

    def test_fetch_grocery_list_diffcal_table_cached(self):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        groceryList = GroceryListItem.builder().native().diffcal_table(self.runNumber1).buildList()
        diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).build()
        diffCalTableName = f"{diffCalTableName}_{wnvf.formatVersion(self.version)}"

        CloneWorkspace(
            InputWorkspace=self.sampleTableWS,
            OutputWorkspace=diffCalTableName,
        )
        assert mtd.doesExist(diffCalTableName)
        testTitle = "I'm a little teapot"
        mtd[diffCalTableName].setTitle(testTitle)
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == diffCalTableName
        assert mtd.doesExist(diffCalTableName)
        assert mtd[diffCalTableName].getTitle() == testTitle

    def test_fetch_grocery_list_diffcal_table_loads_mask(self):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`:
        #   * corresponding mask workspace is also loaded from the hdf5-format file.
        path = Resource.getPath("outputs")
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            self.instance.dataService._constructCalibrationDataPath.return_value = tmpPath
            groceryList = GroceryListItem.builder().native().diffcal_table(self.runNumber1).buildList()
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).build()
            diffCalTableName = f"{diffCalTableName}_{wnvf.formatVersion(self.version)}"
            diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).build()
            diffCalMaskName = f"{diffCalMaskName}_{wnvf.formatVersion(self.version)}"
            diffCalTableFilename = f"{diffCalTableName}.h5"
            shutil.copy2(self.sampleDiffCalFilePath, Path(tmpPath) / diffCalTableFilename)
            assert (Path(tmpPath) / diffCalTableFilename).exists()

            assert not mtd.doesExist(diffCalTableName)
            assert not mtd.doesExist(diffCalMaskName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalTableName
            assert mtd.doesExist(diffCalTableName)
            assert mtd.doesExist(diffCalMaskName)

    def test_fetch_grocery_list_diffcal_mask(self):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`
        path = Resource.getPath("outputs")
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            self.instance.dataService._constructCalibrationDataPath.return_value = tmpPath
            groceryList = GroceryListItem.builder().native().diffcal_mask(self.runNumber1).buildList()
            diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).build()

            # DiffCal filename is constructed from the table name
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).build()
            diffCalTableFilename = diffCalTableName + ".h5"
            shutil.copy2(self.sampleDiffCalFilePath, Path(tmpPath) / diffCalTableFilename)
            assert (Path(tmpPath) / diffCalTableFilename).exists()

            assert not mtd.doesExist(diffCalMaskName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalMaskName
            assert mtd.doesExist(diffCalMaskName)

    def test_fetch_grocery_list_diffcal_mask_cached(self):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        groceryList = GroceryListItem.builder().native().diffcal_mask(self.runNumber1).buildList()
        diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).build()
        CloneWorkspace(
            InputWorkspace=self.sampleMaskWS,
            OutputWorkspace=diffCalMaskName,
        )
        assert mtd.doesExist(diffCalMaskName)
        testTitle = "I'm a little teapot"
        mtd[diffCalMaskName].setTitle(testTitle)

        # Reloading is triggered based on whether or not the corresponding table workspace is in the ADS.
        diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).build()
        CloneWorkspace(
            InputWorkspace=self.sampleTableWS,
            OutputWorkspace=diffCalTableName,
        )
        assert mtd.doesExist(diffCalTableName)

        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == diffCalMaskName
        assert mtd.doesExist(diffCalMaskName)
        assert mtd[diffCalMaskName].getTitle() == testTitle

    def test_fetch_grocery_list_diffcal_mask_loads_table(self):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`:
        #   * corresponding table workspace is also loaded from the hdf5-format file.
        path = Resource.getPath("outputs")
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            self.instance.dataService._constructCalibrationDataPath.return_value = tmpPath
            groceryList = GroceryListItem.builder().native().diffcal_mask(self.runNumber1).buildList()
            diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).build()

            # DiffCal filename is constructed from the table name
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).build()
            diffCalTableFilename = diffCalTableName + ".h5"
            shutil.copy2(self.sampleDiffCalFilePath, Path(tmpPath) / diffCalTableFilename)
            assert (Path(tmpPath) / diffCalTableFilename).exists()

            assert not mtd.doesExist(diffCalMaskName)
            assert not mtd.doesExist(diffCalTableName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalMaskName
            assert mtd.doesExist(diffCalMaskName)
            assert mtd.doesExist(diffCalTableName)

    def test_fetch_grocery_list_normalization(self):
        # Test of workspace type "normalization" as `Input` argument in the `GroceryList`
        path = Resource.getPath("outputs")
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            self.instance.dataService._constructCalibrationDataPath.return_value = tmpPath
            groceryList = GroceryListItem.builder().native().normalization(self.runNumber1).buildList()

            # normalization filename is constructed
            normalizationWorkspaceName = wng.rawVanadium().runNumber(self.runNumber1).build()
            normalizationFilename = normalizationWorkspaceName + ".nxs"
            shutil.copy2(self.sampleWSFilePath, Path(tmpPath) / normalizationFilename)
            assert (Path(tmpPath) / normalizationFilename).exists()

            assert not mtd.doesExist(normalizationWorkspaceName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == normalizationWorkspaceName
            assert mtd.doesExist(normalizationWorkspaceName)

    def test_fetch_grocery_list_normalization_cached(self):
        # Test of workspace type "normalization" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        groceryList = GroceryListItem.builder().native().normalization(self.runNumber1).buildList()
        normalizationWorkspaceName = wng.rawVanadium().runNumber(self.runNumber1).build()
        CloneWorkspace(
            InputWorkspace=self.sampleWS,
            OutputWorkspace=normalizationWorkspaceName,
        )
        assert mtd.doesExist(normalizationWorkspaceName)
        testTitle = "I'm a little teapot"
        mtd[normalizationWorkspaceName].setTitle(testTitle)

        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == normalizationWorkspaceName
        assert mtd.doesExist(normalizationWorkspaceName)
        assert mtd[normalizationWorkspaceName].getTitle() == testTitle

    def test_fetch_grocery_list_unknown_type(self):
        groceryList = GroceryListItem.builder().native().diffcal_mask(self.runNumber).buildList()
        groceryList[0].workspaceType = "banana"
        with pytest.raises(
            RuntimeError,
            match="unrecognized 'workspaceType': 'banana'",
        ):
            self.instance.fetchGroceryList(groceryList)

    def test_fetch_grocery_list_with_source(self):
        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = mock.Mock()

        self.instance.fetchNeutronDataCached = mock.Mock(return_value={"result": True, "workspace": cleanWorkspace})
        self.instance.fetchGroupingDefinition = mock.Mock(return_value={"result": True, "workspace": groupWorkspace})

        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).source(
            InstrumentDonor=cleanWorkspace
        ).add()
        groceryList = clerk.buildList()

        groupItemWithSource = (
            clerk.native()
            .fromRun(self.runNumber)
            .grouping(self.groupingScheme)
            .source(InstrumentDonor=cleanWorkspace)
            .build()
        )

        res = self.instance.fetchGroceryList(groceryList)
        assert res == [cleanWorkspace, groupWorkspace]
        assert self.instance.fetchGroupingDefinition.called_with(groupItemWithSource)
        assert self.instance.fetchNeutronDataCached.called_with(self.runNumber, False, "")

    def test_update_instrument_parameters(self):
        tmpName = mtd.unique_hidden_name()
        CloneWorkspace(
            InputWorkspace=self.sampleWS,
            OutputWorkspace=tmpName,
        )
        assert mtd.doesExist(tmpName)

        # Verify that there is a _zero_ relative location prior to updating instrument location parameters
        ws = mtd[tmpName]
        # Exact float comparison is intended: these should match
        assert ws.getInstrument().getComponentByName("West").getRelativeRot() == Quat(1.0, 0.0, 0.0, 0.0)
        assert ws.getInstrument().getComponentByName("West").getRelativePos() == V3D(0.0, 0.0, 0.0)
        assert ws.getInstrument().getComponentByName("East").getRelativeRot() == Quat(1.0, 0.0, 0.0, 0.0)
        assert ws.getInstrument().getComponentByName("East").getRelativePos() == V3D(0.0, 0.0, 0.0)

        self.instance.updateInstrumentParameters(tmpName, self.detectorState1)

        # Verify that all of the log-derived parameters have been added
        sampleLogs = mapFromSampleLogs(tmpName, ("det_lin1", "det_lin2", "det_arc1", "det_arc2"))

        # Exact float comparison is intended: these should match
        assert sampleLogs["det_lin1"] == self.detectorState1.lin[0]
        assert sampleLogs["det_lin2"] == self.detectorState1.lin[1]
        assert sampleLogs["det_arc1"] == self.detectorState1.arc[0]
        assert sampleLogs["det_arc2"] == self.detectorState1.arc[1]

        # Verify that the relative location changes correctly after updating the instrument location parameters
        ws = mtd[tmpName]
        assert pytest.approx(tupleFromQuat(ws.getInstrument().getComponentByName("West").getRelativeRot()), 1.0e-6) == (
            -0.008726535498373997,
            0.0,
            0.9999619230641713,
            0.0,
        )
        assert pytest.approx(tupleFromV3D(ws.getInstrument().getComponentByName("West").getRelativePos()), 1.0e-6) == (
            0.09598823540505931,
            0.0,
            5.499162323360152,
        )
        assert pytest.approx(tupleFromQuat(ws.getInstrument().getComponentByName("East").getRelativeRot()), 1.0e-6) == (
            -0.017452406437283477,
            0.0,
            0.9998476951563913,
            0.0,
        )
        assert pytest.approx(tupleFromV3D(ws.getInstrument().getComponentByName("East").getRelativePos()), 1.0e-6) == (
            0.2268467285662563,
            0.0,
            6.496040375624123,
        )

    def test_fetch_instrument_donor_neutron_data(self):
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=self.sampleWS)
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {(self.runNumber, self.useLiteMode): 0}
        self.instance._loadedInstruments = {}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
        assert testWS == self.sampleWS

    def test_fetch_instrument_donor_neutron_data_is_cached(self):
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=self.sampleWS)
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {(self.runNumber, self.useLiteMode): 0}
        self.instance._loadedInstruments = {}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)  # noqa: F841
        assert self.instance._loadedInstruments == {(self.runNumber, self.useLiteMode): self.sampleWS}

    def test_fetch_instrument_donor_cached(self):
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {}
        self.instance._loadedInstruments = {(self.runNumber, self.useLiteMode): self.sampleWS}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
        assert testWS == self.sampleWS

    def test_fetch_instrument_donor_empty_instrument(self):
        self.instance._getDetectorState = mock.Mock(return_value=self.detectorState2)
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {}
        self.instance._loadedInstruments = {}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
        assert mtd.doesExist(testWS)
        assert not testWS == self.sampleWS

        # Verify that the instrument workspace has log-derived parameters from `self.detectorState2`.
        sampleLogs = mapFromSampleLogs(testWS, ("det_lin1", "det_lin2", "det_arc1", "det_arc2"))

        # Exact float comparison is intended: these should match
        assert sampleLogs["det_lin1"] == self.detectorState2.lin[0]
        assert sampleLogs["det_lin2"] == self.detectorState2.lin[1]
        assert sampleLogs["det_arc1"] == self.detectorState2.arc[0]
        assert sampleLogs["det_arc2"] == self.detectorState2.arc[1]

    def test_fetch_instrument_donor_empty_instrument_is_cached(self):
        self.instance._getDetectorState = mock.Mock(return_value=self.detectorState2)
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {}
        self.instance._loadedInstruments = {}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
        assert self.instance._loadedInstruments == {(self.runNumber, self.useLiteMode): testWS}

    def test_update_instrument_cache_from_ADS_to_cache(self):
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=self.sampleWS)
        self.instance._loadedInstruments = {}
        self.instance._updateInstrumentCacheFromADS(self.runNumber, self.useLiteMode)
        assert self.instance._loadedInstruments == {(self.runNumber, self.useLiteMode): self.sampleWS}

    def test_update_instrument_cache_from_ADS_cache_del(self):
        testWSName = "not_any_workspace"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=testWSName)
        self.instance._loadedInstruments = {(self.runNumber, self.useLiteMode): testWSName}
        self.instance._updateInstrumentCacheFromADS(self.runNumber, self.useLiteMode)
        assert self.instance._loadedInstruments == {}

    def test_get_detector_state(self):
        detectorState = self.instance._getDetectorState(self.runNumber)
        assert detectorState == self.detectorState1

    @mock.patch(ThisService + "mtd")
    def test_unique_hidden_name(self, mockADS):
        testWSName = "not_any_workspace"
        mockADS.unique_hidden_name.return_value = testWSName
        wsName = self.instance.uniqueHiddenName()
        assert wsName == testWSName

    def test_fetch_grocery_dict(self):
        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = "groupme"
        self.instance.fetchGroceryList = mock.Mock(return_value=[cleanWorkspace, groupWorkspace])

        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).name("InputWorkspace").add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).name("GroupingWorkspace").add()
        groceryDict = clerk.buildDict()

        res = self.instance.fetchGroceryDict(groceryDict)
        assert res == {"InputWorkspace": cleanWorkspace, "GroupingWorkspace": groupWorkspace}
        assert self.instance.fetchGroceryList.called_once_with(
            groceryDict["InputWorkspace"], groceryDict["GroupingWorkspace"]
        )

    def test_fetch_grocery_dict_with_kwargs(self):
        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = "groupme"
        otherWorkspace = "somethingelse"
        self.instance.fetchGroceryList = mock.Mock(return_value=[cleanWorkspace, groupWorkspace])

        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).name("InputWorkspace").add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).name("GroupingWorkspace").add()
        groceryDict = clerk.buildDict()

        res = self.instance.fetchGroceryDict(groceryDict, OtherWorkspace=otherWorkspace)
        assert res == {
            "InputWorkspace": cleanWorkspace,
            "GroupingWorkspace": groupWorkspace,
            "OtherWorkspace": otherWorkspace,
        }
        assert self.instance.fetchGroceryList.called_once_with(
            groceryDict["InputWorkspace"],
            groceryDict["GroupingWorkspace"],
        )

    def test_fetch_default_diffcal_table(self):
        """
        Use the test instrument to create a default DIFC table from the method.
        Compare to known values, found independently in mantid workbench.
        """
        runNumber = "123"
        useLiteMode = True
        testVersion = 17

        ## Create the reference table
        refTable = mtd.unique_name(prefix="_table_")
        # these values found from mantid workbench, CalculateDIFC with test instrument
        known_difc = [
            2434.197617645125,
            1561.1814075314217,
            2741.860927442078,
            2058.4132086720383,
            2741.860927442078,
            2434.197617645125,
            2058.4132086720383,
            1561.1814075314217,
            1561.1814075314217,
            2058.4132086720383,
            2434.197617645125,
            2741.860927442078,
            2058.4132086720383,
            1561.1814075314217,
            2741.860927442078,
            2434.197617645125,
        ]
        known_list_of_dict = [
            {"detid": int(i), "difc": known_difc[i], "difa": 0.0, "tzero": 0.0} for i in range(len(known_difc))
        ]
        GenerateTableWorkspaceFromListOfDict(
            OutputWorkspace=refTable,
            ListOfDict=json.dumps(known_list_of_dict),
        )

        ## Create the default diffcal table
        idfWS = mtd.unique_name(prefix="_idf_")
        LoadEmptyInstrument(
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
            OutputWorkspace=idfWS,
        )
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=idfWS)
        ws = self.instance.fetchDefaultDiffCalTable(runNumber, useLiteMode, testVersion)

        # make sure the correct workspace name is generated
        assert ws == self.instance._createDiffcalTableWorkspaceName("default", useLiteMode, testVersion)
        ## Compare the two diffcal tables to ensure equality
        table1 = mtd[ws]
        table2 = mtd[refTable]
        assert table1.rowCount() == table2.rowCount()
        for i in range(table1.rowCount()):
            print(list(table1.row(i).values()), list(table2.row(i).values()))
            assert list(table1.row(i).values()) == list(table2.row(i).values())

        ## Ensure graceful failure if the workspace is not created
        self.instance.workspaceDoesExist = mock.Mock(return_value=False)
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchDefaultDiffCalTable(runNumber, useLiteMode, testVersion)
        assert runNumber in str(e)

    @mock.patch("snapred.backend.service.LiteDataService.LiteDataService")
    def test_make_lite(self, mockLDS):
        # mock out the fetch grouping part to make it think it fetched the ltie data map
        liteMapWorkspace = "unimportant"
        self.instance.fetchGroupingDefinition = mock.MagicMock(return_value={"workspace": liteMapWorkspace})

        # now call to make lite
        workspacename = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        self.instance.convertToLiteMode(workspacename, self.runNumber)
        # assert the call to lite data mode fetched the lite data map
        assert self.instance.fetchGroupingDefinition.called_once_with("Lite", False)
        # assert that the lite data service was created and called
        assert mockLDS.called_once()
        assert mockLDS.reduceLiteData.called_once_with(workspacename, workspacename)

    def test_getCachedWorkspaces(self):
        rawWsName = self.instance._createRawNeutronWorkspaceName("556854", False)
        self.instance._loadedRuns = {("556854", False): 0}
        self.instance._loadedGroupings = {("column", "556854", True): "d"}
        self.instance._loadedInstruments = {("556854", False): rawWsName, ("556855", True): "hello"}
        assert set(self.instance.getCachedWorkspaces()) == set([rawWsName, "d", "hello"])

    def test_getCachedWorkspaces_empty(self):
        self.instance._loadedRuns = {}
        self.instance._loadedGroupings = {}
        self.instance._loadedInstruments = {}

        assert self.instance.getCachedWorkspaces() == []

    def test_renameWorkspace(self):
        oldName = "old"
        newName = "new"
        self.create_dumb_workspace(oldName)
        assert mtd.doesExist(oldName)
        self.instance.renameWorkspace(oldName, newName)
        assert not mtd.doesExist(oldName)
        assert mtd.doesExist(newName)

    def test_clearADS(self):
        rawWsName = self.instance._createRawNeutronWorkspaceName(0, "a")
        self.instance._loadedRuns = {(0, "a"): rawWsName}
        self.instance._loadedGroupings = {(1, "c"): "d"}

        rebuildCache = self.instance.rebuildCache
        self.instance.rebuildCache = mock.Mock()

        self.create_dumb_workspace("b")
        self.instance.clearADS(exclude=self.exclude)

        assert mtd.doesExist(rawWsName) is False
        self.create_dumb_workspace(rawWsName)
        assert mtd.doesExist(rawWsName) is True

        self.instance.clearADS(exclude=self.exclude, cache=False)

        assert mtd.doesExist(rawWsName) is True
        self.instance.rebuildCache.assert_called()
        self.instance.rebuildCache = rebuildCache
