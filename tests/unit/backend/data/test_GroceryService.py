# ruff: noqa: E722, PT011, PT012

import os
import shutil
import tempfile
import unittest
from curses import raw
from pathlib import Path
from typing import Dict, Tuple
from unittest import mock

import pytest
from mantid.kernel import V3D, Quat
from mantid.simpleapi import (
    CloneWorkspace,
    CompareWorkspaces,
    CreateEmptyTableWorkspace,
    CreateGroupingWorkspace,
    CreateLogPropertyTable,
    CreateWorkspace,
    DeleteWorkspace,
    ExtractMask,
    LoadEmptyInstrument,
    LoadInstrument,
    LoadParameterFile,
    SaveDiffCal,
    SaveNexusProcessed,
    SaveParameterFile,
    mtd,
)
from pydantic import ValidationError
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.state import DetectorState
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask
from util.instrument_helpers import mapFromSampleLogs
from util.kernel_helpers import tupleFromQuat, tupleFromV3D

ThisService = "snapred.backend.data.GroceryService."


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
        cls.version = None
        cls.sampleWSFilePath = Resource.getPath(f"inputs/test_{cls.runNumber}_groceryservice.nxs")

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
        assert os.path.exists(cls.sampleWSFilePath)

        cls.detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        cls.detectorState2 = DetectorState(arc=(7.0, 8.0), wav=9.0, freq=10.0, guideStat=2, lin=(11.0, 12.0))

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

        cls.exclude = [cls.sampleWS, cls.fetchedWSname, cls.sampleTableWS, cls.sampleMaskWS]

    def setUp(self):
        self.instance = GroceryService()
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
            if ws not in (self.sampleWS, self.sampleTableWS, self.sampleMaskWS):
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
        return super().tearDownClass()

    @classmethod
    def create_dumb_workspace(cls, wsName):
        CreateWorkspace(
            OutputWorkspace=wsName,
            DataX=[1],
            DataY=[1],
        )

    ## TESTS OF FILENAME METHODS

    @mock.patch.object(LocalDataService, "getIPTS")
    def test_getIPTS(self, mockGetIPTS):
        mockGetIPTS.return_value = "here/"
        res = self.instance.getIPTS(self.runNumber)
        assert res == mockGetIPTS.return_value
        assert mockGetIPTS.called_with(
            runNumber=self.runNumber,
            instrumentName=Config["instrument.name"],
        )
        res = self.instance.getIPTS(self.runNumber, "CRACKLE")
        assert res == mockGetIPTS.return_value
        assert mockGetIPTS.called_with(
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
        grouping1 = "555"
        runId2 = "2463"
        grouping2 = "556"

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

    @mock.patch.object(LocalDataService, "getIPTS")
    def test_nexus_filename(self, mockGetIPTS):
        """Test the creation of the nexus filename"""
        mockGetIPTS.return_value = "nowhere/"
        res = self.instance._createNeutronFilename(self.runNumber, False)
        assert "nowhere" in res
        assert Config["nexus.native.prefix"] in res
        assert self.runNumber in res
        assert "lite" not in res.lower()

        # now use lite mode
        res = self.instance._createNeutronFilename(self.runNumber, True)
        assert "nowhere" in res
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
        runNumber = GroceryListItem.RESERVED_NATIVE_RUNID
        res = self.instance._createGroupingFilename(runNumber, "Lite", False)
        assert res == str(Config["instrument.lite.map.file"])
        res2 = self.instance._createGroupingFilename(runNumber, "Lite", True)
        assert res2 == res

    ## TESTS OF WORKSPACE NAME METHODS

    def test_neutron_workspacename_plain(self):
        """Test the creation of a plain nexus workspace name"""
        res = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        assert res == f"tof_all_{self.runNumber}"
        # now use lite mode
        res = self.instance._createNeutronWorkspaceName(self.runNumber, True)
        assert res == f"tof_all_lite_{self.runNumber}"

    def test_neutron_workspacename_raw(self):
        """Test the creation of a raw nexus workspace name"""
        res = self.instance._createRawNeutronWorkspaceName(self.runNumber, False)
        plain = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        assert res == plain + "_raw"

    def test_neutron_workspacename_copy(self):
        """Test the creation of a copy nexus workspace name"""
        fakeCopies = 117
        res = self.instance._createCopyNeutronWorkspaceName(self.runNumber, False, fakeCopies)
        plain = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        assert res == plain + f"_copy{fakeCopies}"

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
        res = self.instance._createGroupingWorkspaceName("Lite", self.runNumber, True)
        assert res == "lite_grouping_map"

    def test_diffcal_input_workspacename(self):
        # Test name generation for diffraction-calibration input workspace
        res = self.instance._createDiffcalInputWorkspaceName(self.runNumber)
        assert "tof" in res
        assert self.runNumber in res
        assert "raw" in res

    def test_diffcal_output_tof_workspacename(self):
        # Test name generation for diffraction-calibration focussed-data workspace
        res = self.instance._createDiffcalTOFOutputWorkspaceName(self.runNumber)
        assert "tof" in res
        assert self.runNumber in res
        assert "diffoc" in res

    def test_diffcal_output_dsp_workspacename(self):
        # Test name generation for diffraction-calibration focussed-data workspace
        res = self.instance._createDiffcalDSPOutputWorkspaceName(self.runNumber)
        assert "dsp" in res
        assert self.runNumber in res
        assert "diffoc" in res

    def test_diffcal_table_workspacename(self):
        # Test name generation for diffraction-calibration output table
        res = self.instance._createDiffcalTableWorkspaceName(self.runNumber)
        assert "difc" in res
        assert self.runNumber in res

    def test_diffcal_mask_workspacename(self):
        # Test name generation for diffraction-calibration output mask
        res = self.instance._createDiffcalMaskWorkspaceName(self.runNumber)
        assert "difc" in res
        assert self.runNumber in res
        assert "mask" in res

    @mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
    def test_diffcal_table_filename(self, mockConstructCalibrationDataPath):
        # Test name generation for diffraction-calibration table filename
        mockConstructCalibrationDataPath.return_value = "nowhere/"
        res = self.instance._createDiffcalTableFilename(self.runNumber, self.version)
        assert "difc" in res
        assert self.runNumber in res
        assert ".h5" in res

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
        assert CompareWorkspaces(
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

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    def test_updateInstrumentCacheFromADS_noop(self, mockNeutronWorkspaceName):
        """ws is not in cache and not in ADS"""
        wsname = "_test_ws_"
        mockNeutronWorkspaceName.return_value = wsname
        runId = "555"
        useLiteMode = True
        assert self.instance._loadedInstruments == {}
        assert not mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode)
        assert self.instance._loadedInstruments == {}
        assert not mtd.doesExist(wsname)

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    def test_updateInstrumentCacheFromADS_both(self, mockNeutronWorkspaceName):
        """ws is in cache and in ADS"""
        wsname = "_test_ws_"
        mockNeutronWorkspaceName.return_value = wsname
        runId = "555"
        useLiteMode = True
        key = self.instance._key(runId, useLiteMode)
        self.instance._loadedInstruments[key] = wsname
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode)
        assert self.instance._loadedInstruments == {key: wsname}

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    def test_updateInstrumentCacheFromADS_cache_no_ADS(self, mockNeutronWorkspaceName):
        """ws is in cache but not ADS"""
        wsname = "_test_ws_"
        mockNeutronWorkspaceName.return_value = wsname
        runId = "555"
        useLiteMode = True
        key = self.instance._key(runId, useLiteMode)
        self.instance._loadedInstruments[key] = wsname
        assert not mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode)
        assert self.instance._loadedInstruments == {}

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    def test_updateInstrumentCacheFromADS_ADS_no_cache(self, mockNeutronWorkspaceName):
        """ws is in ADS and not in cache"""
        wsname = "_test_ws_"
        mockNeutronWorkspaceName.return_value = wsname
        runId = "555"
        useLiteMode = True
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

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    def test_rebuildInstrumentCache(self, mockNeutronWorkspaceName):
        """Test the correct behavior of the rebuildInstrumentCache method"""
        wsname = "_test_ws_"
        mockNeutronWorkspaceName.return_value = wsname
        runId = "555"
        useLiteMode = True
        key = self.instance._key(runId, useLiteMode)
        self.instance._loadedInstruments[key] = wsname
        self.instance.rebuildInstrumentCache()
        assert self.instance._loadedInstruments == {}

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    def test_rebuildCache(self, mockNeutronWorkspaceName):
        """Test the correct behavior of the rebuildCache method"""
        wsname = "_test_ws_"
        mockNeutronWorkspaceName.return_value = wsname
        groupingScheme = "column"
        runId = "555"
        useLiteMode = True
        key = (runId, useLiteMode)
        self.instance._loadedRuns[key] = 1
        self.instance._loadedGroupings[self.instance._key(groupingScheme, runId, useLiteMode)] = wsname
        self.instance._loadedInstruments[key] = wsname
        self.instance.rebuildCache()
        assert self.instance._loadedRuns == {}
        assert self.instance._loadedGroupings == {}
        assert self.instance._loadedInstruments == {}

    ## TEST CLEANUP METHODS

    @mock.patch(ThisService + "AlgorithmManager")
    def test_deleteWorkspace(self, mockAlgoManager):
        wsname = "_test_ws"
        mockAlgo = mock.Mock()
        mockAlgoManager.create.return_value = mockAlgo
        self.instance.deleteWorkspace(wsname)
        assert mockAlgoManager.called_once_with("WashDishes")
        assert mockAlgo.setProperty.called_with(wsname)
        assert mockAlgo.execute.called_once

    @mock.patch(ThisService + "AlgorithmManager")
    def test_deleteWorkspaceUnconditional(self, mockAlgoManager):
        wsname = "_test_ws"
        mockAlgo = mock.Mock()
        mockAlgoManager.create.return_value = mockAlgo
        # if the workspace does not exist, then don't do anything
        self.instance.deleteWorkspaceUnconditional(wsname)
        assert mockAlgoManager.not_called
        assert mockAlgo.execute.not_called
        # make the workspace
        self.create_dumb_workspace(wsname)
        # if the workspace does exist, then delete it
        self.instance.deleteWorkspaceUnconditional(wsname)
        assert mockAlgoManager.called_once_with("DeleteWorkspace")
        assert mockAlgo.setProperty.called_with(wsname)
        assert mockAlgo.execute.called_once

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
        assert CompareWorkspaces(
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
        assert CompareWorkspaces(
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

    @mock.patch.object(GroceryService, "convertToLiteMode")
    @mock.patch.object(GroceryService, "_createNeutronFilename")
    def test_fetch_dirty_nexus_native(self, mockFilename, mockMakeLite):
        """Test the correct behavior when fetching raw nexus data"""
        # patch the filename function to point to the test file
        mockFilename.return_value = self.sampleWSFilePath

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
        assert CompareWorkspaces(
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
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

        # test calling with Lite data, that it will call to lite service
        liteItem = (self.runNumber, True)
        testKeyLite = self.instance._key(*liteItem)
        res = self.instance.fetchNeutronDataSingleUse(*liteItem)
        assert self.instance._loadedRuns.get(testKeyLite) is None
        workspaceNameLite = self.instance._createNeutronWorkspaceName(*liteItem)
        mockMakeLite.assert_called_once_with(workspaceNameLite)
        assert mtd.doesExist(workspaceNameLite)

    @mock.patch.object(GroceryService, "_createNeutronFilename")
    def test_fetch_cached_native(self, mockFilename):
        """Test the correct behavior when fetching nexus data"""

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
        mockFilename.return_value = "not/a/real/file.txt"
        assert not os.path.isfile(mockFilename.return_value)
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchNeutronDataCached(*testItem)
        assert self.runNumber in str(e.value)
        assert mockFilename.return_value in str(e.value)

        # mock filename to point at test file
        mockFilename.return_value = self.sampleWSFilePath

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
        assert CompareWorkspaces(
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
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=workspaceNameCopy2,
        )

    @mock.patch.object(GroceryService, "convertToLiteMode")
    @mock.patch.object(GroceryService, "_createNeutronFilename")
    def test_fetch_cached_lite(self, mockFilename, mockMakeLite):  # noqa: ARG002
        """
        Test the correct behavior when fetching nexus data in Lite mode.
        The cases where the data is cached or the file exists are tested above.
        This tests cases of using native-resolution data and auto-reducing.
        """

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
        mockFilename.return_value = fakeFilename
        assert not os.path.isfile(mockFilename.return_value)
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchNeutronDataCached(*testItem)
        assert self.runNumber in str(e.value)
        assert fakeFilename in str(e.value)

        # mock filename -- create situation where Lite file does not exist, native does
        mockFilename.side_effect = [
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
        mockFilename.side_effect = [fakeFilename, self.sampleWSFilePath]
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

    @mock.patch.object(GroceryService, "_createGroupingFilename")
    def test_fetch_grouping(self, mockFilename):
        mockFilename.return_value = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
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

    @mock.patch.object(GroceryService, "_createGroupingFilename")
    def test_failed_fetch_grouping(self, mockFilename):
        # this is some file that it can't load
        mockFilename.return_value = Resource.getPath("inputs/crystalInfo/fake_file.cif")
        with pytest.raises(RuntimeError):
            self.instance.fetchGroupingDefinition(self.groupingItem)

    @mock.patch("snapred.backend.data.GroceryService.GroceryListItem")
    @mock.patch.object(GroceryService, "_fetchInstrumentDonor")
    @mock.patch.object(GroceryService, "_createGroupingWorkspaceName")
    @mock.patch.object(GroceryService, "_createGroupingFilename")
    def test_fetch_lite_data_map(self, mockFilename, mockWSName, mockFetchInstrumentDonor, mockGroceryList):
        """
        This is a special case of the fetch grouping method.
        """
        mockFilename.return_value = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
        mockWSName.return_value = "lite_map"
        mockFetchInstrumentDonor.return_value = self.sampleWS
        # have to subvert the validation methods in grocerylistitem
        mockLiteMapGroceryItem = GroceryListItem(
            workspaceType="grouping", runNumber=self.runNumber, groupingScheme="Lite"
        )
        mockLiteMapGroceryItem.instrumentSource = self.instrumentFilePath
        mockGroceryList.builder.return_value.grouping.return_value.build.return_value = mockLiteMapGroceryItem

        # call once and load
        testItem = ("Lite", GroceryListItem.RESERVED_NATIVE_RUNID, False)
        groupingWorkspaceName = self.instance._createGroupingWorkspaceName(*testItem)
        groupKey = self.instance._key(*testItem)

        res = self.instance.fetchLiteDataMap()
        assert res == groupingWorkspaceName
        assert self.instance._loadedGroupings == {groupKey: groupingWorkspaceName}

    @mock.patch.object(GroceryService, "fetchNeutronDataCached")
    @mock.patch.object(GroceryService, "fetchNeutronDataSingleUse")
    @mock.patch.object(GroceryService, "fetchGroupingDefinition")
    def test_fetch_grocery_list(self, mockFetchGroup, mockFetchDirty, mockFetchClean):
        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).add()
        clerk.native().neutron(self.runNumber).dirty().add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).source(InstrumentDonor=self.sampleWS).add()
        groceryList = clerk.buildList()

        # expected workspaces
        cleanWorkspace = mock.Mock()
        dirtyWorkspace = mock.Mock()
        groupWorkspace = mock.Mock()

        mockFetchClean.return_value = {"result": True, "workspace": cleanWorkspace}
        mockFetchDirty.return_value = {"result": True, "workspace": dirtyWorkspace}
        mockFetchGroup.return_value = {"result": True, "workspace": groupWorkspace}

        res = self.instance.fetchGroceryList(groceryList)
        assert res == [cleanWorkspace, dirtyWorkspace, groupWorkspace]
        for i, fetchMethod in enumerate([mockFetchClean, mockFetchDirty]):
            assert fetchMethod.called_once_with(
                groceryList[i].runNumber,
                groceryList[i].useLiteMode,
                groceryList[i].loader,
            )
        assert mockFetchGroup.called_once_with(groceryList[2])

    @mock.patch.object(GroceryService, "fetchNeutronDataSingleUse")
    def test_fetch_grocery_list_fails(self, mockFetchDirty):
        groceryList = GroceryListItem.builder().native().neutron(self.runNumber).dirty().buildList()
        mockFetchDirty.return_value = {"result": False, "workspace": "unimportant"}
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

    @mock.patch.object(GroceryService, "_getDetectorState")
    def test_fetch_grocery_list_specialOrder_diffcal_output(self, mockDetectorState):
        # Test of workspace type "diffcal_output" as `Output` (i.e. "specialOrder") argument in the `GroceryList`
        mockDetectorState.return_value = self.detectorState1
        groceryList = GroceryListItem.builder().specialOrder().native().diffcal_output_tof(self.runNumber).buildList()
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == wng.diffCalOutput().runNumber(self.runNumber).build()

    @mock.patch.object(GroceryService, "_getDetectorState")
    def test_fetch_grocery_list_specialOrder_diffcal_table(self, mockDetectorState):
        # Test of workspace type "diffcal_table" as `Output` (i.e. "specialOrder") argument in the `GroceryList`
        mockDetectorState.return_value = self.detectorState1
        groceryList = GroceryListItem.builder().specialOrder().native().diffcal_table(self.runNumber).buildList()
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == wng.diffCalTable().runNumber(self.runNumber).build()

    @mock.patch.object(GroceryService, "_getDetectorState")
    def test_fetch_grocery_list_specialOrder_diffcal_mask(self, mockDetectorState):
        # Test of workspace type "diffcal_mask" as `Output` (i.e. "specialOrder") argument in the `GroceryList`
        mockDetectorState.return_value = self.detectorState1
        groceryList = GroceryListItem.builder().specialOrder().native().diffcal_mask(self.runNumber).buildList()
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == wng.diffCalMask().runNumber(self.runNumber).build()

    @mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
    def test_fetch_grocery_list_diffcal_output(self, mockCalibrationDataPath):
        # Test of workspace type "diffcal_output" as `Input` argument in the `GroceryList`
        path = Resource.getPath("outputs")
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            mockCalibrationDataPath.return_value = tmpPath
            groceryList = GroceryListItem.builder().native().diffcal_output_tof(self.runNumber1).buildList()
            diffCalOutputName = wng.diffCalOutput().runNumber(self.runNumber1).build()
            diffCalOutputFilename = diffCalOutputName + ".nxs"
            shutil.copy2(self.sampleWSFilePath, Path(tmpPath) / diffCalOutputFilename)
            assert (Path(tmpPath) / diffCalOutputFilename).exists()

            assert not mtd.doesExist(diffCalOutputName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalOutputName
            assert mtd.doesExist(diffCalOutputName)

    @mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
    def test_fetch_grocery_list_diffcal_output_cached(self, mockCalibrationDataPath):
        # Test of workspace type "diffcal_output" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        mockCalibrationDataPath.return_value = "/does/not/exist"
        groceryList = GroceryListItem.builder().native().diffcal_output_tof(self.runNumber1).buildList()
        diffCalOutputName = wng.diffCalOutput().runNumber(self.runNumber1).build()
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

    @mock.patch.object(GroceryService, "_fetchInstrumentDonor")
    @mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
    def test_fetch_grocery_list_diffcal_table(self, mockCalibrationDataPath, mockFetchInstrumentDonor):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`
        path = Resource.getPath("outputs")
        mockFetchInstrumentDonor.return_value = self.sampleWS
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            mockCalibrationDataPath.return_value = tmpPath
            groceryList = GroceryListItem.builder().native().diffcal_table(self.runNumber1).buildList()
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).build()
            diffCalTableFilename = diffCalTableName + ".h5"
            shutil.copy2(self.sampleDiffCalFilePath, Path(tmpPath) / diffCalTableFilename)
            assert (Path(tmpPath) / diffCalTableFilename).exists()

            assert not mtd.doesExist(diffCalTableName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalTableName
            assert mtd.doesExist(diffCalTableName)

    @mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
    def test_fetch_grocery_list_diffcal_table_cached(self, mockCalibrationDataPath):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        mockCalibrationDataPath.return_value = "/does/not/exist"
        groceryList = GroceryListItem.builder().native().diffcal_table(self.runNumber1).buildList()
        diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).build()
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

    @mock.patch.object(GroceryService, "_fetchInstrumentDonor")
    @mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
    def test_fetch_grocery_list_diffcal_table_loads_mask(self, mockCalibrationDataPath, mockFetchInstrumentDonor):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`:
        #   * corresponding mask workspace is also loaded from the hdf5-format file.
        path = Resource.getPath("outputs")
        mockFetchInstrumentDonor.return_value = self.sampleWS
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            mockCalibrationDataPath.return_value = tmpPath
            groceryList = GroceryListItem.builder().native().diffcal_table(self.runNumber1).buildList()
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).build()
            diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).build()
            diffCalTableFilename = diffCalTableName + ".h5"
            shutil.copy2(self.sampleDiffCalFilePath, Path(tmpPath) / diffCalTableFilename)
            assert (Path(tmpPath) / diffCalTableFilename).exists()

            assert not mtd.doesExist(diffCalTableName)
            assert not mtd.doesExist(diffCalMaskName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalTableName
            assert mtd.doesExist(diffCalTableName)
            assert mtd.doesExist(diffCalMaskName)

    @mock.patch.object(GroceryService, "_fetchInstrumentDonor")
    @mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
    def test_fetch_grocery_list_diffcal_mask(self, mockCalibrationDataPath, mockFetchInstrumentDonor):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`
        path = Resource.getPath("outputs")
        mockFetchInstrumentDonor.return_value = self.sampleWS
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            mockCalibrationDataPath.return_value = tmpPath
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

    @mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
    def test_fetch_grocery_list_diffcal_mask_cached(self, mockCalibrationDataPath):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        mockCalibrationDataPath.return_value = "/does/not/exist"
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

    @mock.patch.object(GroceryService, "_fetchInstrumentDonor")
    @mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
    def test_fetch_grocery_list_diffcal_mask_loads_table(self, mockCalibrationDataPath, mockFetchInstrumentDonor):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`:
        #   * corresponding table workspace is also loaded from the hdf5-format file.
        path = Resource.getPath("outputs")
        mockFetchInstrumentDonor.return_value = self.sampleWS
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
            mockCalibrationDataPath.return_value = tmpPath
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

    def test_fetch_grocery_list_unknown_type(self):
        groceryList = GroceryListItem.builder().native().diffcal_mask(self.runNumber).buildList()
        groceryList[0].workspaceType = "banana"
        with pytest.raises(
            RuntimeError,
            match="unrecognized 'workspaceType': 'banana'",
        ):
            self.instance.fetchGroceryList(groceryList)

    @mock.patch.object(GroceryService, "fetchNeutronDataCached")
    @mock.patch.object(GroceryService, "fetchGroupingDefinition")
    def test_fetch_grocery_list_with_source(self, mockFetchGroup, mockFetchClean):
        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = mock.Mock()

        mockFetchClean.return_value = {"result": True, "workspace": cleanWorkspace}
        mockFetchGroup.return_value = {"result": True, "workspace": groupWorkspace}

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
        assert mockFetchGroup.called_with(groupItemWithSource)
        assert mockFetchClean.called_with(self.runNumber, False, "")

    @mock.patch.object(GroceryService, "_getDetectorState")
    def test_update_instrument_parameters(self, mockGetDetectorState):
        mockGetDetectorState.return_value = self.detectorState1
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

        self.instance._updateInstrumentParameters(self.runNumber, tmpName)

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

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    @mock.patch.object(GroceryService, "_updateNeutronCacheFromADS")
    @mock.patch.object(GroceryService, "_updateInstrumentCacheFromADS")
    def test_fetch_instrument_donor_neutron_data(
        self,
        mockUpdateInstrumentCacheFromADS,  # noqa: ARG002
        mockUpdateNeutronCacheFromADS,  # noqa: ARG002
        mockCreateRawNeutronWorkspaceName,
    ):
        with mock.patch.dict(self.instance._loadedRuns, {(self.runNumber, self.useLiteMode): 0}), mock.patch.dict(
            self.instance._loadedInstruments, {}
        ):
            mockCreateRawNeutronWorkspaceName.return_value = self.sampleWS
            testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
            assert testWS == self.sampleWS

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    @mock.patch.object(GroceryService, "_updateNeutronCacheFromADS")
    @mock.patch.object(GroceryService, "_updateInstrumentCacheFromADS")
    def test_fetch_instrument_donor_neutron_data_is_cached(
        self,
        mockUpdateInstrumentCacheFromADS,  # noqa: ARG002
        mockUpdateNeutronCacheFromADS,  # noqa: ARG002
        mockCreateRawNeutronWorkspaceName,
    ):
        with mock.patch.dict(self.instance._loadedRuns, {(self.runNumber, self.useLiteMode): 0}), mock.patch.dict(
            self.instance._loadedInstruments, {}
        ) as mockLoadedInstruments:
            mockCreateRawNeutronWorkspaceName.return_value = self.sampleWS
            testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)  # noqa: F841
            assert mockLoadedInstruments == {(self.runNumber, self.useLiteMode): self.sampleWS}

    @mock.patch.object(GroceryService, "_updateNeutronCacheFromADS")
    @mock.patch.object(GroceryService, "_updateInstrumentCacheFromADS")
    def test_fetch_instrument_donor_cached(
        self,
        mockUpdateInstrumentCacheFromADS,  # noqa: ARG002
        mockUpdateNeutronCacheFromADS,  # noqa: ARG002
    ):
        with mock.patch.dict(self.instance._loadedRuns, {}), mock.patch.dict(
            self.instance._loadedInstruments, {(self.runNumber, self.useLiteMode): self.sampleWS}
        ):
            testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
            assert testWS == self.sampleWS

    @mock.patch.object(GroceryService, "_getDetectorState")
    @mock.patch.object(GroceryService, "_updateNeutronCacheFromADS")
    @mock.patch.object(GroceryService, "_updateInstrumentCacheFromADS")
    def test_fetch_instrument_donor_empty_instrument(
        self,
        mockUpdateInstrumentCacheFromADS,  # noqa: ARG002
        mockUpdateNeutronCacheFromADS,  # noqa: ARG002
        mockGetDetectorState,
    ):
        mockGetDetectorState.return_value = self.detectorState2
        with mock.patch.dict(self.instance._loadedRuns, {}), mock.patch.dict(self.instance._loadedInstruments, {}):
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

    @mock.patch.object(GroceryService, "_getDetectorState")
    @mock.patch.object(GroceryService, "_updateNeutronCacheFromADS")
    @mock.patch.object(GroceryService, "_updateInstrumentCacheFromADS")
    def test_fetch_instrument_donor_empty_instrument_is_cached(
        self,
        mockUpdateInstrumentCacheFromADS,  # noqa: ARG002
        mockUpdateNeutronCacheFromADS,  # noqa: ARG002
        mockGetDetectorState,
    ):
        mockGetDetectorState.return_value = self.detectorState2
        with mock.patch.dict(self.instance._loadedRuns, {}), mock.patch.dict(
            self.instance._loadedInstruments, {}
        ) as mockLoadedInstruments:
            testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
            assert mockLoadedInstruments == {(self.runNumber, self.useLiteMode): testWS}

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    def test_update_instrument_cache_from_ADS_to_cache(self, mockCreateRawNeutronWorkspaceName):
        mockCreateRawNeutronWorkspaceName.return_value = self.sampleWS
        with mock.patch.dict(self.instance._loadedInstruments, {}) as mockLoadedInstruments:
            self.instance._updateInstrumentCacheFromADS(self.runNumber, self.useLiteMode)
            assert mockLoadedInstruments == {(self.runNumber, self.useLiteMode): self.sampleWS}

    @mock.patch.object(GroceryService, "_createRawNeutronWorkspaceName")
    def test_update_instrument_cache_from_ADS_cache_del(self, mockCreateRawNeutronWorkspaceName):
        testWSName = "not_any_workspace"
        mockCreateRawNeutronWorkspaceName.return_value = testWSName
        with mock.patch.dict(
            self.instance._loadedInstruments, {(self.runNumber, self.useLiteMode): testWSName}
        ) as mockLoadedInstruments:
            self.instance._updateInstrumentCacheFromADS(self.runNumber, self.useLiteMode)
            assert mockLoadedInstruments == {}

    @mock.patch.object(LocalDataService, "readDetectorState")
    def test_get_detector_state(self, mockReadDetectorState):
        mockReadDetectorState.return_value = self.detectorState1
        detectorState = self.instance._getDetectorState(self.runNumber)
        assert detectorState == self.detectorState1

    @mock.patch(ThisService + "mtd")
    def test_unique_hidden_name(self, mockADS):
        testWSName = "not_any_workspace"
        mockADS.unique_hidden_name.return_value = testWSName
        wsName = self.instance.uniqueHiddenName()
        assert wsName == testWSName

    @mock.patch.object(GroceryService, "fetchGroceryList")
    def test_fetch_grocery_dict(self, mockFetchList):
        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).name("InputWorkspace").add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).name("GroupingWorkspace").add()
        groceryDict = clerk.buildDict()

        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = "groupme"

        mockFetchList.return_value = [cleanWorkspace, groupWorkspace]

        res = self.instance.fetchGroceryDict(groceryDict)
        assert res == {"InputWorkspace": cleanWorkspace, "GroupingWorkspace": groupWorkspace}
        assert mockFetchList.called_once_with(groceryDict["InputWorkspace"], groceryDict["GroupingWorkspace"])

    @mock.patch.object(GroceryService, "fetchGroceryList")
    def test_fetch_grocery_dict_with_kwargs(self, mockFetchList):
        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).name("InputWorkspace").add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).name("GroupingWorkspace").add()
        groceryDict = clerk.buildDict()

        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = "groupme"
        otherWorkspace = "somethingelse"

        mockFetchList.return_value = [cleanWorkspace, groupWorkspace]

        res = self.instance.fetchGroceryDict(groceryDict, OtherWorkspace=otherWorkspace)
        assert res == {
            "InputWorkspace": cleanWorkspace,
            "GroupingWorkspace": groupWorkspace,
            "OtherWorkspace": otherWorkspace,
        }
        assert mockFetchList.called_once_with(groceryDict["InputWorkspace"], groceryDict["GroupingWorkspace"])

    @mock.patch("snapred.backend.service.LiteDataService.LiteDataService")
    def test_make_lite(self, mockLDS):
        # mock out the fetch grouping part to make it think it fetched the ltie data map
        liteMapWorkspace = "unimportant"
        self.instance.fetchGroupingDefinition = mock.MagicMock(return_value={"workspace": liteMapWorkspace})

        # now call to make lite
        workspacename = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        self.instance.convertToLiteMode(workspacename)
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
