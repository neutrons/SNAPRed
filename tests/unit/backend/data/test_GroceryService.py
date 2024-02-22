# ruff: noqa: E722, PT011, PT012

import os
import tempfile
import unittest
from curses import raw
from unittest import mock

import pytest
from mantid.simpleapi import (
    CompareWorkspaces,
    CreateEmptyTableWorkspace,
    CreateGroupingWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    SaveNexusProcessed,
    mtd,
)
from pydantic import ValidationError
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

ThisService = "snapred.backend.data.GroceryService."


class TestGroceryService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create a mock data file which will be loaded in tests.
        This is created at the start of this test suite, then deleted at the end.
        """
        cls.runNumber = "555"
        cls.filepath = Resource.getPath(f"inputs/test_{cls.runNumber}_groceryservice.nxs")
        cls.instrumentFilepath = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        cls.fetchedWSname = "_fetched_grocery"
        cls.groupingScheme = "Native"
        # create some sample data
        cls.sampleWS = "_grocery_to_fetch"
        cls.exclude = [cls.sampleWS, cls.fetchedWSname]
        CreateWorkspace(
            OutputWorkspace=cls.sampleWS,
            DataX=[1] * 16,
            DataY=[1] * 16,
            NSpec=16,
        )
        # load an instrument into sample data
        LoadInstrument(
            Workspace=cls.sampleWS,
            Filename=cls.instrumentFilepath,
            InstrumentName="fakeSNAP",
            RewriteSpectraMap=True,
        )
        SaveNexusProcessed(
            InputWorkspace=cls.sampleWS,
            Filename=cls.filepath,
        )
        assert os.path.exists(cls.filepath)

    def setUp(self):
        self.instance = GroceryService()
        self.groupingItem = (
            GroceryListItem.builder()
            .grouping(self.groupingScheme)
            .native()
            .source(InstrumentDonor=self.sampleWS)
            .build()
        )
        return super().setUp()

    def clearoutWorkspaces(self) -> None:
        """Delete the workspaces created by loading"""
        for ws in mtd.getObjectNames():
            if ws != self.sampleWS:
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
        os.remove(cls.filepath)
        return super().tearDownClass()

    def create_dumb_workspace(self, wsname):
        CreateWorkspace(
            OutputWorkspace=wsname,
            DataX=[1],
            DataY=[1],
        )

    def create_dumb_diffcal(self, wsname):
        ws = CreateEmptyTableWorkspace(OutputWorkspace=wsname)
        ws.addColumn(type="int", name="detid", plottype=6)
        ws.addRow({"detid": 0})

    ## TESTS OF FILENAME METHODS

    @mock.patch("mantid.simpleapi.GetIPTS")
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
        grouping1 = "555"
        grouping2 = "556"
        lite = True
        native = False
        # two keys with different groupings are different
        assert self.instance._key(grouping1, lite) != self.instance._key(grouping2, lite)
        # two keys with same grouping but different resolution are different
        assert self.instance._key(grouping1, lite) != self.instance._key(grouping1, native)
        # two keys with different grouping  and resolution are different
        assert self.instance._key(grouping1, lite) != self.instance._key(grouping2, native)
        assert self.instance._key(grouping1, native) != self.instance._key(grouping2, lite)
        # it is shaped like itself
        assert self.instance._key(grouping1, lite) == self.instance._key(grouping1, lite)
        assert self.instance._key(grouping1, native) == self.instance._key(grouping1, native)

    @mock.patch("mantid.simpleapi.GetIPTS", mock.Mock(return_value="nowhere/"))
    def test_nexus_filename(self):
        """Test the creation of the nexus filename"""
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
        uniqueGroupingScheme = "Fruitcake"
        self.groupingItem.groupingScheme = uniqueGroupingScheme
        res = self.instance._createGroupingFilename(uniqueGroupingScheme, False)
        assert uniqueGroupingScheme in res
        assert "lite" not in res.lower()
        # test lite mode
        res = self.instance._createGroupingFilename(uniqueGroupingScheme, True)
        assert uniqueGroupingScheme in res
        assert "lite" in res.lower()

    def test_litedatamap_filename(self):
        """Test it will return name of Lite data map"""
        res = self.instance._createGroupingFilename("Lite", False)
        assert res == str(Config["instrument.lite.map.file"])
        res2 = self.instance._createGroupingFilename("Lite", True)
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
        res = self.instance._createGroupingWorkspaceName(uniqueGroupingScheme, False)
        assert uniqueGroupingScheme in res
        assert "lite" not in res.lower()
        res = self.instance._createGroupingWorkspaceName(uniqueGroupingScheme, True)
        assert uniqueGroupingScheme in res
        assert "lite" in res.lower()

    def test_litedatamap_workspacename(self):
        """Test the creation of name for Lite data map"""
        res = self.instance._createGroupingWorkspaceName("Lite", True)
        assert res == "lite_grouping_map"

    def test_diffcal_input_workspacename(self):
        # Test name generation for diffraction-calibration focussed-data workspace
        res = self.instance._createDiffcalInputWorkspaceName(self.runNumber)
        assert "tof" in res
        assert self.runNumber in res
        assert "raw" in res

    def test_diffcal_output_workspacename(self):
        # Test name generation for diffraction-calibration output workspace
        res = self.instance._createDiffcalOutputWorkspaceName(self.runNumber)
        assert "dsp" in res
        assert self.runNumber in res
        assert "diffoc" in res

    def test_diffcal_table_workspacename(self):
        # Test name generation for diffraction-calibration output workspace
        res = self.instance._createDiffcalTableWorkspaceName(self.runNumber)
        assert "difc" in res
        assert self.runNumber in res

    def test_diffcal_mask_workspacename(self):
        # Test name generation for diffraction-calibration output workspace
        res = self.instance._createDiffcalMaskWorkspaceName(self.runNumber)
        assert "difc" in res
        assert self.runNumber in res
        assert "mask" in res

    ## TESTS OF WRITING METHODS

    def test_writeWorkspace(self):
        path = Resource.getPath("outputs")
        name = "test_write_workspace"
        self.create_dumb_workspace(name)
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmppath:
            self.instance.writeWorkspace(os.path.join(tmppath, name), name)
            assert os.path.exists(os.path.join(tmppath, name))
        assert not os.path.exists(os.path.join(tmppath, name))

    def test_writeGrouping(self):
        path = Resource.getPath("outputs")
        name = "test_write_grouping.hdf"
        CreateGroupingWorkspace(
            OutputWorkspace=name,
            CustomGroupingString="1",
            InstrumentFilename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
        )
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmppath:
            self.instance.writeGrouping(tmppath, name)
            assert os.path.exists(os.path.join(tmppath, name))
        assert not os.path.exists(os.path.join(tmppath, name))

    def test_writeDiffCalTable(self):
        path = Resource.getPath("outputs")
        name = "test_write_diffcal"
        self.create_dumb_diffcal(name)
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmppath:
            self.instance.writeDiffCalTable(tmppath, name)
            assert os.path.exists(os.path.join(tmppath, name))
        assert not os.path.exists(os.path.join(tmppath, name))

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

    def test_getWorkspaceForName_unexists(self):
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
        groupingScheme = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = (groupingScheme, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        assert self.instance._loadedGroupings == {}
        assert not mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        assert self.instance._createGroupingWorkspaceName.called_once_with(groupingScheme, wsname)
        assert self.instance._loadedGroupings == {}
        assert not mtd.doesExist(wsname)

    def test_updateGroupingCacheFromADS_both(self):
        """ws is in cache and in ADS"""
        groupingScheme = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = (groupingScheme, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedGroupings[key] = wsname
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        assert self.instance._createGroupingWorkspaceName.called_once_with(groupingScheme, wsname)
        assert self.instance._loadedGroupings == {key: wsname}

    def test_updateGroupingCacheFromADS_cache_no_ADS(self):
        """ws is in cache but not ADS"""
        groupingScheme = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = (groupingScheme, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedGroupings[key] = wsname
        assert not mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        assert self.instance._createGroupingWorkspaceName.called_once_with(groupingScheme, wsname)
        assert self.instance._loadedGroupings == {}

    def test_updateGroupingCacheFromADS_ADS_no_cache(self):
        """ws is in ADS and not in cache"""
        groupingScheme = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = (groupingScheme, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        self.create_dumb_workspace(wsname)
        assert self.instance._loadedGroupings == {}
        assert mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        assert self.instance._createGroupingWorkspaceName.called_once_with(groupingScheme, wsname)
        assert self.instance._loadedGroupings == {key: wsname}

    def test_rebuildGroupingCache(self):
        """Test the correct behavior of the rebuildGroupingCache method"""
        groupingScheme = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = (groupingScheme, useLiteMode)
        self.instance._loadedGroupings[key] = wsname
        self.instance.rebuildGroupingCache()
        assert self.instance._loadedGroupings == {}

    def test_rebuildNeutronCache(self):
        """Test the correct behavior of the rebuildNeutronCache method"""
        runId = "555"
        useLiteMode = True
        key = (runId, useLiteMode)
        self.instance._loadedRuns[key] = 1
        self.instance.rebuildNeutronCache()
        assert self.instance._loadedRuns == {}

    def test_rebuildCache(self):
        """Test the correct behavior of the rebuildCache method"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = (runId, useLiteMode)
        self.instance._loadedRuns[key] = 1
        self.instance._loadedGroupings[key] = wsname
        self.instance.rebuildCache()
        assert self.instance._loadedRuns == {}
        assert self.instance._loadedGroupings == {}

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

    def test_fetch(self):
        """Test the correct behavior of the fetch method"""
        self.clearoutWorkspaces()
        res = self.instance.fetchWorkspace(self.filepath, self.fetchedWSname, "")
        assert res == self.fetchedWSname
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res,
        )

        # make sure it won't load same workspace name again
        self.instance.grocer.executeRecipe = mock.Mock(return_value="bad")
        assert mtd.doesExist(self.fetchedWSname)
        res = self.instance.fetchWorkspace(self.filepath, self.fetchedWSname, "")
        assert res == self.fetchedWSname
        assert self.instance.grocer.executeRecipe.not_called
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res,
        )

    def test_fetch_failed(self):
        # this is some file that it can't load
        mockFilename = Resource.getPath("inputs/crystalInfo/fake_file.cif")
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchWorkspace(mockFilename, self.fetchedWSname, "")
        assert self.fetchedWSname in str(e.value)
        assert mockFilename in str(e.value)

    def test_fetch_false(self):
        # this is some file that it can't load
        self.instance.grocer.executeRecipe = mock.Mock(return_value={"result": False})
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchWorkspace(self.filepath, self.fetchedWSname, "")
        assert self.fetchedWSname in str(e.value)
        assert self.filepath in str(e.value)

    @mock.patch.object(GroceryService, "convertToLiteMode")
    @mock.patch.object(GroceryService, "_createNeutronFilename")
    def test_fetch_dirty_nexus_native(self, mockFilename, mockMakeLite):
        """Test the correct behavior when fetching raw nexus data"""
        # patch the filename function to point to the test file
        mockFilename.return_value = self.filepath

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
        mockFilename.return_value = self.filepath

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
            self.filepath,  # called inside elif when looking for Native file
            self.filepath,  # called inside the block when making the filename variable
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
        mockFilename.side_effect = [fakeFilename, self.filepath]
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
        testItem = (self.groupingItem.groupingScheme, self.groupingItem.useLiteMode)

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
    @mock.patch.object(GroceryService, "_createGroupingWorkspaceName")
    @mock.patch.object(GroceryService, "_createGroupingFilename")
    def test_fetch_lite_data_map(self, mockFilename, mockWSName, mockGroceryList):
        """
        This is a special case of the fetch grouping method.
        """
        mockFilename.return_value = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
        mockWSName.return_value = "lite_map"
        # have to subvert the validation methods in grocerylistitem
        mockLiteMapGroceryItem = GroceryListItem(workspaceType="grouping", groupingScheme="Lite")
        mockLiteMapGroceryItem.instrumentSource = self.instrumentFilepath
        mockGroceryList.builder.return_value.grouping.return_value.build.return_value = mockLiteMapGroceryItem

        # call once and load
        testItem = ("Lite", False)
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
        clerk.native().grouping(self.groupingScheme).source(InstrumentDonor=self.sampleWS).add()
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

    def test_fetch_grocery_list_diffcal_output(self):
        groceryList = GroceryListItem.builder().native().diffcal_output(self.runNumber).buildList()
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == wng.diffCalOutput().runNumber(self.runNumber).build()

    def test_fetch_grocery_list_diffcal_table(self):
        groceryList = GroceryListItem.builder().native().diffcal_table(self.runNumber).buildList()
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == wng.diffCalTable().runNumber(self.runNumber).build()

    def test_fetch_grocery_list_diffcal_mask(self):
        groceryList = GroceryListItem.builder().native().diffcal_mask(self.runNumber).buildList()
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == wng.diffCalMask().runNumber(self.runNumber).build()

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
    def test_fetch_grocery_list_with_prev(self, mockFetchGroup, mockFetchClean):
        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).add()
        clerk.native().grouping(self.groupingScheme).fromPrev().add()
        groceryList = clerk.buildList()

        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = mock.Mock()

        mockFetchClean.return_value = {"result": True, "workspace": cleanWorkspace}
        mockFetchGroup.return_value = {"result": True, "workspace": groupWorkspace}

        groupItemWithoutPrev = (
            clerk.native().grouping(self.groupingScheme).source(InstrumentDonor=cleanWorkspace).build()
        )

        res = self.instance.fetchGroceryList(groceryList)
        assert res == [cleanWorkspace, groupWorkspace]
        assert mockFetchGroup.called_with(groupItemWithoutPrev)
        assert mockFetchClean.called_with(self.runNumber, False, "")

    @mock.patch.object(GroceryService, "fetchGroceryList")
    def test_fetch_grocery_dict(self, mockFetchList):
        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).name("InputWorkspace").add()
        clerk.native().grouping(self.groupingScheme).fromPrev().name("GroupingWorkspace").add()
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
        clerk.native().grouping(self.groupingScheme).fromPrev().name("GroupingWorkspace").add()
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
        rawWsName = self.instance._createRawNeutronWorkspaceName(0, "a")
        self.instance._loadedRuns = {(0, "a"): "b"}
        self.instance._loadedGroupings = {(1, "c"): "d"}

        assert self.instance.getCachedWorkspaces() == [rawWsName, "d"]

    def test_getCachedWorkspaces_empty(self):
        self.instance._loadedRuns = {}
        self.instance._loadedGroupings = {}

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
