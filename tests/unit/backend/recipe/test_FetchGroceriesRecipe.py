# ruff: noqa: ARG001, E501, PT012

import os
import unittest
from unittest import mock

import pytest
from mantid.simpleapi import (
    CompareWorkspaces,
    CreateWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    SaveNexusProcessed,
    mtd,
)
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.recipe.FetchGroceriesRecipe import (
    FetchGroceriesRecipe as Recipe,  # noqa: E402
)
from snapred.meta.Config import Config, Resource

TheAlgorithmManager: str = "snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager"


@mock.patch("mantid.simpleapi.GetIPTS", mock.Mock(return_value="nowhere"))
class TestFetchGroceriesRecipe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create a mock data file which will be loaded in tests.
        This is created at the start of this test suite, then deleted at the end.
        """
        cls.runNumber = "555"
        cls.filepath = Resource.getPath(f"inputs/test_{cls.runNumber}_fetchgroceriesrx.nxs")
        cls.instrumentFilepath = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        cls.fetchedWSname = "_fetched_grocery"
        cls.groupingScheme = "Native"
        # create some sample data
        cls.sampleWS = "_grocery_to_fetch"
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

        cls.liteMapGroceryItem = GroceryListItem(
            workspaceType="grouping",
            groupingScheme="Lite",
            useLiteMode=False,
        )

    def setUp(self) -> None:
        self.nexusItem = GroceryListItem(
            workspaceType="nexus",
            runNumber=self.runNumber,
            useLiteMode=False,
        )
        self.groupingItem = GroceryListItem(
            workspaceType="grouping",
            groupingScheme=self.groupingScheme,
            useLiteMode=False,
            instrumentPropertySource="InstrumentDonor",
            instrumentSource=self.sampleWS,
        )
        self.groceryList = [self.nexusItem, self.groupingItem]

    def clearoutWorkspaces(self) -> None:
        """Delete the workspaces created by loading"""
        for ws in mtd.getObjectNames():
            if ws != self.sampleWS:
                try:
                    DeleteWorkspace(ws)
                except:  # noqa: E722
                    pass

    def tearDown(self) -> None:
        """At the end of each test, clear out the workspaces"""
        self.clearoutWorkspaces()
        return super().tearDown()

    @classmethod
    def tearDownClass(cls) -> None:
        """
        At the end of the test suite, delete all workspaces
        and remove the test file.
        """
        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        os.remove(cls.filepath)
        return super().tearDownClass()

    ### TESTS OF FILENAME AND WORKSPACE NAME METHODS

    def test_key(self):
        rx = Recipe()
        assert rx._key(self.nexusItem) == (self.runNumber, False)
        assert rx._key(self.nexusItem.toggleLiteMode()) == (self.runNumber, True)

    def test_nexus_filename(self):
        """Test the creation of the nexus filename"""
        rx = Recipe()
        res = rx._createNexusFilename(self.nexusItem)
        assert self.nexusItem.IPTS in res
        assert Config["nexus.native.prefix"] in res
        assert self.runNumber in res
        assert "lite" not in res.lower()

        # now use lite mode
        res = rx._createNexusFilename(self.nexusItem.toggleLiteMode())
        assert self.nexusItem.IPTS in res
        assert Config["nexus.lite.prefix"] in res
        assert self.runNumber in res
        assert "lite" in res.lower()

    def test_nexus_workspacename_plain(self):
        """Test the creation of a plain nexus workspace name"""
        rx = Recipe()
        res = rx._createNexusWorkspaceName(self.nexusItem)
        assert res == f"tof_all_{self.runNumber}"
        # now use lite mode
        res = rx._createNexusWorkspaceName(self.nexusItem.toggleLiteMode())
        assert res == f"tof_all_lite_{self.runNumber}"

    def test_nexus_workspacename_raw(self):
        """Test the creation of a raw nexus workspace name"""
        rx = Recipe()
        res = rx._createRawNexusWorkspaceName(self.nexusItem)
        plain = rx._createNexusWorkspaceName(self.nexusItem)
        assert res == plain + "_raw"

    def test_nexus_workspacename_copy(self):
        """Test the creation of a copy nexus workspace name"""
        rx = Recipe()
        fakeCopies = 117
        res = rx._createCopyNexusWorkspaceName(self.nexusItem, fakeCopies)
        plain = rx._createNexusWorkspaceName(self.nexusItem)
        assert res == plain + f"_copy{fakeCopies}"

    def test_grouping_filename(self):
        """Test the creation of the grouping filename"""
        uniqueGroupingScheme = "Fruitcake"
        self.groupingItem.groupingScheme = uniqueGroupingScheme
        rx = Recipe()
        res = rx._createGroupingFilename(self.groupingItem)
        assert uniqueGroupingScheme in res
        assert "lite" not in res.lower()
        # test lite mode
        res = rx._createGroupingFilename(self.groupingItem.toggleLiteMode())
        assert uniqueGroupingScheme in res
        assert "lite" in res.lower()

    def test_grouping_workspacename(self):
        """Test the creation of a grouping workspace name"""
        uniqueGroupingScheme = "Fruitcake"
        self.groupingItem.groupingScheme = uniqueGroupingScheme
        rx = Recipe()
        res = rx._createGroupingWorkspaceName(self.groupingItem)
        assert uniqueGroupingScheme in res
        assert "lite" not in res.lower()
        res = rx._createGroupingWorkspaceName(self.groupingItem.toggleLiteMode())
        assert uniqueGroupingScheme in res
        assert "lite" in res.lower()

    def test_litedatamap_filename(self):
        """Test it will return name of Lite data map"""
        rx = Recipe()
        res = rx._createGroupingFilename(self.liteMapGroceryItem)
        assert res == str(Config["instrument.lite.map.file"])

    def test_litedatamap_workspacename(self):
        """Test the creation of name for Lite data map"""
        rx = Recipe()
        res = rx._createGroupingWorkspaceName(self.liteMapGroceryItem)
        assert res == "lite_grouping_map"

    ### TESTS OF FETCH METHODS

    def test_fetch(self):
        """Test the correct behavior of the fetch method"""
        self.clearoutWorkspaces()
        rx = Recipe()
        res = rx._fetch(self.filepath, self.fetchedWSname, "")
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == self.fetchedWSname
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

        # make sure it won't load same workspace name again
        assert mtd.doesExist(self.fetchedWSname)
        res = rx._fetch(self.filepath, self.fetchedWSname, res["loader"])
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == ""  # this makes sure no loader called
        assert res["workspace"] == self.fetchedWSname
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

    def test_fetch_failed(self):
        # this is some file that it can't load
        mockFilename = Resource.getPath("inputs/crystalInfo/fake_file.cif")
        rx = Recipe()
        with pytest.raises(RuntimeError):
            rx._fetch(mockFilename, self.fetchedWSname, "")

    @mock.patch.object(Recipe, "_makeLite")
    @mock.patch.object(Recipe, "_createNexusFilename")
    def test_fetch_dirty_nexus(self, mockFilename, mockMakeLite):
        """Test the correct behavior when fetching raw nexus data"""
        mockFilename.return_value = self.filepath

        # set the grocery item to not keep a clean copy
        self.nexusItem.keepItClean = False

        rx = Recipe()
        # ensure a clean ADS
        workspaceName = rx._createNexusWorkspaceName(self.nexusItem)
        rawWorkspaceName = rx._createRawNexusWorkspaceName(self.nexusItem)
        assert len(rx._loadedRuns) == 0
        assert not mtd.doesExist(workspaceName)
        assert not mtd.doesExist(rawWorkspaceName)

        # test that a nexus workspace can be loaded
        res = rx.fetchDirtyNexusData(self.nexusItem)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == workspaceName
        assert len(rx._loadedRuns) == 0
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
        res = rx.fetchCleanNexusData(self.nexusItem)
        assert mtd.doesExist(rawWorkspaceName)
        assert not mtd.doesExist(workspaceName)
        assert len(rx._loadedRuns) == 1
        testKey = rx._key(self.nexusItem)
        assert rx._loadedRuns == {testKey: 1}
        # now load a dirty version, which should clone the raw
        res = rx.fetchDirtyNexusData(self.nexusItem)
        assert mtd.doesExist(workspaceName)
        assert res["workspace"] == workspaceName
        assert res["loader"] == "cached"  # this indicates it used a cached value
        assert rx._loadedRuns == {testKey: 1}  # the number of copies did not increment
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

        # test calling with Lite data, that it will call to _makeLite
        self.nexusItem.toggleLiteMode()
        testKeyLite = rx._key(self.nexusItem)
        res = rx.fetchDirtyNexusData(self.nexusItem)
        assert rx._loadedRuns.get(testKeyLite) is None
        workspaceNameLite = rx._createNexusWorkspaceName(self.nexusItem)
        mockMakeLite.assert_called_once_with(workspaceNameLite)
        assert mtd.doesExist(workspaceNameLite)

    @mock.patch.object(Recipe, "fetchCleanNexusData")
    def test_fetch_dirty_calls_clean(self, mockFetchClean):
        """
        Test that is a grocery list item with keepItClean=True is sent to
        the dirty loader, that it will cal the clean loader instead
        """
        mockFetchClean.return_value = {"loader": "the clean version"}
        # set keepItClean for sanity sake
        self.nexusItem.keepItClean = True
        rx = Recipe()
        res = rx.fetchDirtyNexusData(self.nexusItem)
        assert mockFetchClean.called_once_with(self.nexusItem)
        assert res == mockFetchClean.return_value

    @mock.patch.object(Recipe, "fetchCleanNexusDataLite")
    @mock.patch.object(Recipe, "_createNexusFilename")
    def test_fetch_clean_nexus(self, mockFilename, mockFetchLite):  # noqa: ARG002
        """Test the correct behavior when fetching nexus data"""
        mockFilename.return_value = self.filepath

        workspaceNameRaw = Recipe()._createRawNexusWorkspaceName(self.nexusItem)
        workspaceNameCopy1 = Recipe()._createCopyNexusWorkspaceName(self.nexusItem, 1)
        workspaceNameCopy2 = Recipe()._createCopyNexusWorkspaceName(self.nexusItem, 2)

        # make sure the ADS is clean
        self.clearoutWorkspaces()
        for ws in [workspaceNameRaw, workspaceNameCopy1, workspaceNameCopy2]:
            assert not mtd.doesExist(ws)

        rx = Recipe()
        assert len(rx._loadedRuns) == 0
        testKey = rx._key(self.nexusItem)

        # test that a nexus workspace can be loaded
        res = rx.fetchCleanNexusData(self.nexusItem)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == workspaceNameCopy1
        assert len(rx._loadedRuns) == 1
        assert rx._loadedRuns == {testKey: 1}
        # assert the correct workspaces exist: a raw and a copy
        assert mtd.doesExist(workspaceNameRaw)
        assert mtd.doesExist(workspaceNameCopy1)
        # test the workspace is correct
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=workspaceNameCopy1,
        )

        # test that trying to load data twice only makes a new copy
        res = rx.fetchCleanNexusData(self.nexusItem)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "cached"  # indicates no loader called
        assert res["workspace"] == workspaceNameCopy2
        assert len(rx._loadedRuns) == 1
        assert rx._loadedRuns == {testKey: 2}
        # assert the correct workspaces exist: a raw and two copies
        assert mtd.doesExist(workspaceNameRaw)
        assert mtd.doesExist(workspaceNameCopy1)
        assert mtd.doesExist(workspaceNameCopy2)
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=workspaceNameCopy2,
        )

        # test calling with Lite data, that it will call the lite mode version instead
        mockFetchLite.return_value = {"loader": "called the lite method"}
        self.nexusItem.toggleLiteMode()
        res = rx.fetchCleanNexusData(self.nexusItem)
        assert res == mockFetchLite.return_value
        assert mockFetchLite.called_once_with(self.nexusItem)

    @mock.patch.object(Recipe, "_createGroupingFilename")
    def test_fetch_grouping(self, mockFilename):
        mockFilename.return_value = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
        rx = Recipe()

        # call once and load
        groupingWorkspaceName = rx._createGroupingWorkspaceName(self.groupingItem)
        groupKey = rx._key(self.groupingItem)
        res = rx.fetchGroupingDefinition(self.groupingItem)
        assert res["result"]
        assert res["loader"] == "LoadGroupingDefinition"
        assert res["workspace"] == groupingWorkspaceName
        assert rx._loadedGroupings == {groupKey: groupingWorkspaceName}

        # call once and no load
        res = rx.fetchGroupingDefinition(self.groupingItem)
        assert res["result"]
        assert res["loader"] == "cached"
        assert res["workspace"] == groupingWorkspaceName
        assert rx._loadedGroupings == {groupKey: groupingWorkspaceName}

    @mock.patch.object(Recipe, "_createGroupingFilename")
    def test_failed_fetch_grouping(self, mockFilename):
        # this is some file that it can't load
        mockFilename.return_value = Resource.getPath("inputs/crystalInfo/fake_file.cif")
        rx = Recipe()
        with pytest.raises(RuntimeError):
            rx.fetchGroupingDefinition(self.groupingItem)

    @mock.patch.object(Recipe, "_createGroupingFilename")
    @mock.patch.object(Recipe, "_createNexusFilename")
    def test_fetch_grocery_list(self, mockNexusFilename, mockGroupFilename):
        mockNexusFilename.return_value = self.filepath
        mockGroupFilename.return_value = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
        # add an item meant to load a dirty copy
        dirtyItem = GroceryListItem.makeNativeNexusItem(self.runNumber + "1")
        dirtyItem.keepItClean = False
        self.groceryList.append(dirtyItem)
        # run the recipe
        rx = Recipe()
        res = rx.executeRecipe(self.groceryList)
        assert res["result"]
        assert res.get("workspaces") is not None
        assert len(res["workspaces"]) == len(self.groceryList)
        assert res["workspaces"][0] == rx._createCopyNexusWorkspaceName(self.nexusItem, 1)
        assert res["workspaces"][1] == rx._createGroupingWorkspaceName(self.groupingItem)
        assert self.groceryList[1].instrumentSource != res["workspaces"][1]
        # test the correct workspaces exist
        assert mtd.doesExist(rx._createRawNexusWorkspaceName(self.nexusItem))
        assert mtd.doesExist(res["workspaces"][0])
        assert mtd.doesExist(res["workspaces"][1])

    @mock.patch.object(Recipe, "_createGroupingFilename")
    @mock.patch.object(Recipe, "_createNexusFilename")
    def test_fetch_grocery_list_with_prev(self, mockNexusFilename, mockGroupFilename):
        mockNexusFilename.return_value = self.filepath
        mockGroupFilename.return_value = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
        self.groupingItem.instrumentSource = "prev"
        rx = Recipe()
        res = rx.executeRecipe(self.groceryList)
        assert res["result"]
        assert res.get("workspaces") is not None
        assert len(res["workspaces"]) == len(self.groceryList)
        assert self.groceryList[1].instrumentSource == res["workspaces"][0]

    @mock.patch.object(Recipe, "_createNexusFilename")
    def test_fetch_grocery_list_check_ads(self, mockNexusFilename):
        mockNexusFilename.return_value = self.filepath

        # load data so the raw file exists in the ADS
        self.groceryList = [self.nexusItem]
        rx = Recipe()
        testKey = rx._key(self.nexusItem)
        rawWorkspaceName = rx._createRawNexusWorkspaceName(self.nexusItem)
        copyWorkspaceName = rx._createCopyNexusWorkspaceName(self.nexusItem, 1)
        rx.executeRecipe(self.groceryList)
        assert mtd.doesExist(rawWorkspaceName)
        assert mtd.doesExist(copyWorkspaceName)
        assert rx._loadedRuns[testKey] == 1

        # now clear it out from the ADS
        rx._loadedRuns = {}
        DeleteWorkspace(copyWorkspaceName)
        assert mtd.doesExist(rawWorkspaceName)
        assert not mtd.doesExist(copyWorkspaceName)
        assert rx._loadedRuns.get(testKey) is None

        # re-run, and it should copy the raw fiel from the ADS
        rx._fetch = mock.Mock()
        rx.executeRecipe(self.groceryList)
        assert rx._loadedRuns[testKey] > 0
        rx._fetch.assert_not_called()
        assert mtd.doesExist(rawWorkspaceName)
        assert mtd.doesExist(copyWorkspaceName)

    ## TEST LITE MODE METHODS

    @mock.patch("snapred.backend.recipe.FetchGroceriesRecipe.LiteDataAlgo")
    def test_make_lite(self, mockLDC):
        rx = Recipe()

        # mock out fetchGroupingDefinition so that it
        # makes the recipe think the lite map was loaded
        def set_key(x):
            rx._loadedGroupings[("Lite", False)] = x

        rx.fetchGroupingDefinition = mock.MagicMock(wraps=set_key)

        # now call to make lite
        workspacename = rx._createNexusWorkspaceName(self.nexusItem)
        rx._makeLite(workspacename)
        # assert the lite data algo was created, initialize, and had ptoperties set
        mockLDC.assert_called_once()
        mockLDC().initialize.assert_called_once()
        assert mockLDC().setPropertyValue.call_count == 3
        calls = [
            mock.call("InputWorkspace", workspacename),
            mock.call("OutputWorkspace", workspacename),
            mock.call("LiteDataMapWorkspace", rx._loadedGroupings[("Lite", False)]),
        ]
        mockLDC().setPropertyValue.call_args_list == calls
        # assert that the call to _makeLite called to fetch the lite map
        rx.fetchGroupingDefinition.assert_called_once_with(
            GroceryListItem(
                workspaceType="grouping",
                groupingScheme="Lite",
                useLiteMode=False,
                instrumentPropertySource="InstrumentFilename",
                instrumentSource=str(Config["instrument.native.definition.file"]),
            )
        )

    @mock.patch.object(Recipe, "_makeLite")
    @mock.patch.object(Recipe, "_createNexusFilename")
    def test_fetch_clean_nexus_lite(self, mockFilename, mockMakeLite):
        """Test the correct behavior when fetching lite nexus data"""
        mockFilename.return_value = self.filepath

        rx = Recipe()
        assert len(rx._loadedRuns) == 0

        nativeRawWorkspace = rx._createRawNexusWorkspaceName(self.nexusItem)
        rx._createCopyNexusWorkspaceName(self.nexusItem, 1)
        rx._createCopyNexusWorkspaceName(self.nexusItem, 2)
        assert not mtd.doesExist(nativeRawWorkspace)

        # make sure nothing good happens if we try to use this with non-lite data
        res = rx.fetchCleanNexusDataLite(self.nexusItem)
        assert res["result"] is False
        assert res["loader"] == ""
        assert not mtd.doesExist(nativeRawWorkspace)
        nativeKey = rx._key(self.nexusItem)

        # switch to lite mode
        self.nexusItem.toggleLiteMode(True)
        testKey = rx._key(self.nexusItem)

        liteRawWorkspace = rx._createRawNexusWorkspaceName(self.nexusItem)
        liteCopyWorkspace1 = rx._createCopyNexusWorkspaceName(self.nexusItem, 1)
        liteCopyWorkspace2 = rx._createCopyNexusWorkspaceName(self.nexusItem, 2)
        assert not mtd.doesExist(liteRawWorkspace)

        # run with nothing loaded -- it will find the file and load it
        assert rx._loadedRuns.get(testKey) is None
        res = rx.fetchCleanNexusDataLite(self.nexusItem)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == liteCopyWorkspace1
        assert len(rx._loadedRuns) == 1
        assert rx._loadedRuns == {testKey: 1}
        # assert the correct workspaced exist: a raw and a copy
        assert mtd.doesExist(liteRawWorkspace)
        assert mtd.doesExist(liteCopyWorkspace1)

        # run with a raw workspace in cache -- it will copy it
        assert rx._loadedRuns[testKey] > 0
        res = rx.fetchCleanNexusDataLite(self.nexusItem)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "cached"
        assert res["workspace"] == liteCopyWorkspace2
        assert len(rx._loadedRuns) == 1
        assert rx._loadedRuns == {testKey: 2}
        # assert the correct workspaced exist: a raw and two copies
        assert mtd.doesExist(liteRawWorkspace)
        assert mtd.doesExist(liteCopyWorkspace1)
        assert mtd.doesExist(liteCopyWorkspace2)

        # test situation where the Lite file does not exist
        mockFilename.side_effect = [
            "this_is_not_a_file",
            "this_is_not_a_file",
            self.filepath,
        ]
        assert not os.path.isfile(mockFilename())

        self.clearoutWorkspaces()
        assert not mtd.doesExist(nativeRawWorkspace)
        assert not mtd.doesExist(liteRawWorkspace)
        assert not mtd.doesExist(liteCopyWorkspace1)
        assert not mtd.doesExist(liteCopyWorkspace2)
        rx._loadedRuns = {}

        # there is no lite file and nothing cached
        # load native resolution data instead
        res = rx.fetchCleanNexusDataLite(self.nexusItem)
        assert rx._createNexusFilename.call_count == 5
        mockMakeLite.assert_called_once_with(liteRawWorkspace)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "cached"
        assert res["workspace"] == liteCopyWorkspace1
        assert len(rx._loadedRuns) == 2
        assert rx._loadedRuns == {testKey: 1, nativeKey: 0}
        #
        assert mtd.doesExist(nativeRawWorkspace)
        assert mtd.doesExist(liteRawWorkspace)
        assert mtd.doesExist(liteCopyWorkspace1)

        # clear out ADS before next test
        self.clearoutWorkspaces()
        assert not mtd.doesExist(nativeRawWorkspace)
        assert not mtd.doesExist(liteRawWorkspace)
        assert not mtd.doesExist(liteCopyWorkspace1)
        rx._loadedRuns = {}

        mockFilename.side_effect = [
            "this_is_not_a_file",
            self.filepath,
            "this_is_not_a_file",
            self.filepath,
        ]
        assert not os.path.isfile(mockFilename())

        # there is no lite file and no cached lite file
        # but there is a native file in cache
        res = rx.fetchCleanNexusData(self.nexusItem.toggleLiteMode(False))
        assert mtd.doesExist(nativeRawWorkspace)
        res = rx.fetchCleanNexusDataLite(self.nexusItem.toggleLiteMode(True))
        assert res["loader"]
        assert res["workspace"] == liteCopyWorkspace1
        assert len(rx._loadedRuns) == 2
        assert rx._loadedRuns == {testKey: 1, nativeKey: 1}
        assert mtd.doesExist(liteRawWorkspace)
        assert mtd.doesExist(liteCopyWorkspace1)
        mockMakeLite.assert_called_with(liteRawWorkspace)


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004]
    """Remove handlers from all loggers"""
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
