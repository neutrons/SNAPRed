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


class TestFetchGroceriesRecipe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create a mock data file which will be loaded in tests.
        This is created at the start of this test suite, then deleted at the end.
        """
        cls.runNumber = 555
        cls.runConfigLite = RunConfig(
            runNumber=str(cls.runNumber),
            IPTS=Resource.getPath("inputs/"),
            isLite=True,
        )
        cls.runConfigNonlite = RunConfig(
            runNumber=str(cls.runNumber + 1),
            IPTS=Resource.getPath("inputs/"),
            isLite=False,
        )
        cls.filepath = Resource.getPath(f"inputs/test_{cls.runNumber}_fetchgroceriesrx.nxs")
        cls.instrumentFilepath = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        cls.fetchedWSname = "_fetched_grocery"
        cls.groupingScheme = "Column"
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
        cls.groceryListItemGrouping = GroceryListItem(
            workspaceType="grouping",
            runConfig=cls.runConfigLite,
            isLite=True,
            groupingScheme=cls.groupingScheme,
            instrumentPropertySource="InstrumentDonor",
            instrumentSource=cls.sampleWS,
        )
        cls.groceryListItemNexus = GroceryListItem(
            workspaceType="nexus",
            runConfig=cls.runConfigLite,
            loader="LoadNexusProcessed",
        )
        cls.groceryList = [
            cls.groceryListItemNexus,
            cls.groceryListItemGrouping,
        ]

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

    def test_runkey(self):
        rx = Recipe()
        assert rx._runKey(self.runConfigLite) == (
            self.runConfigLite.runNumber,
            self.runConfigLite.IPTS,
            self.runConfigLite.isLite,
        )
        assert rx._runKey(self.runConfigNonlite) == (
            self.runConfigNonlite.runNumber,
            self.runConfigNonlite.IPTS,
            self.runConfigNonlite.isLite,
        )

    def test_nexus_filename(self):
        """Test the creation of the nexus filename"""
        rx = Recipe()
        res = rx._createFilenameFromRunConfig(self.runConfigLite)
        assert self.runConfigLite.IPTS in res
        assert Config["nexus.lite.prefix"] in res
        assert self.runConfigLite.runNumber in res
        assert "lite" in res.lower()
        res = rx._createFilenameFromRunConfig(self.runConfigNonlite)
        assert self.runConfigLite.IPTS in res
        assert Config["nexus.native.prefix"] in res
        assert self.runConfigNonlite.runNumber in res
        assert "lite" not in res.lower()

    def test_nexus_workspacename_plain(self):
        """Test the creation of a plain nexus workspace name"""
        rx = Recipe()
        res = rx._createNexusWorkspaceName(self.runConfigLite)
        assert res == f"_TOF_{self.runConfigLite.runNumber}_lite"
        res = rx._createNexusWorkspaceName(self.runConfigNonlite)
        assert res == f"_TOF_{self.runConfigNonlite.runNumber}"

    def test_nexus_workspacename_raw(self):
        """Test the creation of a raw nexus workspace name"""
        rx = Recipe()
        res = rx._createRawNexusWorkspaceName(self.runConfigLite)
        plain = rx._createNexusWorkspaceName(self.runConfigLite)
        assert res == plain + "_RAW"

    def test_nexus_workspacename_copy(self):
        """Test the creation of a copy nexus workspace name"""
        rx = Recipe()
        fakeCopies = 117
        res = rx._createCopyNexusWorkspaceName(self.runConfigLite, fakeCopies)
        plain = rx._createNexusWorkspaceName(self.runConfigLite)
        assert res == plain + "_copy_" + str(fakeCopies)

    def test_grouping_filename(self):
        """Test the creation of the grouping filename"""
        uniqueGroupingScheme = "Fruitcake"
        rx = Recipe()
        res = rx._createGroupingFilename(uniqueGroupingScheme, True)
        assert uniqueGroupingScheme in res
        assert "lite" in res.lower()
        res = rx._createGroupingFilename(uniqueGroupingScheme, False)
        assert uniqueGroupingScheme in res
        assert "lite" not in res.lower()

    def test_grouping_workspacename(self):
        """Test the creation of a grouping workspace name"""
        uniqueGroupingScheme = "Fruitcake"
        rx = Recipe()
        res = rx._createGroupingWorkspaceName(uniqueGroupingScheme, True)
        assert uniqueGroupingScheme in res
        assert "lite" in res.lower()
        res = rx._createGroupingWorkspaceName(uniqueGroupingScheme, False)
        assert uniqueGroupingScheme in res
        assert "lite" not in res.lower()

    def test_fecth(self):
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
        res = rx._fetch(self.filepath, self.fetchedWSname, "")
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == ""
        assert res["workspace"] == self.fetchedWSname
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

    def mockMakeLite(self, workspacename):
        from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

        print("MOCK MAKE LITE!")
        groupingWS = "test_lite_data_map"
        mantidSnapper = MantidSnapper(Recipe(), "test_rx")
        mantidSnapper.LoadGroupingDefinition(
            "Load the test instrument's map",
            GroupingFilename=Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml"),
            InstrumentFilename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
            OutputWorkspace=groupingWS,
        )
        mantidSnapper.LiteDataCreationAlgo(
            "Create lite version",
            InputWorkspace=workspacename,
            OutputWorkspace=workspacename,
            LiteDataMapWorkspace=groupingWS,
        )
        mantidSnapper.executeQueue()

    @mock.patch.object(Recipe, "_makeLite")
    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_fetch_dirty_nexus(self, mockFilename, mockLite):
        """Test the correct behavior when fetching raw nexus data"""
        mockFilename.return_value = self.filepath
        mockLite = self.mockMakeLite  # noqa: ARG002
        rx = Recipe()
        # ensure a clean ADS
        workspaceName = rx._createNexusWorkspaceName(self.runConfigLite)
        rawWorkspaceName = rx._createRawNexusWorkspaceName(self.runConfigLite)
        assert len(rx._loadedRuns) == 0
        assert not mtd.doesExist(workspaceName)
        assert not mtd.doesExist(rawWorkspaceName)

        # test that a nexus workspace can be loaded
        res = rx.fetchDirtyNexusData(self.runConfigLite)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == workspaceName
        assert len(rx._loadedRuns) == 0
        # assert the correct workspaces exists
        assert mtd.doesExist(res["workspace"])
        # test the workspace is correct
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

        # test that it will use a raw workspace if one exists
        self.clearoutWorkspaces()
        assert not mtd.doesExist(rawWorkspaceName)
        assert not mtd.doesExist(workspaceName)
        res = rx.fetchCleanNexusData(self.runConfigLite)
        assert mtd.doesExist(rawWorkspaceName)
        assert not mtd.doesExist(workspaceName)
        assert len(rx._loadedRuns) == 1
        testKeyLite = rx._runKey(self.runConfigLite)
        assert rx._loadedRuns[testKeyLite] != 0
        res = rx.fetchDirtyNexusData(self.runConfigLite)
        assert mtd.doesExist(workspaceName)
        assert res["loader"] == ""
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

    @mock.patch.object(Recipe, "_makeLite")
    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_fetch_clean_nexus(self, mockFilename, mockLite):
        """Test the correct behavior when fetching nexus data"""
        mockFilename.return_value = self.filepath
        mockLite = self.mockMakeLite  # noqa: ARG002
        # make sure the workspace is clean
        self.clearoutWorkspaces()
        assert not mtd.doesExist(Recipe()._createRawNexusWorkspaceName(self.runConfigLite))
        assert not mtd.doesExist(Recipe()._createCopyNexusWorkspaceName(self.runConfigLite, 1))
        assert not mtd.doesExist(Recipe()._createCopyNexusWorkspaceName(self.runConfigLite, 2))
        assert not mtd.doesExist(Recipe()._createRawNexusWorkspaceName(self.runConfigNonlite))
        assert not mtd.doesExist(Recipe()._createCopyNexusWorkspaceName(self.runConfigNonlite, 1))

        rx = Recipe()
        assert len(rx._loadedRuns) == 0
        testKeyLite = rx._runKey(self.runConfigLite)
        testKeyNonlite = rx._runKey(self.runConfigNonlite)

        # test that a nexus workspace can be loaded
        res = rx.fetchCleanNexusData(self.runConfigLite)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == rx._createCopyNexusWorkspaceName(self.runConfigLite, 1)
        assert len(rx._loadedRuns) == 1
        assert rx._loadedRuns == {testKeyLite: 1}
        # assert the correct workspaces exist: a raw and a copy
        assert mtd.doesExist(rx._createRawNexusWorkspaceName(self.runConfigLite))
        assert mtd.doesExist(rx._createCopyNexusWorkspaceName(self.runConfigLite, 1))
        # test the workspace is correct
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

        # test that trying to load data twicce only makes a new copy
        res = rx.fetchCleanNexusData(self.runConfigLite)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == ""
        assert res["workspace"] == rx._createCopyNexusWorkspaceName(self.runConfigLite, 2)
        assert len(rx._loadedRuns) == 1
        assert rx._loadedRuns == {testKeyLite: 2}
        # assert the correct workspaces exist: a raw and two copies
        assert mtd.doesExist(rx._createRawNexusWorkspaceName(self.runConfigLite))
        assert mtd.doesExist(rx._createCopyNexusWorkspaceName(self.runConfigLite, 1))
        assert mtd.doesExist(rx._createCopyNexusWorkspaceName(self.runConfigLite, 2))
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

        # test nonlite data can be loaded
        res = rx.fetchCleanNexusData(self.runConfigNonlite)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == rx._createCopyNexusWorkspaceName(self.runConfigNonlite, 1)
        assert len(rx._loadedRuns) == 2
        assert rx._loadedRuns == {testKeyLite: 2, testKeyNonlite: 1}
        assert CompareWorkspaces(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
        )

    def test_failed_fetch(self):
        # this is some file that it can't load
        mockFilename = Resource.getPath("inputs/crystalInfo/fake_file.cif")
        rx = Recipe()
        with pytest.raises(RuntimeError):
            rx._fetch(mockFilename, self.fetchedWSname, "")

    @mock.patch.object(Recipe, "_makeLite")
    @mock.patch.object(Recipe, "_createGroupingFilename")
    def test_fetch_grouping(self, mockFilename, mockLite):
        mockFilename.return_value = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
        mockLite = self.mockMakeLite  # noqa: ARG002
        rx = Recipe()
        res = rx.fetchGroupingDefinition(self.groceryListItemGrouping)
        assert res["result"]
        assert res["loader"] == "LoadGroupingDefinition"
        assert res["workspace"] == rx._createGroupingWorkspaceName(
            self.groceryListItemGrouping.groupingScheme,
            self.groceryListItemGrouping.isLite,
        )

    def test_name_with_many_underscores_in_it(self):
        pass

    @mock.patch.object(Recipe, "_createGroupingFilename")
    def test_failed_fetch_grouping(self, mockFilename):
        # this is some file that it can't load
        mockFilename.return_value = Resource.getPath("inputs/crystalInfo/fake_file.cif")
        rx = Recipe()
        with pytest.raises(RuntimeError):
            rx.fetchGroupingDefinition(self.groceryListItemGrouping)

    @mock.patch.object(Recipe, "_makeLite")
    @mock.patch.object(Recipe, "_createGroupingFilename")
    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_fetch_grocery_list(self, mockNexusFilename, mockGroupFilename, mockLite):
        mockNexusFilename.return_value = self.filepath
        mockGroupFilename.return_value = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
        mockLite = self.mockMakeLite  # noqa: ARG002
        rx = Recipe()
        res = rx.executeRecipe(self.groceryList)
        assert res["result"]
        assert res.get("workspaces") is not None
        assert len(res["workspaces"]) == len(self.groceryList)
        assert res["workspaces"][0] == rx._createCopyNexusWorkspaceName(self.runConfigLite, 1)
        assert res["workspaces"][1] == rx._createGroupingWorkspaceName(
            self.groceryListItemGrouping.groupingScheme,
            self.groceryListItemGrouping.isLite,
        )
        # test the correct workspaces exist
        assert mtd.doesExist(rx._createRawNexusWorkspaceName(self.runConfigLite))
        assert mtd.doesExist(res["workspaces"][0])
        assert mtd.doesExist(res["workspaces"][1])

    @mock.patch.object(Recipe, "_makeLite")
    @mock.patch.object(Recipe, "_createGroupingFilename")
    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_fetch_grocery_list_with_prev(self, mockNexusFilename, mockGroupFilename, mockLite):
        mockNexusFilename.return_value = self.filepath
        mockGroupFilename.return_value = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
        mockLite = self.mockMakeLite  # noqa: ARG002
        groceryList = [
            GroceryListItem(
                workspaceType="nexus",
                runConfig=self.runConfigLite,
                loader="LoadNexusProcessed",
            ),
            GroceryListItem(
                workspaceType="grouping",
                isLite=True,
                groupingScheme=self.groupingScheme,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource="prev",
            ),
        ]
        rx = Recipe()
        res = rx.executeRecipe(groceryList)
        assert res["result"]
        assert res.get("workspaces") is not None
        assert len(res["workspaces"]) == len(groceryList)
        assert groceryList[1].instrumentSource == res["workspaces"][0]


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
