# ruff: noqa: ARG001, PT012

import os
import unittest
from unittest import mock

import pytest
from mantid.simpleapi import (
    CreateSampleWorkspace,
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
from snapred.meta.Config import Resource

TheAlgorithmManager: str = "snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager"


class TestFetchGroceriesRecipe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create a mock data file which will be loaded in tests.
        This is created at the start of this test suite, then deleted at the end.
        """
        cls.runNumber = "555"
        cls.runConfigLite = RunConfig(
            runNumber=str(cls.runNumber),
            IPTS=Resource.getPath("inputs/"),
            isLite=True,
        )
        cls.runConfigNonlite = RunConfig(
            runNumber=str(cls.runNumber),
            IPTS=Resource.getPath("inputs/"),
            isLite=False,
        )
        cls.filepath = Resource.getPath(f"inputs/test_{cls.runNumber}_fetchgroceriesrx.nxs")
        cls.instrumentFilepath = Resource.getPath("inputs/diffcal/fakeSNAPLite.xml")
        cls.fetchedWSname = f"_{cls.runNumber}_fetched"
        cls.groupingScheme = "Column"
        # create some sample data
        cls.sampleWS = f"_{cls.runNumber}_grocery_to_fetch"
        CreateSampleWorkspace(
            OutputWorkspace=cls.sampleWS,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=10,
            Xmax=1000,
            BinWidth=0.01,
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        # load an instrument into sample data
        LoadInstrument(
            Workspace=cls.sampleWS,
            Filename=cls.instrumentFilepath,
            InstrumentName="fakeSNAPLite",
            RewriteSpectraMap=False,
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
        rx = Recipe()
        names = [
            self.fetchedWSname,
            rx._createNexusWorkspaceName(self.runConfigLite),
            rx._createNexusWorkspaceName(self.runConfigNonlite),
            rx._createGroupingWorkspaceName(self.groupingScheme, True),
            rx._createGroupingWorkspaceName(self.groupingScheme, False),
        ]
        for ws in names:
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

    def test_nexus_filename(self):
        """Test the creation of the nexus filename"""
        rx = Recipe()
        res = rx._createFilenameFromRunConfig(self.runConfigLite)
        assert res == f"{self.runConfigLite.IPTS}shared/lite/SNAP_{self.runConfigLite.runNumber}.lite.nxs.h5"
        res = rx._createFilenameFromRunConfig(self.runConfigNonlite)
        assert res == f"{self.runConfigNonlite.IPTS}nexus/SNAP_{self.runConfigNonlite.runNumber}.nxs.h5"

    def test_nexus_workspacename(self):
        """Test the creation of the nexus workspace name"""
        rx = Recipe()
        res = rx._createNexusWorkspaceName(self.runConfigLite)
        assert res == f"_TOF_RAW_{self.runConfigLite.runNumber}_lite"
        res = rx._createNexusWorkspaceName(self.runConfigNonlite)
        assert res == f"_TOF_RAW_{self.runConfigNonlite.runNumber}"

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

    @mock.patch.object(Recipe, "_createNexusWorkspaceName")
    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_fetch_nexus(self, mockFilename, mockWorkspaceName):
        """Test the correct behavior when fetching nexus data"""
        mockFilename.return_value = self.filepath
        mockWorkspaceName.return_value = f"_{self.runNumber}_lite_fetched"
        testKeyLite = (self.runConfigLite.runNumber, self.runConfigLite.IPTS, self.runConfigLite.isLite)
        testKeyNonlite = (self.runConfigNonlite.runNumber, self.runConfigNonlite.IPTS, self.runConfigNonlite.isLite)
        rx = Recipe()
        assert len(rx._loadedRuns) == 0

        # test that a nexus workspace can be loaded
        res = rx.fetchNexusData(self.runConfigLite)
        assert len(rx._loadedRuns) == 1
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == mockWorkspaceName.return_value
        assert rx._loadedRuns == {testKeyLite: 1}

        # test that trying to load data twice does nothing
        res = rx.fetchNexusData(self.runConfigLite)
        assert len(rx._loadedRuns) == 1
        assert res["workspace"] == f"{mockWorkspaceName.return_value}_1"
        assert rx._loadedRuns == {testKeyLite: 2}

        # test nonlike data can be loaded
        mockWorkspaceName.return_value = f"_{self.runNumber}_fetched"
        res = rx.fetchNexusData(self.runConfigNonlite)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == mockWorkspaceName.return_value
        assert len(rx._loadedRuns) == 2
        assert rx._loadedRuns == {testKeyLite: 2, testKeyNonlite: 1}

    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_failed_fetch_nexus(self, mockFilename):
        # this is some file that it can't load
        mockFilename.return_value = Resource.getPath("inputs/crystalInfo/fake_file.cif")
        rx = Recipe()
        with pytest.raises(RuntimeError):
            rx.fetchNexusData(self.runConfigLite)

    @mock.patch.object(Recipe, "_createGroupingFilename")
    def test_fetch_grouping(self, mockFilename):
        mockFilename.return_value = Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml")
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

    @mock.patch.object(Recipe, "_createGroupingFilename")
    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_fetch_grocery_list(self, mockNexusFilename, mockGroupFilename):
        mockNexusFilename.return_value = self.filepath
        mockGroupFilename.return_value = Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml")
        rx = Recipe()
        res = rx.executeRecipe(self.groceryList)
        assert res["result"]
        assert res.get("workspaces") is not None
        assert len(res["workspaces"]) == len(self.groceryList)
        assert res["workspaces"][0] == rx._createNexusWorkspaceName(self.runConfigLite)
        assert res["workspaces"][1] == rx._createGroupingWorkspaceName(
            self.groceryListItemGrouping.groupingScheme,
            self.groceryListItemGrouping.isLite,
        )

    @mock.patch.object(Recipe, "_createGroupingFilename")
    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_fetch_grocery_list_with_prev(self, mockNexusFilename, mockGroupFilename):
        mockNexusFilename.return_value = self.filepath
        mockGroupFilename.return_value = Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml")
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
