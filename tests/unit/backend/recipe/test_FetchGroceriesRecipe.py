# ruff: noqa: PT012

import os
import unittest
from unittest import mock

import pytest
from mantid.simpleapi import (
    AddSampleLog,
    CalculateDIFC,
    CloneWorkspace,
    CompareWorkspaces,
    CreateEmptyTableWorkspace,
    CreateSampleWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    DeleteWorkspaces,
    LoadEventNexus,
    LoadInstrument,
    Plus,
    Rebin,
    SaveNexusPD,
    SaveNexusProcessed,
    mtd,
)
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig

# the algorithm to test
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import (
    FetchGroceriesAlgorithm as Algo,  # noqa: E402
)
from snapred.backend.recipe.FetchGroceriesRecipe import (
    FetchGroceriesRecipe as Recipe,  # noqa: E402
)
from snapred.meta.Config import Resource

TheAlgorithmManager: str = "snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager"


class TestFetchGroceriesRecipe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
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
            groupingScheme="Column",
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

    def tearDown(self) -> None:
        try:
            DeleteWorkspace(f"_{self.runNumber}_fetched")
        except:  # noqa: E722
            pass
        return super().tearDown()

    @classmethod
    def tearDownClass(cls) -> None:
        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        os.remove(cls.filepath)
        return super().tearDownClass()

    def test_create_filename(self):
        rx = Recipe()
        res = rx._createFilenameFromRunConfig(self.runConfigLite)
        assert res == f"{self.runConfigLite.IPTS}shared/lite/SNAP_{self.runConfigLite.runNumber}.lite.nxs.h5"
        res = rx._createFilenameFromRunConfig(self.runConfigNonlite)
        assert res == f"{self.runConfigNonlite.IPTS}nexus/SNAP_{self.runConfigNonlite.runNumber}.nxs.h5"

    def test_create_workspace_name(self):
        rx = Recipe()
        res = rx._createNexusWorkspaceName(self.runConfigLite)
        assert res == f"_TOF_RAW_{self.runConfigLite.runNumber}_lite"
        res = rx._createNexusWorkspaceName(self.runConfigNonlite)
        assert res == f"_TOF_RAW_{self.runConfigNonlite.runNumber}"

    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_fetch_nexus(self, mockFilename):
        mockFilename.return_value = self.filepath
        rx = Recipe()
        res = rx.fetchNexusData(self.runConfigLite)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == rx._createNexusWorkspaceName(self.runConfigLite)
        assert res.get("alreadyLoaded") is None
        assert rx._loadedRuns == [self.runConfigLite]

        res = rx.fetchNexusData(self.runConfigLite)
        assert res.get("alreadyLoaded") is not None

        res = rx.fetchNexusData(self.runConfigNonlite)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == rx._createNexusWorkspaceName(self.runConfigNonlite)
        assert rx._loadedRuns == [self.runConfigLite, self.runConfigNonlite]
        assert res.get("alreadyLoaded") is None

    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_singleton_rx(self, mockFilename):
        mockFilename.return_value = self.filepath
        rx1 = Recipe()
        rx1.fetchNexusData(self.runConfigLite)
        assert rx1._loadedRuns == [self.runConfigLite]
        rx2 = Recipe()
        rx2.fetchNexusData(self.runConfigLite)
        assert rx2._loadedRuns == [self.runConfigLite]

    def test_grouping_workspacename(self):
        rx = Recipe()
        res = rx._createGroupingWorkspaceName("Column", True)
        assert res == "SNAPLite_grouping_Column"
        res = rx._createGroupingWorkspaceName("Column", False)
        assert res == "SNAP_grouping_Column"

    @mock.patch.object(Recipe, "_createGroupingFilename")
    def test_grouping(self, mockFilename):
        mockFilename.return_value = Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml")
        rx = Recipe()
        res = rx.fetchGroupingDefinition(self.groceryListItemGrouping)
        assert res["result"]
        assert res["loader"] == "LoadGroupingDefinition"
        assert res["workspace"] == rx._createGroupingWorkspaceName(
            self.groceryListItemGrouping.groupingScheme,
            self.groceryListItemGrouping.isLite,
        )

    @mock.patch.object(Recipe, "_createGroupingFilename")
    @mock.patch.object(Recipe, "_createFilenameFromRunConfig")
    def test_grocery_list(self, mockNexusFilename, mockGroupFilename):
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
