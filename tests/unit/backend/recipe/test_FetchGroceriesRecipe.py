# ruff: noqa: ARG001, E501, PT012
import os
import unittest
from unittest import mock

import pytest
from mantid.simpleapi import (
    CreateWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    SaveNexusProcessed,
    mtd,
)
from mantid.testing import assert_almost_equal as assert_wksp_almost_equal

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem

# needed to make mocked ingredients
from snapred.backend.recipe.FetchGroceriesRecipe import (
    FetchGroceriesRecipe as Recipe,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestFetchGroceriesRecipe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create a mock data file which will be loaded in tests.
        This is created at the start of this test suite, then deleted at the end.
        """
        cls.runNumber = "555"
        cls.filepath = Resource.getPath(f"inputs/test_{cls.runNumber}_fetchgroceriesrx.nxs")
        cls.instrumentFilepath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
        cls.fetchedWSname = "_fetched_grocery"
        cls.groupingScheme = "Native"
        cls.rtolValue = 1.0e-10
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

        cls.liteMapGroceryItem = GroceryListItem.builder().grouping("Lite").build()

    def setUp(self) -> None:
        self.rx = Recipe()

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
        del self.rx
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

    ### TESTS OF FETCH METHODS

    def test_fetch(self):
        """Test the correct behavior of the fetch method"""
        self.clearoutWorkspaces()
        res = self.rx.executeRecipe(self.filepath, self.fetchedWSname, "")
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == self.fetchedWSname
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
            rtol=self.rtolValue,
        )

        # make sure it won't load same workspace name again
        assert mtd.doesExist(self.fetchedWSname)
        res = self.rx.executeRecipe(self.filepath, self.fetchedWSname, res["loader"])
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == ""  # this makes sure no loader called
        assert res["workspace"] == self.fetchedWSname
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
            rtol=self.rtolValue,
        )

    @mock.patch("snapred.backend.recipe.FetchGroceriesRecipe.FetchGroceriesAlgorithm")
    def test_fetch_with_load_event_nexus(self, mockAlgo):
        """Test the correct behavior of the fetch method"""
        mock_instance = mockAlgo.return_value
        mock_instance.execute.return_value = "data"
        mock_instance.getPropertyValue.return_value = "LoadEventNexus"
        self.rx.mantidSnapper.RemovePromptPulse = mock.MagicMock()

        self.clearoutWorkspaces()
        res = self.rx.executeRecipe(self.filepath, self.fetchedWSname, "LoadEventNexus")
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadEventNexus"
        assert res["workspace"] == self.fetchedWSname
        assert self.rx.mantidSnapper.RemovePromptPulse.called

    def test_fetch_failed(self):
        # this is some file that it can't load
        mockFilename = Resource.getPath("inputs/crystalInfo/blank_file.cif")
        with pytest.raises(RuntimeError):
            self.rx.executeRecipe(mockFilename, self.fetchedWSname, "")

    def test_fetch_grouping(self):
        groupFilename = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")

        # call once and load
        res = self.rx.executeRecipe(
            groupFilename,
            self.fetchedWSname,
            "LoadGroupingDefinition",
            "InstrumentFilename",
            self.instrumentFilepath,
        )
        assert res["result"]
        assert res["loader"] == "LoadGroupingDefinition"
        assert res["workspace"] == self.fetchedWSname

    def test_failed_fetch_grouping(self):
        groupFilename = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
        with pytest.raises(RuntimeError):
            self.rx.executeRecipe(
                groupFilename,
                self.fetchedWSname,
                "LoadGroupingDefinition",
            )
