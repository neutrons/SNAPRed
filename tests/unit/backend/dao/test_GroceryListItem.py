# ruff: noqa: E722, PT011, PT012

import unittest

import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    LoadEmptyInstrument,
)
from pydantic.error_wrappers import ValidationError
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.RunConfig import RunConfig
from snapred.meta.Config import Resource


class TestGroceryListItem(unittest.TestCase):
    # we only need this workspace made once for entire test suite
    @classmethod
    def setUpClass(cls):
        cls.instrumentFilename = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        cls.instrumentDonor = "_test_grocerylistitem_instrument"
        LoadEmptyInstrument(
            OutputWorkspace=cls.instrumentDonor,
            Filename=cls.instrumentFilename,
        )
        return super().setUpClass()

    # we only need to delete this workspace when the test suite ends
    @classmethod
    def tearDownClass(cls) -> None:
        DeleteWorkspace(cls.instrumentDonor)
        return super().tearDownClass()

    # at the beginning of each test, make a new runConfig object
    def setUp(self):
        self.runConfig = RunConfig(
            runNumber=555,
            IPTS=Resource.getPath("inputs"),
        )
        return super().setUp()

    def test_make_correct(self):
        """
        Test that valid grocery list items can be made, so that below negative tests can be trusted
        """
        # make a nexus item
        try:
            GroceryListItem(
                workspaceType="nexus",
                runConfig=self.runConfig,
                useLiteMode=True,
            )
        except:
            pytest.fail("Failed to make a valid GroceryListItem for nexus")
        # make a grouping item
        try:
            GroceryListItem(
                workspaceType="grouping",
                useLiteMode=True,
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
        except:
            pytest.fail("Failed to make a valid GroceryListItem for grouping")

    def test_next_and_useLiteMode(self):
        # check that it fails if neither runconfig.useLiteMode nor useLiteMode are set
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="nexus",
                runConfig=self.runConfig,
            )
            assert "useLiteMode" in e.msg()

        # check that if runConfig.useLiteMode is not set, will be set by useLiteMode
        assert self.runConfig.useLiteMode is None
        item = GroceryListItem(
            workspaceType="nexus",
            runConfig=self.runConfig,
            useLiteMode=True,
        )
        assert item.runConfig.useLiteMode is True

        # check that if useLiteMode is not set, will be set by runcConfig.useLiteMode
        self.runConfig.useLiteMode = True
        item = GroceryListItem(
            workspaceType="nexus",
            runConfig=self.runConfig,
        )
        assert item.useLiteMode is True

    def test_nexus_needs_runconfig(self):
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="nexus",
                loader="LoadEventNexus",
                useLiteMode=True,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "you must set the run config" in e.msg()

    def test_grouping_needs_stuff(self):
        # test needs useLiteMode
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "Lite mode" in e.msg()
        # test needs grouping scheme
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                useLiteMode=True,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "grouping scheme" in e.msg()
        # test needs instrument property source
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                useLiteMode=True,
                groupingScheme="Column",
                instrumentSource=self.instrumentDonor,
            )
            assert "instrument source" in e.msg()
        # test needs instrument source
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                useLiteMode=True,
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
            )
            assert "instrument source" in e.msg()
