# ruff: noqa: E722, PT011, PT012

import unittest
from unittest import mock

import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    LoadEmptyInstrument,
)
from pydantic import ValidationError
from snapred.backend.dao.ingredients.GroceryListBuilder import GroceryListBuilder
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.meta.Config import Resource


class TestGroceryListItem(unittest.TestCase):
    # we only need this workspace made once for entire test suite
    @classmethod
    def setUpClass(cls):
        cls.runNumber = "555"
        cls.IPTS = Resource.getPath("inputs")

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

    def test_make_correct(self):
        """
        Test that valid grocery list items can be made, so that below negative tests can be trusted
        """
        # make a nexus item
        try:
            GroceryListItem(
                workspaceType="nexus",
                runNumber=self.runNumber,
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

    def test_nexus_needs_runNumber(self):
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="nexus",
                useLiteMode=True,
            )
            assert "you must set the run config" in e.msg()

    def test_grouping_needs_useLiteMode(self):
        # test needs useLiteMode
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "Lite mode" in e.msg()

    def test_grouping_needs_groupingScheme(self):
        # test needs grouping scheme
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                useLiteMode=True,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "grouping scheme" in e.msg()

    def test_grouping_needs_propertySource(self):
        # test needs instrument property source
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                useLiteMode=True,
                groupingScheme="Column",
                instrumentSource=self.instrumentDonor,
            )
            assert "instrument source" in e.msg()

    def test_grouping_needs_instrumentSource(self):
        # test needs instrument source
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                useLiteMode=True,
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
            )
            assert "instrument source" in e.msg()

    def test_creation_methods(self):
        # TODO remove these?
        item = GroceryListItem.makeNativeNexusItem(self.runNumber)
        assert item.runNumber == self.runNumber
        assert item.useLiteMode is False
        assert item.workspaceType == "nexus"

        item = GroceryListItem.makeLiteNexusItem(self.runNumber)
        assert item.runNumber == self.runNumber
        assert item.useLiteMode is True
        assert item.workspaceType == "nexus"

        item = GroceryListItem.makeNativeGroupingItem("Native")
        assert item.groupingScheme == "Native"
        assert item.useLiteMode is False
        assert item.workspaceType == "grouping"

        item = GroceryListItem.makeLiteGroupingItem("Native")
        assert item.groupingScheme == "Native"
        assert item.useLiteMode is True
        assert item.workspaceType == "grouping"

    def test_builder(self):
        builder = GroceryListItem.builder()
        assert isinstance(builder, GroceryListBuilder)
        item1 = GroceryListItem.builder().native().nexus().using(self.runNumber).build()
        item2 = GroceryListBuilder().native().nexus().using(self.runNumber).build()
        assert item1 == item2
