# ruff: noqa: E722, PT011, PT012
import time
import unittest

import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    LoadEmptyInstrument,
)
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.meta.builder.GroceryListBuilder import GroceryListBuilder
from snapred.meta.Config import Resource


class TestGroceryListItem(unittest.TestCase):
    # we only need this workspace made once for entire test suite
    @classmethod
    def setUpClass(cls):
        cls.runNumber = "555"
        cls.useLiteMode = True
        cls.version = 1
        cls.timestamp = time.time()
        cls.IPTS = Resource.getPath("inputs")

        cls.instrumentFilename = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
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
                workspaceType="neutron",
                runNumber=self.runNumber,
                useLiteMode=True,
            )
        except:
            pytest.fail("Failed to make a valid GroceryListItem for neutron")
        # make a grouping item
        try:
            GroceryListItem(
                workspaceType="grouping",
                runNumber=self.runNumber,
                useLiteMode=True,
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
        except:
            pytest.fail("Failed to make a valid GroceryListItem for grouping")
        # make a reduction_pixel_mask item
        try:
            GroceryListItem(
                workspaceType="reduction_pixel_mask",
                runNumber=self.runNumber,
                useLiteMode=True,
                timestamp=self.timestamp,
            )
        except:
            pytest.fail("Failed to make a valid GroceryListItem for 'reduction_pixel_mask'")

    def test_nexus_needs_runNumber(self):
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="neutron",
                useLiteMode=True,
            )
            assert "you must set the run config" in e.msg()

    def test_grouping_needs_runNumber(self):
        # test needs runNumber
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "run number" in e.msg()

    def test_grouping_needs_useLiteMode(self):
        # test needs useLiteMode
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                runNumber=self.runNumber,
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
                runNumber=self.runNumber,
                useLiteMode=True,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "grouping scheme" in e.msg()

    def test_neutron_with_instrument(self):
        # workspaceType "neutron" should not specify an instrument
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="neutron",
                runNumber=self.runNumber,
                useLiteMode=True,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "should not specify an instrument" in e.msg()

    def test_grouping_incomplete_instrumentSource(self):
        # workspaceType "grouping" needs a complete instrument source
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                runNumber=self.runNumber,
                useLiteMode=True,
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
            )
            assert "if 'instrumentPropertySource' is specified then 'instrumentSource' must be specified" in e.msg()

    def test_reduction_pixel_mask_needs_runNumber(self):
        with pytest.raises(ValueError, match="requires a run number"):
            GroceryListItem(
                workspaceType="reduction_pixel_mask",
                useLiteMode=True,
                timestamp=self.timestamp,
            )

    def test_reduction_pixel_mask_needs_timestamp(self):
        with pytest.raises(ValueError, match="requires a timestamp"):
            GroceryListItem(
                workspaceType="reduction_pixel_mask",
                runNumber=self.runNumber,
                useLiteMode=True,
            )

    def test_builder(self):
        builder = GroceryListItem.builder()
        assert isinstance(builder, GroceryListBuilder)
        item1 = GroceryListItem.builder().native().neutron(self.runNumber).build()
        item2 = GroceryListBuilder().native().neutron(self.runNumber).build()
        assert item1 == item2
