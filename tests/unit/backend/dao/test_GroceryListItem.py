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
    def setUp(self):
        self.runConfig = RunConfig(
            runNumber=555,
            IPTS=Resource.getPath("inputs"),
        )
        self.instrumentFilename = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        self.instrumentDonor = f"_test_grocerylistitem_{self.runConfig.runNumber}"
        LoadEmptyInstrument(
            OutputWorkspace=self.instrumentDonor,
            Filename=self.instrumentFilename,
        )

    def tearDown(self) -> None:
        DeleteWorkspace(self.instrumentDonor)
        return super().tearDown()

    def test_make_correct(self):
        """
        Test that valid grocery list items can be made, so that below negative tests can be trusted
        """
        # make a nexus item
        try:
            GroceryListItem(
                workspaceType="nexus",
                runConfig=self.runConfig,
            )
        except:
            pytest.fail("Failed to make a valid GroceryListItem for nexus")
        # make a grouping item
        try:
            GroceryListItem(
                workspaceType="grouping",
                isLite=True,
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
        except:
            pytest.fail("Failed to make a valid GroceryListItem for grouping")

    def test_literals(self):
        with pytest.raises(ValidationError) as e:
            GroceryListItem(
                workspaceType="fruitcake",
                runConfig=self.runConfig,
            )
            assert "'nexus', 'grouping'" in e.msg()
        with pytest.raises(ValidationError) as e:
            GroceryListItem(
                workspaceType="nexus",
                runConfig=self.runConfig,
                loader="fruitcake",
            )
            assert "'LoadGroupingDefinition', 'LoadNexus', 'LoadEventNexus', 'LoadNexusProcessed'" in e.msg()
        with pytest.raises(ValidationError) as e:
            GroceryListItem(
                workspaceType="grouping",
                isLite=True,
                groupingScheme="Column",
                instrumentPropertySource="fruitcake",
                instrumentSource=self.instrumentDonor,
            )
            assert "'InstrumentName', 'InstrumentFilename', 'InstrumentDonor'" in e.msg()

    def test_nexus_needs_runconfig(self):
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="nexus",
                loader="LoadEventNexus",
                isLite=True,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "you must set the run config" in e.msg()

    def test_grouping_needs_stuff(self):
        # test needs isLite
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
                isLite=True,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource=self.instrumentDonor,
            )
            assert "grouping scheme" in e.msg()
        # test needs instrument property source
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                isLite=True,
                groupingScheme="Column",
                instrumentSource=self.instrumentDonor,
            )
            assert "instrument source" in e.msg()
        # test needs instrument source
        with pytest.raises(ValueError) as e:
            GroceryListItem(
                workspaceType="grouping",
                isLite=True,
                groupingScheme="Column",
                instrumentPropertySource="InstrumentDonor",
            )
            assert "instrument source" in e.msg()
