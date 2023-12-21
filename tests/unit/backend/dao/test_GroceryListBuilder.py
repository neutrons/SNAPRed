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


class TestGroceryListBuilder(unittest.TestCase):
    # we only need this workspace made once for entire test suite
    @classmethod
    def setUpClass(cls):
        cls.runNumber = "555"
        cls.groupingScheme = "Native"
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

    def test_nexus_native_lite(self):
        item = GroceryListBuilder().neutron(self.runNumber).native().build()
        assert item.runNumber == self.runNumber
        assert item.useLiteMode is False
        assert item.workspaceType == "neutron"

        item = GroceryListBuilder().neutron(self.runNumber).lite().build()
        assert item.runNumber == self.runNumber
        assert item.useLiteMode is True
        assert item.workspaceType == "neutron"

        for useLite in [True, False]:
            item = GroceryListBuilder().neutron(self.runNumber).useLiteMode(useLite).build()
            assert item.runNumber == self.runNumber
            assert item.useLiteMode == useLite
            assert item.workspaceType == "neutron"

    def test_nexus_propname(self):
        propertyName = "inputWorkspace"
        item = GroceryListBuilder().neutron(self.runNumber).native().name(propertyName).build()
        assert item.runNumber == self.runNumber
        assert item.useLiteMode is False
        assert item.workspaceType == "neutron"
        assert item.propertyName == propertyName

    def test_grouping_native_lite(self):
        item = GroceryListBuilder().grouping(self.groupingScheme).native().build()
        assert item.groupingScheme == self.groupingScheme
        assert item.useLiteMode is False
        assert item.workspaceType == "grouping"

        item = GroceryListBuilder().grouping(self.groupingScheme).lite().build()
        assert item.groupingScheme == self.groupingScheme
        assert item.useLiteMode is True
        assert item.workspaceType == "grouping"

        for useLite in [True, False]:
            item = GroceryListBuilder().grouping(self.groupingScheme).useLiteMode(useLite).build()
            assert item.groupingScheme == self.groupingScheme
            assert item.useLiteMode == useLite
            assert item.workspaceType == "grouping"

    def test_grouping_with_source(self):
        item = (
            GroceryListBuilder()
            .grouping(self.groupingScheme)
            .native()
            .source(InstrumentDonor=self.instrumentDonor)
            .build()
        )
        assert item.instrumentPropertySource == "InstrumentDonor"
        assert item.instrumentSource == self.instrumentDonor

        item = (
            GroceryListBuilder()
            .grouping(self.groupingScheme)
            .native()
            .source(InstrumentFilename=self.instrumentFilename)
            .build()
        )
        assert item.instrumentPropertySource == "InstrumentFilename"
        assert item.instrumentSource == self.instrumentFilename

        item = GroceryListBuilder().grouping(self.groupingScheme).native().source(InstrumentName="SNAP").build()
        assert item.instrumentPropertySource == "InstrumentName"
        assert item.instrumentSource == "SNAP"

    def test_fail_bad_property_source(self):
        with pytest.raises(ValidationError):
            GroceryListBuilder().grouping(self.groupingScheme).native().source(MyBestFriend="trust me").build()

    def test_nexus_with_instrument(self):
        item = GroceryListBuilder().neutron(self.runNumber).native().source(InstrumentName="SNAP").build()
        assert item.runNumber == self.runNumber
        assert item.useLiteMode is False
        assert item.workspaceType == "neutron"

    def test_build_list(self):
        builder = GroceryListBuilder()
        builder.neutron(self.runNumber).native().add()
        builder.grouping(self.groupingScheme).native().source(InstrumentDonor=self.instrumentDonor).add()
        builder.grouping(self.groupingScheme).native().fromPrev().add()
        groceryList = builder.buildList()
        # test the list built correctly
        assert len(groceryList) == 3
        # first item is native nexus item
        assert groceryList[0].workspaceType == "neutron"
        assert groceryList[0].runNumber == self.runNumber
        assert groceryList[0].useLiteMode is False
        # second item is native grouping froma donor
        assert groceryList[1].workspaceType == "grouping"
        assert groceryList[1].groupingScheme == self.groupingScheme
        assert groceryList[1].useLiteMode is False
        assert groceryList[1].instrumentPropertySource == "InstrumentDonor"
        assert groceryList[1].instrumentSource == self.instrumentDonor
        # third item is native grouping from prev
        assert groceryList[2].workspaceType == "grouping"
        assert groceryList[2].groupingScheme == self.groupingScheme
        assert groceryList[2].useLiteMode is False
        assert groceryList[2].instrumentPropertySource == "InstrumentDonor"
        assert groceryList[2].instrumentSource == "prev"

    def test_build_list_hanging(self):
        builder = GroceryListBuilder()
        builder.neutron(self.runNumber).native()  # NO ADD
        groceryList = builder.buildList()
        # test the list built correctly
        assert len(groceryList) == 1
        assert groceryList[0].workspaceType == "neutron"
        assert groceryList[0].runNumber == self.runNumber
        assert groceryList[0].useLiteMode is False

    def test_build_list_oneitem(self):
        builder = GroceryListBuilder()
        groceryList = builder.neutron(self.runNumber).native().buildList()
        # test the list built correctly
        assert len(groceryList) == 1
        assert groceryList[0].workspaceType == "neutron"
        assert groceryList[0].runNumber == self.runNumber
        assert groceryList[0].useLiteMode is False

    def test_build_dict_empty(self):
        # make the list with out any property names
        builder = GroceryListBuilder()
        builder.neutron(self.runNumber).native().add()
        builder.grouping(self.groupingScheme).native().source(InstrumentDonor=self.instrumentDonor).add()
        builder.grouping(self.groupingScheme).native().fromPrev().add()
        groceryDict = builder.buildDict()
        # test the dictionary has nothing in it
        assert len(groceryDict) == 0
        assert groceryDict == {}

    def test_build_dict_correctly(self):
        testPropName = "TestName"
        builder = GroceryListBuilder()
        builder.name(testPropName).neutron(self.runNumber).native().add()
        groceryList = builder.buildList()

        builder.name(testPropName).neutron(self.runNumber).native().add()
        groceryDict = builder.buildDict()
        # test the list built correctly
        assert len(groceryDict) == 1
        assert list(groceryDict.values()) == groceryList
        assert groceryDict[testPropName] == groceryList[0]

    def test_build_dict_oneitem(self):
        builder = GroceryListBuilder()
        groceryDict = builder.name("TestName").neutron(self.runNumber).native().buildDict()
        # test the list built correctly
        assert len(groceryDict) == 1
        assert groceryDict["TestName"]
