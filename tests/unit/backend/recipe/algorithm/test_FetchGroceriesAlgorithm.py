# ruff: noqa: ARG001, PT012

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

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig

# the algorithm to test
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import (
    FetchGroceriesAlgorithm as Algo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestFetchGroceriesAlgorithm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create a mock data file which will be loaded in tests.
        This is created at the start of this test suite, then deleted at the end.
        """
        cls.runNumber = "555"
        cls.runConfig = RunConfig(
            runNumber=str(cls.runNumber),
            IPTS=Resource.getPath("inputs/"),
        )
        cls.filepath = Resource.getPath(f"inputs/test_{cls.runNumber}_fetchgroceriesalgo.nxs")
        cls.instrumentFilepath = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        cls.fetchedWS = f"_{cls.runNumber}_fetched"
        # create some sample data
        cls.sampleWS = f"_{cls.runNumber}_grocery_to_fetch"
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
            RewriteSpectraMap=False,
        )
        SaveNexusProcessed(
            InputWorkspace=cls.sampleWS,
            Filename=cls.filepath,
        )
        assert os.path.exists(cls.filepath)

    def tearDown(self) -> None:
        """At the end of each test, delete the loaded workspace to avoid pollution"""
        try:
            DeleteWorkspace(self.fetchedWS)
        except:  # noqa: E722
            pass
        return super().tearDown()

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Delete all workspaces created by this test, and remove the ceated filed.
        This is run once at the end of this test suite.
        """
        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        os.remove(cls.filepath)
        return super().tearDownClass()

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        algo = Algo()
        algo.initialize()

        # check failure if no file property given
        with pytest.raises(RuntimeError) as e:
            algo.execute()
            assert "FileProperty" in e.msg()
        algo.setPropertyValue("Filename", self.filepath)
        assert self.filepath == algo.getPropertyValue("Filename")

        # check failure if no workspace property given
        fetched_groceries = f"_fetched_groceries_{self.runNumber}"
        with pytest.raises(RuntimeError) as e:
            algo.execute()
            assert "Must specify the Workspace to load into" in e.msg()
        algo.setPropertyValue("OutputWorkspace", fetched_groceries)
        assert fetched_groceries == algo.getPropertyValue("OutputWorkspace")
        algo.setPropertyValue("LoaderType", "LoadNexus")
        assert "LoadNexus" == algo.getPropertyValue("LoaderType")

        # set instrument name
        algo.setPropertyValue("InstrumentName", "SNAP")
        assert "SNAP" == algo.getPropertyValue("InstrumentName")

        # set instrument filename
        algo.setPropertyValue("InstrumentFilename", self.instrumentFilepath)
        assert self.instrumentFilepath == algo.getPropertyValue("InstrumentFileName")

        # check errors if more than one instrument source set
        errors = algo.validateInputs()
        assert errors.get("InstrumentName") is not None
        assert errors.get("InstrumentFilename") is not None
        assert errors.get("InstrumentDonor") is None

        with pytest.raises(RuntimeError) as e:
            algo.execute()
        errorMsg = str(e.value)
        assert "invalid properties found" in errorMsg.lower()
        assert "InstrumentName" in errorMsg
        assert "InstrumentFilename" in errorMsg
        # unset the instrument name
        algo.setPropertyValue("InstrumentName", "")
        assert len(algo.validateInputs()) == 0

        # set instrument donor
        algo.setPropertyValue("InstrumentDonor", self.sampleWS)
        assert self.sampleWS == algo.getPropertyValue("InstrumentDonor")
        # check errors if two instrument sources
        errors = algo.validateInputs()
        assert errors.get("InstrumentName") is None
        assert errors.get("InstrumentFilename") is not None
        assert errors.get("InstrumentDonor") is not None
        with pytest.raises(RuntimeError) as e:
            algo.execute()
        errorMsg = str(e.value)
        assert "invalid properties found" in errorMsg.lower()
        assert "InstrumentDonor" in errorMsg
        assert "InstrumentFilename" in errorMsg
        # unset the instrument name
        algo.setPropertyValue("InstrumentFilename", "")
        assert len(algo.validateInputs()) == 0

    def test_loadNexusNoLoader(self):
        """Test that loading works if no loader specified"""
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "")
        algo.setPropertyValue("OutputWorkspace", self.fetchedWS)
        assert algo.execute()
        assert_wksp_almost_equal(
            Workspace1=self.fetchedWS,
            Workspace2=self.sampleWS,
        )
        assert "LoadNexusProcessed" == algo.getPropertyValue("LoaderType")

    def test_loadNexusLoader(self):
        """Test that loading works using Nexus loader"""
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "LoadNexus")
        algo.setPropertyValue("OutputWorkspace", self.fetchedWS)
        assert algo.execute()
        assert_wksp_almost_equal(
            Workspace1=self.fetchedWS,
            Workspace2=self.sampleWS,
        )
        assert "LoadNexus" == algo.getPropertyValue("LoaderType")

    def test_loadNexusEventLoader(self):
        """
        Test that loading works using the EventNexus loader.
        Note, it is very difficult to generate a correct event Nexus file,
        so this test is currently incomplete.
        """
        # TODO save data in a way that can be loaded by LoadEventNexus
        pass

    def test_loadNexusProcessedLoader(self):
        """Test that loading works using NexusProcessed loader"""
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "LoadNexusProcessed")
        algo.setPropertyValue("OutputWorkspace", self.fetchedWS)
        assert algo.execute()
        assert_wksp_almost_equal(
            Workspace1=self.fetchedWS,
            Workspace2=self.sampleWS,
        )
        assert "LoadNexusProcessed" == algo.getPropertyValue("LoaderType")

    def test_loadGroupings(self):
        """
        Test that a grouping file can be loaded.
        Checked using all three methods of specifying an instrument.
        """
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "LoadGroupingDefinition")
        algo.setPropertyValue("OutputWorkspace", f"_{self.runNumber}_grouping_file")
        algo.setPropertyValue("InstrumentFilename", self.instrumentFilepath)
        assert algo.execute()
        algo.setProperty("InstrumentFilename", "")

        algo.setPropertyValue("OutputWorkspace", f"_{self.runNumber}_grouping_name")
        algo.setPropertyValue("InstrumentName", "fakeSNAP")
        assert algo.execute()
        assert_wksp_almost_equal(
            Workspace1=f"_{self.runNumber}_grouping_file",
            Workspace2=f"_{self.runNumber}_grouping_name",
        )
        algo.setProperty("InstrumentName", "")

        algo.setPropertyValue("OutputWorkspace", f"_{self.runNumber}_grouping_donor")
        algo.setPropertyValue("InstrumentDonor", self.sampleWS)
        assert algo.execute()
        assert_wksp_almost_equal(
            Workspace1=f"_{self.runNumber}_grouping_file",
            Workspace2=f"_{self.runNumber}_grouping_donor",
        )

    def test_loadGroupingTwice(self):
        """
        The algorithm will skip the load process if the output workspace
        already exists.  This checks that the skip occurs.
        """
        # ensure the workspace does not already exist
        assert not mtd.doesExist(self.fetchedWS)

        # mock mantid snapper to ensure number of times called
        def mockLoad(*args, **kwargs):
            CreateWorkspace(
                OutputWorkspace=self.fetchedWS,
                DataX=1,
                DataY=1,
            )
            assert mtd.doesExist(self.fetchedWS)

        def mockDoesExist(val):
            return mtd.doesExist(val)

        mockSnapper = mock.MagicMock()
        mockSnapper.LoadGroupingDefinition = mock.MagicMock(side_effect=mockLoad)
        mockSnapper.mtd = mock.MagicMock()
        mockSnapper.mtd.doesExist = mock.MagicMock(side_effect=mockDoesExist)

        # call once
        algo = Algo()
        algo.initialize()
        algo.mantidSnapper = mockSnapper
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "LoadGroupingDefinition")
        algo.setPropertyValue("OutputWorkspace", self.fetchedWS)
        algo.setPropertyValue("InstrumentFilename", self.instrumentFilepath)
        assert algo.execute()
        algo.mantidSnapper.mtd.doesExist.assert_called_once()
        algo.mantidSnapper.LoadGroupingDefinition.assert_called_once()
        algo.mantidSnapper.executeQueue.assert_called_once()
        assert mtd.doesExist(self.fetchedWS)

        # call again and make sure not executed again
        assert algo.execute()
        algo.mantidSnapper.LoadGroupingDefinition.assert_called_once()
        assert algo.mantidSnapper.mtd.doesExist.call_count == 2
        assert algo.mantidSnapper.executeQueue.call_count == 2
