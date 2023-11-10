# ruff: noqa: PT012

import os
import unittest

import pytest
from mantid.simpleapi import (
    CompareWorkspaces,
    CreateSampleWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    SaveNexusProcessed,
    mtd,
)

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig

# the algorithm to test
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import (
    FetchGroceriesAlgorithm as Algo,  # noqa: E402
)
from snapred.meta.Config import Resource

TheAlgorithmManager: str = "snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager"


class TestFetchGroceriesAlgorithm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        cls.runNumber = "555"
        cls.runConfig = RunConfig(
            runNumber=str(cls.runNumber),
            IPTS=Resource.getPath("inputs/"),
        )
        cls.filepath = Resource.getPath(f"inputs/test_{cls.runNumber}_fetchgroceriesalgo.nxs")
        cls.instrumentFilepath = Resource.getPath("inputs/diffcal/fakeSNAPLite.xml")
        cls.fetchedWS = f"_{cls.runNumber}_fetched"
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

    def tearDown(self) -> None:
        try:
            DeleteWorkspace(self.fetchedWS)
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

        # chek failure if no workspace property given
        fetched_groceries = f"_fetched_groceries_{self.runNumber}"
        with pytest.raises(RuntimeError) as e:
            algo.execute()
            assert "Must specify the Workspace to load into" in e.msg()
        algo.setPropertyValue("Workspace", fetched_groceries)
        assert fetched_groceries == algo.getPropertyValue("Workspace")
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
        assert errors.get("InstrumentDonor") is not None
        with pytest.raises(RuntimeError) as e:
            algo.execute()
            assert "invalid Properties found" in e.msg()
            assert "InsturmentDonor" in e.msg()
            assert "InstrumentName" in e.msg()
            assert "InstrumentFilename" in e.msg()
        # unset the instrument name
        algo.setPropertyValue("InstrumentName", "")
        assert len(algo.validateInputs()) == 0

        # set instrument donor
        algo.setPropertyValue("InstrumentDonor", self.sampleWS)
        assert self.sampleWS == algo.getPropertyValue("InstrumentDonor")
        # check errors if two instrument sources
        errors = algo.validateInputs()
        assert errors.get("InstrumentName") is not None
        assert errors.get("InstrumentFilename") is not None
        assert errors.get("InstrumentDonor") is not None
        with pytest.raises(RuntimeError) as e:
            algo.execute()
            assert "invalid Properties found" in e.msg()
            assert "InsturmentDonor" in e.msg()
            assert "InstrumentName" in e.msg()
            assert "InstrumentFilename" in e.msg()
        # unset the instrument name
        algo.setPropertyValue("InstrumentFilename", "")
        assert len(algo.validateInputs()) == 0

    def test_loadNexusNoLoader(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "")
        algo.setPropertyValue("Workspace", self.fetchedWS)
        assert algo.execute()
        assert CompareWorkspaces(
            Workspace1=self.fetchedWS,
            Workspace2=self.sampleWS,
        )
        assert "LoadNexusProcessed" == algo.getPropertyValue("LoaderType")

    def test_loadNexusLoader(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "LoadNexus")
        algo.setPropertyValue("Workspace", self.fetchedWS)
        assert algo.execute()
        assert CompareWorkspaces(
            Workspace1=self.fetchedWS,
            Workspace2=self.sampleWS,
        )
        assert "LoadNexus" == algo.getPropertyValue("LoaderType")

    def test_loadNexusEventLoader(self):
        # TODO save data in a way that can be loaded by LoadEventNexus
        pass

    def test_loadNexusProcessedLoader(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "LoadNexusProcessed")
        algo.setPropertyValue("Workspace", self.fetchedWS)
        assert algo.execute()
        assert CompareWorkspaces(
            Workspace1=self.fetchedWS,
            Workspace2=self.sampleWS,
        )
        assert "LoadNexusProcessed" == algo.getPropertyValue("LoaderType")

    def test_loadGroupings(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "LoadGroupingDefinition")
        algo.setPropertyValue("Workspace", f"_{self.runNumber}_grouping_file")
        algo.setPropertyValue("InstrumentFilename", self.instrumentFilepath)
        assert algo.execute()
        algo.setProperty("InstrumentFilename", "")

        algo.setPropertyValue("Workspace", f"_{self.runNumber}_grouping_name")
        algo.setPropertyValue("InstrumentName", "fakeSNAPLite")
        assert algo.execute()
        assert CompareWorkspaces(
            Workspace1=f"_{self.runNumber}_grouping_file",
            Workspace2=f"_{self.runNumber}_grouping_name",
        )
        algo.setProperty("InstrumentName", "")

        algo.setPropertyValue("Workspace", f"_{self.runNumber}_grouping_donor")
        algo.setPropertyValue("InstrumentDonor", self.sampleWS)
        assert algo.execute()
        assert CompareWorkspaces(
            Workspace1=f"_{self.runNumber}_grouping_file",
            Workspace2=f"_{self.runNumber}_grouping_donor",
        )

    def test_loadGroupingTwice(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("Filename", self.filepath)
        algo.setPropertyValue("LoaderType", "LoadGroupingDefinition")
        algo.setPropertyValue("Workspace", self.fetchedWS)
        algo.setPropertyValue("InstrumentFilename", self.instrumentFilepath)
        assert algo.execute()
        assert algo.getPropertyValue("LoaderType") == "LoadGroupingDefinition"
        assert algo.execute()
        assert algo.getPropertyValue("LoaderType") == ""


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
