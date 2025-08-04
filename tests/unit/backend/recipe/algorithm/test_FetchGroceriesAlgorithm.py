# ruff: noqa: ARG001, PT012

import json
import os
from tempfile import TemporaryDirectory

from mantid.api import MatrixWorkspace, WorkspaceGroup
from mantid.dataobjects import MaskWorkspace, TableWorkspace
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

import unittest
from unittest import mock
import pytest

from util.helpers import createCompatibleDiffCalTable, createCompatibleMask

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
        cls.groupWorkspaceFilepath = Resource.getPath(f"inputs/test_{cls.runNumber}_fetchgroceriesalgo_group.nxs")
        cls.instrumentFilepath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
        cls.fetchedWS = f"_{cls.runNumber}_fetched"
        cls.rtolValue = 1.0e-10
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
        Delete all workspaces created by this test, and remove any created files.
        This is run once at the end of this test suite.
        """
        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        os.remove(cls.filepath)
        return super().tearDownClass()

    class _MockMtd:
        # In order to test flow-of-control, the only thing that matters is
        #   the _type_ of the workspace.
        
        # Track the `populateInstrumentParameters` calls with a single mock.
        populateInstrumentParameters = mock.Mock()
        
        @classmethod
        def _mockWorkspace(cls, wsClass, wsName, wsNames=None):
            # return a mock where `isinstance(mock, wsClass) == True`
            #   and <mock>.getTitle() == wsName
            _mock = mock.Mock(
                spec=wsClass,
                getTitle=mock.Mock(return_value=wsName),
                run=mock.Mock(),
                populateInstrumentParameters=TestFetchGroceriesAlgorithm._MockMtd.populateInstrumentParameters
            )
            if wsClass is WorkspaceGroup:
                assert wsNames is not None
                _mock.getNames = mock.Mock(return_value=wsNames)
            assert isinstance(_mock, wsClass)
            return _mock

        def __init__(self, wsNames=None):
            self._wsNames = wsNames

        def __getitem__(self, name):
            ws = None
            if "group" in name:
                ws = self._mockWorkspace(WorkspaceGroup, name, self._wsNames)
            elif "table" in name:
                ws = self._mockWorkspace(TableWorkspace, name)
            elif "mask" in name:
                ws = self._mockWorkspace(MaskWorkspace, name)
            else:
                ws = self._mockWorkspace(MatrixWorkspace, name)
            return ws

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

    def test_validateCalibrationFile(self):
        """Test that the calibration file is validated correctly"""
        algo = Algo()
        algo.initialize()
        # check that the file does not exist
        with pytest.raises(RuntimeError) as e:
            algo.validateCalibrationFile("non_existent_file.h5")
            assert "does not exist" in e.msg()

        # check that the file exists but does not contain a calibration group
        with pytest.raises(RuntimeError) as e:
            algo.validateCalibrationFile(self.filepath)
            assert "does not contain a 'calibration' group" in e.msg()

        # create a valid calibration file
        import h5py

        with TemporaryDirectory() as tempdir_path:
            calibrationFile = os.path.join(tempdir_path, "valid_calibration.h5")
            with h5py.File(calibrationFile, "w") as f:
                calibrationGroup = f.create_group("calibration")
                calibrationGroup.create_dataset("detid", data=[1, 2, 3])
                calibrationGroup.create_dataset("difc", data=[0.1, 0.2, 0.3])
                calibrationGroup.create_dataset("group", data=[1, 1, 2])
                calibrationGroup.create_dataset("instrument", data=["SNAP"] * 3)

            # validate file missing fields
            with pytest.raises(RuntimeError, match="does not contain the required dataset 'use'"):
                algo.validateCalibrationFile(calibrationFile)

            # add missing datasets
            with h5py.File(calibrationFile, "a") as f:
                calibrationGroup = f["calibration"]
                calibrationGroup.create_dataset("use", data=[True] * 3)
                # validate the valid calibration file
            algo.validateCalibrationFile(calibrationFile)

            # create a file with an unexpected dataset
            with h5py.File(calibrationFile, "a") as f:
                calibrationGroup = f["calibration"]
                calibrationGroup.create_dataset("unexpected", data=[1, 2, 3])
            with pytest.raises(RuntimeError, match="contains unexpected dataset 'unexpected'"):
                algo.validateCalibrationFile(calibrationFile)

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
            rtol=self.rtolValue,
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
            rtol=self.rtolValue,
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
            rtol=self.rtolValue,
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
            rtol=self.rtolValue,
        )
        algo.setProperty("InstrumentName", "")

        algo.setPropertyValue("OutputWorkspace", f"_{self.runNumber}_grouping_donor")
        algo.setPropertyValue("InstrumentDonor", self.sampleWS)
        assert algo.execute()
        assert_wksp_almost_equal(
            Workspace1=f"_{self.runNumber}_grouping_file",
            Workspace2=f"_{self.runNumber}_grouping_donor",
            rtol=self.rtolValue,
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
        assert algo.mantidSnapper.executeQueue.call_count == 1
    
    ##########################################################################################
    ## Analogies to the following unit tests also occur at `test_PV_logs_util.py` to allow  ##
    ##   testing of the fallback utility method `PV_logs_util.populateInstrumentParameters`.##
    ##########################################################################################
    
    def test_populateInstrumentParameters_single_workspace(self):
        # Test that `populateInstrumentParameters` is called when a `MatrixWorkspace` is loaded.
        test_ws = "test_PIP_workspace"
        mockSnapper_ = mock.Mock()
        mockSnapper_.Load=mock.Mock(return_value=(None, "LoadNexusProcessed", None))
        mockSnapper_.mtd = TestFetchGroceriesAlgorithm._MockMtd()
        mockSnapper_.mtd.doesExist = mock.Mock(return_value=False)
        assert isinstance(mockSnapper_.mtd[test_ws], MatrixWorkspace)
        
        algo = Algo()
        algo.initialize()        
        algo.setPropertyValue("Filename", "does/not/exist.nxs")
        algo.setPropertyValue("OutputWorkspace", test_ws)
        with (
            mock.patch.object(algo, "mantidSnapper", mockSnapper_) as mockSnapper,
            mock.patch.object(TestFetchGroceriesAlgorithm._MockMtd, "populateInstrumentParameters") as populateInstrumentParameters
        ):
            assert algo.execute()
            
            populateInstrumentParameters.assert_called_once()
            mockSnapper.executeQueue.assert_called_once()
    
    def test_populateInstrumentParameters_single_table_workspace(self):
        # Test that `populateInstrumentParameters` is not called when a `TableWorkspace` is loaded.
        test_ws = "test_PIP_table"
        mockSnapper_ = mock.Mock()
        mockSnapper_.LoadCalibrationWorkspaces=mock.Mock()
        mockSnapper_.mtd = TestFetchGroceriesAlgorithm._MockMtd()
        mockSnapper_.mtd.doesExist = mock.Mock(return_value=False)
        assert isinstance(mockSnapper_.mtd[test_ws], TableWorkspace)
        
        algo = Algo()
        algo.initialize()        
        algo.setPropertyValue("Filename", "does/not/exist.nxs")
        algo.setPropertyValue("LoaderType", "LoadCalibrationWorkspaces")
        algo.setPropertyValue(
            "LoaderArgs",
            json.dumps(
                {
                    "CalibrationTable": test_ws,
                    "MaskWorkspace": ""
                }
            )
        )
        algo.setPropertyValue("OutputWorkspace", "")
        with (
            mock.patch.object(algo, "mantidSnapper", mockSnapper_) as mockSnapper,
            mock.patch.object(TestFetchGroceriesAlgorithm._MockMtd, "populateInstrumentParameters") as populateInstrumentParameters
        ):
            assert algo.execute()            
            populateInstrumentParameters.assert_not_called()
    
    def test_populateInstrumentParameters_single_mask_workspace(self):
        # Test that `populateInstrumentParameters` is NOT called by `FetchGroceriesAlgorithm`
        #   when a `MaskWorkspace` is loaded.
        #   As appropriate (i.e.  not for `TableWorkspace`), instrument parameters for any workspaces loaded using
        #  `LoadCalibrationWorkspaces`, are populated within that algorithm. 
        test_table_ws = "test_PIP_table"
        test_mask_ws = "test_PIP_mask"
        mockSnapper_ = mock.Mock()
        mockSnapper_.LoadCalibrationWorkspaces=mock.Mock()
        mockSnapper_.mtd = TestFetchGroceriesAlgorithm._MockMtd()
        mockSnapper_.mtd.doesExist = mock.Mock(return_value=False)
        assert isinstance(mockSnapper_.mtd[test_table_ws], TableWorkspace)
        assert isinstance(mockSnapper_.mtd[test_mask_ws], MaskWorkspace)
        
        algo = Algo()
        algo.initialize()        
        algo.setPropertyValue("Filename", "does/not/exist.nxs")
        algo.setPropertyValue("LoaderType", "LoadCalibrationWorkspaces")
        algo.setPropertyValue(
            "LoaderArgs",
            json.dumps(
                {
                    "CalibrationTable": test_table_ws,
                    "MaskWorkspace": test_mask_ws
                }
            )
        )
        algo.setPropertyValue("OutputWorkspace", test_mask_ws)
        with (
            mock.patch.object(algo, "mantidSnapper", mockSnapper_) as mockSnapper,
            mock.patch.object(TestFetchGroceriesAlgorithm._MockMtd, "populateInstrumentParameters") as populateInstrumentParameters
        ):
            assert algo.execute()            
            populateInstrumentParameters.assert_not_called()

    def test_populateInstrumentParameters_group_workspace(self):
        # Test that `populateInstrumentParameters` is called for each workspace when a `WorkspaceGroup` is loaded.
        test_ws = "test_PIP_group"
        mockSnapper_ = mock.Mock()
        mockSnapper_.Load=mock.Mock(return_value=(None, "LoadNexusProcessed", None))
        mockSnapper_.mtd = TestFetchGroceriesAlgorithm._MockMtd(wsNames=["ws1", "ws2", "ws3", "ws4", "ws5"])
        mockSnapper_.mtd.doesExist = mock.Mock(return_value=False)
        assert isinstance(mockSnapper_.mtd[test_ws], WorkspaceGroup)
        N_wss = len(mockSnapper_.mtd[test_ws].getNames())

        
        algo = Algo()
        algo.initialize()        
        algo.setPropertyValue("Filename", "does/not/exist.nxs")
        algo.setPropertyValue("OutputWorkspace", test_ws)
        with (
            mock.patch.object(algo, "mantidSnapper", mockSnapper_) as mockSnapper,
            mock.patch.object(TestFetchGroceriesAlgorithm._MockMtd, "populateInstrumentParameters") as populateInstrumentParameters
        ):
            assert algo.execute()
            assert populateInstrumentParameters.call_count == N_wss
            mockSnapper.executeQueue.assert_called_once()


    def test_populateInstrumentParameters_group_workspace_with_table(self):
        # Test that `populateInstrumentParameters` is called for any `TableWorkspace` within a `WorkspaceGroup`.        
        #   * Question: can `LoadNexusProcessed` actually deal with this type of workspace group?
        test_ws = "test_PIP_group_with_table"
        mockSnapper_ = mock.Mock()
        mockSnapper_.Load=mock.Mock(return_value=(None, "LoadNexusProcessed", None))
        mockSnapper_.mtd = TestFetchGroceriesAlgorithm._MockMtd(wsNames=["ws1", "ws2", "table_ws3", "ws4", "ws5"])
        mockSnapper_.mtd.doesExist = mock.Mock(return_value=False)
        assert isinstance(mockSnapper_.mtd[test_ws], WorkspaceGroup)
        N_wss = len(mockSnapper_.mtd[test_ws].getNames())
        
        algo = Algo()
        algo.initialize()        
        algo.setPropertyValue("Filename", "does/not/exist.nxs")
        algo.setPropertyValue("OutputWorkspace", test_ws)
        with (
            mock.patch.object(algo, "mantidSnapper", mockSnapper_) as mockSnapper,
            mock.patch.object(TestFetchGroceriesAlgorithm._MockMtd, "populateInstrumentParameters") as populateInstrumentParameters
        ):
            assert algo.execute()
            assert populateInstrumentParameters.call_count == N_wss - 1
            mockSnapper.executeQueue.assert_called_once()

    def test_populateInstrumentParameters_group_workspace_all_tables(self):
        # Test that `populateInstrumentParameters` is NOT called for a `WorkspaceGroup` consisting entirely of `TableWorkspace`s.
        #   * Question: can `LoadNexusProcessed` actually deal with this type of workspace group?
        test_ws = "test_PIP_group_all_tables"
        mockSnapper_ = mock.Mock()
        mockSnapper_.Load=mock.Mock(return_value=(None, "LoadNexusProcessed", None))
        mockSnapper_.mtd = TestFetchGroceriesAlgorithm._MockMtd(wsNames=["table_ws1", "table_ws2", "table_ws3", "table_ws4", "table_ws5"])
        mockSnapper_.mtd.doesExist = mock.Mock(return_value=False)
        assert isinstance(mockSnapper_.mtd[test_ws], WorkspaceGroup)
        
        algo = Algo()
        algo.initialize()        
        algo.setPropertyValue("Filename", "does/not/exist.nxs")
        algo.setPropertyValue("OutputWorkspace", test_ws)
        with (
            mock.patch.object(algo, "mantidSnapper", mockSnapper_) as mockSnapper,
            mock.patch.object(TestFetchGroceriesAlgorithm._MockMtd, "populateInstrumentParameters") as populateInstrumentParameters
        ):
            assert algo.execute()
            populateInstrumentParameters.assert_not_called()
            mockSnapper.executeQueue.assert_called_once()
