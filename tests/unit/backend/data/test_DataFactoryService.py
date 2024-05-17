import hashlib
import os.path
import tempfile
import unittest
import unittest.mock as mock

from mantid.simpleapi import CreateSingleValuedWorkspace, DeleteWorkspace, mtd
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.LocalDataService import LocalDataService


class TestDataFactoryService(unittest.TestCase):
    def expected(cls, *args):
        hasher = hashlib.shake_256()
        decodedArgs = str(args).encode("utf-8")
        hasher.update(decodedArgs)
        return hasher.digest(8).hex()

    @classmethod
    def setUpClass(cls):
        """
        Create a lookup service, which is the underlying service called by the data factory
        This service almost always returns a hashed result of the argument list, for validation
        """
        cls.mockLookupService = mock.create_autospec(LocalDataService, spec_set=True, instance=True)
        method_list = [
            func
            for func in dir(LocalDataService)
            if callable(getattr(LocalDataService, func)) and not func.startswith("__")
        ]
        # these are treated specially for specific returns
        exceptions = ["readInstrumentConfig", "readStateConfig", "readRunConfig"]
        method_list = [method for method in method_list if method not in exceptions]
        for x in method_list:
            setattr(getattr(cls.mockLookupService, x), "side_effect", lambda *x: cls.expected(cls, *x))
        # these are treated specially as returning specific object types
        cls.mockLookupService.readInstrumentConfig.return_value = InstrumentConfig.construct({})
        cls.mockLookupService.readStateConfig.return_value = StateConfig.construct({})
        cls.mockLookupService.readRunConfig.return_value = RunConfig.construct({})

    def setUp(self):
        self.instance = DataFactoryService()
        self.instance.lookupService = self.mockLookupService
        assert isinstance(self.instance, DataFactoryService)
        return super().setUp()

    def tearDown(self):
        """At the end of each test, clear out the workspaces"""
        del self.instance
        return super().tearDown()

    def test_fileExists_yes(self):
        # create a temp file that exists, and verify it exists
        with tempfile.NamedTemporaryFile(suffix=".biscuit") as existent:
            assert DataFactoryService().fileExists(existent.name)

    def test_fileExists_no(self):
        # assert that a file that does not exist, does not exist
        with tempfile.TemporaryDirectory() as tmpdir:
            nonexistent = tmpdir + "/0x0f.biscuit"
            assert not os.path.isfile(nonexistent)
            assert not DataFactoryService().fileExists(nonexistent)

    def test_getReductionState(self):
        actual = self.instance.getReductionState("123", False)
        assert type(actual) == ReductionState

    def test_getReductionState_cache(self):
        previous = ReductionState.construct()
        self.instance.cache["456"] = previous
        actual = self.instance.getReductionState("456", False)
        assert actual == previous

    def test_getRunConfig(self):
        actual = self.instance.getRunConfig(mock.Mock())
        assert type(actual) == RunConfig

    def test_getStateConfig(self):
        actual = self.instance.getStateConfig(mock.Mock(), mock.Mock())
        assert type(actual) == StateConfig

    def test_constructStateId(self):
        arg = mock.Mock()
        actual = self.instance.constructStateId(arg)
        assert actual == self.expected(arg)

    def test_getCalibrationState(self):
        actual = self.instance.getCalibrationState("123", False)
        assert actual == self.expected("123", False)

    def test_getGroupingMap(self):
        arg = mock.Mock()
        actual = self.instance.getGroupingMap(arg)
        assert actual == self.expected(arg)

    def test_checkCalibrationStateExists(self):
        actual = self.instance.checkCalibrationStateExists("123")
        assert actual == self.expected("123")

    def test_getSampleFilePaths(self):
        actual = self.instance.getSampleFilePaths()
        assert actual == self.expected()

    def test_getCalibrantSample(self):
        actual = self.instance.getCalibrantSample("testId")
        assert actual == self.expected("testId")

    def test_getCifFilePath(self):
        actual = self.instance.getCifFilePath("testId")
        assert actual == self.expected("testId")

    def test_getCalibrationIndex(self):
        run = "123"
        useLiteMode = False
        actual = self.instance.getCalibrationIndex(run, useLiteMode)
        assert actual == self.expected(run, useLiteMode)

    def test_getCalibrationDataPath(self):
        run = "123"
        version = 17
        useLiteMode = False
        actual = self.instance.getCalibrationDataPath(run, useLiteMode, version)
        assert actual == self.expected(run, useLiteMode, version)

    def test_getCalibrationRecord(self):
        runId = "345"
        useLiteMode = False
        version = 12
        actual = self.instance.getCalibrationRecord(runId, useLiteMode, version)
        assert actual == self.expected(runId, useLiteMode, version)

    def test_getCalibrationDataWorkspace(self):
        self.instance.groceryService.fetchWorkspace = mock.Mock()
        actual = self.instance.getCalibrationDataWorkspace("456", True, 8, "bunko")
        assert actual == self.instance.groceryService.fetchWorkspace.return_value

    ## TEST NORMALIZATION METHODS

    def test_getNormalizationDataPath(self):
        actual = self.instance.getNormalizationDataPath("123", True, 0)
        assert actual == self.expected("123", True, 0)

    def test_getNormalizationState(self):
        actual = self.instance.getNormalizationState("123", False)
        assert actual == self.expected("123", False)

    def test_getNormalizationIndex(self):
        actual = self.instance.getNormalizationIndex("123", False)
        assert actual == self.expected("123", False)

    def test_getNormalizationRecord(self):
        actual = self.instance.getNormalizationRecord("123", False, 7)
        assert actual == self.expected("123", False, 7)

    def test_getNormalizationDataWorkspace(self):
        self.instance.groceryService.fetchWorkspace = mock.Mock()
        actual = self.instance.getNormalizationDataWorkspace("456", True, 8, "bunko")
        assert actual == self.instance.groceryService.fetchWorkspace.return_value

    ## TEST REDUCTION METHODS

    def test_getReductionDataPath(self):
        actual = self.instance.getReductionDataPath("12345", True, "Column", "11")
        assert actual == self.expected("12345", True, "Column", "11")

    def test_getReductionRecord(self):
        actual = self.instance.getReductionRecord("12345", True, "Column", 11)
        assert actual == self.expected("12345", True, "Column", 11)

    def test_getReductionData(self):
        actual = self.instance.getReductionData("12345", True, "Column", 11)
        assert actual == self.expected("12345", True, "Column", 11)

    ##### TEST WORKSPACE METHODS ####

    def test_workspaceDoesExist(self):
        wsname = mtd.unique_name()
        assert not self.instance.workspaceDoesExist(wsname)
        ws = CreateSingleValuedWorkspace()
        mtd.add(wsname, ws)
        assert self.instance.workspaceDoesExist(wsname)
        DeleteWorkspace(wsname)

    def test_getWorkspaceForName(self):
        wsname = mtd.unique_name()
        assert not self.instance.workspaceDoesExist(wsname)
        ws1 = CreateSingleValuedWorkspace()
        mtd.add(wsname, ws1)
        assert self.instance.workspaceDoesExist(wsname)
        ws2 = self.instance.getWorkspaceForName(wsname)
        assert ws1.name() == ws2.name()
        ws2.delete()
        assert not self.instance.workspaceDoesExist(wsname)

    def test_getCloneOfWprkspace(self):
        wsname1 = mtd.unique_name()
        wsname2 = mtd.unique_name()
        assert not self.instance.workspaceDoesExist(wsname1)
        assert not self.instance.workspaceDoesExist(wsname2)
        ws1 = CreateSingleValuedWorkspace()
        ws1.setComment(wsname1 + wsname2)
        mtd.add(wsname1, ws1)
        assert self.instance.workspaceDoesExist(wsname1)
        ws2 = self.instance.getCloneOfWorkspace(wsname1, wsname2)
        assert ws1.getComment() == ws2.getComment()
        DeleteWorkspace(wsname1)
        assert self.instance.workspaceDoesExist(wsname2)
        DeleteWorkspace(wsname2)

    def test_getWorkspaceCached(self):
        self.instance.groceryService.fetchNeutronDataCached = mock.Mock()
        self.instance.getWorkspaceCached("123", True)
        assert self.instance.groceryService.fetchNeutronDataCached.called

    def test_getWorkspaceSingleUse(self):
        self.instance.groceryService.fetchNeutronDataSingleUse = mock.Mock()
        self.instance.getWorkspaceSingleUse("123", True)
        assert self.instance.groceryService.fetchNeutronDataSingleUse.called

    def test_deleteWorkspace(self):
        from snapred.meta.Config import Config

        wsname = mtd.unique_name()
        assert not self.instance.workspaceDoesExist(wsname)
        ws = CreateSingleValuedWorkspace()
        mtd.add(wsname, ws)
        assert self.instance.workspaceDoesExist(wsname)

        # won't delete in cis mode
        Config._config["cis_mode"] = True
        self.instance.deleteWorkspace(wsname)
        assert self.instance.workspaceDoesExist(wsname)

        # will delete otherwise
        Config._config["cis_mode"] = False
        self.instance.deleteWorkspace(wsname)
        assert not self.instance.workspaceDoesExist(wsname)

    def test_deleteWorkspaceUnconditional(self):
        wsname = mtd.unique_name()
        assert not self.instance.workspaceDoesExist(wsname)
        ws = CreateSingleValuedWorkspace()
        mtd.add(wsname, ws)
        assert self.instance.workspaceDoesExist(wsname)
        self.instance.deleteWorkspaceUnconditional(wsname)
        assert not self.instance.workspaceDoesExist(wsname)
