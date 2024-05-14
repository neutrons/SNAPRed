import hashlib
import os.path
import tempfile
import unittest
import unittest.mock as mock

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
        print(decodedArgs)
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
        for x in method_list:
            setattr(getattr(cls.mockLookupService, x), "side_effect", lambda *x: cls.expected(cls, *x))
        # these are treated specially as returning specific object types
        cls.mockLookupService.readInstrumentConfig.side_effect = None
        cls.mockLookupService.readRunConfig.side_effect = None
        cls.mockLookupService.readStateConfig.side_effect = None
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

    def test_getSamplePaths(self):
        actual = self.instance.getSamplePaths()
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
