import hashlib
import time
import unittest
import unittest.mock as mock
from pathlib import Path
from random import randint

from mantid.simpleapi import CreateSingleValuedWorkspace, DeleteWorkspace, mtd

from snapred.backend.dao.calibration import Calibration
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state import InstrumentState
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
        Almost all tests work by calling the data factory method, and ensuring the return is the same
        as the hashed value returned by the underlying lookup service when called with the same arguments.
        """
        cls.mockLookupService = mock.create_autospec(LocalDataService, spec_set=True, instance=True)
        method_list = [
            func
            for func in dir(LocalDataService)
            if callable(getattr(LocalDataService, func)) and not func.startswith("__")
        ]
        # these are treated specially for specific returns
        exceptions = ["readInstrumentConfig", "readStateConfig", "readRunConfig"]
        needIndexer = ["calibrationIndexer", "normalizationIndexer"]
        method_list = [method for method in method_list if method not in exceptions and method not in needIndexer]
        for x in method_list:
            setattr(getattr(cls.mockLookupService, x), "side_effect", lambda *x: cls.expected(cls, *x))

        mockStateId = "04bd2c53f6bf6754"
        mockInstrumentState = InstrumentState.model_construct(id=mockStateId)
        mockCalibration = Calibration.model_construct(instrumentState=mockInstrumentState)

        # these are treated specially as returning specific object types
        cls.mockLookupService.readInstrumentConfig.return_value = InstrumentConfig.model_construct({})
        #
        # ... allow the `StateConfig` to actually complete validation:
        #   this is required because `getReductionState` is declared in the wrong place... :(
        #
        cls.mockLookupService.readStateConfig.return_value = StateConfig.model_construct(
            stateId=mockStateId,
            calibration=mockCalibration,
        )
        cls.mockLookupService.readRunConfig.return_value = RunConfig.model_construct({})

        cls.mockLookupService.fileExists.return_value = lambda filePath: Path(filePath).exists()

        # these are treated specially to give the return of a mocked indexer
        cls.mockLookupService.calibrationIndexer.return_value = mock.Mock(
            versionPath=mock.Mock(side_effect=lambda *x: cls.expected(cls, "Calibration", *x)),
            getIndex=mock.Mock(return_value=[cls.expected(cls, "Calibration")]),
            latestApplicableVersion=mock.Mock(side_effect=lambda *x: cls.expected(cls, "Calibration", *x)),
        )
        cls.mockLookupService.normalizationIndexer.return_value = mock.Mock(
            versionPath=mock.Mock(side_effect=lambda *x: cls.expected(cls, "Normalization", *x)),
            getIndex=mock.Mock(return_value=[cls.expected(cls, "Normalization")]),
            latestApplicableVersion=mock.Mock(side_effect=lambda *x: cls.expected(cls, "Normalization", *x)),
        )

    def setUp(self):
        self.version = randint(2, 120)
        self.timestamp = time.time()
        self.instance = DataFactoryService()
        self.instance.lookupService = self.mockLookupService
        assert isinstance(self.instance, DataFactoryService)
        return super().setUp()

    def tearDown(self):
        """At the end of each test, clear out the workspaces"""
        del self.instance
        return super().tearDown()

    def test_fileExists(self):
        arg = mock.Mock()
        actual = self.instance.fileExists(arg)
        assert actual == self.expected(arg)

    def test_getRunConfig(self):
        actual = self.instance.getRunConfig(mock.Mock())
        assert type(actual) is RunConfig

    def test_getStateConfig(self):
        actual = self.instance.getStateConfig(mock.Mock(), mock.Mock())
        assert type(actual) is StateConfig

    def test_constructStateId(self):
        arg = mock.Mock()
        actual = self.instance.constructStateId(arg)
        assert actual == self.expected(arg)

    def test_stateExists(self):
        self.instance.lookupService.stateExists = mock.Mock(return_value=True)
        actual = self.instance.stateExists("123")
        assert actual

    def test_getGroupingMap(self):
        arg = mock.Mock()
        actual = self.instance.getGroupingMap(arg)
        assert actual == self.expected(arg)

    def test_getSampleFilePaths(self):
        actual = self.instance.getSampleFilePaths()
        assert actual == self.expected()

    def test_getCalibrantSample(self):
        actual = self.instance.getCalibrantSample("testId")
        assert actual == self.expected("testId")

    def test_getCifFilePath(self):
        actual = self.instance.getCifFilePath("testId")
        assert actual == self.expected("testId")

    def test_getDefaultGroupingMap(self):
        self.instance.lookupService.readDefaultGroupingMap = mock.Mock()
        actual = self.instance.getDefaultGroupingMap()
        assert actual == self.instance.lookupService.readDefaultGroupingMap.return_value

    def test_getDefaultInstrumentState(self):
        self.instance.lookupService.generateInstrumentState = mock.Mock()
        actual = self.instance.getDefaultInstrumentState("123")
        assert actual == self.instance.lookupService.generateInstrumentState.return_value

    ## TEST CALIBRATION METHODS

    def test_getCalibrationDataPath(self):
        run = "123"
        for useLiteMode in [True, False]:
            actual = self.instance.getCalibrationDataPath(run, useLiteMode, self.version)
            assert actual == self.expected("Calibration", self.version)  # NOTE mock indexer called only with version

    def test_checkCalibrationStateExists(self):
        actual = self.instance.checkCalibrationStateExists("123")
        assert actual == self.expected("123")

    def test_createCalibrationIndexEntry(self):
        request = mock.Mock()
        actual = self.instance.createCalibrationIndexEntry(request)
        assert actual == self.expected(request)

    def test_createCalibrationRecord(self):
        request = mock.Mock()
        actual = self.instance.createCalibrationRecord(request)
        assert actual == self.expected(request)

    def test_getCalibrationState(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getCalibrationState("123", useLiteMode)
            assert actual == self.expected("123", useLiteMode)

    def test_getCalibrationIndex(self):
        run = "123"
        for useLiteMode in [True, False]:
            actual = self.instance.getCalibrationIndex(run, useLiteMode)
            assert actual == [self.expected("Calibration")]

    def test_getCalibrationRecord(self):
        runId = "345"
        for useLiteMode in [True, False]:
            actual = self.instance.getCalibrationRecord(runId, useLiteMode, self.version)
            assert actual == self.expected(runId, useLiteMode, self.version)

    def test_getCalibrationDataWorkspace(self):
        self.instance.groceryService.fetchWorkspace = mock.Mock()
        for useLiteMode in [True, False]:
            actual = self.instance.getCalibrationDataWorkspace("456", useLiteMode, self.version, "bunko")
            assert actual == self.instance.groceryService.fetchWorkspace.return_value

    def test_getLatestCalibrationVersion(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getLatestApplicableCalibrationVersion("123", useLiteMode)
            assert actual == self.expected("Calibration", "123")

    ## TEST NORMALIZATION METHODS

    def test_getNormalizationDataPath(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getNormalizationDataPath("123", useLiteMode, self.version)
            assert actual == self.expected("Normalization", self.version)  # NOTE mock indexer called only with version

    def test_createNormalizationIndexEntry(self):
        request = mock.Mock()
        actual = self.instance.createNormalizationIndexEntry(request)
        assert actual == self.expected(request)

    def test_createNormalizationRecord(self):
        request = mock.Mock()
        actual = self.instance.createNormalizationRecord(request)
        assert actual == self.expected(request)

    def test_getNormalizationState(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getNormalizationState("123", useLiteMode)
            assert actual == self.expected("123", useLiteMode)

    def test_getNormalizationIndex(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getNormalizationIndex("123", useLiteMode)
            assert actual == [self.expected("Normalization")]

    def test_getNormalizationRecord(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getNormalizationRecord("123", useLiteMode, self.version)
            assert actual == self.expected("123", useLiteMode, self.version)

    def test_getNormalizationDataWorkspace(self):
        self.instance.groceryService.fetchWorkspace = mock.Mock()
        for useLiteMode in [True, False]:
            actual = self.instance.getNormalizationDataWorkspace("456", useLiteMode, self.version, "bunko")
            assert actual == self.instance.groceryService.fetchWorkspace.return_value

    def test_getLatestNormalizationVersion(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getLatestApplicableNormalizationVersion("123", useLiteMode)
            assert actual == self.expected("Normalization", "123")

    ## TEST REDUCTION METHODS

    def test_getReductionState(self):
        actual = self.instance.getReductionState("123", False)
        assert type(actual) is ReductionState

    def test_getReductionState_cache(self):
        previous = ReductionState.model_construct()
        self.instance.cache["456"] = previous
        actual = self.instance.getReductionState("456", False)
        assert actual == previous

    def test_getCompatibleReductionMasks(self):
        runNumber = "12345"
        useLiteMode = True
        arg = (runNumber, useLiteMode)
        actual = self.instance.getCompatibleReductionMasks(*arg)
        assert actual == self.expected(*arg)

    def test_getReductionDataPath(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getReductionDataPath("12345", useLiteMode, self.timestamp)
            assert actual == self.expected("12345", useLiteMode, self.timestamp)

    def test_getReductionRecord(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getReductionRecord("12345", useLiteMode, self.timestamp)
            assert actual == self.expected("12345", useLiteMode, self.timestamp)

    def test_getReductionData(self):
        for useLiteMode in [True, False]:
            actual = self.instance.getReductionData("12345", useLiteMode, self.timestamp)
            assert actual == self.expected("12345", useLiteMode, self.timestamp)

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

    def test_getCloneOfWorkspace(self):
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
        Config._config["cis_mode"]["enabled"] = True
        self.instance.deleteWorkspace(wsname)
        assert self.instance.workspaceDoesExist(wsname)

        # will delete otherwise
        Config._config["cis_mode"]["enabled"] = False
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

    ##### TEST LIVE-DATA SUPPORT METHODS ####

    def test_getLiveMetadata(self):
        actual = self.instance.getLiveMetadata()
        assert actual == self.expected()
