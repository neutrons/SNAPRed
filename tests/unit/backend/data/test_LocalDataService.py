# ruff: noqa: E402

import functools
import importlib
import json
import logging
import os
import socket
import tempfile
import unittest.mock as mock
from pathlib import Path
from random import randint, shuffle
from typing import List

import h5py
import pytest
from mantid.api import ITableWorkspace, MatrixWorkspace
from mantid.dataobjects import MaskWorkspace
from mantid.kernel import amend_config
from mantid.simpleapi import (
    CloneWorkspace,
    CompareWorkspaces,
    CreateGroupingWorkspace,
    CreateSampleWorkspace,
    DeleteWorkspace,
    LoadEmptyInstrument,
    LoadInstrument,
    RenameWorkspaces,
    mtd,
)
from pydantic import parse_raw_as
from snapred.backend.dao import StateConfig
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.data.Indexor import IndexorType
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.meta.redantic import write_model_pretty
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask
from util.state_helpers import reduction_root_redirect, state_root_redirect

LocalDataServiceModule = importlib.import_module(LocalDataService.__module__)
ThisService = "snapred.backend.data.LocalDataService."

VERSION_START = Config["version.calibration.start"]
IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")


@pytest.fixture(autouse=True)
def _capture_logging(monkeypatch):
    # For some reason pytest 'caplog' doesn't work with the SNAPRed logging setup.  (TODO: fix this!)
    # This patch bypasses the issue, by renaming and
    # patching the `LocalDataService` module's logger to a standard python `Logger`.
    defaultLogger = logging.getLogger(LocalDataServiceModule.__name__ + "_patch")
    defaultLogger.propagate = True
    monkeypatch.setattr(LocalDataServiceModule, "logger", defaultLogger)


fakeInstrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
reductionIngredientsPath = Resource.getPath("inputs/calibration/ReductionIngredients.json")
reductionIngredients = ReductionIngredients.parse_file(reductionIngredientsPath)

### TESTS OF MISCELLANEOUS METHODS ###

"""
THESE SHOULD ONLY TEST METHODS IN THE MISCELLANEOUS METHODS
SECTION OF LOCAL DATA SERVICE.

NOT FOR ANY OL' RANDOM METHOD YOU WANT TO TEST

I can see your name in the gitblame.
I know where your office is.
I will find you.
"""


def test_fileExists_yes():
    # create a temp file that exists, and verify it exists
    with tempfile.NamedTemporaryFile(suffix=".biscuit") as existent:
        assert LocalDataService().fileExists(existent.name)


def test_fileExists_no():
    # assert that a file that does not exist, does not exist
    with tempfile.TemporaryDirectory() as tmpdir:
        nonexistent = tmpdir + "/0x0f.biscuit"
        assert not os.path.isfile(nonexistent)
        assert not LocalDataService().fileExists(nonexistent)


def test_readInstrumentConfig():
    localDataService = LocalDataService()
    localDataService._readInstrumentParameters = _readInstrumentParameters
    actual = localDataService.readInstrumentConfig()
    assert actual is not None
    assert actual.version == 1.4
    assert actual.name == "SNAP"


def test_readInstrumentParameters():
    localDataService = LocalDataService()
    localDataService.instrumentConfigPath = Resource.getPath("inputs/SNAPInstPrm.json")
    actual = localDataService._readInstrumentParameters()
    assert actual is not None
    assert actual["version"] == 1.4
    assert actual["name"] == "SNAP"


# NOTE: This test fails on analysis because the instrument home actually does exist!
@pytest.mark.skipif(
    IS_ON_ANALYSIS_MACHINE, reason="This test fails on analysis because the instrument home actually does exist!"
)
def test_badPaths():
    """This verifies that a broken configuration (from production) can't find all of the files"""
    # get a handle on the service
    service = LocalDataService()
    service.verifyPaths = True  # override test setting
    prevInstrumentHome = Config._config["instrument"]["home"]
    Config._config["instrument"]["home"] = "/this/path/does/not/exist"
    with pytest.raises(FileNotFoundError):
        service.readInstrumentConfig()
    Config._config["instrument"]["home"] = prevInstrumentHome
    service.verifyPaths = False  # put the setting back


def test_noInstrumentConfig():
    """This verifies that a broken configuration (from production) can't find all of the files"""
    # get a handle on the service
    service = LocalDataService()
    service.verifyPaths = True  # override test setting
    prevInstrumentConfig = Config._config["instrument"]["config"]
    Config._config["instrument"]["config"] = "/this/path/does/not/exist"
    with pytest.raises(FileNotFoundError):
        service.readInstrumentConfig()
    Config._config["instrument"]["config"] = prevInstrumentConfig
    service.verifyPaths = False  # put the setting back


def getMockInstrumentConfig():
    instrumentConfig = mock.Mock()
    instrumentConfig.calibrationDirectory = Path("test")
    instrumentConfig.sharedDirectory = "test"
    instrumentConfig.reducedDataDirectory = "test"
    instrumentConfig.pixelGroupingDirectory = "test"
    instrumentConfig.delTOverT = 1
    instrumentConfig.nexusDirectory = "test"
    instrumentConfig.nexusFileExtension = "test"
    return instrumentConfig


def _readInstrumentParameters():
    instrumentParameters = None
    with Resource.open("inputs/SNAPInstPrm.json", "r") as file:
        instrumentParameters = json.loads(file.read())
    return instrumentParameters


def test_readStateConfig():
    groupingMapPath = Resource.getPath("inputs/pixel_grouping/groupingMap.json")
    parametersPath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        tmpRoot.addFileAs(
            groupingMapPath,
            localDataService._groupingMapPath(tmpRoot.stateId),
        )
        tmpRoot.addFileAs(parametersPath, localDataService.calibrationIndex("57514", True).parametersPath(1))
        actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    assert actual.stateId == "ab8704b0bc2a2342"


def test_readStateConfig_attaches_grouping_map():
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    groupingMapPath = Resource.getPath("inputs/pixel_grouping/groupingMap.json")
    parametersPath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        tmpRoot.addFileAs(
            groupingMapPath,
            localDataService._groupingMapPath(tmpRoot.stateId),
        )
        tmpRoot.addFileAs(parametersPath, localDataService.calibrationIndex("57514", True).parametersPath(1))
        actual = localDataService.readStateConfig("57514", True)
    expectedMap = GroupingMap.parse_file(groupingMapPath)
    assert actual.groupingMap == expectedMap


def test_readStateConfig_invalid_grouping_map():
    # Test that the attached grouping-schema map's 'stateId' is checked.
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    groupingMapPath = Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
    parametersPath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        tmpRoot.addFileAs(
            groupingMapPath,
            localDataService._groupingMapPath(tmpRoot.stateId),
        )
        tmpRoot.addFileAs(parametersPath, localDataService.calibrationIndex("57514", True).parametersPath(1))
        # 'GroupingMap.defaultStateId' is _not_ a valid grouping-map 'stateId' for an existing `StateConfig`.
        with pytest.raises(  # noqa: PT012
            RuntimeError,
            match="the state configuration's grouping map must have the same 'stateId' as the configuration",
        ):
            localDataService.readStateConfig("57514", True)


def test_readStateConfig_calls_prepareStateRoot():
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    groupingMapPath = Resource.getPath("inputs/pixel_grouping/groupingMap.json")
    parametersPath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        tmpRoot.addFileAs(
            groupingMapPath,
            localDataService._groupingMapPath(tmpRoot.stateId),
        )
        tmpRoot.addFileAs(parametersPath, localDataService.calibrationIndex("57514", True).parametersPath(1))
        localDataService._prepareStateRoot = mock.Mock()
        actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    localDataService._prepareStateRoot.assert_called_once()


def test_write_model_pretty_StateConfig_excludes_grouping_map():
    # At present there is no `writeStateConfig` method, and there is no `readStateConfig` that doesn't
    #   actually build up the `StateConfig` from its components.
    # This test verifies that `GroupingMap` is excluded from any future `StateConfig` JSON serialization.
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value="IPTS-123")
    localDataService._readPVFile = mock.Mock()
    fileMock = mock.Mock()
    localDataService._readPVFile.return_value = fileMock
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
    localDataService.readCalibrationState = mock.Mock()
    localDataService.readCalibrationState.return_value = Calibration.parse_file(
        Resource.getPath("inputs/calibration/CalibrationParameters.json")
    )

    localDataService._groupingMapPath = mock.Mock()
    localDataService._groupingMapPath.return_value = Path(Resource.getPath("inputs/pixel_grouping/groupingMap.json"))
    stateGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/groupingMap.json"))
    localDataService._readGroupingMap = mock.Mock()
    localDataService._readGroupingMap.return_value = stateGroupingMap

    localDataService.instrumentConfig = getMockInstrumentConfig()

    actual = localDataService.readStateConfig("57514", True)
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        stateConfigPath = Path(tempdir) / "stateConfig.json"
        write_model_pretty(actual, stateConfigPath)
        # read it back in:
        stateConfig = None
        with open(stateConfigPath, "r") as file:
            stateConfig = parse_raw_as(StateConfig, file.read())
        assert stateConfig.groupingMap is None


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists(GetIPTS):  # noqa ARG002
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        tmpRoot.path().mkdir()
        runNumber = "654321"
        assert localDataService.checkCalibrationFileExists(runNumber)


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists_stupid_number(GetIPTS):
    localDataService = LocalDataService()

    # try with a non-number
    runNumber = "fruitcake"
    assert not localDataService.checkCalibrationFileExists(runNumber)
    assert not GetIPTS.called

    # try with a too-small number
    runNumber = "7"
    assert not localDataService.checkCalibrationFileExists(runNumber)
    assert not GetIPTS.called


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists_bad_ipts(GetIPTS):
    GetIPTS.side_effect = RuntimeError("YOU IDIOT!")
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        assert Path(tmpDir).exists()
        localDataService = LocalDataService()
        runNumber = "654321"
        assert not localDataService.checkCalibrationFileExists(runNumber)


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists_not(GetIPTS):  # noqa ARG002
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        nonExistentPath = tmpRoot.path() / "1755"
        assert not nonExistentPath.exists()
        runNumber = "654321"
        assert not localDataService.checkCalibrationFileExists(runNumber)


@mock.patch(ThisService + "GetIPTS")
def test_getIPTS(mockGetIPTS):
    mockGetIPTS.return_value = "nowhere/"
    localDataService = LocalDataService()
    runNumber = "123456"
    res = localDataService.getIPTS(runNumber)
    assert res == mockGetIPTS.return_value
    assert mockGetIPTS.called_with(
        runNumber=runNumber,
        instrumentName=Config["instrument.name"],
    )
    res = localDataService.getIPTS(runNumber, "CRACKLE")
    assert res == mockGetIPTS.return_value
    assert mockGetIPTS.called_with(
        runNumber=runNumber,
        instrumentName="CRACKLE",
    )


# NOTE this test calls `GetIPTS` (via `getIPTS`) with no mocks
# this is intentional, to ensure it is being called correctly
def test_getIPTS_cache():
    localDataService = LocalDataService()
    localDataService.getIPTS.cache_clear()
    assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=0, misses=0, maxsize=128, currsize=0)

    # test data
    instrument = "SNAP"
    runNumber = "123"
    key = (runNumber, instrument)
    correctIPTS = Path(Resource.getPath("inputs/testInstrument/IPTS-456"))
    incorrectIPTS = Path(Resource.getPath("inputs/testInstrument/IPTS-789"))

    # direct GetIPTS to look in the exact folder where it should look
    # it is very stupid, so if you don't tell it exactly then it won't look there
    with amend_config(data_dir=str(correctIPTS / "nexus")):
        res = localDataService.getIPTS(*key)
        assert res == str(correctIPTS) + os.sep
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=0, misses=1, maxsize=128, currsize=1)

        # call again and make sure the cached value is being returned
        res = localDataService.getIPTS(*key)
        assert res == str(correctIPTS) + os.sep

        # WARNING: For this test: mocking `GetIPTS` at the module level is also possible,
        #   but this was not implemented correctly previously.
        #   If that approach is taken, the important thing is to make sure
        #   that the mock is _removed_ at the end of the test.
        #   Note that `localDataService.GetIPTS` is not the same thing as
        #   `LocalDataService <module>.GetIPTS`.
        #   For these tests, the @Singleton aspect of the class is removed,
        #   but that doesn't mean that changes to the module won't affect
        #   other tests.

        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=1, misses=1, maxsize=128, currsize=1)

    # now try it again, but with another IPTS directory
    with amend_config(data_dir=str(incorrectIPTS / "nexus")):
        # previous correct value should still be the cached value
        res = localDataService.getIPTS(*key)
        assert res == str(correctIPTS) + os.sep
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=2, misses=1, maxsize=128, currsize=1)

        # clear the cache,  make sure the new value is being returned
        localDataService.getIPTS.cache_clear()
        res = localDataService.getIPTS(*key)
        assert res == str(incorrectIPTS) + os.sep
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=0, misses=1, maxsize=128, currsize=1)


def test_workspaceIsInstance():
    localDataService = LocalDataService()
    # Create a sample workspace.
    testWS0 = "test_ws"
    LoadEmptyInstrument(
        Filename=fakeInstrumentFilePath,
        OutputWorkspace=testWS0,
    )
    assert mtd.doesExist(testWS0)
    assert localDataService.workspaceIsInstance(testWS0, MatrixWorkspace)

    # Create diffraction-calibration table and mask workspaces.
    tableWS = "test_table"
    maskWS = "test_mask"
    createCompatibleDiffCalTable(tableWS, testWS0)
    createCompatibleMask(maskWS, testWS0, fakeInstrumentFilePath)
    assert mtd.doesExist(tableWS)
    assert mtd.doesExist(maskWS)
    assert localDataService.workspaceIsInstance(tableWS, ITableWorkspace)
    assert localDataService.workspaceIsInstance(maskWS, MaskWorkspace)
    mtd.clear()


def test_workspaceIsInstance_no_ws():
    localDataService = LocalDataService()
    # A sample workspace which doesn't exist.
    testWS0 = "test_ws"
    assert not mtd.doesExist(testWS0)
    assert not localDataService.workspaceIsInstance(testWS0, MatrixWorkspace)


def test_readRunConfig():
    # test of public `readRunConfig` method
    localDataService = LocalDataService()
    localDataService._readRunConfig = mock.Mock()
    localDataService._readRunConfig.return_value = "57514"
    actual = localDataService.readRunConfig(mock.Mock())
    assert actual is not None
    assert actual == "57514"


def test__readRunConfig():
    # Test of private `_readRunConfig` method
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value="IPTS-123")
    localDataService.instrumentConfig = getMockInstrumentConfig()
    actual = localDataService._readRunConfig("57514")
    assert actual is not None
    assert actual.runNumber == "57514"


def test_constructPVFilePath():
    # ensure the file path points to inside the IPTS folder
    localDataService = LocalDataService()
    # mock the IPTS to point to somewhere then construct the path
    mockIPTS = Resource.getPath("inputs/testInstrument/IPTS-456")
    mockRunConfig = mock.Mock(IPTS=mockIPTS)
    localDataService._readRunConfig = mock.Mock(return_value=mockRunConfig)
    path = localDataService._constructPVFilePath("123")
    # the path should be /path/to/testInstrument/IPTS-456/nexus/SNAP_123.nxs.h5
    assert mockIPTS == str(path.parents[1])


@mock.patch("h5py.File", return_value="not None")
def test_readPVFile(h5pyMock):  # noqa: ARG001
    localDataService = LocalDataService()
    localDataService.instrumentConfig = getMockInstrumentConfig()
    localDataService._constructPVFilePath = mock.Mock()
    localDataService._constructPVFilePath.return_value = Path(Resource.getPath("./"))
    actual = localDataService._readPVFile(mock.Mock())
    assert actual is not None


def test__generateStateId():
    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()
    fileMock = mock.Mock()
    localDataService._readPVFile.return_value = fileMock
    fileMock.get.side_effect = [[0.1], [0.1], [0.1], [0.1], [1], [0.1], [0.1]]
    actual, _ = localDataService._generateStateId("12345")
    assert actual == "9618b936a4419a6e"


def test__generateStateId_cache():
    localDataService = LocalDataService()
    localDataService._generateStateId.cache_clear()
    assert localDataService._generateStateId.cache_info() == functools._CacheInfo(
        hits=0, misses=0, maxsize=128, currsize=0
    )

    localDataService._readPVFile = mock.Mock()
    fileMock = mock.Mock()
    localDataService._readPVFile.return_value = fileMock
    fileMock.get.side_effect = [
        [0.1],
        [0.1],
        [0.1],
        [0.1],
        [1],
        [0.1],
        [0.1],  # => "9618b936a4419a6e"
        [0.2],
        [0.2],
        [0.2],
        [0.2],
        [1],
        [0.2],
        [0.2],
    ]
    stateSHA1 = "9618b936a4419a6e"
    stateSHA2 = "fa0bb25b44874edb"

    actual, _ = localDataService._generateStateId("12345")
    assert actual == stateSHA1
    assert localDataService._generateStateId.cache_info() == functools._CacheInfo(
        hits=0, misses=1, maxsize=128, currsize=1
    )

    # check cached value
    actual, _ = localDataService._generateStateId("12345")
    assert actual == stateSHA1
    assert localDataService._generateStateId.cache_info() == functools._CacheInfo(
        hits=1, misses=1, maxsize=128, currsize=1
    )

    # check a different value
    actual, _ = localDataService._generateStateId("67890")
    assert actual == stateSHA2
    assert localDataService._generateStateId.cache_info() == functools._CacheInfo(
        hits=1, misses=2, maxsize=128, currsize=2
    )
    # ... and its cached value
    actual, _ = localDataService._generateStateId("67890")
    assert actual == stateSHA2
    assert localDataService._generateStateId.cache_info() == functools._CacheInfo(
        hits=2, misses=2, maxsize=128, currsize=2
    )


def test__findMatchingFileList():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = getMockInstrumentConfig()
    actual = localDataService._findMatchingFileList(Resource.getPath("inputs/SNAPInstPrm.json"), False)
    assert actual is not None
    assert len(actual) == 1


### TESTS OF PATH METHODS ###


def test_constructCalibrationStateRoot():
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    ans = localDataService._constructCalibrationStateRoot(fakeState)
    assert isinstance(ans, Path)
    assert ans.parts[-1] == fakeState


def test_constructCalibrationStatePath():
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        ans = localDataService._constructCalibrationStatePath(fakeState, useLiteMode)
        assert isinstance(ans, Path)
        assert ans.parts[-1] == "diffraction"
        assert ans.parts[-2] == "lite" if useLiteMode else "native"
        assert ans.parts[:-2] == localDataService._constructCalibrationStateRoot(fakeState).parts


def test_constructNormalizationStatePath():
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        ans = localDataService._constructNormalizationStatePath(fakeState, useLiteMode)
        assert isinstance(ans, Path)
        assert ans.parts[-1] == "normalization"
        assert ans.parts[-2] == "lite" if useLiteMode else "native"
        assert ans.parts[:-2] == localDataService._constructCalibrationStateRoot(fakeState).parts


def test_constructReductionStateRoot():
    fakeIPTS = "gumdrop"
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value=fakeIPTS)
    localDataService._generateStateId = mock.Mock(return_value=fakeState)
    runNumber = "xyz"
    ans = localDataService._constructReductionStateRoot(runNumber)
    assert isinstance(ans, Path)
    assert ans.parts[-1] == fakeState
    assert fakeIPTS in ans.parts


def test_constructReductionDataRoot():
    fakeIPTS = "gumdrop"
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value=fakeIPTS)
    localDataService._generateStateId = mock.Mock(return_value=fakeState)
    runNumber = "xyz"
    for useLiteMode in [True, False]:
        ans = localDataService._constructReductionDataRoot(runNumber, useLiteMode)
        assert isinstance(ans, Path)
        assert ans.parts[-1] == runNumber
        assert ans.parts[-2] == "lite" if useLiteMode else "native"
        assert ans.parts[:-2] == localDataService._constructReductionStateRoot(runNumber).parts


def test__constructReductionDataFilePath():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)
    testRecord = ReductionRecord.parse_file(inputRecordFilePath)

    # Temporarily use a single run number
    useLiteMode = testRecord.useLiteMode
    runNumber = testRecord.runNumbers[0]
    version = int(testRecord.version)
    stateId = "ab8704b0bc2a2342"
    testIPTS = "IPTS-12345"
    fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
    fileName += Config["nexus.file.extension"]

    expectedFilePath = (
        Path(Config["instrument.reduction.home"].format(IPTS=testIPTS))
        / stateId
        / ("lite" if useLiteMode else "native")
        / runNumber
        / "v_{}".format(wnvf.formatVersion(version=version, use_v_prefix=False))
        / fileName
    )

    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = (stateId, None)
    localDataService.getIPTS = mock.Mock(return_value=testIPTS)
    actualFilePath = localDataService._constructReductionDataFilePath(runNumber, useLiteMode, version)
    assert actualFilePath == expectedFilePath


### TESTS OF VERSIONING / INDEX METHODS ###


def test_statePathForWorkflow():
    fakeStateId = "boogersoup"
    runNumber = "xyz"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        # CALIBRATION
        ans = localDataService._statePathForWorkflow(fakeStateId, useLiteMode, IndexorType.CALIBRATION)
        exp = localDataService._constructCalibrationStatePath(fakeStateId, useLiteMode)
        assert ans == exp
        # NORMALIZATION
        ans = localDataService._statePathForWorkflow(fakeStateId, useLiteMode, IndexorType.NORMALIZATION)
        exp = localDataService._constructNormalizationStatePath(fakeStateId, useLiteMode)
        assert ans == exp
        # REDUCTION
        ans = localDataService._statePathForWorkflow(fakeStateId, useLiteMode, IndexorType.REDUCTION)
        exp = localDataService._constructReductionDataRoot(fakeStateId, useLiteMode)
        assert ans == exp
        # DEFAULT
        ans = localDataService._statePathForWorkflow(fakeStateId, useLiteMode, IndexorType.DEFAULT)
        mode = "lite" if useLiteMode else "native"
        exp = localDataService._constructCalibrationStateRoot(fakeStateId) / mode
        assert ans == exp


def test_indexor():
    fakeStateId = "boogersoup"


def test_readCalibrationIndexMissing():
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        assert len(localDataService.readCalibrationIndex("123", True)) == 0


def test_readNormalizationIndexMissing():
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        assert len(localDataService.readNormalizationIndex("123", True)) == 0


def test_readWriteCalibrationIndexEntry():
    entry = CalibrationIndexEntry(
        runNumber="57514",
        useLiteMode=True,
        comments="test comment",
        author="test author",
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        localDataService.writeNormalizationIndexEntry(entry)
        actualEntries = localDataService.readNormalizationIndex(entry.runNumber, entry.useLiteMode)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


def test_readWriteNormalizationIndexEntry():
    entry = NormalizationIndexEntry(
        runNumber="57514",
        backgroundRunNumber="58813",
        useLiteMode=True,
        comments="test comment",
        author="test author",
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        localDataService.writeNormalizationIndexEntry(entry)
        actualEntries = localDataService.readNormalizationIndex(entry.runNumber, entry.useLiteMode)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


# def readReductionIngredientsFromFile():
#     with Resource.open("/inputs/calibration/ReductionIngredients.json", "r") as f:
#         return ReductionIngredients.parse_raw(f.read())


# def test_readWriteCalibrationRecord_version_numbers():
#     localDataService = LocalDataService()
#     stateId = "ab8704b0bc2a2342"
#     testCalibrationRecord_v0001 = CalibrationRecord.parse_raw(
#         Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
#     )
#     testCalibrationRecord_v0002 = CalibrationRecord.parse_file(
#         Resource.getPath("inputs/calibration/CalibrationRecord_v0002.json")
#     )
#     with state_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         # WARNING: 'writeCalibrationRecord' modifies <incoming record>.version,
#         #   and <incoming record>.calibrationFittingIngredients.version.

#         # write: version == 1
#         localDataService.writeCalibrationRecord(testCalibrationRecord_v0001)
#         actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True)
#         assert actualRecord.version == 1
#         assert actualRecord.calibrationFittingIngredients.version == 1

#         # write: version == 2
#         localDataService.writeCalibrationRecord(testCalibrationRecord_v0002)
#         actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True)
#         assert actualRecord.version == 2
#         assert actualRecord.calibrationFittingIngredients.version == 2
#         assert isinstance(actualRecord, CalibrationRecord)
#     assert actualRecord.runNumber == "57514"
#     assert actualRecord == testCalibrationRecord_v0002


# ##
# def test_readWriteCalibrationRecord_specified_version():
#     localDataService = LocalDataService()
#     stateId = "ab8704b0bc2a2342"
#     testCalibrationRecord_v0001 = CalibrationRecord.parse_raw(
#         Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
#     )
#     testCalibrationRecord_v0002 = CalibrationRecord.parse_file(
#         Resource.getPath("inputs/calibration/CalibrationRecord_v0002.json")
#     )
#     with state_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         # WARNING: 'writeCalibrationRecord' modifies <incoming record>.version,
#         #   and <incoming record>.calibrationFittingIngredients.version.


def test_readWriteCalibrationRecord_version_numbers():
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    testCalibrationRecord_v0001 = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testCalibrationRecord_v0002 = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0002.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        # WARNING: 'writeCalibrationRecord' modifies <incoming record>.version,
        #   and <incoming record>.calibrationFittingIngredients.version.


#         # Important: start with version > 1: should not depend on any existing directory structure!

#         # write: version == 3
#         localDataService.writeCalibrationRecord(testCalibrationRecord_v0001, version="3")
#         actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True, version="3")
#         assert actualRecord.version == 3
#         assert actualRecord.calibrationFittingIngredients.version == 3

#         # write: version == 4
#         localDataService.writeCalibrationRecord(testCalibrationRecord_v0002, version="4")
#         actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True, version="3")
#         assert actualRecord.version == 3
#         actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True, version="4")
#         assert actualRecord.version == 4


# def test_readWriteCalibrationRecord_with_version():
#     localDataService = LocalDataService()
#     stateId = "ab8704b0bc2a2342"
#     with state_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         localDataService.writeCalibrationRecord(
#             CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord_v0001.json"))
#         )
#         actualRecord = localDataService.readCalibrationRecord("57514", True, "1")
#     assert actualRecord.runNumber == "57514"
#     assert actualRecord.version == 1


# def test_readWriteCalibrationRecord():
#     localDataService = LocalDataService()
#     stateId = "ab8704b0bc2a2342"
#     testCalibrationRecord = CalibrationRecord.parse_raw(
#         Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
#     )
#     with state_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         localDataService.writeCalibrationRecord(testCalibrationRecord)
#         actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True)
#     assert actualRecord.runNumber == "57514"
#     assert actualRecord == testCalibrationRecord


# def test_writeCalibrationWorkspaces():
#     localDataService = LocalDataService()
#     stateId = "ab8704b0bc2a2342"
#     testCalibrationRecord = CalibrationRecord.parse_raw(
#         Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
#     )
#     with state_root_redirect(localDataService, stateId=stateId):
#         basePath = localDataService._constructCalibrationDataPath(testCalibrationRecord.runNumber, True, 1)

#         # Workspace names need to match the names that are used in the test record.
#         workspaces = testCalibrationRecord.workspaces.copy()
#         runNumber = testCalibrationRecord.runNumber
#         version = testCalibrationRecord.version
#         outputDSPWSName = workspaces.pop(wngt.DIFFCAL_OUTPUT)[0]
#         diagnosticWSname = workspaces.pop(wngt.DIFFCAL_DIAG)[0]
#         tableWSName = workspaces.pop(wngt.DIFFCAL_TABLE)[0]
#         maskWSName = workspaces.pop(wngt.DIFFCAL_MASK)[0]
#         if workspaces:
#             raise RuntimeError(f"unexpected workspace-types in record.workspaces: {workspaces}")

#         # Create sample workspaces.
#         CreateSampleWorkspace(
#             OutputWorkspace=outputDSPWSName,
#             Function="One Peak",
#             NumBanks=1,
#             NumMonitors=1,
#             BankPixelWidth=5,
#             NumEvents=500,
#             Random=True,
#             XUnit="DSP",
#             XMin=0,
#             XMax=8000,
#             BinWidth=100,
#         )
#         LoadInstrument(
#             Workspace=outputDSPWSName,
#             Filename=fakeInstrumentFilePath,
#             RewriteSpectraMap=True,
#         )
#         assert mtd.doesExist(outputDSPWSName)

#         # Create a grouping workspace to save as the diagnostic workspace.
#         ws1 = CreateSingleValuedWorkspace()
#         GroupWorkspaces(
#             InputWorkspaces=[ws1],
#             OutputWorkspace=diagnosticWSname,
#         )
#         assert mtd.doesExist(diagnosticWSname)

#         # Create diffraction-calibration table and mask workspaces.
#         createCompatibleDiffCalTable(tableWSName, outputDSPWSName)
#         createCompatibleMask(maskWSName, outputDSPWSName, fakeInstrumentFilePath)
#         assert mtd.doesExist(tableWSName)
#         assert mtd.doesExist(maskWSName)

#         localDataService.writeCalibrationWorkspaces(testCalibrationRecord)

#         dspFilename = Path(outputDSPWSName + Config["calibration.diffraction.output.extension"])
#         diagFilename = Path(diagnosticWSname + Config["calibration.diffraction.diagnostic.extension"])
#         diffCalFilename = Path(wng.diffCalTable().runNumber(runNumber).version(version).build() + ".h5")
#         assert (basePath / dspFilename).exists()
#         assert (basePath / diagFilename).exists()
#         assert (basePath / diffCalFilename).exists()
#         mtd.clear()


# def test_readWriteNormalizationRecord_version_numbers():
#     stateId = "ab8704b0bc2a2342"
#     testNormalizationRecord = NormalizationRecord.parse_raw(
#         Resource.read("inputs/normalization/NormalizationRecord.json")
#     )
#     useLiteMode = True
#     testNormalizationRecord.useLiteMode = useLiteMode
#     testNormalizationRecord.version = VERSION_START
#     localDataService = LocalDataService()
#     with state_root_redirect(localDataService, stateId=stateId):
#         # WARNING: 'writeNormalizationRecord' modifies <incoming record>.version,
#         # and <incoming record>.normalization.version.

#         # write: version == VERSION_START
#         testVersion = VERSION_START
#         localDataService.writeNormalizationRecord(record)
#         localDataService.writeNormalizationIndexEntry(entry)
#         actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode)
#         assert actualRecord.version == testVersion
#         assert actualRecord.calibration.version == testVersion
#         assert actualRecord.useLiteMode == useLiteMode
#         assert isinstance(actualRecord, NormalizationRecord)

#         # write: version == VERSION_START + 1
#         record.version == "*"
#         testVersion = VERSION_START + 1
#         testNormalizationRecord.version = testVersion
#         localDataService.writeNormalizationRecord(testNormalizationRecord)
#         actualRecord = localDataService.readNormalizationRecord("57514", useLiteMode)
#         assert actualRecord.version == testVersion
#         assert actualRecord.calibration.version == testVersion
#         assert actualRecord.useLiteMode == useLiteMode

#         # write: version == VERSION_START + 2
#         testVersion = VERSION_START + 2
#         localDataService.writeNormalizationRecord(testNormalizationRecord)
#         actualRecord = localDataService.readNormalizationRecord("57514", useLiteMode)
#         assert actualRecord.version == testVersion
#         assert actualRecord.calibration.version == testVersion
#         assert actualRecord.useLiteMode == useLiteMode
#         assert isinstance(actualRecord, NormalizationRecord)
#     assert actualRecord.runNumber == "57514"
#     assert actualRecord == record


# def test_readWriteNormalizationRecord_specified_version():
#     stateId = "ab8704b0bc2a2342"
#     runNumber = "57514"
#     useLiteMode = False
#     record = NormalizationRecord.parse_raw(Resource.read("inputs/normalization/NormalizationRecord.json"))
#     record.runNumber = runNumber
#     record.version = "*"
#     record.useLiteMode = useLiteMode
#     entry = NormalizationIndexEntry(
#         runNumber=record.runNumber, backgroundRunNumber="x", useLiteMode=useLiteMode, version="*"
#     )
#     testNormalizationRecord.version = VERSION_START
#     testNormalizationRecord.useLiteMode = useLiteMode
#     localDataService = LocalDataService()
#     with state_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         # WARNING: 'writeNormalizationRecord' modifies <incoming record>.version,
#         # and <incoming record>.normalization.version.

#         # write: version == VERSION_START
#         firstVersion = VERSION_START
#         localDataService.writeNormalizationRecord(record)
#         localDataService.writeNormalizationIndexEntry(entry)
#         actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode)
#         assert actualRecord.version == firstVersion
#         assert actualRecord.calibration.version == firstVersion
#         assert actualRecord.useLiteMode == useLiteMode
#         assert localDataService.getNormalizationRecordFilePath(runNumber, useLiteMode, firstVersion).exists()
#         # write: version == testVersion
#         testVersion = VERSION_START + 3
#         record.version = testVersion
#         entry.version = testVersion
#         localDataService.writeNormalizationRecord(record, testVersion)
#         localDataService.writeNormalizationIndexEntry(entry, testVersion)
#         actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode)
#         assert actualRecord.version == testVersion
#         assert actualRecord.calibration.version == testVersion
#         assert actualRecord.useLiteMode == useLiteMode
#         actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode, testVersion)
#         assert actualRecord.version == testVersion
#         assert actualRecord.calibration.version == testVersion
#         assert actualRecord.useLiteMode == useLiteMode
#         assert localDataService.getNormalizationRecordFilePath(runNumber, useLiteMode, firstVersion).exists()
#         assert localDataService.getNormalizationRecordFilePath(runNumber, useLiteMode, testVersion).exists()
#         # test can still read earlier version
#         actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode, firstVersion)
#         assert actualRecord.version == firstVersion
#         assert actualRecord.useLiteMode == useLiteMode


def test_readWriteCalibrationRecord():
    stateId = "ab8704b0bc2a2342"
    useLiteMode = False
    testCalibrationRecord = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testCalibrationRecord.useLiteMode = useLiteMode
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.writeCalibrationRecord(testCalibrationRecord)
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode)
    assert actualRecord.runNumber == "57514"
    assert actualRecord.useLiteMode == useLiteMode
    assert actualRecord == testCalibrationRecord


##
def test_readWriteCalibrationRecord_specified_version():
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    testCalibrationRecord_v0001 = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testCalibrationRecord_v0002 = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0002.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        # WARNING: 'writeCalibrationRecord' modifies <incoming record>.version,
        #   and <incoming record>.calibrationFittingIngredients.version.

        # Important: start with version > 1: should not depend on any existing directory structure!

        # write: version == 3
        localDataService.writeCalibrationRecord(testCalibrationRecord_v0001, version="3")
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True, version="3")
        assert actualRecord.version == 3
        assert actualRecord.calibrationFittingIngredients.version == 3

        # write: version == 4
        localDataService.writeCalibrationRecord(testCalibrationRecord_v0002, version="4")
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True, version="3")
        assert actualRecord.version == 3
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True, version="4")
        assert actualRecord.version == 4


def test_readWriteCalibrationRecord_with_version():
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.writeCalibrationRecord(
            CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord_v0001.json"))
        )
        actualRecord = localDataService.readCalibrationRecord("57514", True, "1")
    assert actualRecord.runNumber == "57514"
    assert actualRecord.version == 1


def test_writeCalibrationWorkspaces():
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    testCalibrationRecord = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        basePath = localDataService._constructCalibrationDataPath(testCalibrationRecord.runNumber, True, 1)

        # Workspace names need to match the names that are used in the test record.
        workspaces = testCalibrationRecord.workspaces.copy()
        runNumber = testCalibrationRecord.runNumber
        version = testCalibrationRecord.version
        outputTOFWSName, outputDSPWSName = workspaces.pop(wngt.DIFFCAL_OUTPUT)
        tableWSName = workspaces.pop(wngt.DIFFCAL_TABLE)[0]
        maskWSName = workspaces.pop(wngt.DIFFCAL_MASK)[0]
        if workspaces:
            raise RuntimeError(f"unexpected workspace-types in record.workspaces: {workspaces}")

        # Create sample workspaces.
        CreateSampleWorkspace(
            OutputWorkspace=outputTOFWSName,
            Function="One Peak",
            NumBanks=1,
            NumMonitors=1,
            BankPixelWidth=5,
            NumEvents=500,
            Random=True,
            XUnit="TOF",
            XMin=0,
            XMax=8000,
            BinWidth=100,
        )
        LoadInstrument(
            Workspace=outputTOFWSName,
            Filename=fakeInstrumentFilePath,
            RewriteSpectraMap=True,
        )
        CreateSampleWorkspace(
            OutputWorkspace=outputDSPWSName,
            Function="One Peak",
            NumBanks=1,
            NumMonitors=1,
            BankPixelWidth=5,
            NumEvents=500,
            Random=True,
            XUnit="DSP",
            XMin=0,
            XMax=8000,
            BinWidth=100,
        )
        LoadInstrument(
            Workspace=outputDSPWSName,
            Filename=fakeInstrumentFilePath,
            RewriteSpectraMap=True,
        )
        assert mtd.doesExist(outputTOFWSName)
        assert mtd.doesExist(outputDSPWSName)

        # Create diffraction-calibration table and mask workspaces.
        createCompatibleDiffCalTable(tableWSName, outputTOFWSName)
        createCompatibleMask(maskWSName, outputTOFWSName, fakeInstrumentFilePath)
        assert mtd.doesExist(tableWSName)
        assert mtd.doesExist(maskWSName)

        localDataService.writeCalibrationWorkspaces(testCalibrationRecord)

        diffCalFilename = Path(wng.diffCalTable().runNumber(runNumber).version(version).build() + ".h5")
        for wsNames in testCalibrationRecord.workspaces.values():
            for wsName in wsNames:
                ws = mtd[wsName]
                filename = (
                    Path(wsName + Config["calibration.diffraction.output.extension"])
                    if not (isinstance(ws, ITableWorkspace) or isinstance(ws, MaskWorkspace))
                    else diffCalFilename
                )
                assert (basePath / filename).exists()
        mtd.clear()


def test_writeCalibrationWorkspaces_no_units():
    # test that diffraction-calibration output workspace names require units
    localDataService = LocalDataService()
    localDataService.writeWorkspace = mock.Mock()
    localDataService._constructCalibrationDataPath = mock.Mock(return_value="not/a/path")
    testCalibrationRecord = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testCalibrationRecord.workspaces = {
        wngt.DIFFCAL_OUTPUT: ["_diffoc_057514_v0001", "_dsp_diffoc_057514_v0001"],
        wngt.DIFFCAL_TABLE: ["_diffract_consts_057514_v0001"],
        wngt.DIFFCAL_MASK: ["_diffract_consts_mask_057514_v0001"],
    }
    with pytest.raises(  # noqa: PT012
        RuntimeError,
        match=f"cannot save a workspace-type: {wngt.DIFFCAL_OUTPUT} without a units token in its name",
    ):
        localDataService.writeCalibrationWorkspaces(testCalibrationRecord)


def test_readWriteNormalizationRecord_version_numbers():
    stateId = "ab8704b0bc2a2342"
    testNormalizationRecord = NormalizationRecord.parse_raw(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    useLiteMode = True
    testNormalizationRecord.useLiteMode = useLiteMode
    testNormalizationRecord.version = VERSION_START
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=stateId):
        # WARNING: 'writeNormalizationRecord' modifies <incoming record>.version,
        # and <incoming record>.normalization.version.

        # write: version == VERSION_START + 1
        testVersion = VERSION_START + 1
        testNormalizationRecord.version = testVersion
        localDataService.writeNormalizationRecord(testNormalizationRecord)
        actualRecord = localDataService.readNormalizationRecord("57514", useLiteMode)
        assert actualRecord.version == testVersion
        assert actualRecord.calibration.version == testVersion
        assert actualRecord.useLiteMode == useLiteMode

        # write: version == VERSION_START + 2
        testVersion = VERSION_START + 2
        localDataService.writeNormalizationRecord(testNormalizationRecord)
        actualRecord = localDataService.readNormalizationRecord("57514", useLiteMode)
        assert actualRecord.version == testVersion
        assert actualRecord.calibration.version == testVersion
        assert actualRecord.useLiteMode == useLiteMode
    assert actualRecord.runNumber == "57514"
    assert actualRecord == testNormalizationRecord


def test_readWriteNormalizationRecord_specified_version():
    stateId = "ab8704b0bc2a2342"
    runNumber = "57514"
    useLiteMode = False

    testNormalizationRecord = NormalizationRecord.parse_raw(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    testNormalizationRecord.version = VERSION_START
    testNormalizationRecord.useLiteMode = useLiteMode
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        # WARNING: 'writeNormalizationRecord' modifies <incoming record>.version,
        # and <incoming record>.normalization.version.

        # Important to include version steps > 1: should not depend on any existing directory structure!

        # write: version == VERSION_START + 1
        firstVersion = VERSION_START + 1
        localDataService.writeNormalizationRecord(testNormalizationRecord)
        actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode)
        assert actualRecord.version == firstVersion
        assert actualRecord.calibration.version == firstVersion
        assert actualRecord.useLiteMode == useLiteMode
        assert localDataService.getNormalizationRecordFilePath(runNumber, useLiteMode, firstVersion).exists()
        # write: version == testVersion
        testVersion = VERSION_START + 3
        testNormalizationRecord.version = testVersion
        localDataService.writeNormalizationRecord(testNormalizationRecord)
        actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode)
        assert actualRecord.version == testVersion
        assert actualRecord.useLiteMode == useLiteMode
        actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode, testVersion)
        assert actualRecord.version == testVersion
        assert actualRecord.useLiteMode == useLiteMode
        assert localDataService.getNormalizationRecordFilePath(runNumber, useLiteMode, firstVersion).exists()
        assert localDataService.getNormalizationRecordFilePath(runNumber, useLiteMode, testVersion).exists()
        # test can still read earlier version
        actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode, firstVersion)
        assert actualRecord.version == firstVersion
        assert actualRecord.useLiteMode == useLiteMode


def test_readWriteNormalizationRecord():
    stateId = "ab8704b0bc2a2342"
    useLiteMode = True
    testNormalizationRecord = NormalizationRecord.parse_raw(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    testNormalizationRecord.useLiteMode = useLiteMode
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.writeNormalizationRecord(testNormalizationRecord)
        actualRecord = localDataService.readNormalizationRecord("57514", useLiteMode)
    assert actualRecord.runNumber == "57514"
    assert actualRecord.useLiteMode == useLiteMode
    assert actualRecord == testNormalizationRecord


def test_writeNormalizationWorkspaces():
    stateId = "ab8704b0bc2a2342"
    localDataService = LocalDataService()
    testNormalizationRecord = NormalizationRecord.parse_raw(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        # Workspace names need to match the names that are used in the test record.
        runNumber = testNormalizationRecord.runNumber  # noqa: F841
        useLiteMode = testNormalizationRecord.useLiteMode
        version = testNormalizationRecord.version  # noqa: F841
        testWS0, testWS1, testWS2 = testNormalizationRecord.workspaceNames

        basePath = localDataService.normalizationIndex(runNumber, useLiteMode).versionPath(version)
        basePath = localDataService._constructNormalizationDataPath(runNumber, useLiteMode, version)

        # Create sample workspaces.
        LoadEmptyInstrument(
            Filename=fakeInstrumentFilePath,
            OutputWorkspace=testWS0,
        )
        CloneWorkspace(InputWorkspace=testWS0, OutputWorkspace=testWS1)
        CloneWorkspace(InputWorkspace=testWS0, OutputWorkspace=testWS2)
        assert mtd.doesExist(testWS0)
        assert mtd.doesExist(testWS1)
        assert mtd.doesExist(testWS2)

        localDataService.writeNormalizationWorkspaces(testNormalizationRecord)

        for wsName in testNormalizationRecord.workspaceNames:
            filename = Path(wsName + "_" + wnvf.formatVersion(version) + ".nxs")
            assert (basePath / filename).exists()
    mtd.clear()


# def _writeSyntheticReductionRecord(filePath: Path, version: str):
#     # Create a `ReductionRecord` JSON file to be used by the unit tests.

#     # TODO: Implement methods to create the synthetic `CalibrationRecord` and `NormalizationRecord`.
#     testCalibration = CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord_v0001.json"))
#     testNormalization = NormalizationRecord.parse_raw(Resource.read("inputs/normalization/NormalizationRecord.json"))
#     testRecord = ReductionRecord(
#         runNumbers=[testCalibration.runNumber],
#         useLiteMode=testCalibration.useLiteMode,
#         calibration=testCalibration,
#         normalization=testNormalization,
#         pixelGroupingParameters={
#             pg.focusGroup.name: list(pg.pixelGroupingParameters.values()) for pg in testCalibration.pixelGroups
#         },
#         version=int(version),
#         stateId=testCalibration.calibrationFittingIngredients.instrumentState.id,
#         workspaceNames=[
#             wng.reductionOutput()
#             .runNumber(testCalibration.runNumber)
#             .group(pg.focusGroup.name)
#             .version(testCalibration.version)
#             .build()
#             for pg in testCalibration.pixelGroups
#         ],
#     )
#     write_model_pretty(testRecord, filePath)


# def test_readWriteReductionRecord_version_numbers():
#     inputRecordFilePath = Resource.getPath("inputs/reduction/ReductionRecord_v0001.json")
#     # Create the input data for this test:
#     # _writeSyntheticReductionRecord(inputRecordFilePath, "1")

#     testReductionRecord_v0001 = ReductionRecord.parse_file(inputRecordFilePath)
#     # Get a second copy (version still set to `1`)
#     testReductionRecord_v0002 = ReductionRecord.parse_file(inputRecordFilePath)

#     # Temporarily use a single run number
#     useLiteMode = testReductionRecord_v0001.useLiteMode
#     runNumber = testReductionRecord_v0001.runNumbers[0]
#     stateId = "ab8704b0bc2a2342"
#     localDataService = LocalDataService()
#     with reduction_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         # WARNING: 'writeReductionRecord' modifies <incoming record>.version,

#         # write: version == 1
#         localDataService.writeReductionRecord(testReductionRecord_v0001)
#         actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode)
#         assert actualRecord.version == 1

#         # write: version == 2
#         localDataService.writeReductionRecord(testReductionRecord_v0002)
#         actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode)
#         assert actualRecord.version == 2
#     assert actualRecord == testReductionRecord_v0002


# def test_readWriteReductionRecord_specified_version():
#     inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
#     # Create the input data for this test:
#     # _writeSyntheticReductionRecord(inputRecordFilePath, "1")

#     testReductionRecord_v0001 = ReductionRecord.parse_file(inputRecordFilePath)
#     # Get a second copy (version still set to `1`)
#     testReductionRecord_v0002 = ReductionRecord.parse_file(inputRecordFilePath)

#     # Temporarily use a single run number
#     useLiteMode = testReductionRecord_v0001.useLiteMode
#     runNumber = testReductionRecord_v0001.runNumbers[0]
#     stateId = "ab8704b0bc2a2342"
#     localDataService = LocalDataService()
#     with reduction_root_redirect(localDataService, stateId=stateId):
#         # WARNING: 'writeReductionRecord' modifies <incoming record>.version,

#         #  Important: start with version > 1: should not depend on any existing directory structure!

#         # write: version == 3
#         localDataService.writeReductionRecord(testReductionRecord_v0001, version="3")
#         actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, version="3")
#         assert actualRecord.version == 3

#         # write: version == 4
#         actualRecord = localDataService.writeReductionRecord(testReductionRecord_v0002, version="4")
#         # -- version should have been modified to `4`
#         assert actualRecord.version == 4

#         actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, version="3")
#         assert actualRecord.version == 3
#         actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, version="4")
#         assert actualRecord.version == 4


# def test_readWriteReductionRecord_with_version():
#     inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
#     # Create the input data for this test:
#     # _writeSyntheticReductionRecord("1", inputRecordFilePath)

#     testRecord = ReductionRecord.parse_file(inputRecordFilePath)
#     # Important: version != 1: should not depend on any existing directory structure.
#     testVersion = "10"

#     # Temporarily use a single run number
#     runNumber = testRecord.runNumbers[0]
#     stateId = "ab8704b0bc2a2342"
#     localDataService = LocalDataService()
#     with reduction_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()

#         actualRecord = localDataService.writeReductionRecord(testRecord, testVersion)
#         # -- version should have been modified to int(testVersion)
#         assert actualRecord.version == int(testVersion)

#         actualRecord = localDataService.readReductionRecord(runNumber, testRecord.useLiteMode, testVersion)
#     assert actualRecord.version == int(testVersion)


# def test_readWriteReductionRecord():
#     inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
#     # Create the input data for this test:
#     # _writeSyntheticReductionRecord("1", inputRecordFilePath)
#     testRecord = ReductionRecord.parse_file(inputRecordFilePath)

#     # Temporarily use a single run number
#     runNumber = testRecord.runNumbers[0]
#     stateId = "ab8704b0bc2a2342"
#     localDataService = LocalDataService()
#     with reduction_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)
#         localDataService.groceryService = mock.Mock()
#         localDataService.writeReductionRecord(testRecord)
#         actualRecord = localDataService.readReductionRecord(runNumber, testRecord.useLiteMode, testRecord.version)
#     assert actualRecord == testRecord


# @pytest.fixture()
# def createReductionWorkspaces():
#     # Create sample workspaces from a list of names:
#     #   * delete the workspaces in the list at teardown;
#     #   * any additional workspaces that need to be cleaned up
#     #   can be added to the _returned_ list.
#     _wss = []

#     def _createWorkspaces(wss: List[str]):
#         # Create sample reduction event workspaces with DSP units
#         src = mtd.unique_hidden_name()
#         CreateSampleWorkspace(
#             OutputWorkspace=src,
#             Function="One Peak",
#             NumBanks=1,
#             NumMonitors=1,
#             BankPixelWidth=5,
#             NumEvents=500,
#             Random=True,
#             XUnit="DSP",
#             XMin=0,
#             XMax=8000,
#             BinWidth=100,
#         )
#         LoadInstrument(
#             Workspace=src,
#             Filename=fakeInstrumentFilePath,
#             RewriteSpectraMap=True,
#         )
#         assert mtd.doesExist(src)
#         for ws in wss:
#             CloneWorkspace(InputWorkspace=src, OutputWorkspace=ws)
#             assert mtd.doesExist(ws)
#         DeleteWorkspace(Workspace=src)
#         _wss.extend(wss)
#         return _wss

#     yield _createWorkspaces

#     # teardown
#     for ws in _wss:
#         if mtd.doesExist(ws):
#             try:
#                 DeleteWorkspace(ws)
#             except:  # noqa: E722
#                 pass


# def test_writeReductionData(createReductionWorkspaces):
#     _uniquePrefix = "LDS_WRD_"
#     inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
#     # Create the input data for this test:
#     # _writeSyntheticReductionRecord("1", inputRecordFilePath)

#     testRecord = ReductionRecord.parse_file(inputRecordFilePath)
#     # Change the workspace names so that they will be unique to this test:
#     # => enables parallel testing.
#     testRecord.workspaceNames = [_uniquePrefix + ws for ws in testRecord.workspaceNames]

#     # Temporarily use a single run number
#     useLiteMode = testRecord.useLiteMode
#     runNumber = testRecord.runNumbers[0]
#     version = int(testRecord.version)
#     stateId = "ab8704b0bc2a2342"
#     fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
#     fileName += Config["nexus.file.extension"]

#     wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
#     localDataService = LocalDataService()
#     with reduction_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)

#         # Important to this test: use a path that doesn't already exist
#         reductionFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, version)
#         assert not reductionFilePath.exists()

#         # `writeReductionRecord` must be called first
#         localDataService.writeReductionRecord(testRecord)
#         localDataService.writeReductionData(testRecord)

#         assert reductionFilePath.exists()


# def test_writeReductionData_no_directories(createReductionWorkspaces):
#     _uniquePrefix = "LDS_WRD_"
#     inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
#     # Create the input data for this test:
#     # _writeSyntheticReductionRecord("1", inputRecordFilePath)

#     testRecord = ReductionRecord.parse_file(inputRecordFilePath)
#     # Change the workspace names so that they will be unique to this test:
#     # => enables parallel testing.
#     testRecord.workspaceNames = [_uniquePrefix + ws for ws in testRecord.workspaceNames]

#     # Temporarily use a single run number
#     useLiteMode = testRecord.useLiteMode
#     runNumber = testRecord.runNumbers[0]
#     version = int(testRecord.version)
#     stateId = "ab8704b0bc2a2342"
#     fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
#     fileName += Config["nexus.file.extension"]

#     wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
#     localDataService = LocalDataService()
#     with reduction_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)

#         # Important to this test: use a path that doesn't already exist
#         reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, version)
#         assert not reductionRecordFilePath.exists()

#         # `writeReductionRecord` must be called first
#         # * deliberately _not_ done in this test => <reduction-data root> directory won't exist
#         with pytest.raises(RuntimeError) as einfo:
#             localDataService.writeReductionData(testRecord)
#         msg = str(einfo.value)
#     assert "reduction version directories" in msg
#     assert "do not exist" in msg


# def test_writeReductionData_metadata(createReductionWorkspaces):
#     _uniquePrefix = "LDS_WRD_"
#     inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
#     # Create the input data for this test:
#     # _writeSyntheticReductionRecord("1", inputRecordFilePath)

#     testRecord = ReductionRecord.parse_file(inputRecordFilePath)
#     # Change the workspace names so that they will be unique to this test:
#     # => enables parallel testing.
#     testRecord.workspaceNames = [_uniquePrefix + ws for ws in testRecord.workspaceNames]

#     # Temporarily use a single run number
#     useLiteMode = testRecord.useLiteMode
#     runNumber = testRecord.runNumbers[0]
#     version = int(testRecord.version)
#     stateId = "ab8704b0bc2a2342"
#     fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
#     fileName += Config["nexus.file.extension"]

#     wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
#     localDataService = LocalDataService()
#     with reduction_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)

#         # Important to this test: use a path that doesn't already exist
#         reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, version)
#         assert not reductionRecordFilePath.exists()

#         # `writeReductionRecord` must be called first
#         localDataService.writeReductionRecord(testRecord)
#         localDataService.writeReductionData(testRecord)

#         filePath = reductionRecordFilePath.parent / fileName
#         assert filePath.exists()
#         with h5py.File(filePath, "r") as h5:
#             dict_ = n5m.extractMetadataGroup(h5, "/metadata")
#             actualRecord = ReductionRecord.parse_obj(dict_)
#             assert actualRecord == testRecord


# def test_readWriteReductionData(createReductionWorkspaces):
#     _uniquePrefix = "LDS_RWRD_"
#     inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
#     # Create the input data for this test:
#     # _writeSyntheticReductionRecord("1", inputRecordFilePath)

#     testRecord = ReductionRecord.parse_file(inputRecordFilePath)
#     # Change the workspace names so that they will be unique to this test:
#     # => enables parallel testing.
#     testRecord.workspaceNames = [_uniquePrefix + ws for ws in testRecord.workspaceNames]

#     # Temporarily use a single run number
#     useLiteMode = testRecord.useLiteMode
#     runNumber = testRecord.runNumbers[0]
#     version = int(testRecord.version)
#     stateId = "ab8704b0bc2a2342"
#     fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
#     fileName += Config["nexus.file.extension"]

#     wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
#     localDataService = LocalDataService()
#     with reduction_root_redirect(localDataService, stateId=stateId):
#         localDataService.instrumentConfig = mock.Mock()
#         localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)

#         # Important to this test: use a path that doesn't already exist
#         reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, version)
#         assert not reductionRecordFilePath.exists()

#         # `writeReductionRecord` needs to be called first
#         localDataService.writeReductionRecord(testRecord)
#         localDataService.writeReductionData(testRecord)

#         filePath = reductionRecordFilePath.parent / fileName
#         assert filePath.exists()

#         # move the existing test workspaces out of the way:
#         #   * this just adds the `_uniquePrefix` one more time.
#         RenameWorkspaces(InputWorkspaces=wss, Prefix=_uniquePrefix)
#         # append to the cleanup list
#         wss.extend([_uniquePrefix + ws for ws in wss.copy()])

#         actualRecord = localDataService.readReductionData(runNumber, useLiteMode, version)
#         assert actualRecord == testRecord
#         # workspaces should have been reloaded with their original names
#         # Implementation note:
#         #   * the workspaces must match _exactly_ here, so `CompareWorkspaces` must be used;
#         #   please do _not_ replace this with one of the `assert_almost_equal` methods:
#         #   -- they do not necessarily do what you think they should do...
#         for ws in actualRecord.workspaceNames:
#             equal, _ = CompareWorkspaces(
#                 Workspace1=ws,
#                 Workspace2=_uniquePrefix + ws,
#             )
#             assert equal


def _writeSyntheticReductionRecord(filePath: Path, version: str):
    # Create a `ReductionRecord` JSON file to be used by the unit tests.

    # TODO: Implement methods to create the synthetic `CalibrationRecord` and `NormalizationRecord`.
    testCalibration = CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord_v0001.json"))
    testNormalization = NormalizationRecord.parse_raw(Resource.read("inputs/normalization/NormalizationRecord.json"))
    testRecord = ReductionRecord(
        runNumbers=[testCalibration.runNumber],
        useLiteMode=testCalibration.useLiteMode,
        calibration=testCalibration,
        normalization=testNormalization,
        pixelGroupingParameters={
            pg.focusGroup.name: list(pg.pixelGroupingParameters.values()) for pg in testCalibration.pixelGroups
        },
        version=int(version),
        stateId=testCalibration.calibrationFittingIngredients.instrumentState.id,
        workspaceNames=[
            wng.reductionOutput()
            .runNumber(testCalibration.runNumber)
            .group(pg.focusGroup.name)
            .version(testCalibration.version)
            .build()
            for pg in testCalibration.pixelGroups
        ],
    )
    write_model_pretty(testRecord, filePath)


def test_readWriteReductionRecord_version_numbers():
    inputRecordFilePath = Resource.getPath("inputs/reduction/ReductionRecord_v0001.json")
    # Create the input data for this test:
    # _writeSyntheticReductionRecord(inputRecordFilePath, "1")

    testReductionRecord_v0001 = ReductionRecord.parse_file(inputRecordFilePath)
    # Get a second copy (version still set to `1`)
    testReductionRecord_v0002 = ReductionRecord.parse_file(inputRecordFilePath)

    # Temporarily use a single run number
    useLiteMode = testReductionRecord_v0001.useLiteMode
    runNumber = testReductionRecord_v0001.runNumbers[0]
    stateId = "ab8704b0bc2a2342"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        # WARNING: 'writeReductionRecord' modifies <incoming record>.version,

        # write: version == 1
        localDataService.writeReductionRecord(testReductionRecord_v0001)
        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode)
        assert actualRecord.version == 1

        # write: version == 2
        localDataService.writeReductionRecord(testReductionRecord_v0002)
        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode)
        assert actualRecord.version == 2
    assert actualRecord == testReductionRecord_v0002


def test_readWriteReductionRecord_specified_version():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord(inputRecordFilePath, "1")

    testReductionRecord_v0001 = ReductionRecord.parse_file(inputRecordFilePath)
    # Get a second copy (version still set to `1`)
    testReductionRecord_v0002 = ReductionRecord.parse_file(inputRecordFilePath)

    # Temporarily use a single run number
    useLiteMode = testReductionRecord_v0001.useLiteMode
    runNumber = testReductionRecord_v0001.runNumbers[0]
    stateId = "ab8704b0bc2a2342"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        # WARNING: 'writeReductionRecord' modifies <incoming record>.version,

        #  Important: start with version > 1: should not depend on any existing directory structure!

        # write: version == 3
        localDataService.writeReductionRecord(testReductionRecord_v0001, version="3")
        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, version="3")
        assert actualRecord.version == 3

        # write: version == 4
        actualRecord = localDataService.writeReductionRecord(testReductionRecord_v0002, version="4")
        # -- version should have been modified to `4`
        assert actualRecord.version == 4

        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, version="3")
        assert actualRecord.version == 3
        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, version="4")
        assert actualRecord.version == 4


def test_readWriteReductionRecord_with_version():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)

    testRecord = ReductionRecord.parse_file(inputRecordFilePath)
    # Important: version != 1: should not depend on any existing directory structure.
    testVersion = "10"

    # Temporarily use a single run number
    runNumber = testRecord.runNumbers[0]
    stateId = "ab8704b0bc2a2342"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()

        actualRecord = localDataService.writeReductionRecord(testRecord, testVersion)
        # -- version should have been modified to int(testVersion)
        assert actualRecord.version == int(testVersion)

        actualRecord = localDataService.readReductionRecord(runNumber, testRecord.useLiteMode, testVersion)
    assert actualRecord.version == int(testVersion)


def test_readWriteReductionRecord():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)
    testRecord = ReductionRecord.parse_file(inputRecordFilePath)

    # Temporarily use a single run number
    runNumber = testRecord.runNumbers[0]
    stateId = "ab8704b0bc2a2342"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)
        localDataService.groceryService = mock.Mock()
        localDataService.writeReductionRecord(testRecord)
        actualRecord = localDataService.readReductionRecord(runNumber, testRecord.useLiteMode, testRecord.version)
    assert actualRecord == testRecord


@pytest.fixture()
def createReductionWorkspaces():
    # Create sample workspaces from a list of names:
    #   * delete the workspaces in the list at teardown;
    #   * any additional workspaces that need to be cleaned up
    #   can be added to the _returned_ list.
    _wss = []

    def _createWorkspaces(wss: List[str]):
        # Create sample reduction event workspaces with DSP units
        src = mtd.unique_hidden_name()
        CreateSampleWorkspace(
            OutputWorkspace=src,
            Function="One Peak",
            NumBanks=1,
            NumMonitors=1,
            BankPixelWidth=5,
            NumEvents=500,
            Random=True,
            XUnit="DSP",
            XMin=0,
            XMax=8000,
            BinWidth=100,
        )
        LoadInstrument(
            Workspace=src,
            Filename=fakeInstrumentFilePath,
            RewriteSpectraMap=True,
        )
        assert mtd.doesExist(src)
        for ws in wss:
            CloneWorkspace(InputWorkspace=src, OutputWorkspace=ws)
            assert mtd.doesExist(ws)
        DeleteWorkspace(Workspace=src)
        _wss.extend(wss)
        return _wss

    yield _createWorkspaces

    # teardown
    for ws in _wss:
        if mtd.doesExist(ws):
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass


def test_writeReductionData(createReductionWorkspaces):
    _uniquePrefix = "LDS_WRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)

    testRecord = ReductionRecord.parse_file(inputRecordFilePath)
    # Change the workspace names so that they will be unique to this test:
    # => enables parallel testing.
    testRecord.workspaceNames = [_uniquePrefix + ws for ws in testRecord.workspaceNames]

    # Temporarily use a single run number
    useLiteMode = testRecord.useLiteMode
    runNumber = testRecord.runNumbers[0]
    version = int(testRecord.version)
    stateId = "ab8704b0bc2a2342"
    fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
    fileName += Config["nexus.file.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)

        # Important to this test: use a path that doesn't already exist
        reductionFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, version)
        assert not reductionFilePath.exists()

        # `writeReductionRecord` must be called first
        localDataService.writeReductionRecord(testRecord)
        localDataService.writeReductionData(testRecord)

        assert reductionFilePath.exists()


def test_writeReductionData_no_directories(createReductionWorkspaces):
    _uniquePrefix = "LDS_WRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)

    testRecord = ReductionRecord.parse_file(inputRecordFilePath)
    # Change the workspace names so that they will be unique to this test:
    # => enables parallel testing.
    testRecord.workspaceNames = [_uniquePrefix + ws for ws in testRecord.workspaceNames]

    # Temporarily use a single run number
    useLiteMode = testRecord.useLiteMode
    runNumber = testRecord.runNumbers[0]
    version = int(testRecord.version)
    stateId = "ab8704b0bc2a2342"
    fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
    fileName += Config["nexus.file.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, version)
        assert not reductionRecordFilePath.exists()

        # `writeReductionRecord` must be called first
        # * deliberately _not_ done in this test => <reduction-data root> directory won't exist
        with pytest.raises(RuntimeError) as einfo:
            localDataService.writeReductionData(testRecord)
        msg = str(einfo.value)
    assert "reduction version directories" in msg
    assert "do not exist" in msg


def test_writeReductionData_metadata(createReductionWorkspaces):
    _uniquePrefix = "LDS_WRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)

    testRecord = ReductionRecord.parse_file(inputRecordFilePath)
    # Change the workspace names so that they will be unique to this test:
    # => enables parallel testing.
    testRecord.workspaceNames = [_uniquePrefix + ws for ws in testRecord.workspaceNames]

    # Temporarily use a single run number
    useLiteMode = testRecord.useLiteMode
    runNumber = testRecord.runNumbers[0]
    version = int(testRecord.version)
    stateId = "ab8704b0bc2a2342"
    fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
    fileName += Config["nexus.file.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, version)
        assert not reductionRecordFilePath.exists()

        # `writeReductionRecord` must be called first
        localDataService.writeReductionRecord(testRecord)
        localDataService.writeReductionData(testRecord)

        filePath = reductionRecordFilePath.parent / fileName
        assert filePath.exists()
        with h5py.File(filePath, "r") as h5:
            dict_ = n5m.extractMetadataGroup(h5, "/metadata")
            actualRecord = ReductionRecord.parse_obj(dict_)
            assert actualRecord == testRecord


def test_readWriteReductionData(createReductionWorkspaces):
    _uniquePrefix = "LDS_RWRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)

    testRecord = ReductionRecord.parse_file(inputRecordFilePath)
    # Change the workspace names so that they will be unique to this test:
    # => enables parallel testing.
    testRecord.workspaceNames = [_uniquePrefix + ws for ws in testRecord.workspaceNames]

    # Temporarily use a single run number
    useLiteMode = testRecord.useLiteMode
    runNumber = testRecord.runNumbers[0]
    version = int(testRecord.version)
    stateId = "ab8704b0bc2a2342"
    fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
    fileName += Config["nexus.file.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, version)
        assert not reductionRecordFilePath.exists()

        # `writeReductionRecord` needs to be called first
        localDataService.writeReductionRecord(testRecord)
        localDataService.writeReductionData(testRecord)

        filePath = reductionRecordFilePath.parent / fileName
        assert filePath.exists()

        # move the existing test workspaces out of the way:
        #   * this just adds the `_uniquePrefix` one more time.
        RenameWorkspaces(InputWorkspaces=wss, Prefix=_uniquePrefix)
        # append to the cleanup list
        wss.extend([_uniquePrefix + ws for ws in wss.copy()])

        actualRecord = localDataService.readReductionData(runNumber, useLiteMode, version)
        assert actualRecord == testRecord
        # workspaces should have been reloaded with their original names
        # Implementation note:
        #   * the workspaces must match _exactly_ here, so `CompareWorkspaces` must be used;
        #   please do _not_ replace this with one of the `assert_almost_equal` methods:
        #   -- they do not necessarily do what you think they should do...
        for ws in actualRecord.workspaceNames:
            equal, _ = CompareWorkspaces(
                Workspace1=ws,
                Workspace2=_uniquePrefix + ws,
            )
            assert equal


def test_getCalibrationRecordFilePath():
    testVersion = randint(1, 20)
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._constructCalibrationStatePath = mock.Mock()
    localDataService._constructCalibrationStatePath.return_value = Path(Resource.getPath("outputs"))
    actualPath = localDataService.getCalibrationRecordFilePath("57514", True, testVersion)
    assert actualPath == Path(Resource.getPath("outputs")) / wnvf.fileVersion(testVersion) / "CalibrationRecord.json"


def test_getNormalizationRecordFilePath():
    testVersion = randint(1, 20)
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._constructNormalizationStatePath = mock.Mock()
    localDataService._constructNormalizationStatePath.return_value = Path(Resource.getPath("outputs"))
    actualPath = localDataService.getNormalizationRecordFilePath("57514", True, testVersion)
    assert actualPath == Path(Resource.getPath("outputs")) / wnvf.fileVersion(testVersion) / "NormalizationRecord.json"


def test_getReductionRecordFilePath():
    testVersion = randint(1, 20)
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._constructReductionDataRoot = mock.Mock()
    localDataService._constructReductionDataRoot.return_value = Path(Resource.getPath("outputs"))
    actualPath = localDataService._constructReductionRecordFilePath("57514", True, testVersion)
    assert actualPath == Path(Resource.getPath("outputs")) / wnvf.fileVersion(testVersion) / "ReductionRecord.json"


def test_extractFileVersion():
    testVersion = randint(1, 20)
    testFile = f"Powder/1234/{wnvf.fileVersion(testVersion)}/CalibrationRecord.json"
    localDataService = LocalDataService()
    actualVersion = localDataService._extractFileVersion(testFile)
    assert actualVersion == testVersion


def test_getLatestThing():
    localDataService = LocalDataService()

    # with nothing, results defaults
    ans = localDataService._getLatestThing([])
    assert ans == VERSION_START
    ans1, ans2 = localDataService._getLatestThing([], otherThings=[])
    assert ans1 == VERSION_START
    assert ans2 is None

    # with integers
    result = randint(10, 20)
    things = list(range(result + 1))
    shuffle(things)
    ans = localDataService._getLatestThing(things)
    assert ans == result

    # with floats
    result = 3.4
    things = [1.2, result, 1.7]
    ans = localDataService._getLatestThing(things)
    assert ans == result

    # with other things
    result = (randint(4, 10), "bundt")
    things = [1, 2, 3]
    cakes = ["devil", "angel", "lemon"]
    place = 1
    things.insert(place, result[0])
    cakes.insert(place, result[1])
    ans = localDataService._getLatestThing(things, otherThings=cakes)
    assert ans == result


def test__getFileOfVersion():
    expected = randint(10, 20)
    file_pattern = lambda x: f"/{x}/CalibrationRecord.json"  # noqa E731
    someFiles = [file_pattern(wnvf.fileVersion(i)) for i in range(expected + 1)]
    shuffle(someFiles)
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock(return_value=someFiles)
    actualFile = localDataService._getFileOfVersion(file_pattern("*"), expected)
    assert actualFile == file_pattern(wnvf.fileVersion(expected))


def test__getLatestFile():
    expected = randint(10, 20)
    file_pattern = lambda x: f"Powder/1234/{x}/CalibrationRecord.json"  # noqa E731
    someFiles = [file_pattern(wnvf.fileVersion(i)) for i in range(expected + 1)]
    shuffle(someFiles)
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock(return_value=someFiles)
    actualFile = localDataService._getLatestFile(file_pattern("*"))
    assert actualFile == file_pattern(wnvf.fileVersion(expected))


def test__isApplicableEntry_equals():
    localDataService = LocalDataService()
    entry = mock.Mock()
    entry.appliesTo = "123"
    assert localDataService._isApplicableEntry(entry, "123")


def test__isApplicableEntry_greaterThan():
    localDataService = LocalDataService()
    entry = mock.Mock()
    entry.appliesTo = ">123"
    assert localDataService._isApplicableEntry(entry, "456")


def test__isApplicableEntry_lessThan():
    localDataService = LocalDataService()
    entry = mock.Mock()
    entry.appliesTo = "<123"
    assert localDataService._isApplicableEntry(entry, "99")


def test_isApplicableEntry_lessThanEquals():
    localDataService = LocalDataService()
    entry = mock.Mock()
    entry.appliesTo = "<=123"
    assert localDataService._isApplicableEntry(entry, "123")
    assert localDataService._isApplicableEntry(entry, "99")
    assert not localDataService._isApplicableEntry(entry, "456")


def test_isApplicableEntry_greaterThanEquals():
    localDataService = LocalDataService()
    entry = mock.Mock()
    entry.appliesTo = ">=123"
    assert localDataService._isApplicableEntry(entry, "123")
    assert localDataService._isApplicableEntry(entry, "456")
    assert not localDataService._isApplicableEntry(entry, "99")


def test__getVersionFromCalibrationIndex():
    localDataService = LocalDataService()
    localDataService.readCalibrationIndex = mock.Mock()
    localDataService.readCalibrationIndex.return_value = [mock.Mock()]
    localDataService.readCalibrationIndex.return_value[0] = CalibrationIndexEntry(
        timestamp=123,
        useLiteMode=True,
        version=1,
        appliesTo="123",
        runNumber="123",
        comments="",
        author="",
    )
    actualVersion = localDataService._getVersionFromCalibrationIndex("123", True)
    assert actualVersion == 1


def test__getVersionFromCalibrationIndex_nuffink():
    localDataService = LocalDataService()
    localDataService.readCalibrationIndex = mock.Mock()
    localDataService.readCalibrationIndex.return_value = [mock.Mock()]
    localDataService.readCalibrationIndex.return_value[0] = CalibrationIndexEntry(
        timestamp=123,
        useLiteMode=True,
        version=1,
        appliesTo=">123",
        runNumber="123",
        comments="",
        author="",
    )
    actualVersion = localDataService._getVersionFromCalibrationIndex("123", True)
    assert actualVersion is None


def test__getVersionFromNormalizationIndex():
    localDataService = LocalDataService()
    localDataService.readNormalizationIndex = mock.Mock()
    localDataService.readNormalizationIndex.return_value = [mock.Mock()]
    localDataService.readNormalizationIndex.return_value[0] = NormalizationIndexEntry(
        timestamp=123,
        version=1,
        appliesTo="123",
        runNumber="123",
        useLiteMode=True,
        backgroundRunNumber="456",
        comments="",
        author="",
    )
    actualVersion = localDataService._getVersionFromNormalizationIndex("123", True)
    assert actualVersion == 1


def test__getVersionFromNormalizationIndex_nuffink():
    localDataService = LocalDataService()
    localDataService.readNormalizationIndex = mock.Mock()
    localDataService.readNormalizationIndex.return_value = [mock.Mock()]
    localDataService.readNormalizationIndex.return_value[0] = NormalizationIndexEntry(
        timestamp=123,
        version=1,
        appliesTo=">123",
        runNumber="123",
        useLiteMode=True,
        backgroundRunNumber="456",
        comments="",
        author="",
    )
    actualVersion = localDataService._getVersionFromNormalizationIndex("123", True)
    assert actualVersion is None


def test__getCurrentCalibrationRecord():
    localDataService = LocalDataService()
    localDataService._getVersionFromCalibrationIndex = mock.Mock()
    localDataService._getVersionFromCalibrationIndex.return_value = "1"
    localDataService.readCalibrationRecord = mock.Mock()
    mockRecord = mock.Mock()
    localDataService.readCalibrationRecord.return_value = mockRecord
    actualRecord = localDataService._getCurrentCalibrationRecord("123", True)
    assert actualRecord == mockRecord


def test__getCurrentNormalizationRecord():
    localDataService = LocalDataService()
    localDataService._getVersionFromNormalizationIndex = mock.Mock()
    localDataService._getVersionFromNormalizationIndex.return_value = "1"
    mockRecord = mock.Mock()
    localDataService.readNormalizationRecord = mock.Mock(return_value=mockRecord)
    actualRecord = localDataService._getCurrentNormalizationRecord("123", True)
    assert actualRecord == mockRecord


def test__constructCalibrationParametersFilePath():
    testVersion = randint(10, 20)
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
    localDataService._constructCalibrationStatePath = mock.Mock()
    localDataService._constructCalibrationStatePath.return_value = Path(Resource.getPath("outputs/"))
    actualPath = localDataService._constructCalibrationParametersFilePath("57514", True, testVersion)
    assert (
        actualPath == Path(Resource.getPath("outputs")) / wnvf.fileVersion(testVersion) / "CalibrationParameters.json"
    )


def test_readCalibrationState():
    inputPath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        tmpRoot.addFileAs(
            inputPath,
            localDataService.calibrationIndex("57514", True).parametersPath(1),
        )
        actualState = localDataService.readCalibrationState("57514", True)
    expectedState = Calibration.parse_file(inputPath)
    assert actualState == expectedState
    assert isinstance(actualState, Calibration)


def test_readCalibrationState_no_file():
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        with pytest.raises(RecoverableException):
            localDataService.readCalibrationState("57514", False)


def test_readNormalizationState():
    inputPath = Resource.getPath("inputs/normalization/NormalizationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        tmpRoot.addFileAs(
            inputPath,
            localDataService.normalizationIndex("57514", True).parametersPath(1),
        )
        actualState = localDataService.readNormalizationState("57514", True, 1)
    expectedState = Normalization.parse_file(inputPath)
    assert actualState == expectedState
    assert isinstance(actualState, Normalization)


def test_writeCalibrationState():
    runNumber = "123"
    useLiteMode = True
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        calibration = Calibration.parse_file(Resource.getPath("/inputs/calibration/CalibrationParameters.json"))
        localDataService.writeCalibrationState(calibration)
        file = localDataService.calibrationIndex(runNumber, useLiteMode).parametersPath(calibration.version)
        assert file.exists()


def test_writeCalibrationState_overwrite_warning(caplog):
    # Test that overwriting an existing calibration logs a warning.
    runNumber = "123"
    useLiteMode = True
    localDataService = LocalDataService()
    version = 1
    with state_root_redirect(localDataService) as tmpRoot:
        with caplog.at_level(logging.WARNING):
            # add the file inside the state root at version 1
            filePath = localDataService.calibrationIndex(runNumber, useLiteMode).parametersPath(version)
            tmpRoot.addFileAs(
                Resource.getPath("/inputs/calibration/CalibrationParameters.json"),
                filePath,
            )
            # now try to overwrite it
            calibration = Calibration.parse_raw(Resource.read("/inputs/calibration/CalibrationParameters.json"))
            localDataService.writeCalibrationState(calibration, version)
            assert filePath.exists()
        assert f"Overwriting Calibration parameters at {filePath}" in caplog.text


@mock.patch("snapred.backend.data.GroceryService.GroceryService._createDiffcalTableWorkspaceName")
@mock.patch("snapred.backend.data.GroceryService.GroceryService._fetchInstrumentDonor")
def test_writeDefaultDiffCalTable(fetchInstrumentDonor, createDiffCalTableWorkspaceName):
    runNumber = "default"
    version = VERSION_START
    useLiteMode = True
    # mock the grocery service to return the fake instrument to use for geometry
    idfWS = mtd.unique_name(prefix="_idf_")
    LoadEmptyInstrument(
        Filename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
        OutputWorkspace=idfWS,
    )
    fetchInstrumentDonor.return_value = idfWS
    # mock the file names to check them later
    wsName = f"diffcal_{runNumber}_{wnvf.formatVersion(version)}"
    createDiffCalTableWorkspaceName.return_value = wsName
    # now write the diffcal file
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        localDataService._writeDefaultDiffCalTable(runNumber, useLiteMode)
        file = localDataService.calibrationIndex(runNumber, useLiteMode).versionPath(version) / wsName
        file = file.with_suffix(".h5")
        assert file.exists()


def test_writeNormalizationState():
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        normalization = Normalization.parse_file(Resource.getPath("/inputs/normalization/NormalizationParameters.json"))
        normalization.version = 1
        localDataService.writeNormalizationState(normalization, 1)
        assert localDataService.normalizationIndex("123", True).versionPath(1).exists()


def test_readDetectorState():
    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()

    pvFileMock = mock.Mock()
    # 1X: seven required `readDetectorState` log entries:
    #   * generated `DetectorInfo` matches that from 'inputs/calibration/CalibrationParameters.json'
    pvFileMock.get.side_effect = [
        [1],
        [2],
        [1.1],
        [1.2],
        [1],
        [1.0],
        [2.0],
    ]
    localDataService._readPVFile.return_value = pvFileMock

    testCalibration = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
    testDetectorState = testCalibration.instrumentState.detectorState

    actualDetectorState = localDataService.readDetectorState("123")
    assert actualDetectorState == testDetectorState


def test_readDetectorState_bad_logs():
    localDataService = LocalDataService()
    localDataService._constructPVFilePath = mock.Mock()
    localDataService._constructPVFilePath.return_value = "/not/a/path"
    localDataService._readPVFile = mock.Mock()

    pvFileMock = mock.Mock()
    # 1X: seven required `readDetectorState` log entries:
    #   * generated `DetectorInfo` matches that from 'inputs/calibration/CalibrationParameters.json'
    pvFileMock.get.side_effect = [
        "glitch",
        [2],
        [1.1],
        [1.2],
        [1],
        [1.0],
        [2.0],
    ]
    localDataService._readPVFile.return_value = pvFileMock

    with pytest.raises(ValueError, match="Could not find all required logs"):
        localDataService.readDetectorState("123")


def test_initializeState():
    # Test 'initializeState'; test basic functionality.
    runNumber = "123"
    useLiteMode = True

    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()

    pvFileMock = mock.Mock()
    # 2X: seven required `readDetectorState` log entries:
    #   * generated stateId hex-digest: 'ab8704b0bc2a2342',
    #   * generated `DetectorInfo` matches that from 'inputs/calibration/CalibrationParameters.json'
    pvFileMock.get.side_effect = [
        [1],
        [2],
        [1.1],
        [1.2],
        [1],
        [1.0],
        [2.0],
        [1],
        [2],
        [1.1],
        [1.2],
        [1],
        [1.0],
        [2.0],
    ]
    localDataService._readPVFile.return_value = pvFileMock
    localDataService._writeDefaultDiffCalTable = mock.Mock()

    testCalibrationData = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
    testCalibrationData.useLiteMode = useLiteMode

    localDataService.readInstrumentConfig = mock.Mock()
    localDataService.readInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    localDataService.writeCalibrationState = mock.Mock()
    localDataService._prepareStateRoot = mock.Mock()
    actual = localDataService.initializeState(runNumber, useLiteMode, "test")
    actual.creationDate = testCalibrationData.creationDate

    assert actual == testCalibrationData
    assert localDataService._writeDefaultDiffCalTable.called_once_with(runNumber, useLiteMode)


# @mock.patch.object(LocalDataService, "_prepareStateRoot")
def test_initializeState_calls_prepareStateRoot():
    # Test that 'initializeState' initializes the <state root> directory.

    runNumber = "123"
    useLiteMode = True

    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()

    pvFileMock = mock.Mock()
    # 2X: seven required `readDetectorState` log entries:
    #   * generated stateId hex-digest: 'ab8704b0bc2a2342',
    #   * generated `DetectorInfo` matches that from 'inputs/calibration/CalibrationParameters.json'
    pvFileMock.get.side_effect = [
        [1],
        [2],
        [1.1],
        [1.2],
        [1],
        [1.0],
        [2.0],
        [1],
        [2],
        [1.1],
        [1.2],
        [1],
        [1.0],
        [2.0],
    ]
    localDataService._readPVFile.return_value = pvFileMock
    localDataService._writeDefaultDiffCalTable = mock.Mock()

    testCalibrationData = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))

    localDataService.readInstrumentConfig = mock.Mock()
    localDataService.readInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    localDataService.writeCalibrationState = mock.Mock()
    localDataService._readDefaultGroupingMap = mock.Mock(return_value=mock.Mock(isDirty=False))

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=stateRootPath)

        assert not stateRootPath.exists()
        localDataService.initializeState(runNumber, useLiteMode, "test")
        assert stateRootPath.exists()


def test_prepareStateRoot_creates_state_root_directory():
    # Test that the <state root> directory is created when it doesn't exist.
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)

        assert not localDataService._constructCalibrationStateRoot().exists()
        localDataService._prepareStateRoot(stateId)
        assert localDataService._constructCalibrationStateRoot().exists()


def test_prepareStateRoot_existing_state_root():
    # Test that an already existing <state root> directory is not an error.
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService._constructCalibrationStateRoot().mkdir()
        defaultGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        assert localDataService._constructCalibrationStateRoot().exists()
        localDataService._prepareStateRoot(stateId)


def test_prepareStateRoot_writes_grouping_map():
    # Test that the first time a <state root> directory is initialized,
    #   the `StateConfig.groupingMap` is written to the directory.
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)

        assert not localDataService._groupingMapPath(stateId).exists()
        localDataService._prepareStateRoot(stateId)
        assert localDataService._groupingMapPath(stateId).exists()


def test_prepareStateRoot_sets_grouping_map_stateid():
    # Test that the first time a <state root> directory is initialized,
    #   the 'stateId' of the `StateConfig.groupingMap` is set to match that of the state.
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)

        localDataService._prepareStateRoot(stateId)

        groupingMap = GroupingMap.parse_file(localDataService._groupingMapPath(stateId))
    assert groupingMap.stateId == stateId


def test_prepareStateRoot_no_default_grouping_map():
    # Test that the first time a <state root> directory is initialized,
    #   the 'defaultGroupingMap.json' at Config['instrument.calibration.powder.grouping.home']
    #   is required to exist.
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMapFilePath = Resource.getPath("inputs/pixel_grouping/does_not_exist.json")
        with pytest.raises(  # noqa: PT012
            FileNotFoundError,
            match=f'required default grouping-schema map "{defaultGroupingMapFilePath}" does not exist',
        ):
            localDataService._defaultGroupingMapPath = mock.Mock(return_value=Path(defaultGroupingMapFilePath))
            localDataService._prepareStateRoot(stateId)


def test_prepareStateRoot_does_not_overwrite_grouping_map():
    # If a 'groupingMap.json' file already exists at the <state root> directory,
    #   it should not be overwritten.
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService._constructCalibrationStateRoot().mkdir()
        defaultGroupingMapFilePath = Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")

        # Write a 'groupingMap.json' file to the <state root>, but with a different stateId;
        #   note that the _value_ of the stateId field is _not_ validated at this stage, except for its format.
        groupingMap = GroupingMap.parse_file(defaultGroupingMapFilePath)
        otherStateId = "bbbbaaaabbbbeeee"
        groupingMap.coerceStateId(otherStateId)
        write_model_pretty(groupingMap, localDataService._groupingMapPath(stateId))

        defaultGroupingMap = GroupingMap.parse_file(defaultGroupingMapFilePath)
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        localDataService._prepareStateRoot(stateId)

        groupingMap = GroupingMap.parse_file(localDataService._groupingMapPath(stateId))
    assert groupingMap.stateId == otherStateId


##### TESTS OF GROUPING MAP METHODS #####


@mock.patch(ThisService + "parse_file_as")
def test_readGroupingMap_no(parse_file_as):
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        localDataService = LocalDataService()
        localDataService.checkCalibrationFileExists = mock.Mock(return_value=False)
        localDataService._generateStateId = mock.Mock(side_effect=RuntimeError("YOU IDIOT!"))
        localDataService._defaultGroupingMapPath = mock.Mock(return_value=Path(tmpDir))

        runNumber = "flan"
        res = localDataService.readGroupingMap(runNumber)
        assert res == parse_file_as.return_value
        assert not localDataService._generateStateId.called


@mock.patch(ThisService + "parse_file_as")
def test_readGroupingMap_yes(parse_file_as):
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        localDataService = LocalDataService()
        localDataService.checkCalibrationFileExists = mock.Mock(return_value=True)
        localDataService._generateStateId = mock.Mock(return_value=(mock.Mock(), mock.Mock()))
        localDataService._groupingMapPath = mock.Mock(return_value=Path(tmpDir))
        localDataService._readDefaultGroupingMap = mock.Mock(side_effect=RuntimeError("YOU IDIOT!"))

        runNumber = "flan"
        res = localDataService.readGroupingMap(runNumber)
        assert res == parse_file_as.return_value
        assert not localDataService._readDefaultGroupingMap.called


def test_readDefaultGroupingMap():
    service = LocalDataService()
    savePath = Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"]
    Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = Resource.getPath(
        "inputs/pixel_grouping/"
    )
    groupingMap = None
    groupingMap = service._readDefaultGroupingMap()
    assert groupingMap.isDefault
    Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = savePath


def test_readGroupingMap_default_not_found():
    service = LocalDataService()
    savePath = Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"]
    Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = Resource.getPath("inputs/")
    with pytest.raises(  # noqa: PT012
        FileNotFoundError,
        match="default grouping-schema map",
    ):
        service._readDefaultGroupingMap()
    Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = savePath


def test_readGroupingMap_initialized_state():
    # Test that '_readGroupingMap' for an initialized state returns the state's <grouping map>.
    service = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(service, stateId=stateId) as tmpRoot:
        service._constructCalibrationStateRoot(stateId).mkdir()
        tmpRoot.addFileAs(
            Resource.getPath("inputs/pixel_grouping/groupingMap.json"),
            service._groupingMapPath(stateId),
        )
        groupingMap = service._readGroupingMap(stateId)
    assert groupingMap.stateId == stateId


def test_writeGroupingMap_relative_paths():
    # Test that '_writeGroupingMap' preserves relative-path information.
    localDataService = LocalDataService()
    savePath = Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"]
    try:
        Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = Resource.getPath(
            "inputs/pixel_grouping/"
        )

        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
            stateId = "ab8704b0bc2a2342"
            stateRootPath = Path(tmpDir) / stateId
            os.makedirs(stateRootPath)

            defaultGroupingMapFilePath = Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
            groupingMapFilePath = stateRootPath / "groupingMap.json"
            localDataService._groupingMapPath = mock.Mock(return_value=groupingMapFilePath)

            # Write a 'groupingMap.json' file to the <state root>, with the _correct_ stateId;
            #   note that the _value_ of the stateId field is _not_ validated at this stage, except for its format.
            with open(defaultGroupingMapFilePath, "r") as file:
                groupingMap = parse_raw_as(GroupingMap, file.read())
            groupingMap.coerceStateId(stateId)
            localDataService._writeGroupingMap(stateId, groupingMap)

            # read it back
            groupingMap = GroupingMap.parse_file(groupingMapFilePath)
            defaultGroupingMap = GroupingMap.parse_file(defaultGroupingMapFilePath)

        # test that relative paths are preserved
        relativePathCount = 0
        for n, focusGroup in enumerate(groupingMap.liteFocusGroups):
            assert focusGroup == defaultGroupingMap.liteFocusGroups[n]
            if not Path(focusGroup.definition).is_absolute():
                relativePathCount += 1
        for n, focusGroup in enumerate(groupingMap.nativeFocusGroups):
            assert focusGroup == defaultGroupingMap.nativeFocusGroups[n]
            if not Path(focusGroup.definition).is_absolute():
                relativePathCount += 1
        assert relativePathCount > 0
    finally:
        Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = savePath


##### TESTS OF CALIBRANT SAMPLE METHODS #####


def test_readSampleFilePaths():
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock()
    localDataService._findMatchingFileList.return_value = [
        "/sample1.json",
        "/sample2.json",
    ]
    result = localDataService.readSampleFilePaths()
    assert len(result) == 2
    assert "/sample1.json" in result
    assert "/sample2.json" in result


def test_readNoSampleFilePaths():
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock()
    localDataService._findMatchingFileList.return_value = []

    with pytest.raises(RuntimeError) as e:
        localDataService.readSampleFilePaths()
    assert "No samples found" in str(e.value)


@mock.patch("os.path.exists", return_value=True)
def test_writeCalibrantSample_failure(mock1):  # noqa: ARG001
    localDataService = LocalDataService()
    sample = mock.MagicMock()
    sample.name = "apple"
    sample.unique_id = "banana"
    with pytest.raises(ValueError) as e:  # noqa: PT011
        localDataService.writeCalibrantSample(sample)
    assert sample.name in str(e.value)
    assert sample.unique_id in str(e.value)
    assert Config["samples.home"] in str(e.value)


def test_writeCalibrantSample_success():  # noqa: ARG002
    localDataService = LocalDataService()
    sample = mock.MagicMock()
    sample.name = "apple"
    sample.unique_id = "banana"
    sample.json.return_value = "I like to eat, eat, eat"
    temp = Config._config["samples"]["home"]
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        Config._config["samples"]["home"] = tempdir
        # mock_os_join.return_value = f"{tempdir}{sample.name}_{sample.unique_id}"
        filePath = f"{tempdir}/{sample.name}_{sample.unique_id}.json"
        localDataService.writeCalibrantSample(sample)
        assert os.path.exists(filePath)
    Config._config["samples"]["home"] = temp


@mock.patch("os.path.exists", return_value=True)
def test_readCalibrantSample(mock1):  # noqa: ARG001
    localDataService = LocalDataService()

    result = localDataService.readCalibrantSample(
        Resource.getPath("inputs/calibrantSamples/Silicon_NIST_640D_001.json")
    )
    assert type(result) == CalibrantSamples
    assert result.name == "Silicon_NIST_640D"


@mock.patch("os.path.exists", return_value=True)
def test_readCifFilePath(mock1):  # noqa: ARG001
    localDataService = LocalDataService()

    result = localDataService.readCifFilePath("testid")
    assert result == "/SNS/SNAP/shared/Calibration_dynamic/CalibrantSamples/EntryWithCollCode52054_diamond.cif"


##### TESTS OF WORKSPACE WRITE METHODS #####


def test_writeWorkspace():
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as tmpPath:
        workspaceName = "test_workspace"
        basePath = Path(tmpPath)
        filename = Path(workspaceName + ".nxs")
        # Create a test workspace to write.
        LoadEmptyInstrument(
            Filename=fakeInstrumentFilePath,
            OutputWorkspace=workspaceName,
        )
        assert mtd.doesExist(workspaceName)
        localDataService.writeWorkspace(basePath, filename, workspaceName)
        assert (basePath / filename).exists()
    mtd.clear()


def test_writeRaggedWorkspace():
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
        workspaceName = "test_ragged"
        basePath = Path(tmpPath)
        filename = Path(workspaceName + ".tar")
        # Create a test ragged workspace to write.
        CreateSampleWorkspace(
            OutputWorkspace=workspaceName,
            Function="One Peak",
            NumBanks=1,
            NumMonitors=1,
            BankPixelWidth=5,
            NumEvents=500,
            Random=True,
            XUnit="DSP",
            XMin=0,
            XMax=8000,
            BinWidth=100,
        )
        assert mtd.doesExist(workspaceName)
        localDataService.writeRaggedWorkspace(basePath, filename, workspaceName)
        assert (basePath / filename).exists()
        localDataService.readRaggedWorkspace(basePath, filename, "test_out")
        assert mtd.doesExist("test_out")
    mtd.clear()


def test_writeGroupingWorkspace():
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as tmpPath:
        workspaceName = "test_grouping"
        basePath = Path(tmpPath)
        filename = Path(workspaceName + ".h5")
        # Create a test grouping workspace to write.
        CreateGroupingWorkspace(
            OutputWorkspace=workspaceName,
            CustomGroupingString="1",
            InstrumentFilename=fakeInstrumentFilePath,
        )
        localDataService.writeGroupingWorkspace(basePath, filename, workspaceName)
        assert (basePath / filename).exists()
    mtd.clear()


def test_writeDiffCalWorkspaces():
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
        basePath = Path(basePath)
        tableWSName = "test_table"
        maskWSName = "test_mask"
        filename = Path(tableWSName + ".h5")
        # Create an instrument workspace.
        instrumentDonor = "test_instrument_donor"
        LoadEmptyInstrument(
            Filename=fakeInstrumentFilePath,
            OutputWorkspace=instrumentDonor,
        )
        assert mtd.doesExist(instrumentDonor)
        # Create table and mask workspaces to write.
        createCompatibleMask(maskWSName, instrumentDonor, fakeInstrumentFilePath)
        assert mtd.doesExist(maskWSName)
        createCompatibleDiffCalTable(tableWSName, instrumentDonor)
        assert mtd.doesExist(tableWSName)
        localDataService.writeDiffCalWorkspaces(
            basePath, filename, tableWorkspaceName=tableWSName, maskWorkspaceName=maskWSName
        )
        assert (basePath / filename).exists()
    mtd.clear()


def test_writeDiffCalWorkspaces_bad_path():
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with pytest.raises(  # noqa: PT012
        RuntimeError,
        match="specify filename including '.h5' extension",
    ):
        with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
            basePath = Path(basePath)
            tableWSName = "test_table"
            maskWSName = "test_mask"
            # do not add required: ".h5" suffix
            filename = Path(tableWSName)
            # Create an instrument workspace.
            instrumentDonor = "test_instrument_donor"
            LoadEmptyInstrument(
                Filename=fakeInstrumentFilePath,
                OutputWorkspace=instrumentDonor,
            )
            assert mtd.doesExist(instrumentDonor)
            # Create table and mask workspaces to write.
            createCompatibleMask(maskWSName, instrumentDonor, fakeInstrumentFilePath)
            assert mtd.doesExist(maskWSName)
            createCompatibleDiffCalTable(tableWSName, instrumentDonor)
            assert mtd.doesExist(tableWSName)
            localDataService.writeDiffCalWorkspaces(
                basePath, filename, tableWorkspaceName=tableWSName, maskWorkspaceName=maskWSName
            )
            assert (basePath / filename).exists()
    mtd.clear()
