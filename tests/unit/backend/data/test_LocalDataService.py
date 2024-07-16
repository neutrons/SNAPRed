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
from typing import List, Literal

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
    GroupWorkspaces,
    LoadEmptyInstrument,
    LoadInstrument,
    RenameWorkspaces,
    mtd,
)
from snapred.backend.dao import StateConfig
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT
from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.request import (
    CreateCalibrationRecordRequest,
    CreateIndexEntryRequest,
    CreateNormalizationRecordRequest,
)
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.data.Indexer import IndexerType
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.meta.redantic import parse_file_as, parse_raw_as, write_model_pretty
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask
from util.state_helpers import reduction_root_redirect, state_root_redirect

LocalDataServiceModule = importlib.import_module(LocalDataService.__module__)
ThisService = "snapred.backend.data.LocalDataService."

IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")


@pytest.fixture(autouse=True)
def _capture_logging(monkeypatch):
    # For some reason pytest 'caplog' doesn't work with the SNAPRed logging setup.  (TODO: fix this!)
    # This patch bypasses the issue, by renaming and
    # patching the `LocalDataService` module's logger to a standard python `Logger`.
    defaultLogger = logging.getLogger(LocalDataServiceModule.__name__ + "_patch")
    defaultLogger.propagate = True
    monkeypatch.setattr(LocalDataServiceModule, "logger", defaultLogger)


fakeInstrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
reductionIngredients = ReductionIngredients.model_validate_json(
    Resource.read("inputs/calibration/ReductionIngredients.json")
)

### GENERALIZED METHODS FOR TESTING NORMALIZATION / CALIBRATION / REDUCTION METHODS ###


def do_test_index_missing(workflow):
    # NOTE this is already covered by Indexer tests,
    # but it existed and didn't hurt to retain
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        assert len(getattr(localDataService, f"read{workflow}Index")("123", True)) == 0


def do_test_workflow_indexer(workflow):
    # verify the correct indexer is being returned
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock()
    for useLiteMode in [True, False]:
        getattr(localDataService, f"{workflow.lower()}Indexer")("xyz", useLiteMode)
        assert localDataService.indexer.called_once_with("xyz", useLiteMode, workflow.upper())


def do_test_read_index(workflow):
    # verify that calls to read index call out to the indexer
    mockIndex = ["nope"]
    mockIndexer = mock.Mock(getIndex=mock.Mock(return_value=mockIndex))
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock(return_value=mockIndexer)
    for useLiteMode in [True, False]:
        ans = getattr(localDataService, f"read{workflow}Index")("xyz", useLiteMode)
        assert ans == mockIndex


def do_test_read_record_with_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    # ensure it is calling the functionality in the indexer
    mockIndexer = mock.Mock()
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock(return_value=mockIndexer)
    for useLiteMode in [True, False]:
        version = randint(1, 20)
        res = getattr(localDataService, f"read{workflow}Record")("xyz", useLiteMode, version)
        assert res == mockIndexer.readRecord.return_value
        mockIndexer.readRecord.assert_called_with(version)


def do_test_read_record_no_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    # ensure it is calling the functionality in the indexer
    # if no version is given, it should get latest applicable version
    latestVersion = randint(20, 120)
    mockIndexer = mock.Mock(latestApplicableVersion=mock.Mock(return_value=latestVersion))
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock(return_value=mockIndexer)
    for useLiteMode in [True, False]:
        res = getattr(localDataService, f"read{workflow}Record")("xyz", useLiteMode)  # NOTE no version
        mockIndexer.latestApplicableVersion.assert_called_with("xyz")
        mockIndexer.readRecord.assert_called_with(latestVersion)
        assert res == mockIndexer.readRecord.return_value


def do_test_write_record_with_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    # ensure it is calling the methods inside the indexer service
    mockIndexer = mock.Mock()
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock(return_value=mockIndexer)
    for useLiteMode in [True, False]:
        record = globals()[f"{workflow}Record"].model_construct(
            runNumber="xyz",
            useLiteMode=useLiteMode,
            workspaces={},
            version=randint(1, 120),
            calculationParameters=mock.Mock(),
        )
        getattr(localDataService, f"write{workflow}Record")(record)
        mockIndexer.writeRecord.assert_called_with(record)
        mockIndexer.writeParameters.assert_called_with(record.calculationParameters)


def do_test_read_state_with_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    inputPath = Resource.getPath(f"inputs/{workflow.lower()}/{workflow}Parameters.json")
    localDataService = LocalDataService()
    versions = list(range(randint(10, 20)))
    shuffle(versions)
    for version in versions:
        for useLiteMode in [True, False]:
            with state_root_redirect(localDataService) as tmpRoot:
                indexer = localDataService.indexer("xyz", useLiteMode, workflow)
                tmpRoot.addFileAs(inputPath, indexer.parametersPath(version))
                assert indexer.parametersPath(version).exists()
                actualState = getattr(localDataService, f"read{workflow}State")("xyz", useLiteMode, version)
            expectedState = parse_file_as(globals()[workflow], inputPath)
            assert actualState == expectedState


def do_test_read_state_no_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    currentVersion = randint(20, 120)
    inputPath = Resource.getPath(f"inputs/{workflow.lower()}/{workflow}Parameters.json")
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with state_root_redirect(localDataService) as tmpRoot:
            indexer = localDataService.indexer("xyz", useLiteMode, workflow)
            tmpRoot.addFileAs(inputPath, indexer.parametersPath(currentVersion))
            indexer.index = {currentVersion: mock.Mock()}  # make index point at the added version
            actualState = getattr(localDataService, f"read{workflow}State")("xyz", useLiteMode)  # NOTE no version
        expectedState = parse_file_as(globals()[workflow], inputPath)
        assert actualState == expectedState


### TESTS OF MISCELLANEOUS METHODS ###

"""
These tests should ONLY cover methods in the MISCELLANEOUS METHODS section
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


def _readInstrumentParameters():
    instrumentParameters = None
    with Resource.open("inputs/SNAPInstPrm.json", "r") as file:
        instrumentParameters = json.loads(file.read())
    return instrumentParameters


def test_readInstrumentConfig():
    localDataService = LocalDataService()
    localDataService._readInstrumentParameters = _readInstrumentParameters
    actual = localDataService.readInstrumentConfig()
    assert actual is not None
    assert actual.version == "1.4"
    assert actual.name == "SNAP"


def test_readInstrumentParameters():
    localDataService = LocalDataService()
    localDataService.instrumentConfigPath = Resource.getPath("inputs/SNAPInstPrm.json")
    actual = localDataService._readInstrumentParameters()
    assert actual is not None
    assert actual["version"] == 1.4
    assert actual["name"] == "SNAP"


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


def test_readStateConfig_default():
    # readstateConfig will load the default parameters file
    groupingMapPath = Resource.getPath("inputs/pixel_grouping/groupingMap.json")
    parametersPath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        tmpRoot.addFileAs(groupingMapPath, localDataService._groupingMapPath(tmpRoot.stateId))
        tmpRoot.addFileAs(parametersPath, indexer.parametersPath(VERSION_DEFAULT))
        indexer.index = {VERSION_DEFAULT: mock.Mock()}  # NOTE manually update the Indexer
        actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    assert actual.stateId == "ab8704b0bc2a2342"


def test_readStateConfig_previous():
    # readStateConfig will load the previous version's parameters file
    groupingMapPath = Resource.getPath("inputs/pixel_grouping/groupingMap.json")
    parametersPath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        tmpRoot.addFileAs(groupingMapPath, localDataService._groupingMapPath(tmpRoot.stateId))
        version = randint(2, 10)
        tmpRoot.addFileAs(parametersPath, indexer.parametersPath(version))
        indexer.index = {version: mock.Mock()}  # NOTE manually update the Indexer
        actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    assert actual.stateId == "ab8704b0bc2a2342"


def test_readStateConfig_attaches_grouping_map():
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    groupingMapPath = Resource.getPath("inputs/pixel_grouping/groupingMap.json")
    parametersPath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        tmpRoot.addFileAs(groupingMapPath, localDataService._groupingMapPath(tmpRoot.stateId))
        version = randint(2, 10)
        tmpRoot.addFileAs(parametersPath, indexer.parametersPath(version))
        indexer.index = {version: mock.Mock()}  # NOTE manually update the Indexer
        actual = localDataService.readStateConfig("57514", True)
    expectedMap = parse_file_as(GroupingMap, groupingMapPath)
    assert actual.groupingMap == expectedMap


def test_readStateConfig_invalid_grouping_map():
    # Test that the attached grouping-schema map's 'stateId' is checked.
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    groupingMapPath = Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
    parametersPath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        tmpRoot.addFileAs(groupingMapPath, localDataService._groupingMapPath(tmpRoot.stateId))
        version = randint(2, 10)
        tmpRoot.addFileAs(parametersPath, indexer.parametersPath(version))
        indexer.index = {version: mock.Mock()}  # NOTE manually update the Indexer
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
    expected = parse_file_as(Calibration, parametersPath)
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=expected.instrumentState.id.hex) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        version = randint(2, 10)
        tmpRoot.addFileAs(parametersPath, indexer.parametersPath(version))
        indexer.index = {version: mock.Mock()}  # NOTE manually update the Indexer
        assert not localDataService._groupingMapPath(tmpRoot.stateId).exists()
        localDataService._prepareStateRoot = mock.Mock(
            side_effect=lambda x: tmpRoot.addFileAs(groupingMapPath, localDataService._groupingMapPath(x))
        )
        actual = localDataService.readStateConfig("57514", True)
        assert localDataService._groupingMapPath(tmpRoot.stateId).exists()
    assert actual is not None
    assert actual.stateId.hex == tmpRoot.stateId
    localDataService._prepareStateRoot.assert_called_once()


def test_prepareStateRoot_creates_state_root_directory():
    # Test that the <state root> directory is created when it doesn't exist.
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = parse_file_as(
            GroupingMap, Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
        )
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
        defaultGroupingMap = parse_file_as(
            GroupingMap, Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
        )
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        assert localDataService._constructCalibrationStateRoot().exists()
        localDataService._prepareStateRoot(stateId)


def test_prepareStateRoot_writes_grouping_map():
    # Test that the first time a <state root> directory is initialized,
    #   the `StateConfig.groupingMap` is written to the directory.
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = parse_file_as(
            GroupingMap, Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
        )
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
        defaultGroupingMap = parse_file_as(
            GroupingMap, Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
        )
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)

        localDataService._prepareStateRoot(stateId)

        groupingMap = parse_file_as(GroupingMap, localDataService._groupingMapPath(stateId))
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
        groupingMap = parse_file_as(GroupingMap, defaultGroupingMapFilePath)
        otherStateId = "bbbbaaaabbbbeeee"
        groupingMap.coerceStateId(otherStateId)
        write_model_pretty(groupingMap, localDataService._groupingMapPath(stateId))

        defaultGroupingMap = parse_file_as(GroupingMap, defaultGroupingMapFilePath)
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        localDataService._prepareStateRoot(stateId)

        groupingMap = parse_file_as(GroupingMap, localDataService._groupingMapPath(stateId))
    assert groupingMap.stateId == otherStateId


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
            groupingMap = parse_file_as(GroupingMap, defaultGroupingMapFilePath)
            groupingMap.coerceStateId(stateId)
            localDataService._writeGroupingMap(stateId, groupingMap)

            # read it back
            groupingMap = parse_file_as(GroupingMap, groupingMapFilePath)
            defaultGroupingMap = parse_file_as(GroupingMap, defaultGroupingMapFilePath)

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


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists(GetIPTS):  # noqa ARG002
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        tmpRoot.path().mkdir()
        runNumber = "12345"
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


def test_write_model_pretty_StateConfig_excludes_grouping_map():
    # At present there is no `writeStateConfig` method, and there is no `readStateConfig` that doesn't
    #   actually build up the `StateConfig` from its components.
    # This test verifies that `GroupingMap` is excluded from any future `StateConfig` JSON serialization.
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        # move the calculation parameters and grouping map into correct folder
        indexer = localDataService.calibrationIndexer("57514", True)
        tmpRoot.addFileAs(
            Resource.getPath("inputs/calibration/CalibrationParameters.json"),
            indexer.parametersPath(VERSION_DEFAULT),
        )
        indexer.index = {VERSION_DEFAULT: mock.Mock()}
        tmpRoot.addFileAs(
            Resource.getPath("inputs/pixel_grouping/groupingMap.json"),
            localDataService._groupingMapPath(tmpRoot.stateId),
        )

        # construct the state config object
        actual = localDataService.readStateConfig("57514", True)
        # now save it to a path in the directory
        stateConfigPath = tmpRoot.path() / "stateConfig.json"
        write_model_pretty(actual, stateConfigPath)
        # read it back in and make sure there is no grouping map
        stateConfig = parse_file_as(StateConfig, stateConfigPath)
        assert stateConfig.groupingMap is None


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
    localDataService._generateStateId = mock.Mock(return_value=(fakeState, "gibberish"))
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
    localDataService._generateStateId = mock.Mock(return_value=(fakeState, "gibberish"))
    runNumber = "xyz"
    for useLiteMode in [True, False]:
        ans = localDataService._constructReductionDataRoot(runNumber, useLiteMode)
        assert isinstance(ans, Path)
        assert ans.parts[-1] == runNumber
        assert ans.parts[-2] == "lite" if useLiteMode else "native"
        assert ans.parts[:-2] == localDataService._constructReductionStateRoot(runNumber).parts


### TESTS OF VERSIONING / INDEX METHODS ###


def test_statePathForWorkflow_calibration():
    indexerType = IndexerType.CALIBRATION
    fakeStateId = "boogersoup"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        ans = localDataService._statePathForWorkflow(fakeStateId, useLiteMode, indexerType)
        exp = localDataService._constructCalibrationStatePath(fakeStateId, useLiteMode)
        assert ans == exp


def test_statePathForWorkflow_normalization():
    indexerType = IndexerType.NORMALIZATION
    fakeStateId = "boogersoup"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        ans = localDataService._statePathForWorkflow(fakeStateId, useLiteMode, indexerType)
        exp = localDataService._constructNormalizationStatePath(fakeStateId, useLiteMode)
        assert ans == exp


def test_statePathForWorkflow_reduction():
    indexerType = IndexerType.REDUCTION
    runNumber = "xyz"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with pytest.raises(NotImplementedError):
            localDataService._statePathForWorkflow(runNumber, useLiteMode, indexerType)


def test_statePathForWorkflow_default():
    indexerType = IndexerType.DEFAULT
    fakeStateId = "boogersoup"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with pytest.raises(NotImplementedError):
            localDataService._statePathForWorkflow(fakeStateId, useLiteMode, indexerType)


def test_statePathForWorkflow_nonsense():
    indexerType = "chumbawumba"
    fakeStateId = "boogersoup"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with pytest.raises(NotImplementedError):
            localDataService._statePathForWorkflow(fakeStateId, useLiteMode, indexerType)


@mock.patch("snapred.backend.data.LocalDataService.Indexer")
def test_index_default(Indexer):
    indexer = Indexer.construct()
    Indexer.return_value = indexer
    localDataService = LocalDataService()

    # assert there are no indexers
    localDataService._indexer.cache_clear()
    assert localDataService._indexer.cache_info() == functools._CacheInfo(hits=0, misses=0, maxsize=128, currsize=0)

    # make an indexer
    stateId = "abc"
    localDataService._generateStateId = mock.Mock(return_value=(stateId, "gibberish"))
    localDataService._statePathForWorkflow = mock.Mock(return_value="/not/real/path")
    indexerType = IndexerType.DEFAULT
    for useLiteMode in [True, False]:
        res = localDataService.indexer("xyz", useLiteMode, indexerType)
        assert res == indexer
        assert localDataService._generateStateId.called
        assert localDataService._statePathForWorkflow.called
        assert Indexer.called

        # reset call counts for next check
        Indexer.reset_mock()
        localDataService._generateStateId.reset_mock()
        localDataService._statePathForWorkflow.reset_mock()

        # call again and make sure cached version in returned
        res = localDataService.indexer("xyz", useLiteMode, indexerType)
        assert res == indexer
        assert localDataService._generateStateId.called
        assert not localDataService._statePathForWorkflow.called
        assert not Indexer.called


@mock.patch("snapred.backend.data.LocalDataService.Indexer")
def test_index_reduction(Indexer):
    # Reduction Indexers are not supported by LocalDataService
    indexer = Indexer.construct()
    Indexer.return_value = indexer
    # TODO check the indexer points inside correct file
    localDataService = LocalDataService()

    # make an indexer
    localDataService._generateStateId = mock.Mock(return_value=("xyz", "123"))

    indexerType = IndexerType.REDUCTION
    for useLiteMode in [True, False]:
        with pytest.raises(NotImplementedError):
            localDataService.indexer("xyz", useLiteMode, indexerType)


def test_calibrationIndexer():
    do_test_workflow_indexer("Calibration")


def test_normalizationIndexer():
    do_test_workflow_indexer("Normalization")


def test_readCalibrationIndex():
    # verify that calls to read index call to the indexer
    do_test_read_index("Calibration")


def test_readNormalizationIndex():
    # verify that calls to read index call to the indexer
    do_test_read_index("Normalization")


def test_readWriteCalibrationIndexEntry():
    entry = IndexEntry(
        runNumber="57514",
        useLiteMode=True,
        comments="test comment",
        author="test author",
        version=randint(2, 120),
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        localDataService.writeCalibrationIndexEntry(entry)
        actualEntries = localDataService.readCalibrationIndex(entry.runNumber, entry.useLiteMode)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


def test_readWriteNormalizationIndexEntry():
    entry = IndexEntry(
        runNumber="57514",
        useLiteMode=True,
        comments="test comment",
        author="test author",
        version=randint(2, 120),
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        localDataService.writeNormalizationIndexEntry(entry)
        actualEntries = localDataService.readNormalizationIndex(entry.runNumber, entry.useLiteMode)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


def test_readCalibrationIndexMissing():
    do_test_index_missing("Calibration")


def test_readNormalizationIndexMissing():
    do_test_index_missing("Normalization")


def readReductionIngredientsFromFile():
    with Resource.open("/inputs/calibration/ReductionIngredients.json", "r") as f:
        return ReductionIngredients.model_validate_json(f.read())


### TESTS OF CALIBRATION METHODS ###


def test_createCalibrationIndexEntry():
    request = CreateIndexEntryRequest(
        runNumber="123",
        useLiteMode=True,
        version=2,
        comments="",
        author="",
        appliesTo=">=123",
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        ans = localDataService.createCalibrationIndexEntry(request)
        assert isinstance(ans, IndexEntry)
        assert ans.runNumber == request.runNumber
        assert ans.useLiteMode == request.useLiteMode
        assert ans.version == request.version

        request.version = None
        indexer = localDataService.calibrationIndexer(request.runNumber, request.useLiteMode)
        ans = localDataService.createCalibrationIndexEntry(request)
        assert ans.version == indexer.nextVersion()


def test_createCalibrationRecord():
    record = CalibrationRecord.model_validate_json(Resource.read("inputs/calibration/CalibrationRecord_v0001.json"))
    request = CreateCalibrationRecordRequest(**record.model_dump())
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        ans = localDataService.createCalibrationRecord(request)
        assert isinstance(ans, CalibrationRecord)
        assert ans.runNumber == request.runNumber
        assert ans.useLiteMode == request.useLiteMode
        assert ans.version == request.version

        request.version = None
        indexer = localDataService.calibrationIndexer(request.runNumber, request.useLiteMode)
        ans = localDataService.createCalibrationRecord(request)
        assert ans.version == indexer.nextVersion()


def test_readCalibrationRecord_with_version():
    # ensure it is calling the functionality in the indexer
    do_test_read_record_with_version("Calibration")


def test_readCalibrationRecord_no_version():
    # ensure it is calling the functionality in the indexer
    # if no version is given, it should get latest applicable version
    do_test_read_record_no_version("Calibration")


def test_writeCalibrationRecord_with_version():
    # ensure it is calling the methods inside the indexer service
    do_test_write_record_with_version("Calibration")


def test_readWriteCalibrationRecord():
    # ensure that reading and writing a calibration record will correctly interact with the file system
    # NOTE a similar test is done of the indexer, but this pre-existed and doesn't hurt to retain
    record = CalibrationRecord.model_validate_json(Resource.read("inputs/calibration/CalibrationRecord_v0001.json"))
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        record.useLiteMode = useLiteMode
        with state_root_redirect(localDataService):
            localDataService.writeCalibrationRecord(record)
            actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode)
        assert actualRecord.version == record.version
        assert actualRecord == record


def test_writeCalibrationWorkspaces():
    version = randint(2, 120)
    localDataService = LocalDataService()
    stateId = "ab8704b0bc2a2342"
    testCalibrationRecord = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        basePath = localDataService.calibrationIndexer(testCalibrationRecord.runNumber, True).versionPath(1)

        # Workspace names need to match the names that are used in the test record.
        workspaces = testCalibrationRecord.workspaces.copy()
        runNumber = testCalibrationRecord.runNumber
        version = testCalibrationRecord.version
        outputWSName = workspaces.pop(wngt.DIFFCAL_OUTPUT)[0]
        diagnosticWSName = workspaces.pop(wngt.DIFFCAL_DIAG)[0]
        tableWSName = workspaces.pop(wngt.DIFFCAL_TABLE)[0]
        maskWSName = workspaces.pop(wngt.DIFFCAL_MASK)[0]
        if workspaces:
            raise RuntimeError(f"unexpected workspace-types in record.workspaces: {workspaces}")

        # Create sample workspaces.
        LoadEmptyInstrument(OutputWorkspace=outputWSName, Filename=fakeInstrumentFilePath)
        # create the diagnostic workspace group
        ws1 = CloneWorkspace(outputWSName)
        GroupWorkspaces(InputWorkspaces=[ws1], OutputWorkspace=diagnosticWSName)
        assert mtd.doesExist(outputWSName)
        assert mtd.doesExist(diagnosticWSName)

        # Create diffraction-calibration table and mask workspaces.
        createCompatibleDiffCalTable(tableWSName, outputWSName)
        createCompatibleMask(maskWSName, outputWSName, fakeInstrumentFilePath)
        assert mtd.doesExist(tableWSName)
        assert mtd.doesExist(maskWSName)

        localDataService.writeCalibrationWorkspaces(testCalibrationRecord)

        outputFilename = Path(outputWSName + Config["calibration.diffraction.output.extension"])
        diagnosticFilename = Path(diagnosticWSName + Config["calibration.diffraction.diagnostic.extension"])
        diffCalFilename = Path(wng.diffCalTable().runNumber(runNumber).version(version).build() + ".h5")
        for filename in [outputFilename, diagnosticFilename, diffCalFilename]:
            assert (basePath / filename).exists()
        mtd.clear()


### TESTS OF NORMALIZATION METHODS ###


def test_createNormalizationIndexEntry():
    request = CreateIndexEntryRequest(
        runNumber="123",
        useLiteMode=True,
        version=2,
        comments="",
        author="",
        appliesTo=">=123",
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        ans = localDataService.createNormalizationIndexEntry(request)
        assert isinstance(ans, IndexEntry)
        assert ans.runNumber == request.runNumber
        assert ans.useLiteMode == request.useLiteMode
        assert ans.version == request.version

        request.version = None
        indexer = localDataService.normalizationIndexer(request.runNumber, request.useLiteMode)
        ans = localDataService.createNormalizationIndexEntry(request)
        assert ans.version == indexer.nextVersion()


def test_createNormalizationRecord():
    record = NormalizationRecord.model_validate_json(Resource.read("inputs/normalization/NormalizationRecord.json"))
    request = CreateNormalizationRecordRequest(**record.model_dump())
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        ans = localDataService.createNormalizationRecord(request)
        assert isinstance(ans, NormalizationRecord)
        assert ans.runNumber == request.runNumber
        assert ans.useLiteMode == request.useLiteMode
        assert ans.version == request.version

        request.version = None
        indexer = localDataService.normalizationIndexer(request.runNumber, request.useLiteMode)
        ans = localDataService.createNormalizationRecord(request)
        assert ans.version == indexer.nextVersion()


def test_readNormalizationRecord_with_version():
    # ensure it is calling the functionality in the indexer
    do_test_read_record_with_version("Normalization")


def test_readNormalizationRecord_no_version():
    # ensure it is calling the functionality in the indexer
    # if no version is given, it should get latest applicable version
    do_test_read_record_no_version("Normalization")


def test_writeNormalizationRecord_with_version():
    # ensure it is calling the methods inside the indexer service
    do_test_write_record_with_version("Normalization")


def test_readWriteNormalizationRecord():
    # ensure that reading and writing a normalization record will correctly interact with the file system
    record = parse_raw_as(NormalizationRecord, Resource.read("inputs/normalization/NormalizationRecord.json"))
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        record.useLiteMode = useLiteMode
        # NOTE redirect nested so assertion occurs outside of redirect
        # failing assertions inside tempdirs can create unwanted files
        with state_root_redirect(localDataService):
            localDataService.writeNormalizationRecord(record)
            actualRecord = localDataService.readNormalizationRecord("57514", useLiteMode)
        assert actualRecord.version == record.version
        assert actualRecord.calculationParameters.version == record.calculationParameters.version
        assert actualRecord == record


def test_writeNormalizationWorkspaces():
    version = randint(2, 120)
    stateId = "ab8704b0bc2a2342"
    localDataService = LocalDataService()
    testNormalizationRecord = NormalizationRecord.model_validate_json(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    testNormalizationRecord.version = version
    with state_root_redirect(localDataService, stateId=stateId):
        # Workspace names need to match the names that are used in the test record.
        runNumber = testNormalizationRecord.runNumber  # noqa: F841
        useLiteMode = testNormalizationRecord.useLiteMode
        newWorkspaceNames = []
        for ws in testNormalizationRecord.workspaceNames:
            newWorkspaceNames.append(ws + "_" + wnvf.formatVersion(version))
        testNormalizationRecord.workspaceNames = newWorkspaceNames
        testWS0, testWS1, testWS2 = testNormalizationRecord.workspaceNames

        basePath = localDataService.normalizationIndexer(runNumber, useLiteMode).versionPath(version)

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
            filename = Path(wsName + ".nxs")
            assert (basePath / filename).exists()
    mtd.clear()


### TESTS OF REDUCTION METHODS ###


def _writeSyntheticReductionRecord(filePath: Path, version: str):
    # Create a `ReductionRecord` JSON file to be used by the unit tests.

    # TODO: Implement methods to create the synthetic `CalibrationRecord` and `NormalizationRecord`.
    testCalibration = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testNormalization = NormalizationRecord.model_validate_json(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    testRecord = ReductionRecord(
        runNumbers=[testCalibration.runNumber],
        useLiteMode=testCalibration.useLiteMode,
        calibration=testCalibration,
        normalization=testNormalization,
        pixelGroupingParameters={
            pg.focusGroup.name: list(pg.pixelGroupingParameters.values()) for pg in testCalibration.pixelGroups
        },
        version=int(version),
        stateId=testCalibration.calculationParameters.instrumentState.id,
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


def test_readWriteReductionRecord_no_version():
    inputRecordFilePath = Resource.getPath("inputs/reduction/ReductionRecord_v0001.json")
    # Create the input data for this test:
    # _writeSyntheticReductionRecord(inputRecordFilePath, "1")

    record_v0001 = parse_file_as(ReductionRecord, inputRecordFilePath)
    # Get a second copy (version still set to `1`)
    record_v0002 = parse_file_as(ReductionRecord, inputRecordFilePath)

    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService):
        # WARNING: 'writeReductionRecord' modifies <incoming record>.version,

        # write: version will be set to a time
        localDataService.writeReductionRecord(record_v0001)
        actualRecord1 = localDataService.readReductionRecord(record_v0001.runNumbers[0], record_v0001.useLiteMode)

        # write: version wille be set to a time
        localDataService.writeReductionRecord(record_v0002)
        actualRecord2 = localDataService.readReductionRecord(record_v0002.runNumbers[0], record_v0002.useLiteMode)

    assert actualRecord1.version <= actualRecord2.version


def test_readWriteReductionRecord_specified_version():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord(inputRecordFilePath, "1")

    record_v0001 = parse_file_as(ReductionRecord, inputRecordFilePath)
    # Get a second copy (version still set to `1`)
    record_v0002 = parse_file_as(ReductionRecord, inputRecordFilePath)

    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService):
        # WARNING: 'writeReductionRecord' modifies <incoming record>.version,

        #  Important: start with version > 1: should not depend on any existing directory structure!

        # write first version
        version1 = randint(3, 20)
        record_v0001.version = version1
        localDataService.writeReductionRecord(record_v0001)

        # write second version
        version2 = randint(version1 + 1, 120)
        assert version1 != version2
        record_v0002.version = version2
        localDataService.writeReductionRecord(record_v0002)

        actualRecord = localDataService.readReductionRecord(
            record_v0001.runNumbers[0],
            record_v0001.useLiteMode,
            version1,
        )
        assert actualRecord.version == version1
        actualRecord = localDataService.readReductionRecord(
            record_v0002.runNumbers[0],
            record_v0002.useLiteMode,
            version2,
        )
        assert actualRecord.version == version2


def test_readWriteReductionRecord_with_version():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)

    testRecord = parse_file_as(ReductionRecord, inputRecordFilePath)
    # Important: version != 1: should not depend on any existing directory structure.
    testVersion = 10
    testRecord.version = testVersion

    # Temporarily use a single run number
    runNumber = testRecord.runNumbers[0]
    stateId = "ab8704b0bc2a2342"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()

        localDataService.writeReductionRecord(testRecord)

        actualRecord = localDataService.readReductionRecord(runNumber, testRecord.useLiteMode, testVersion)
    assert actualRecord.version == int(testVersion)


def test_readWriteReductionRecord():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)
    with open(inputRecordFilePath, "r") as f:
        testRecord = ReductionRecord.model_validate_json(f.read())

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

    testRecord = parse_file_as(ReductionRecord, inputRecordFilePath)
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
        # Important to this test: use a path that doesn't already exist
        reductionFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, version)
        assert not reductionFilePath.exists()

        localDataService.writeReductionData(testRecord)

        assert reductionFilePath.exists()


def test_writeReductionData_metadata(createReductionWorkspaces):
    _uniquePrefix = "LDS_WRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)

    testRecord = parse_file_as(ReductionRecord, inputRecordFilePath)
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
            actualRecord = ReductionRecord.model_validate(dict_)
            assert actualRecord == testRecord


def test_readWriteReductionData(createReductionWorkspaces):
    _uniquePrefix = "LDS_RWRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)

    with open(inputRecordFilePath, "r") as f:
        testRecord = ReductionRecord.model_validate_json(f.read())
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


# interlude -- missplaced path and version method tests #


def test__constructReductionDataFilePath():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)
    testRecord = parse_file_as(ReductionRecord, inputRecordFilePath)

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
    localDataService._generateStateId = mock.Mock(return_value=(stateId, None))
    localDataService.getIPTS = mock.Mock(return_value=testIPTS)
    actualFilePath = localDataService._constructReductionDataFilePath(runNumber, useLiteMode, version)
    assert actualFilePath == expectedFilePath


def test_getReductionRecordFilePath():
    testVersion = randint(1, 20)
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._constructReductionDataRoot = mock.Mock()
    localDataService._constructReductionDataRoot.return_value = Path(Resource.getPath("outputs"))
    actualPath = localDataService._constructReductionRecordFilePath("57514", True, testVersion)
    assert actualPath == Path(Resource.getPath("outputs")) / wnvf.fileVersion(testVersion) / "ReductionRecord.json"


# end interlude #


### TESTS OF READ / WRITE STATE METHODS ###


def test_readCalibrationState_with_version():
    do_test_read_state_with_version("Calibration")


def test_readCalibrationState_no_version():
    do_test_read_state_no_version("Calibration")


def test_readNormalizationState_with_version():
    do_test_read_state_with_version("Normalization")


def test_readNormalizationState_no_version():
    do_test_read_state_no_version("Normalization")


def test_readWriteCalibrationState():
    # NOTE this test is already covered by tests of the Indexer
    # but it doesn't hurt to retain this test anyway
    runNumber = "123"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with state_root_redirect(localDataService):
            calibration = parse_file_as(Calibration, Resource.getPath("/inputs/calibration/CalibrationParameters.json"))
            calibration.seedRun = runNumber
            calibration.useLiteMode = useLiteMode
            localDataService.writeCalibrationState(calibration)
            ans = localDataService.readCalibrationState(runNumber, useLiteMode)
        assert ans == calibration


@mock.patch("snapred.backend.data.GroceryService.GroceryService._createDiffcalTableWorkspaceName")
@mock.patch("snapred.backend.data.GroceryService.GroceryService._fetchInstrumentDonor")
def test_writeDefaultDiffCalTable(fetchInstrumentDonor, createDiffCalTableWorkspaceName):
    # verify that the default diffcal table is being written to the default state directory
    runNumber = "default"
    version = VERSION_DEFAULT
    useLiteMode = True
    # mock the grocery service to return the fake instrument to use for geometry
    idfWS = mtd.unique_name(prefix="_idf_")
    LoadEmptyInstrument(
        Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
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
        file = localDataService.calibrationIndexer(runNumber, useLiteMode).versionPath(version) / wsName
        file = file.with_suffix(".h5")
        assert file.exists()


def test_readWriteNormalizationState():
    # NOTE this test is already covered by tests of the Indexer
    # but it doesn't hurt to retain this test anyway
    runNumber = "123"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with state_root_redirect(localDataService):
            normalization = parse_file_as(
                Normalization,
                Resource.getPath("/inputs/normalization/NormalizationParameters.json"),
            )
            normalization.seedRun = runNumber
            normalization.useLiteMode = useLiteMode
            localDataService.writeNormalizationState(normalization)
            ans = localDataService.readNormalizationState(runNumber, useLiteMode)
        assert ans == normalization


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

    testCalibration = Calibration.model_validate_json(Resource.read("inputs/calibration/CalibrationParameters.json"))
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

    testCalibrationData = Calibration.model_validate_json(
        Resource.read("inputs/calibration/CalibrationParameters.json")
    )
    testCalibrationData.useLiteMode = useLiteMode
    testCalibrationData.version = VERSION_DEFAULT

    localDataService.readInstrumentConfig = mock.Mock()
    localDataService.readInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    localDataService.writeCalibrationState = mock.Mock()
    localDataService._prepareStateRoot = mock.Mock()

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=stateRootPath)

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

    testCalibrationData = Calibration.model_validate_json(
        Resource.read("inputs/calibration/CalibrationParameters.json")
    )

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


# interlude -- a missplaced calibrant sample method test #


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


# end interlude


##### TESTS OF GROUPING MAP METHODS #####


def test_readGroupingMap_no_calibration_file():
    localDataService = LocalDataService()
    localDataService.checkCalibrationFileExists = mock.Mock(return_value=False)
    localDataService._generateStateId = mock.Mock(side_effect=RuntimeError("YOU IDIOT!"))
    localDataService._readDefaultGroupingMap = mock.Mock()
    localDataService._readGroupingMap = mock.Mock()

    runNumber = "flan"
    res = localDataService.readGroupingMap(runNumber)
    assert res == localDataService._readDefaultGroupingMap.return_value
    assert not localDataService._readGroupingMap.called


def test_readGroupingMap_yes_calibration_file():
    localDataService = LocalDataService()
    localDataService.checkCalibrationFileExists = mock.Mock(return_value=True)
    localDataService._generateStateId = mock.Mock(return_value=(mock.Mock(), mock.Mock()))
    localDataService._readGroupingMap = mock.Mock()
    localDataService._readDefaultGroupingMap = mock.Mock(side_effect=RuntimeError("YOU IDIOT!"))

    runNumber = "flan"
    res = localDataService.readGroupingMap(runNumber)
    assert res == localDataService._readGroupingMap.return_value
    assert not localDataService._readDefaultGroupingMap.called


##### TESTS OF CALIBRANT SAMPLE METHODS #####


def test_readNoSampleFilePaths():
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock()
    localDataService._findMatchingFileList.return_value = []

    with pytest.raises(RuntimeError) as e:
        localDataService.readSampleFilePaths()
    assert "No samples found" in str(e.value)


# interlude -- missplaced tests of grouping map #


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


# end interlude #


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
    sample.model_dump_json.return_value = "I like to eat, eat, eat"
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
    assert type(result) is CalibrantSamples
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
