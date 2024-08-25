import functools
import importlib
import json
import logging
import os
import re
import socket
import tempfile
import time
import unittest.mock as mock
from pathlib import Path
from random import randint, shuffle
from typing import List

import h5py
import pydantic
import pytest
from mantid.api import ITableWorkspace, MatrixWorkspace
from mantid.dataobjects import MaskWorkspace
from mantid.kernel import amend_config
from mantid.simpleapi import (
    CloneWorkspace,
    CompareWorkspaces,
    CreateGroupingWorkspace,
    CreateSampleWorkspace,
    CreateSingleValuedWorkspace,
    DeleteWorkspace,
    DeleteWorkspaces,
    GroupWorkspaces,
    LoadEmptyInstrument,
    LoadInstrument,
    RenameWorkspaces,
    mtd,
)
from snapred.backend.dao import StateConfig
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import (
    ValueFormatter as wnvf,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceName,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceNameGenerator as wng,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceType as wngt,
)
from snapred.meta.redantic import write_model_pretty
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask
from util.state_helpers import reduction_root_redirect, state_root_redirect

LocalDataServiceModule = importlib.import_module(LocalDataService.__module__)
ThisService = "snapred.backend.data.LocalDataService."

VERSION_START = Config["instrument.startingVersionNumber"]
IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

# NOTE: Devs, never changing the comparison value:
UNCHANGING_STATE_ID = "9618b936a4419a6e"
ENDURING_STATE_ID = "ab8704b0bc2a2342"


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


def test_readStateConfig():
    localDataService = LocalDataService()

    localDataService.getIPTS = mock.Mock(return_value="IPTS-123")
    localDataService._readPVFile = mock.Mock()
    fileMock = mock.Mock()
    localDataService._readPVFile.return_value = fileMock
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = (ENDURING_STATE_ID, None)
    localDataService.readCalibrationState = mock.Mock()
    localDataService.readCalibrationState.return_value = Calibration.model_validate_json(
        Resource.read("inputs/calibration/CalibrationParameters.json")
    )

    localDataService._groupingMapPath = mock.Mock()
    localDataService._groupingMapPath.return_value = Path(Resource.getPath("inputs/pixel_grouping/groupingMap.json"))
    stateGroupingMap = GroupingMap.model_validate_json(Resource.read("inputs/pixel_grouping/groupingMap.json"))
    localDataService._readGroupingMap = mock.Mock()
    localDataService._readGroupingMap.return_value = stateGroupingMap

    localDataService.instrumentConfig = getMockInstrumentConfig()

    actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    assert actual.stateId == ENDURING_STATE_ID


def test_readStateConfig_attaches_grouping_map():
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value="IPTS-123")
    localDataService._readPVFile = mock.Mock()
    fileMock = mock.Mock()
    localDataService._readPVFile.return_value = fileMock
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = (ENDURING_STATE_ID, None)
    localDataService.readCalibrationState = mock.Mock()
    localDataService.readCalibrationState.return_value = Calibration.model_validate_json(
        Resource.read("inputs/calibration/CalibrationParameters.json")
    )

    localDataService._groupingMapPath = mock.Mock()
    localDataService._groupingMapPath.return_value = Path(Resource.getPath("inputs/pixel_grouping/groupingMap.json"))

    stateGroupingMap = GroupingMap.model_validate_json(Resource.read("inputs/pixel_grouping/groupingMap.json"))
    localDataService._readGroupingMap = mock.Mock()
    localDataService._readGroupingMap.return_value = stateGroupingMap

    localDataService.instrumentConfig = getMockInstrumentConfig()

    actual = localDataService.readStateConfig("57514", True)
    assert actual.groupingMap == stateGroupingMap


def test_readStateConfig_invalid_grouping_map():
    # Test that the attached grouping-schema map's 'stateId' is checked.
    with pytest.raises(  # noqa: PT012
        RuntimeError,
        match="the state configuration's grouping map must have the same 'stateId' as the configuration",
    ):
        localDataService = LocalDataService()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-123")
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        localDataService.generateStateId = mock.Mock()
        localDataService.generateStateId.return_value = (ENDURING_STATE_ID, None)
        localDataService.readCalibrationState = mock.Mock()
        localDataService.readCalibrationState.return_value = Calibration.model_validate_json(
            Resource.read("inputs/calibration/CalibrationParameters.json")
        )

        localDataService._groupingMapPath = mock.Mock()
        # 'GroupingMap.defaultStateId' is _not_ a valid grouping-map 'stateId' for an existing `StateConfig`.
        localDataService._groupingMapPath.return_value = Path(
            Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
        )
        stateGroupingMap = GroupingMap.model_validate_json(
            Resource.read("inputs/pixel_grouping/defaultGroupingMap.json")
        )
        localDataService._readGroupingMap = mock.Mock()
        localDataService._readGroupingMap.return_value = stateGroupingMap

        localDataService.instrumentConfig = getMockInstrumentConfig()

        localDataService.readStateConfig("57514", True)


@mock.patch.object(LocalDataService, "_prepareStateRoot")
def test_readStateConfig_calls_prepareStateRoot(mockPrepareStateRoot):
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value="IPTS-123")
    localDataService._readPVFile = mock.Mock()
    fileMock = mock.Mock()
    localDataService._readPVFile.return_value = fileMock
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = (ENDURING_STATE_ID, None)
    localDataService.readCalibrationState = mock.Mock()
    localDataService.readCalibrationState.return_value = Calibration.model_validate_json(
        Resource.read("inputs/calibration/CalibrationParameters.json")
    )

    localDataService._groupingMapPath = mock.Mock()
    localDataService._groupingMapPath.side_effect = [
        Path(Resource.getPath("inputs/pixel_grouping/does_not_exist.json")),
        Path(Resource.getPath("inputs/pixel_grouping/groupingMap.json")),
    ]
    stateGroupingMap = GroupingMap.model_validate_json(Resource.read("inputs/pixel_grouping/groupingMap.json"))
    localDataService._readGroupingMap = mock.Mock()
    localDataService._readGroupingMap.return_value = stateGroupingMap

    localDataService.instrumentConfig = getMockInstrumentConfig()

    actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    mockPrepareStateRoot.assert_called_once()


def test_getUniqueTimestamp():
    localDataService = LocalDataService()
    numberToGenerate = 10
    tss = set([localDataService.getUniqueTimestamp() for n in range(numberToGenerate)])

    # generated values should not be 'None' or 'int', they must be 'float'
    for ts in tss:
        assert isinstance(ts, float)
    # generated values shall be distinct
    assert len(tss) == numberToGenerate

    # generated values should have distinct `struct_time`:
    #   this check ensures that they differ by at least 1 second
    ts_structs = set([time.gmtime(ts) for ts in tss])
    assert len(ts_structs) == numberToGenerate


def test_prepareStateRoot_creates_state_root_directory():
    # Test that the <state root> directory is created when it doesn't exist.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = GroupingMap.model_validate_json(
            Resource.read("inputs/pixel_grouping/defaultGroupingMap.json")
        )
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        assert not localDataService.constructCalibrationStateRoot().exists()
        localDataService._prepareStateRoot(stateId)
        assert localDataService.constructCalibrationStateRoot().exists()


def test_prepareStateRoot_existing_state_root():
    # Test that an already existing <state root> directory is not an error.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID

    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.constructCalibrationStateRoot().mkdir()
        defaultGroupingMap = GroupingMap.model_validate_json(
            Resource.read("inputs/pixel_grouping/defaultGroupingMap.json")
        )
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        assert localDataService.constructCalibrationStateRoot().exists()
        localDataService._prepareStateRoot(stateId)


def test_prepareStateRoot_writes_grouping_map():
    # Test that the first time a <state root> directory is initialized,
    #   the `StateConfig.groupingMap` is written to the directory.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = GroupingMap.model_validate_json(
            Resource.read("inputs/pixel_grouping/defaultGroupingMap.json")
        )
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)

        assert not localDataService._groupingMapPath(stateId).exists()
        localDataService._prepareStateRoot(stateId)
        assert localDataService._groupingMapPath(stateId).exists()


def test_prepareStateRoot_sets_grouping_map_stateid():
    # Test that the first time a <state root> directory is initialized,
    #   the 'stateId' of the `StateConfig.groupingMap` is set to match that of the state.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = GroupingMap.model_validate_json(
            Resource.read("inputs/pixel_grouping/defaultGroupingMap.json")
        )
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)

        localDataService._prepareStateRoot(stateId)

        with open(localDataService._groupingMapPath(stateId), "r") as f:
            groupingMap = GroupingMap.model_validate_json(f.read())
    assert groupingMap.stateId == stateId


def test_prepareStateRoot_no_default_grouping_map():
    # Test that the first time a <state root> directory is initialized,
    #   the 'defaultGroupingMap.json' at Config['instrument.calibration.powder.grouping.home']
    #   is required to exist.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
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
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.constructCalibrationStateRoot().mkdir()
        defaultGroupingMapFilePath = Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")

        # Write a 'groupingMap.json' file to the <state root>, but with a different stateId;
        #   note that the _value_ of the stateId field is _not_ validated at this stage, except for its format.
        with open(defaultGroupingMapFilePath, "r") as f:
            groupingMap = GroupingMap.model_validate_json(f.read())
        otherStateId = "bbbbaaaabbbbeeee"
        groupingMap.coerceStateId(otherStateId)
        write_model_pretty(groupingMap, localDataService._groupingMapPath(stateId))

        with open(defaultGroupingMapFilePath, "r") as f:
            defaultGroupingMap = GroupingMap.model_validate_json(f.read())
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        localDataService._prepareStateRoot(stateId)

        with open(localDataService._groupingMapPath(stateId), "r") as f:
            groupingMap = GroupingMap.model_validate_json(f.read())
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
            stateId = ENDURING_STATE_ID
            stateRootPath = Path(tmpDir) / stateId
            os.makedirs(stateRootPath)

            defaultGroupingMapFilePath = Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
            groupingMapFilePath = stateRootPath / "groupingMap.json"
            localDataService._groupingMapPath = mock.Mock(return_value=groupingMapFilePath)

            # Write a 'groupingMap.json' file to the <state root>, with the _correct_ stateId;
            #   note that the _value_ of the stateId field is _not_ validated at this stage, except for its format.
            with open(defaultGroupingMapFilePath, "r") as f:
                groupingMap = GroupingMap.model_validate_json(f.read())
            groupingMap.coerceStateId(ObjectSHA(hex=stateId, decodedKey=None))
            localDataService._writeGroupingMap(stateId, groupingMap)

            # read it back
            with open(groupingMapFilePath, "r") as f:
                groupingMap = GroupingMap.model_validate_json(f.read())
            with open(defaultGroupingMapFilePath, "r") as f:
                defaultGroupingMap = GroupingMap.model_validate_json(f.read())

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
    stateId = ENDURING_STATE_ID
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
    stateId = ENDURING_STATE_ID
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
    mockGetIPTS.assert_called_with(
        RunNumber=runNumber,
        Instrument=Config["instrument.name"],
    )
    res = localDataService.getIPTS(runNumber, "CRACKLE")
    assert res == mockGetIPTS.return_value
    mockGetIPTS.assert_called_with(
        RunNumber=runNumber,
        Instrument="CRACKLE",
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
    localDataService.getIPTS = mock.Mock(return_value="IPTS-123")
    localDataService._readPVFile = mock.Mock()
    fileMock = mock.Mock()
    localDataService._readPVFile.return_value = fileMock
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = (UNCHANGING_STATE_ID, None)
    localDataService.readCalibrationState = mock.Mock()
    localDataService.readCalibrationState.return_value = Calibration.model_validate_json(
        Resource.read("inputs/calibration/CalibrationParameters.json")
    )

    localDataService._groupingMapPath = mock.Mock()
    localDataService._groupingMapPath.return_value = Path(Resource.getPath("inputs/pixel_grouping/groupingMap.json"))
    stateGroupingMap = GroupingMap.model_validate_json(Resource.read("inputs/pixel_grouping/groupingMap.json"))
    localDataService._readGroupingMap = mock.Mock()
    localDataService._readGroupingMap.return_value = stateGroupingMap

    localDataService.instrumentConfig = getMockInstrumentConfig()

    actual = localDataService.readStateConfig("57514", True)
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        stateConfigPath = Path(tempdir) / "stateConfig.json"
        write_model_pretty(actual, stateConfigPath)
        # read it back in:
        stateConfig = None
        with open(stateConfigPath, "r") as f:
            stateConfig = StateConfig.model_validate_json(f.read())
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


def test_generateStateId():
    localDataService = LocalDataService()

    # Mock the _readPVFile method
    localDataService._readPVFile = mock.Mock()

    # Create a mock pvFile object
    pvFile = {
        "entry/DASlogs/BL3:Chop:Gbl:WavelengthReq/value": [0.1],
        "entry/DASlogs/det_arc1/value": [0.1],
        "entry/DASlogs/det_arc2/value": [0.1],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [0.1],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [1],
        "entry/DASlogs/det_lin1/value": [0.1],
        "entry/DASlogs/det_lin2/value": [0.1],
    }

    # Configure the mock to return the mock pvFile dictionary
    localDataService._readPVFile.return_value = pvFile

    # Call the method being tested
    actual, _ = localDataService.generateStateId("12345")

    # Check that the returned value matches the expected result
    assert actual == UNCHANGING_STATE_ID


def test_generateStateId_old_wav_key():
    # Ensures the old value of the wav key is still accepted
    localDataService = LocalDataService()

    # Mock the _readPVFile method
    localDataService._readPVFile = mock.Mock()

    # Create a mock pvFile object
    pvFile = {
        "entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value": [0.1],
        "entry/DASlogs/det_arc1/value": [0.1],
        "entry/DASlogs/det_arc2/value": [0.1],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [0.1],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [1],
        "entry/DASlogs/det_lin1/value": [0.1],
        "entry/DASlogs/det_lin2/value": [0.1],
    }

    # Configure the mock to return the mock pvFile dictionary
    localDataService._readPVFile.return_value = pvFile

    # Call the method being tested
    actual, _ = localDataService.generateStateId("12345")

    # Check that the returned value matches the expected result
    assert actual == UNCHANGING_STATE_ID


def test_generateStateId_cache():
    localDataService = LocalDataService()
    localDataService.generateStateId.cache_clear()
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=0, misses=0, maxsize=128, currsize=0
    )

    localDataService._readPVFile = mock.Mock()

    # Mock the readDetectorState method to return different DetectorState objects
    def mock_readDetectorState(runId):
        if runId == "12345":
            return DetectorState(arc=(0.1, 0.1), wav=0.1, freq=0.1, guideStat=1, lin=(0.1, 0.1))
        elif runId == "67890":
            return DetectorState(arc=(0.2, 0.2), wav=0.2, freq=0.2, guideStat=1, lin=(0.2, 0.2))
        return None

    localDataService.readDetectorState = mock.Mock(side_effect=mock_readDetectorState)

    stateSHA1 = UNCHANGING_STATE_ID
    stateSHA2 = "fa0bb25b44874edb"

    # Call the method being tested and check the cache behavior
    actual, _ = localDataService.generateStateId("12345")
    assert actual == stateSHA1
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=0, misses=1, maxsize=128, currsize=1
    )

    # Check cached value
    actual, _ = localDataService.generateStateId("12345")
    assert actual == stateSHA1
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=1, misses=1, maxsize=128, currsize=1
    )

    # check a different value
    actual, _ = localDataService.generateStateId("67890")
    assert actual == stateSHA2
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=1, misses=2, maxsize=128, currsize=2
    )

    # ... and its cached value
    actual, _ = localDataService.generateStateId("67890")
    assert actual == stateSHA2
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=2, misses=2, maxsize=128, currsize=2
    )


def test__findMatchingFileList():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = getMockInstrumentConfig()
    actual = localDataService._findMatchingFileList(Resource.getPath("inputs/SNAPInstPrm.json"), False)
    assert actual is not None
    assert len(actual) == 1


### TESTS OF PATH METHODS ###


@mock.patch("stat.S_IMODE", return_value=0o777)
@mock.patch("os.stat")
@mock.patch("pathlib.Path.exists", return_value=True)
def test__hasWritePermissionsToPath_fileExistsWithPermission(mockExists, mockStat, mockS_IMODE):  # noqa: ARG001
    mockStat.return_value = mock.Mock(
        st_uid=os.getuid(),
        st_gid=os.getgroups()[0],
        st_mode=0o777
    )
    filePath = Path("/some/path/to/file")
    localDS = LocalDataService()
    result = localDS._hasWritePermissionstoPath(filePath)
    assert result is True


@mock.patch("pathlib.Path.exists", return_value=False)
def test__hasWritePermissionsToPath_fileDoesNotExist(mockExists):  # noqa: ARG001
    filePath = Path("/some/path/to/nonexistent/file")
    localDS = LocalDataService()
    result = localDS._hasWritePermissionstoPath(filePath)
    assert result is False


def test_checkWritePermissions_path_exists():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        path = Path(tmpDir) / "one" / "two" / "three"
        path.mkdir(parents=True)
        assert path.exists()
        status = LocalDataService().checkWritePermissions(path)
        assert status


@mock.patch("stat.S_IMODE", return_value=0o000)
def test_checkWritePermissions_path_exists_no_permissions(mockOsAccess):
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        path = Path(tmpDir) / "one" / "two" / "three"
        path.mkdir(parents=True)
        assert path.exists()
        status = LocalDataService().checkWritePermissions(path)
        assert not status
        mockOsAccess.assert_called_once()


def test_checkWritePermissions_parent_exists():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        path = Path(tmpDir) / "one" / "two" / "three"
        path.mkdir(parents=True)
        assert path.exists()
        path = path / "four"
        assert not path.exists()
        status = LocalDataService().checkWritePermissions(path)
        assert status

@mock.patch("stat.S_IMODE", return_value=0o000)
def test_checkWritePermissions_parent_exists_no_permissions(mockOsAccess):
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        path = Path(tmpDir) / "one" / "two" / "three"
        path.mkdir(parents=True)
        assert path.exists()
        path = path / "four"
        assert not path.exists()
        status = LocalDataService().checkWritePermissions(path)
        assert not status
        mockOsAccess.assert_called_once()


def test_checkWritePermissions_path_does_not_exist():
    path = Path("/does_not_exist") / "one" / "two" / "three"
    assert not path.exists()
    status = LocalDataService().checkWritePermissions(path)
    assert not status


def test_constructCalibrationStateRoot():
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    ans = localDataService.constructCalibrationStateRoot(fakeState)
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
        assert ans.parts[:-2] == localDataService.constructCalibrationStateRoot(fakeState).parts


def test_constructNormalizationStatePath():
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        ans = localDataService._constructNormalizationStatePath(fakeState, useLiteMode)
        assert isinstance(ans, Path)
        assert ans.parts[-1] == "normalization"
        assert ans.parts[-2] == "lite" if useLiteMode else "native"
        assert ans.parts[:-2] == localDataService.constructCalibrationStateRoot(fakeState).parts


def test_constructReductionStateRoot():
    fakeIPTS = "gumdrop"
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value=fakeIPTS)
    localDataService.generateStateId = mock.Mock(return_value=(fakeState, "gibberish"))
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
    localDataService.generateStateId = mock.Mock(return_value=(fakeState, "gibberish"))
    runNumber = "xyz"
    for useLiteMode in [True, False]:
        ans = localDataService._constructReductionDataRoot(runNumber, useLiteMode)
        assert isinstance(ans, Path)
        assert ans.parts[-1] == runNumber
        assert ans.parts[-2] == "lite" if useLiteMode else "native"
        assert ans.parts[:-2] == localDataService._constructReductionStateRoot(runNumber).parts


def test_readCalibrationIndexMissing():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService.generateStateId = mock.Mock(return_value=("123", "456"))
    localDataService._readReductionParameters = mock.Mock()
    localDataService.constructCalibrationStateRoot = mock.Mock(return_value=Path(Resource.getPath("outputs")))
    assert len(localDataService.readCalibrationIndex("123", True)) == 0


def test_readNormalizationIndexMissing():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService.generateStateId = mock.Mock(return_value=("123", "456"))
    localDataService._readReductionParameters = mock.Mock()
    localDataService.constructCalibrationStateRoot = mock.Mock(return_value=Path(Resource.getPath("outputs")))
    assert len(localDataService.readNormalizationIndex("123", True)) == 0


def test_writeCalibrationIndexEntry():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService.generateStateId = mock.Mock(return_value=("123", "456"))
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructCalibrationStatePath = mock.Mock(return_value=Path(Resource.getPath("outputs")))
    expectedFilePath = Path(Resource.getPath("outputs")) / "CalibrationIndex.json"
    localDataService.writeCalibrationIndexEntry(
        CalibrationIndexEntry(runNumber="57514", useLiteMode=True, comments="test comment", author="test author"),
    )
    assert expectedFilePath.exists()

    fileContent = ""
    with open(expectedFilePath, "r") as indexFile:
        fileContent = indexFile.read()
    os.remove(expectedFilePath)
    assert len(fileContent) > 0

    actualEntries = pydantic.TypeAdapter(List[CalibrationIndexEntry]).validate_json(fileContent)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"

    # def test_writeNormalizationIndexEntry():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = ("123", "456")
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructNormalizationStatePath = mock.Mock()
    localDataService._constructNormalizationStatePath.return_value = Path(Resource.getPath("outputs"))
    expectedFilePath = Path(Resource.getPath("outputs")) / "NormalizationIndex.json"
    localDataService.writeNormalizationIndexEntry(
        NormalizationIndexEntry(
            runNumber="57514",
            useLiteMode=True,
            backgroundRunNumber="58813",
            comments="test comment",
            author="test author",
        ),
    )
    assert expectedFilePath.exists()

    fileContent = ""
    with open(expectedFilePath, "r") as indexFile:
        fileContent = indexFile.read()
    os.remove(expectedFilePath)
    assert len(fileContent) > 0

    actualEntries = pydantic.TypeAdapter(List[NormalizationIndexEntry]).validate_json(fileContent)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


def test_readCalibrationIndexExisting():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = ("123", "456")
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructCalibrationStatePath = mock.Mock()
    localDataService._constructCalibrationStatePath.return_value = Path(Resource.getPath("outputs"))
    expectedFilePath = Path(Resource.getPath("outputs")) / "CalibrationIndex.json"
    localDataService.writeCalibrationIndexEntry(
        CalibrationIndexEntry(runNumber="57514", useLiteMode=True, comments="test comment", author="test author"),
    )
    actualEntries = localDataService.readCalibrationIndex("57514", True)
    os.remove(expectedFilePath)

    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


def test_readNormalizationIndexExisting():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = ("123", "456")
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructNormalizationStatePath = mock.Mock()
    localDataService._constructNormalizationStatePath.return_value = Path(Resource.getPath("outputs"))
    expectedFilePath = Path(Resource.getPath("outputs")) / "NormalizationIndex.json"
    localDataService.writeNormalizationIndexEntry(
        NormalizationIndexEntry(
            runNumber="57514",
            useLiteMode=True,
            backgroundRunNumber="58813",
            comments="test comment",
            author="test author",
        )
    )
    actualEntries = localDataService.readNormalizationIndex("57514", True)
    os.remove(expectedFilePath)

    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


def readReductionIngredientsFromFile():
    with Resource.open("/inputs/calibration/ReductionIngredients.json", "r") as f:
        return ReductionIngredients.model_validate_json(f.read())


def test_readWriteCalibrationRecord_version_numbers():
    localDataService = LocalDataService()
    stateId = UNCHANGING_STATE_ID
    testCalibrationRecord_v0001 = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testCalibrationRecord_v0002 = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0002.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        # WARNING: 'writeCalibrationRecord' modifies <incoming record>.version,
        #   and <incoming record>.calibrationFittingIngredients.version.

        # write: version == 1
        localDataService.writeCalibrationRecord(testCalibrationRecord_v0001)
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True)
        assert actualRecord.version == 1
        assert actualRecord.calibrationFittingIngredients.version == 1

        # write: version == 2
        localDataService.writeCalibrationRecord(testCalibrationRecord_v0002)
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True)
        assert actualRecord.version == 2
        assert actualRecord.calibrationFittingIngredients.version == 2
    assert actualRecord.runNumber == "57514"
    assert actualRecord == testCalibrationRecord_v0002


##
def test_readWriteCalibrationRecord_specified_version():
    localDataService = LocalDataService()
    stateId = UNCHANGING_STATE_ID
    testCalibrationRecord_v0001 = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testCalibrationRecord_v0002 = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0002.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        # WARNING: 'writeCalibrationRecord' modifies <incoming record>.version,
        #   and <incoming record>.calibrationFittingIngredients.version.

        # Important: start with version > 1: should not depend on any existing directory structure!

        # write: version == 3
        localDataService.writeCalibrationRecord(testCalibrationRecord_v0001, version=3)
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True, version=3)
        assert actualRecord.version == 3
        assert actualRecord.calibrationFittingIngredients.version == 3

        # write: version == 4
        localDataService.writeCalibrationRecord(testCalibrationRecord_v0002, version=4)
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True, version=3)
        assert actualRecord.version == 3
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True, version=4)
        assert actualRecord.version == 4


def test_readWriteCalibrationRecord_with_version():
    localDataService = LocalDataService()
    stateId = UNCHANGING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.writeCalibrationRecord(
            CalibrationRecord.model_validate_json(Resource.read("inputs/calibration/CalibrationRecord_v0001.json"))
        )
        actualRecord = localDataService.readCalibrationRecord("57514", True, "1")
    assert actualRecord.runNumber == "57514"
    assert actualRecord.version == 1


def test_readWriteCalibrationRecord():
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    testCalibrationRecord = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.writeCalibrationRecord(testCalibrationRecord)
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True)
    assert actualRecord.runNumber == "57514"
    assert actualRecord == testCalibrationRecord


def test_writeCalibrationWorkspaces():
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    testCalibrationRecord = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        basePath = localDataService._constructCalibrationDataPath(testCalibrationRecord.runNumber, True, 1)

        # Workspace names need to match the names that are used in the test record.
        workspaces = testCalibrationRecord.workspaces.copy()
        runNumber = testCalibrationRecord.runNumber
        version = testCalibrationRecord.version
        outputDSPWSName = workspaces.pop(wngt.DIFFCAL_OUTPUT)[0]
        diagnosticWSName = workspaces.pop(wngt.DIFFCAL_DIAG)[0]
        tableWSName = workspaces.pop(wngt.DIFFCAL_TABLE)[0]
        maskWSName = workspaces.pop(wngt.DIFFCAL_MASK)[0]
        if workspaces:
            raise RuntimeError(f"unexpected workspace-types in record.workspaces: {workspaces}")

        # Create sample workspaces.
        CreateSampleWorkspace(
            OutputWorkspace=outputDSPWSName,
            Function="One Peak",
            NumBanks=1,
            NumMonitors=1,
            BankPixelWidth=5,
            NumEvents=500,
            Random=True,
            XUnit="DSP",
            XMin=0.5,
            XMax=8000,
            BinWidth=100,
        )
        LoadInstrument(
            Workspace=outputDSPWSName,
            Filename=fakeInstrumentFilePath,
            RewriteSpectraMap=True,
        )
        assert mtd.doesExist(outputDSPWSName)

        # Create a grouping workspace to save as the diagnostic workspace.
        ws1 = CreateSingleValuedWorkspace()
        GroupWorkspaces(
            InputWorkspaces=[ws1],
            OutputWorkspace=diagnosticWSName,
        )
        assert mtd.doesExist(diagnosticWSName)

        # Create diffraction-calibration table and mask workspaces.
        createCompatibleDiffCalTable(tableWSName, outputDSPWSName)
        createCompatibleMask(maskWSName, outputDSPWSName, fakeInstrumentFilePath)
        assert mtd.doesExist(tableWSName)
        assert mtd.doesExist(maskWSName)

        localDataService.writeCalibrationWorkspaces(testCalibrationRecord)

        dspFilename = Path(outputDSPWSName + Config["calibration.diffraction.output.extension"])
        diagFilename = Path(diagnosticWSName + Config["calibration.diffraction.diagnostic.extension"])
        diffCalFilename = Path(wng.diffCalTable().runNumber(runNumber).version(version).build() + ".h5")
        assert (basePath / dspFilename).exists()
        assert (basePath / diagFilename).exists()
        assert (basePath / diffCalFilename).exists()
        mtd.clear()


def test_writeCalibrationWorkspaces_no_units():
    # test that diffraction-calibration output workspace names require units
    localDataService = LocalDataService()
    localDataService.writeWorkspace = mock.Mock()
    localDataService._constructCalibrationDataPath = mock.Mock(return_value="not/a/path")
    testCalibrationRecord = CalibrationRecord.model_validate_json(
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
    stateId = ENDURING_STATE_ID
    testNormalizationRecord = NormalizationRecord.model_validate_json(
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
    stateId = ENDURING_STATE_ID
    runNumber = "57514"
    useLiteMode = False

    testNormalizationRecord = NormalizationRecord.model_validate_json(
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
    stateId = ENDURING_STATE_ID
    useLiteMode = True
    testNormalizationRecord = NormalizationRecord.model_validate_json(
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
    stateId = ENDURING_STATE_ID
    localDataService = LocalDataService()
    testNormalizationRecord = NormalizationRecord.model_validate_json(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    with state_root_redirect(localDataService, stateId=stateId):
        # Workspace names need to match the names that are used in the test record.
        runNumber = testNormalizationRecord.runNumber  # noqa: F841
        useLiteMode = testNormalizationRecord.useLiteMode
        version = testNormalizationRecord.version  # noqa: F841
        testWS0, testWS1, testWS2 = testNormalizationRecord.workspaceNames

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


### TESTS OF REDUCTION METHODS ###


def _writeSyntheticReductionRecord(filePath: Path, timestamp: float):
    # Create a `ReductionRecord` JSON file to be used by the unit tests.

    # TODO: Implement methods to create the synthetic `CalibrationRecord` and `NormalizationRecord`.
    testCalibration = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testNormalization = NormalizationRecord.model_validate_json(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    testRecord = ReductionRecord(
        runNumber=testCalibration.runNumber,
        useLiteMode=testCalibration.useLiteMode,
        timestamp=timestamp,
        calibration=testCalibration,
        normalization=testNormalization,
        pixelGroupingParameters={
            pg.focusGroup.name: list(pg.pixelGroupingParameters.values()) for pg in testCalibration.pixelGroups
        },
        workspaceNames=[
            wng.reductionOutput()
            .runNumber(testCalibration.runNumber)
            .group(pg.focusGroup.name)
            .timestamp(timestamp)
            .build()
            for pg in testCalibration.pixelGroups
        ],
    )
    write_model_pretty(testRecord, filePath)


def test_readWriteReductionRecord_timestamps(readSyntheticReductionRecord):
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord(inputRecordFilePath, "1")

    oldTimestamp = 1718384660.538665
    testReductionRecord_v0001 = readSyntheticReductionRecord(inputRecordFilePath, oldTimestamp)

    # Get a second copy, with a different timestamp
    newTimestamp = time.time()
    testReductionRecord_v0002 = readSyntheticReductionRecord(inputRecordFilePath, newTimestamp)
    assert oldTimestamp != newTimestamp

    runNumber, useLiteMode = testReductionRecord_v0001.runNumber, testReductionRecord_v0001.useLiteMode
    stateId = "ab8704b0bc2a2342"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        # write: old timestamp
        localDataService.writeReductionRecord(testReductionRecord_v0001)
        # write call should not modify timestamp
        assert testReductionRecord_v0001.timestamp == oldTimestamp
        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, oldTimestamp)
        assert actualRecord.timestamp == oldTimestamp

        # write: new timestamp
        actualRecord = localDataService.writeReductionRecord(testReductionRecord_v0002)
        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, newTimestamp)
        assert actualRecord.timestamp == newTimestamp


def test_readWriteReductionRecord(readSyntheticReductionRecord):
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)

    oldTimestamp = 1718384660.538665
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, oldTimestamp)

    runNumber = testRecord.runNumber
    stateId = "ab8704b0bc2a2342"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.groceryService = mock.Mock()
        localDataService.writeReductionRecord(testRecord)
        actualRecord = localDataService.readReductionRecord(runNumber, testRecord.useLiteMode, testRecord.timestamp)
    assert actualRecord == testRecord


@pytest.fixture()
def readSyntheticReductionRecord():
    # Read a `ReductionRecord` from the specified file path:
    #   * update the record's timestamp and workspace-name list using the specified value.

    def _readSyntheticReductionRecord(filePath: Path, timestamp: float) -> ReductionRecord:
        dict_ = {}
        with open(filePath, "r") as f:
            dict_ = json.loads(f.read())

        # Work around for "staging" (from next), replace `NormalizationRecord` and `CalibrationRecord` components:

        dict_["calibration"] = json.loads(Resource.read("inputs/calibration/CalibrationRecord_v0001.json"))
        dict_["normalization"] = json.loads(Resource.read("inputs/normalization/NormalizationRecord.json"))
        record = ReductionRecord.model_validate(dict_)

        # Update the record's list of workspace names:
        #   * reconstruct complete `WorkspaceName` with builder.
        #   * ensure that the names are unique to this test by using the new timestap => enables parallel testing;

        wngReducedOutput = re.compile(
            r"_reduced_([A-Za-z]+)_([A-Za-z]+)_([0-9]{6,})_([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})"
        )
        wngReductionPixelMask = re.compile(
            r"_pixelmask_([0-9]{6,})_([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})"
        )

        wss: List[WorkspaceName] = []
        for ws in record.workspaceNames:
            wngOutput = wngReducedOutput.match(ws)
            if wngOutput:
                unit, grouping, runNumber = wngOutput.group(1), wngOutput.group(2), wngOutput.group(3)
                ws_ = wng.reductionOutput().unit(unit).group(grouping).runNumber(runNumber).timestamp(timestamp).build()
                wss.append(ws_)
                continue
            wngPixelMask = wngReductionPixelMask.match(ws)
            if wngPixelMask:
                runNumber = wngPixelMask.group(1)
                ws_ = wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()
                wss.append(ws_)
                continue
            raise RuntimeError(f"unable to reconstruct 'WorkspaceName' from '{ws}'")

        # Reconstruct the record, in order to modify the frozen fields
        dict_ = record.model_dump()
        dict_["timestamp"] = timestamp
        #    WARNING: we cannot just use `model_validate` here,
        #      it will recreate the `WorkspaceName(<original name>)` and
        #        the `_builder` args will be stripped.
        #   (TODO: is this still correct?  I think it works now.)
        record = ReductionRecord.model_validate(dict_)
        record.workspaceNames = wss

        return record

    yield _readSyntheticReductionRecord

    # teardown...
    pass


def _cleanup_workspace_at_exit():
    # Allow cleanup of workspaces in the ADS
    #   in a manner compatible with _parallel_ testing.
    _workspaces = []

    def _cleanup_workspace_at_exit(wsName: str):
        _workspaces.append(wsName)

    yield _cleanup_workspace_at_exit

    # teardown
    try:
        if _workspaces:
            # Warning: `DeleteWorkspaces`' input validator throws an exception
            #   if a specified workspace doesn't exist in the ADS;
            #   but its implementation body does not. (A really stupid design. :( )
            DeleteWorkspaces(WorkspaceList=_workspaces)
    except RuntimeError:
        pass


@pytest.fixture(scope="function")  # noqa: PT003
def cleanup_workspace_at_exit():
    for f in _cleanup_workspace_at_exit():
        yield f


@pytest.fixture()
def createReductionWorkspaces(cleanup_workspace_at_exit):
    # Create sample workspaces from a list of names:
    #   * these workspaces are automatically deleted at teardown.

    def _createWorkspaces(wss: List[WorkspaceName]):
        # Create several sample reduction event workspaces with DSP units
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
            wsType = ws.tokens("workspaceType")
            match wsType:
                case wngt.REDUCTION_PIXEL_MASK:
                    createCompatibleMask(ws, src, fakeInstrumentFilePath)
                case _:
                    CloneWorkspace(OutputWorkspace=ws, InputWorkspace=src)
            assert mtd.doesExist(ws)
            cleanup_workspace_at_exit(ws)

        DeleteWorkspace(Workspace=src)
        return wss

    yield _createWorkspaces

    # teardown
    pass


def test_writeReductionData(readSyntheticReductionRecord, createReductionWorkspaces):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718908813.0250459
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    # Temporarily use a single run number
    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = "ab8704b0bc2a2342"
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        assert not reductionFilePath.exists()

        # `writeReductionRecord` must be called first
        localDataService.writeReductionRecord(testRecord)
        localDataService.writeReductionData(testRecord)

        assert reductionFilePath.exists()


def test_writeReductionData_no_directories(readSyntheticReductionRecord, createReductionWorkspaces):  # noqa: ARG001
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718908816.106522
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = "ab8704b0bc2a2342"
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]

    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        assert not reductionRecordFilePath.exists()

        # `writeReductionRecord` must be called first
        # * deliberately _not_ done in this test => <reduction-data root> directory won't exist
        with pytest.raises(RuntimeError) as einfo:
            localDataService.writeReductionData(testRecord)
        msg = str(einfo.value)
    assert "reduction version directories" in msg
    assert "do not exist" in msg


def test_writeReductionData_metadata(readSyntheticReductionRecord, createReductionWorkspaces):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718909723.027197
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = "ab8704b0bc2a2342"
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
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


def test_readWriteReductionData(readSyntheticReductionRecord, createReductionWorkspaces, cleanup_workspace_at_exit):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    _uniquePrefix = "_test_RWRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718909801.91552
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = "ab8704b0bc2a2342"
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
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
        for ws in wss:
            cleanup_workspace_at_exit(_uniquePrefix + ws)

        actualRecord = localDataService.readReductionData(runNumber, useLiteMode, timestamp)
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


def test_readWriteReductionData_pixel_mask(
    readSyntheticReductionRecord, createReductionWorkspaces, cleanup_workspace_at_exit
):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    _uniquePrefix = "_test_RWRD_PM_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718909911.6432922
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = "ab8704b0bc2a2342"
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]
    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService.instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        assert not reductionRecordFilePath.exists()

        # `writeReductionRecord` needs to be called first
        localDataService.writeReductionRecord(testRecord)
        localDataService.writeReductionData(testRecord)

        filePath = reductionRecordFilePath.parent / fileName
        assert filePath.exists()

        # 1) Verify that a pixel mask was separately written in `SaveDiffCal` format
        maskName = wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()
        assert (reductionRecordFilePath.parent / (maskName + ".h5")).exists()

        # move the existing test workspaces out of the way:
        #   * this just adds the `_uniquePrefix`.
        RenameWorkspaces(InputWorkspaces=wss, Prefix=_uniquePrefix)
        # append to the cleanup list
        for ws in wss:
            cleanup_workspace_at_exit(_uniquePrefix + ws)

        actualRecord = localDataService.readReductionData(runNumber, useLiteMode, timestamp)
        # 2) Verify that a pixel mask was appended to the 'workspaceNames' list
        assert maskName in actualRecord.workspaceNames

        # 3) Verify that the pixel mask has been appended to the combined data file,
        #      and that it is reloaded as a `MaskWorkspace` instance
        pixelMaskKeyword = Config["mantid.workspace.nameTemplate.template.reduction.pixelMask"].split(",")[0]
        # verify that a pixel mask was appended to the
        maskIsAppendedToData = False
        for ws in actualRecord.workspaceNames:
            if pixelMaskKeyword in ws:
                if isinstance(mtd[ws], MaskWorkspace):
                    maskIsAppendedToData = True
        assert maskIsAppendedToData


def test__constructReductionDataFilePath(readSyntheticReductionRecord):
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))

    oldTimestamp = 1718384660.538665
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, oldTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = "ab8704b0bc2a2342"
    testIPTS = "IPTS-12345"
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]

    expectedFilePath = (
        Path(Config["instrument.reduction.home"].format(IPTS=testIPTS))
        / stateId
        / ("lite" if useLiteMode else "native")
        / runNumber
        / wnvf.pathTimestamp(timestamp)
        / fileName
    )

    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock(return_value=(stateId, None))
    localDataService.getIPTS = mock.Mock(return_value=testIPTS)
    actualFilePath = localDataService._constructReductionDataFilePath(runNumber, useLiteMode, timestamp)
    assert actualFilePath == expectedFilePath


def test_getReductionRecordFilePath():
    timestamp = time.time()
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = ("123", "456")
    localDataService._constructReductionDataRoot = mock.Mock()
    localDataService._constructReductionDataRoot.return_value = Path(Resource.getPath("outputs"))
    actualPath = localDataService._constructReductionRecordFilePath("57514", True, timestamp)
    expectedPath = Path(Resource.getPath("outputs")) / wnvf.pathTimestamp(timestamp) / "ReductionRecord.json"
    assert actualPath == expectedPath


def test_getCalibrationRecordFilePath():
    testVersion = randint(1, 20)
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = ("123", "456")
    localDataService._constructCalibrationStatePath = mock.Mock()
    localDataService._constructCalibrationStatePath.return_value = Path(Resource.getPath("outputs"))
    actualPath = localDataService.getCalibrationRecordFilePath("57514", True, testVersion)
    assert actualPath == Path(Resource.getPath("outputs")) / wnvf.pathVersion(testVersion) / "CalibrationRecord.json"


def test_getNormalizationRecordFilePath():
    testVersion = randint(1, 20)
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = ("123", "456")
    localDataService._constructNormalizationStatePath = mock.Mock()
    localDataService._constructNormalizationStatePath.return_value = Path(Resource.getPath("outputs"))
    actualPath = localDataService.getNormalizationRecordFilePath("57514", True, testVersion)
    assert actualPath == Path(Resource.getPath("outputs")) / wnvf.pathVersion(testVersion) / "NormalizationRecord.json"


def test_extractFileVersion():
    testVersion = randint(1, 20)
    testFile = f"Powder/1234/{wnvf.pathVersion(testVersion)}/CalibrationRecord.json"
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
    someFiles = [file_pattern(wnvf.pathVersion(i)) for i in range(expected + 1)]
    shuffle(someFiles)
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock(return_value=someFiles)
    actualFile = localDataService._getFileOfVersion(file_pattern("*"), expected)
    assert actualFile == file_pattern(wnvf.pathVersion(expected))


def test__getLatestFile():
    expected = randint(10, 20)
    file_pattern = lambda x: f"Powder/1234/{x}/CalibrationRecord.json"  # noqa E731
    someFiles = [file_pattern(wnvf.pathVersion(i)) for i in range(expected + 1)]
    shuffle(someFiles)
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock(return_value=someFiles)
    actualFile = localDataService._getLatestFile(file_pattern("*"))
    assert actualFile == file_pattern(wnvf.pathVersion(expected))


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
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = (UNCHANGING_STATE_ID, None)
    localDataService._constructCalibrationStatePath = mock.Mock()
    localDataService._constructCalibrationStatePath.return_value = Path(Resource.getPath("outputs/"))
    actualPath = localDataService._constructCalibrationParametersFilePath("57514", True, testVersion)
    assert (
        actualPath == Path(Resource.getPath("outputs")) / wnvf.pathVersion(testVersion) / "CalibrationParameters.json"
    )


def test_readCalibrationState():
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock(return_value=(ENDURING_STATE_ID, None))
    localDataService.calibrationExists = mock.Mock(return_value=True)
    localDataService._constructCalibrationParametersFilePath = mock.Mock()
    localDataService._constructCalibrationParametersFilePath.return_value = Resource.getPath(
        f"{ENDURING_STATE_ID}/v_0001/CalibrationParameters.json"
    )
    localDataService._getLatestFile = mock.Mock()
    localDataService._getLatestFile.return_value = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    testCalibrationState = Calibration.model_validate_json(
        Resource.read("inputs/calibration/CalibrationParameters.json")
    )
    actualState = localDataService.readCalibrationState("57514", True)

    assert actualState == testCalibrationState


def test_readCalibrationState_no_file():
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = (ENDURING_STATE_ID, None)
    localDataService._constructCalibrationParametersFilePath = mock.Mock()
    localDataService._constructCalibrationParametersFilePath.return_value = Resource.getPath(
        f"{ENDURING_STATE_ID}/v_0001/CalibrationParameters.json"
    )
    localDataService._getLatestFile = mock.Mock()
    localDataService._getLatestFile.return_value = None
    with pytest.raises(RecoverableException):
        localDataService.readCalibrationState("57514", False)


def test_readNormalizationState():
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = (ENDURING_STATE_ID, None)
    localDataService.getNormalizationStatePath = mock.Mock()
    localDataService.getNormalizationStatePath.return_value = Path(
        Resource.getPath(f"{ENDURING_STATE_ID}/v_0001/NormalizationParameters.json")
    )
    localDataService._getLatestFile = mock.Mock()
    localDataService._getLatestFile.return_value = Resource.getPath("inputs/normalization/NormalizationParameters.json")
    localDataService._getCurrentNormalizationRecord = mock.Mock()
    testNormalizationState = Normalization.model_validate_json(
        Resource.read("inputs/normalization/NormalizationParameters.json")
    )
    actualState = localDataService.readNormalizationState("57514", True)
    assert actualState == testNormalizationState


def test_writeCalibrationState():
    runNumber = "123"
    useLiteMode = True
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        calibration = Calibration.model_validate_json(Resource.read("/inputs/calibration/CalibrationParameters.json"))
        localDataService.writeCalibrationState(calibration)
        file = localDataService._constructCalibrationParametersFilePath(runNumber, useLiteMode, calibration.version)
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
            filePath = localDataService._constructCalibrationParametersFilePath(runNumber, useLiteMode, version)
            tmpRoot.addFileAs(
                Resource.getPath("/inputs/calibration/CalibrationParameters.json"),
                filePath,
            )
            # now try to overwrite it
            calibration = Calibration.model_validate_json(
                Resource.read("/inputs/calibration/CalibrationParameters.json")
            )
            localDataService.writeCalibrationState(calibration, version)
            assert filePath.exists()
        assert f"overwriting calibration parameters at {filePath}" in caplog.text


@mock.patch("snapred.backend.data.GroceryService.GroceryService._createDiffcalTableWorkspaceName")
@mock.patch("snapred.backend.data.GroceryService.GroceryService._fetchInstrumentDonor")
def test_writeDefaultDiffCalTable(fetchInstrumentDonor, createDiffCalTableWorkspaceName):
    runNumber = "default"
    version = VERSION_START
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
        file = localDataService._constructCalibrationDataPath(runNumber, useLiteMode, version) / wsName
        file = file.with_suffix(".h5")
        assert file.exists()


def test_writeNormalizationState():
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        localDataService._getCurrentNormalizationRecord = mock.Mock()
        localDataService._getCurrentNormalizationRecord.return_value = Normalization.construct(
            {"seedRun": "123", "useLiteMode": True, "name": "test"}
        )
        normalization = Normalization.model_validate_json(
            Resource.read("/inputs/normalization/NormalizationParameters.json")
        )
        normalization.version = 1
        localDataService.writeNormalizationState(normalization, 1)
        assert localDataService._constructNormalizationParametersFilePath("123", True, 1).exists()


def test_readDetectorState():
    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()

    # Create a mock pvFile object matching the calibrationParameters.json values
    pvFile = {
        "entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value": [1.1],
        "entry/DASlogs/det_arc1/value": [1.0],
        "entry/DASlogs/det_arc2/value": [2.0],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [1.2],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [1],
        "entry/DASlogs/det_lin1/value": [1.0],
        "entry/DASlogs/det_lin2/value": [2.0],
    }
    localDataService._readPVFile.return_value = pvFile

    # Mock the _constructPVFilePath method
    localDataService._constructPVFilePath = mock.Mock(return_value="/mock/path")

    # Read the expected detector state from the calibration file
    testCalibration = Calibration.model_validate_json(Resource.read("inputs/calibration/CalibrationParameters.json"))
    testDetectorState = testCalibration.instrumentState.detectorState

    actualDetectorState = localDataService.readDetectorState("123")
    assert actualDetectorState == testDetectorState


def test_readDetectorState_bad_wav_key():
    # Ensures a failure is thrown if a bad wav key is encountered
    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()
    localDataService._constructPVFilePath = mock.Mock(return_value="/mock/path")

    # Create a mock pvFile object
    pvFile = {
        "entry/DASlogs/not:a:real:log/value": [0.1],
        "entry/DASlogs/det_arc1/value": [0.1],
        "entry/DASlogs/det_arc2/value": [0.1],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [0.1],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [1],
        "entry/DASlogs/det_lin1/value": [0.1],
        "entry/DASlogs/det_lin2/value": [0.1],
    }
    localDataService._readPVFile.return_value = pvFile

    # Call the method being tested
    with pytest.raises(ValueError) as e:  # noqa PT011
        localDataService.readDetectorState("12345")
    assert "Could not find wavelength logs in file" in str(e.value)


def test_readDetectorState_missing_value():
    # Ensures a failure is thrown if a needed key is missing
    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()
    localDataService._constructPVFilePath = mock.Mock(return_value="/mock/path")

    # Create a mock pvFile object
    pvFile = {
        "entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value": [0.1],
        "entry/DASlogs/det_arc2/value": [0.1],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [0.1],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [1],
        "entry/DASlogs/det_lin1/value": [0.1],
        "entry/DASlogs/det_lin2/value": [0.1],
    }
    localDataService._readPVFile.return_value

    # Call the method being tested
    localDataService._readPVFile.return_value = pvFile
    with pytest.raises(ValueError) as e:  # noqa: PT011
        localDataService.readDetectorState("12345")
    assert "Could not find all required logs" in str(e.value)


def test_readDetectorState_bad_logs():
    localDataService = LocalDataService()
    localDataService._constructPVFilePath = mock.Mock()
    localDataService._constructPVFilePath.return_value = "/not/a/path"
    localDataService._readPVFile = mock.Mock()

    # Create a mock pvFile object with incorrect logs
    pvFile = {
        "entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value": "glitch",
        "entry/DASlogs/det_arc1/value": [2],
        "entry/DASlogs/det_arc2/value": [1.1],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [0.1],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [1],
        "entry/DASlogs/det_lin1/value": [1.0],
        "entry/DASlogs/det_lin2/value": [2.0],
    }
    """
    {
        "entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value": [detectorState.wav],
        "entry/DASlogs/det_arc1/value": [detectorState.arc[0]],
        "entry/DASlogs/det_arc2/value": [detectorState.arc[1]],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [detectorState.freq],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [detectorState.guideStat],
        "entry/DASlogs/det_lin1/value": [detectorState.lin[0]],
        "entry/DASlogs/det_lin2/value": [detectorState.lin[1]],
    }
    """
    localDataService._readPVFile.return_value = pvFile

    with pytest.raises(ValueError, match=r".*unable to parse string as a number.*"):  # noqa: PT011
        localDataService.readDetectorState("123")


def test_initializeState():
    # Test 'initializeState'; test basic functionality.
    runNumber = "123"
    useLiteMode = True

    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()

    # Create a mock pvFile object matching the calibrationParameters.json values
    pvFile = {
        "entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value": [1.1],
        "entry/DASlogs/det_arc1/value": [1],
        "entry/DASlogs/det_arc2/value": [2],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [1.2],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [1],
        "entry/DASlogs/det_lin1/value": [1],
        "entry/DASlogs/det_lin2/value": [2],
    }
    localDataService._readPVFile.return_value = pvFile

    localDataService._writeDefaultDiffCalTable = mock.Mock()

    testCalibrationData = Calibration.model_validate_json(
        Resource.read("inputs/calibration/CalibrationParameters.json")
    )
    testCalibrationData.useLiteMode = useLiteMode

    localDataService.readInstrumentConfig = mock.Mock()
    localDataService.readInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    localDataService.writeCalibrationState = mock.Mock()
    localDataService._prepareStateRoot = mock.Mock()

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = testCalibrationData.instrumentState.id.hex  # "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        localDataService.constructCalibrationStateRoot = mock.Mock(return_value=stateRootPath)

        actual = localDataService.initializeState(runNumber, useLiteMode, "test")
        actual.creationDate = testCalibrationData.creationDate
    assert actual == testCalibrationData
    assert localDataService._writeDefaultDiffCalTable.call_count == 2
    localDataService._writeDefaultDiffCalTable.assert_any_call(runNumber, True)
    localDataService._writeDefaultDiffCalTable.assert_any_call(runNumber, False)


def test_initializeState_calls_prepareStateRoot():
    # Test that 'initializeState' initializes the <state root> directory.

    runNumber = "123"
    useLiteMode = True

    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()

    pvFile = {
        "entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value": [1.1],
        "entry/DASlogs/det_arc1/value": [1.0],
        "entry/DASlogs/det_arc2/value": [2.0],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [0.1],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [1],
        "entry/DASlogs/det_lin1/value": [1.0],
        "entry/DASlogs/det_lin2/value": [2.0],
    }
    localDataService._readPVFile.return_value = pvFile
    localDataService._writeDefaultDiffCalTable = mock.Mock()

    testCalibrationData = Calibration.model_validate_json(
        Resource.read("inputs/calibration/CalibrationParameters.json")
    )

    localDataService.readInstrumentConfig = mock.Mock()
    localDataService.readInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    localDataService.writeCalibrationState = mock.Mock()
    localDataService._readDefaultGroupingMap = mock.Mock(return_value=mock.Mock(isDirty=False))

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = ENDURING_STATE_ID
        stateRootPath = Path(tmpDir) / stateId
        localDataService.constructCalibrationStateRoot = mock.Mock(return_value=stateRootPath)

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


def test_readGroupingMap_no():
    localDataService = LocalDataService()
    localDataService.checkCalibrationFileExists = mock.Mock(return_value=False)
    localDataService.generateStateId = mock.Mock(side_effect=RuntimeError("YOU IDIOT!"))
    localDataService._readDefaultGroupingMap = mock.Mock()

    runNumber = "flan"
    res = localDataService.readGroupingMap(runNumber)  # noqa: F841
    assert localDataService._readDefaultGroupingMap.called


def test_readGroupingMap_yes():
    localDataService = LocalDataService()
    localDataService.checkCalibrationFileExists = mock.Mock(return_value=True)
    localDataService.generateStateId = mock.Mock(return_value=(mock.Mock(), mock.Mock()))
    localDataService._readGroupingMap = mock.Mock()
    localDataService._readDefaultGroupingMap = mock.Mock(side_effect=RuntimeError("YOU IDIOT!"))

    runNumber = "flan"
    res = localDataService.readGroupingMap(runNumber)  # noqa: F841
    assert not localDataService._readDefaultGroupingMap.called


def test_readNoSampleFilePaths():
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock()
    localDataService._findMatchingFileList.return_value = []

    with pytest.raises(RuntimeError) as e:
        localDataService.readSampleFilePaths()
    assert "No samples found" in str(e.value)


def test__readDefaultGroupingMap():
    # test of private `readDefaultGroupingMap` method
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
    stateId = ENDURING_STATE_ID
    with state_root_redirect(service, stateId=stateId) as tmpRoot:
        service.constructCalibrationStateRoot(stateId).mkdir()
        tmpRoot.addFileAs(
            Resource.getPath("inputs/pixel_grouping/groupingMap.json"),
            service._groupingMapPath(stateId),
        )
        groupingMap = service._readGroupingMap(stateId)
    assert groupingMap.stateId == stateId


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
    assert type(result) == CalibrantSamples
    assert result.name == "Silicon_NIST_640D"


@mock.patch("os.path.exists", return_value=True)
def test_readCifFilePath(mock1):  # noqa: ARG001
    localDataService = LocalDataService()

    result = localDataService.readCifFilePath("testid")
    assert result == "/SNS/SNAP/shared/Calibration_dynamic/CalibrantSamples/EntryWithCollCode52054_diamond.cif"


## TESTS OF WORKSPACE WRITE METHODS


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
            XMin=0.5,
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
