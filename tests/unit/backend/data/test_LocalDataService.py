# ruff: noqa: E402

import importlib
import json
import logging
import os
import shutil
import socket
import tempfile
import unittest.mock as mock
from pathlib import Path
from random import randint, shuffle
from typing import List

import pytest
from mantid.api import ITableWorkspace, MatrixWorkspace
from mantid.dataobjects import MaskWorkspace
from mantid.simpleapi import (
    CloneWorkspace,
    CreateGroupingWorkspace,
    CreateSampleWorkspace,
    LoadEmptyInstrument,
    LoadInstrument,
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
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as WNG
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.meta.redantic import write_model_pretty
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask

LocalDataServiceModule = importlib.import_module(LocalDataService.__module__)
ThisService = "snapred.backend.data.LocalDataService."

VERSION_START = Config["instrument.startingVersionNumber"]
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

    localDataService._readDiffractionCalibrant = mock.Mock()
    localDataService._readDiffractionCalibrant.return_value = (
        reductionIngredients.reductionState.stateConfig.diffractionCalibrant
    )
    localDataService._readNormalizationCalibrant = mock.Mock()
    localDataService._readNormalizationCalibrant.return_value = (
        reductionIngredients.reductionState.stateConfig.normalizationCalibrant
    )

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
    assert actual is not None
    assert actual.stateId == "ab8704b0bc2a2342"


def test_readStateConfig_attaches_grouping_map():
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
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
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService.readCalibrationState = mock.Mock()
        localDataService.readCalibrationState.return_value = Calibration.parse_file(
            Resource.getPath("inputs/calibration/CalibrationParameters.json")
        )

        localDataService._groupingMapPath = mock.Mock()
        # 'GroupingMap.defaultStateId' is _not_ a valid grouping-map 'stateId' for an existing `StateConfig`.
        localDataService._groupingMapPath.return_value = Path(
            Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
        )
        stateGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
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
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
    localDataService.readCalibrationState = mock.Mock()
    localDataService.readCalibrationState.return_value = Calibration.parse_file(
        Resource.getPath("inputs/calibration/CalibrationParameters.json")
    )

    localDataService._groupingMapPath = mock.Mock()
    localDataService._groupingMapPath.side_effect = [
        Path(Resource.getPath("inputs/pixel_grouping/does_not_exist.json")),
        Path(Resource.getPath("inputs/pixel_grouping/groupingMap.json")),
    ]
    stateGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/groupingMap.json"))
    localDataService._readGroupingMap = mock.Mock()
    localDataService._readGroupingMap.return_value = stateGroupingMap

    localDataService.instrumentConfig = getMockInstrumentConfig()

    actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    mockPrepareStateRoot.assert_called_once()


def test_prepareStateRoot_creates_state_root_directory():
    # Test that the <state root> directory is created when it doesn't exist.
    localDataService = LocalDataService()

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        groupingMapFilePath = stateRootPath / "groupingMap.json"
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=str(stateRootPath))
        defaultGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        localDataService._groupingMapPath = mock.Mock(return_value=groupingMapFilePath)

        assert not stateRootPath.exists()
        localDataService._prepareStateRoot(stateId)
        assert stateRootPath.exists()


def test_prepareStateRoot_existing_state_root():
    # Test that an already existing <state root> directory is not an error.
    localDataService = LocalDataService()

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        os.makedirs(stateRootPath)

        groupingMapFilePath = stateRootPath / "groupingMap.json"
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=str(stateRootPath))
        defaultGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        localDataService._groupingMapPath = mock.Mock(return_value=groupingMapFilePath)
        assert stateRootPath.exists()
        localDataService._prepareStateRoot(stateId)


def test_prepareStateRoot_writes_grouping_map():
    # Test that the first time a <state root> directory is initialized,
    #   the `StateConfig.groupingMap` is written to the directory.
    localDataService = LocalDataService()

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        groupingMapFilePath = stateRootPath / "groupingMap.json"
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=str(stateRootPath))
        defaultGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        localDataService._groupingMapPath = mock.Mock(return_value=groupingMapFilePath)

        assert not groupingMapFilePath.exists()
        localDataService._prepareStateRoot(stateId)
        assert groupingMapFilePath.exists()


def test_prepareStateRoot_sets_grouping_map_stateid():
    # Test that the first time a <state root> directory is initialized,
    #   the 'stateId' of the `StateConfig.groupingMap` is set to match that of the state.
    localDataService = LocalDataService()

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        groupingMapFilePath = stateRootPath / "groupingMap.json"
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=str(stateRootPath))
        defaultGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        localDataService._groupingMapPath = mock.Mock(return_value=groupingMapFilePath)

        localDataService._prepareStateRoot(stateId)

        with open(groupingMapFilePath, "r") as file:
            groupingMap = parse_raw_as(GroupingMap, file.read())
        assert groupingMap.stateId == stateId


def test_prepareStateRoot_no_default_grouping_map():
    # Test that the first time a <state root> directory is initialized,
    #   the 'defaultGroupingMap.json' at Config['instrument.calibration.powder.grouping.home']
    #   is required to exist.
    localDataService = LocalDataService()

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        groupingMapFilePath = stateRootPath / "groupingMap.json"
        defaultGroupingMapFilePath = Resource.getPath("inputs/pixel_grouping/does_not_exist.json")
        with pytest.raises(  # noqa: PT012
            FileNotFoundError,
            match=f'required default grouping-schema map "{defaultGroupingMapFilePath}" does not exist',
        ):
            localDataService._constructCalibrationStateRoot = mock.Mock(return_value=str(stateRootPath))
            localDataService._defaultGroupingMapPath = mock.Mock(return_value=Path(defaultGroupingMapFilePath))
            localDataService._groupingMapPath = mock.Mock(return_value=groupingMapFilePath)
            localDataService._prepareStateRoot(stateId)


def test_prepareStateRoot_does_not_overwrite_grouping_map():
    # If a 'groupingMap.json' file already exists at the <state root> directory,
    #   it should not be overwritten.
    localDataService = LocalDataService()

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        os.makedirs(stateRootPath)

        defaultGroupingMapFilePath = Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
        groupingMapFilePath = stateRootPath / "groupingMap.json"

        # Write a 'groupingMap.json' file to the <state root>, but with a different stateId;
        #   note that the _value_ of the stateId field is _not_ validated at this stage, except for its format.
        with open(defaultGroupingMapFilePath, "r") as file:
            groupingMap = parse_raw_as(GroupingMap, file.read())
        otherStateId = "bbbbaaaabbbbeeee"
        groupingMap.coerceStateId(otherStateId)
        write_model_pretty(groupingMap, groupingMapFilePath)

        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=str(stateRootPath))
        defaultGroupingMap = GroupingMap.parse_file(defaultGroupingMapFilePath)
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        localDataService._groupingMapPath = mock.Mock(return_value=groupingMapFilePath)

        localDataService._prepareStateRoot(stateId)

        with open(groupingMapFilePath, "r") as file:
            groupingMap = parse_raw_as(GroupingMap, file.read())
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


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists(GetIPTS):  # noqa ARG002
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        assert Path(tmpDir).exists()
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock(return_value=("ab8704b0bc2a2342", None))
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=tmpDir)
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
        localDataService.iptsCache = {}  # clear the ipts cache
        runNumber = "654321"
        assert not localDataService.checkCalibrationFileExists(runNumber)


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists_not(GetIPTS):  # noqa ARG002
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock(return_value=("ab8704b0bc2a2342", None))
        nonExistentPath = Path(tmpDir) / "1755"
        assert not nonExistentPath.exists()
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=str(nonExistentPath))
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


# NOTE this test calls GetIPTS directly with no mocks
# this is intentional, to ensure it is being called correctly
def test_getIPTS_cache():
    from mantid.kernel import amend_config

    localDataService = LocalDataService()
    localDataService.iptsCache = {}
    # test data
    instrument = "SNAP"
    runNumber = "123"
    key = (runNumber, instrument)
    correctIPTS = Resource.getPath("inputs/testInstrument/IPTS-456/")

    # direct GetIPTS to look in the exact folder where it should look
    # it is very stupid, so if you don't tell it exactly then it won't look there
    with amend_config(data_dir=correctIPTS):
        res = localDataService.getIPTS(*key)
        assert res == correctIPTS

        # ensure it is in the cache
        assert (runNumber, instrument) in localDataService.iptsCache
        assert localDataService.iptsCache[key] == correctIPTS

        # call again and make sure the cache is being returned
        localDataService.GetIPTS = mock.Mock()
        res = localDataService.getIPTS(*key)
        assert localDataService.GetIPTS.not_called
        assert res == correctIPTS


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
    localDataService._readDiffractionCalibrant = mock.Mock()
    localDataService._readDiffractionCalibrant.return_value = (
        reductionIngredients.reductionState.stateConfig.diffractionCalibrant
    )
    localDataService._readNormalizationCalibrant = mock.Mock()
    localDataService._readNormalizationCalibrant.return_value = (
        reductionIngredients.reductionState.stateConfig.normalizationCalibrant
    )
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


def test_readRunConfig():
    # test of public `readRunConfig` method
    localDataService = LocalDataService()
    localDataService._readRunConfig = mock.Mock()
    localDataService._readRunConfig.return_value = reductionIngredients.runConfig
    actual = localDataService.readRunConfig(mock.Mock())
    assert actual is not None
    assert actual.runNumber == "57514"


def test__readRunConfig():
    # Test of private `_readRunConfig` method
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value="IPTS-123")
    localDataService.instrumentConfig = getMockInstrumentConfig()
    actual = localDataService._readRunConfig("57514")
    assert actual is not None
    assert actual.runNumber == "57514"


@mock.patch("h5py.File", return_value="not None")
def test_readPVFile(h5pyMock):  # noqa: ARG001
    localDataService = LocalDataService()
    localDataService.instrumentConfig = getMockInstrumentConfig()
    localDataService._constructPVFilePath = mock.Mock()
    localDataService._constructPVFilePath.return_value = Resource.getPath("./")
    actual = localDataService._readPVFile(mock.Mock())
    assert actual is not None


def test__generateStateId():
    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()
    fileMock = mock.Mock()
    localDataService._readPVFile.return_value = fileMock
    fileMock.get.side_effect = [[0.1], [0.1], [0.1], [0.1], [1], [0.1], [0.1]]
    actual, _ = localDataService._generateStateId(mock.Mock())
    assert actual == "9618b936a4419a6e"


def test__findMatchingFileList():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = getMockInstrumentConfig()
    actual = localDataService._findMatchingFileList(Resource.getPath("inputs/SNAPInstPrm.json"), False)
    assert actual is not None
    assert len(actual) == 1


def test_readCalibrationIndexMissing():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructCalibrationStateRoot = mock.Mock()
    localDataService._constructCalibrationStateRoot.return_value = Resource.getPath("outputs")
    assert len(localDataService.readCalibrationIndex("123", True)) == 0


def test_readNormalizationIndexMissing():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructCalibrationStateRoot = mock.Mock()
    localDataService._constructCalibrationStateRoot.return_value = Resource.getPath("outputs")
    assert len(localDataService.readNormalizationIndex("123", True)) == 0


def test_writeCalibrationIndexEntry():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructCalibrationStatePath = mock.Mock()
    localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")
    expectedFilePath = Resource.getPath("outputs/") + "CalibrationIndex.json"
    localDataService.writeCalibrationIndexEntry(
        CalibrationIndexEntry(runNumber="57514", useLiteMode=True, comments="test comment", author="test author"),
    )
    assert os.path.exists(expectedFilePath)

    fileContent = ""
    with open(expectedFilePath, "r") as indexFile:
        fileContent = indexFile.read()
    os.remove(expectedFilePath)
    assert len(fileContent) > 0

    actualEntries = parse_raw_as(List[CalibrationIndexEntry], fileContent)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"

    # def test_writeNormalizationIndexEntry():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructNormalizationStatePath = mock.Mock(return_value=Resource.getPath("outputs/"))
    expectedFilePath = Resource.getPath("outputs/") + "NormalizationIndex.json"
    localDataService.writeNormalizationIndexEntry(
        NormalizationIndexEntry(
            runNumber="57514",
            useLiteMode=True,
            backgroundRunNumber="58813",
            comments="test comment",
            author="test author",
        ),
    )
    assert os.path.exists(expectedFilePath)

    fileContent = ""
    with open(expectedFilePath, "r") as indexFile:
        fileContent = indexFile.read()
    os.remove(expectedFilePath)
    assert len(fileContent) > 0

    actualEntries = parse_raw_as(List[NormalizationIndexEntry], fileContent)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


def test_readCalibrationIndexExisting():
    localDataService = LocalDataService()
    localDataService.instrumentConfig = mock.Mock()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructCalibrationStatePath = mock.Mock()
    localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")
    expectedFilePath = Resource.getPath("outputs/") + "CalibrationIndex.json"
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
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._readReductionParameters = mock.Mock()
    localDataService._constructNormalizationStatePath = mock.Mock(return_value=Resource.getPath("outputs/"))
    expectedFilePath = Resource.getPath("outputs/") + "NormalizationIndex.json"
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
        return ReductionIngredients.parse_raw(f.read())


##
def test_readWriteCalibrationRecord_version_numbers():
    testCalibrationRecord_v0001 = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testCalibrationRecord_v0002 = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0002.json")
    )
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock(return_value=("ab8704b0bc2a2342", None))
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=f"{tempdir}/")
        # WARNING: 'writeCalibrationRecord' modifies <incoming record>.version,
        #   and <incoming record>.calibrationFittingIngredients.version.

        # write: version == 1
        localDataService.writeCalibrationRecord(testCalibrationRecord_v0001)
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True)
        print(actualRecord)
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
    testCalibrationRecord_v0001 = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    testCalibrationRecord_v0002 = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0002.json")
    )
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock(return_value=("ab8704b0bc2a2342", None))
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=f"{tempdir}/")
        # WARNING: 'writeCalibrationRecord' modifies <incoming record>.version,
        #   and <incoming record>.calibrationFittingIngredients.version.

        # write: version == 1
        localDataService.writeCalibrationRecord(testCalibrationRecord_v0001)
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True)
        assert actualRecord.version == 1
        assert actualRecord.calibrationFittingIngredients.version == 1
        # write: version == 2
        localDataService.writeCalibrationRecord(testCalibrationRecord_v0002)
        actualRecord = localDataService.readCalibrationRecord("57514", True, "1")
        assert actualRecord.version == 1
        actualRecord = localDataService.readCalibrationRecord("57514", True, "2")
        assert actualRecord.version == 2


def test_readWriteCalibrationRecord_with_version():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock(return_value=("123", "456"))
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=f"{tempdir}/")
        localDataService.writeCalibrationRecord(
            CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord_v0001.json"))
        )
        actualRecord = localDataService.readCalibrationRecord("57514", True, "1")
    assert actualRecord.runNumber == "57514"
    assert actualRecord.version == 1


def test_readWriteCalibrationRecord():
    testCalibrationRecord = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock(return_value=("ab8704b0bc2a2342", None))
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=f"{tempdir}/")
        localDataService.writeCalibrationRecord(testCalibrationRecord)
        actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode=True)
    assert actualRecord.runNumber == "57514"
    assert actualRecord == testCalibrationRecord


@mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
def test_writeCalibrationWorkspaces(mockConstructCalibrationDataPath):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    testCalibrationRecord = CalibrationRecord.parse_raw(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
        basePath = Path(basePath)
        mockConstructCalibrationDataPath.return_value = str(basePath)

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

        diffCalFilename = Path(WNG.diffCalTable().runNumber(runNumber).version(version).build() + ".h5")
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


@mock.patch.object(LocalDataService, "writeWorkspace")
@mock.patch.object(LocalDataService, "_constructCalibrationDataPath")
def test_writeCalibrationWorkspaces_no_units(
    mockConstructCalibrationDataPath,
    mockWriteWorkspace,  # noqa: ARG001
):
    # test that diffraction-calibration output workspace names require units
    localDataService = LocalDataService()
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
        mockConstructCalibrationDataPath.return_value = "not/a/path"
        localDataService.writeCalibrationWorkspaces(testCalibrationRecord)


def test_readWriteNormalizationRecord_version_numbers():
    testNormalizationRecord = NormalizationRecord.parse_raw(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    useLiteMode = True
    testNormalizationRecord.useLiteMode = useLiteMode
    testNormalizationRecord.version = VERSION_START
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService._constructNormalizationStatePath = mock.Mock(return_value=f"{tempdir}/")
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
    runNumber = "57514"
    useLiteMode = False

    testNormalizationRecord = NormalizationRecord.parse_raw(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    testNormalizationRecord.version = VERSION_START
    testNormalizationRecord.useLiteMode = useLiteMode
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService._constructNormalizationStatePath = mock.Mock(return_value=f"{tempdir}/")
        # WARNING: 'writeNormalizationRecord' modifies <incoming record>.version,
        # and <incoming record>.normalization.version.

        # write: version == VERSION_START
        firstVersion = VERSION_START + 1
        localDataService.writeNormalizationRecord(testNormalizationRecord)
        actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode)
        assert actualRecord.version == firstVersion
        assert actualRecord.calibration.version == firstVersion
        assert actualRecord.useLiteMode == useLiteMode
        assert os.path.exists(f"{tempdir}/{wnvf.fileVersion(firstVersion)}/NormalizationRecord.json")
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
        assert os.path.exists(f"{tempdir}/{wnvf.fileVersion(firstVersion)}/NormalizationRecord.json")
        assert os.path.exists(f"{tempdir}/{wnvf.fileVersion(testVersion)}/NormalizationRecord.json")
        # test can still read earlier version
        actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode, firstVersion)
        assert actualRecord.version == firstVersion
        assert actualRecord.useLiteMode == useLiteMode


def test_readWriteNormalizationRecord():
    useLiteMode = True
    testNormalizationRecord = NormalizationRecord.parse_raw(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    testNormalizationRecord.useLiteMode = useLiteMode
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock(return_value=("ab8704b0bc2a2342", None))
        localDataService._constructNormalizationStatePath = mock.Mock(return_value=f"{tempdir}/")
        localDataService.writeNormalizationRecord(testNormalizationRecord)
        actualRecord = localDataService.readNormalizationRecord("57514", useLiteMode)
    assert actualRecord.runNumber == "57514"
    assert actualRecord.useLiteMode == useLiteMode
    assert actualRecord == testNormalizationRecord


def test_writeNormalizationWorkspaces():
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    testNormalizationRecord = NormalizationRecord.parse_raw(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
        basePath = Path(basePath)
        localDataService._constructNormalizationDataPath = mock.Mock(return_value=str(basePath))

        # Workspace names need to match the names that are used in the test record.
        runNumber = testNormalizationRecord.runNumber  # noqa: F841
        version = testNormalizationRecord.version  # noqa: F841
        testWS0, testWS1, testWS2 = testNormalizationRecord.workspaceNames

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


def test_getCalibrationRecordPath():
    testVersion = randint(1, 20)
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._constructCalibrationStatePath = mock.Mock()
    localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")
    actualPath = localDataService.getCalibrationRecordPath("57514", True, testVersion)
    assert actualPath == Resource.getPath("outputs") + f"/{wnvf.fileVersion(testVersion)}/CalibrationRecord.json"


def test_getNormalizationRecordPath():
    testVersion = randint(1, 20)
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("123", "456")
    localDataService._constructNormalizationStatePath = mock.Mock()
    localDataService._constructNormalizationStatePath.return_value = Resource.getPath("outputs/")
    actualPath = localDataService.getNormalizationRecordPath("57514", True, testVersion)
    assert actualPath == Resource.getPath("outputs") + f"/{wnvf.fileVersion(testVersion)}/NormalizationRecord.json"


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
        timestamp=123, useLiteMode=True, version="1", appliesTo="123", runNumber="123", comments="", author=""
    )
    actualVersion = localDataService._getVersionFromCalibrationIndex("123", True)
    assert actualVersion == "1"


def test__getVersionFromNormalizationIndex():
    localDataService = LocalDataService()
    localDataService.readNormalizationIndex = mock.Mock()
    localDataService.readNormalizationIndex.return_value = [mock.Mock()]
    localDataService.readNormalizationIndex.return_value[0] = NormalizationIndexEntry(
        timestamp=123,
        version="1",
        appliesTo="123",
        runNumber="123",
        useLiteMode=True,
        backgroundRunNumber="456",
        comments="",
        author="",
    )
    actualVersion = localDataService._getVersionFromNormalizationIndex("123", True)
    assert actualVersion == "1"


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
    localDataService._generateStateId = mock.Mock(return_value=("ab8704b0bc2a2342", None))
    localDataService._constructCalibrationStatePath = mock.Mock(return_value=Resource.getPath("outputs/"))
    actualPath = localDataService._constructCalibrationParametersFilePath("57514", True, testVersion)
    assert actualPath == Resource.getPath("outputs") + f"/{wnvf.fileVersion(testVersion)}/CalibrationParameters.json"


def test_readCalibrationState():
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock(return_value=("ab8704b0bc2a2342", None))
    localDataService._constructCalibrationParametersFilePath = mock.Mock()
    localDataService._constructCalibrationParametersFilePath.return_value = Resource.getPath(
        "ab8704b0bc2a2342/v_0001/CalibrationParameters.json"
    )
    localDataService._getLatestFile = mock.Mock()
    localDataService._getLatestFile.return_value = Resource.getPath("inputs/calibration/CalibrationParameters.json")
    testCalibrationState = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
    actualState = localDataService.readCalibrationState("57514", True)

    assert actualState == testCalibrationState


def test_readCalibrationState_no_file():
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
    localDataService._constructCalibrationParametersFilePath = mock.Mock()
    localDataService._constructCalibrationParametersFilePath.return_value = Resource.getPath(
        "ab8704b0bc2a2342/v_0001/CalibrationParameters.json"
    )
    localDataService._getLatestFile = mock.Mock()
    localDataService._getLatestFile.return_value = None
    with pytest.raises(RecoverableException):
        localDataService.readCalibrationState("57514", False)


def test_readNormalizationState():
    localDataService = LocalDataService()
    localDataService._generateStateId = mock.Mock()
    localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
    localDataService.getNormalizationStatePath = mock.Mock()
    localDataService.getNormalizationStatePath.return_value = Resource.getPath(
        "ab8704b0bc2a2342/v_0001/NormalizationParameters.json"
    )
    localDataService._getLatestFile = mock.Mock()
    localDataService._getLatestFile.return_value = Resource.getPath("inputs/normalization/NormalizationParameters.json")
    localDataService._getCurrentNormalizationRecord = mock.Mock()
    testNormalizationState = Normalization.parse_raw(Resource.read("inputs/normalization/NormalizationParameters.json"))
    actualState = localDataService.readNormalizationState("57514", True)
    assert actualState == testNormalizationState


def test_writeCalibrationState():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock(return_value=("123", "456"))
        localDataService._constructCalibrationStatePath = mock.Mock(return_value=f"{tempdir}/")
        localDataService._getCurrentCalibrationRecord = mock.Mock(return_value=Calibration.construct({"name": "test"}))
        calibration = Calibration.parse_raw(Resource.read("/inputs/calibration/CalibrationParameters.json"))
        localDataService.writeCalibrationState(calibration)
        assert os.path.exists(tempdir + f"/{wnvf.fileVersion(calibration.version)}/CalibrationParameters.json")


def test_writeCalibrationState_overwrite_warning(caplog):
    # Test that overwriting an existing calibration logs a warning.
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        with caplog.at_level(logging.WARNING):
            calibrationDataPath = Path(tmpDir) / "v_0001"
            calibrationParametersFilePath = calibrationDataPath / "CalibrationParameters.json"
            calibration = Calibration.parse_raw(Resource.read("/inputs/calibration/CalibrationParameters.json"))
            os.makedirs(calibrationDataPath)
            write_model_pretty(calibration, calibrationParametersFilePath)

            localDataService = LocalDataService()
            localDataService._generateStateId = mock.Mock(return_value=("123", "456"))
            localDataService._constructCalibrationStatePath = mock.Mock(return_value=f"{tmpDir}/")
            localDataService._getCurrentCalibrationRecord = mock.Mock(Calibration.construct({"name": "test"}))

            # Force the output path: otherwise it will be written to "v_2".
            localDataService._constructCalibrationParametersFilePath = mock.Mock()
            localDataService._constructCalibrationParametersFilePath.return_value = calibrationParametersFilePath

            calibration = Calibration.parse_raw(Resource.read("/inputs/calibration/CalibrationParameters.json"))
            localDataService.writeCalibrationState(calibration)
            assert os.path.exists(calibrationParametersFilePath)
        assert f"overwriting calibration parameters at {calibrationParametersFilePath}" in caplog.text


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
    filename = f"diffcal_{runNumber}_{wnvf.formatVersion(version, use_v_prefix=False)}"
    createDiffCalTableWorkspaceName.return_value = filename
    # now write the diffcal file
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        # mock the calibration state path to write to the tempdir
        localDataService._generateStateId = mock.Mock(return_value=("", ""))
        localDataService._constructCalibrationStatePath = mock.Mock(return_value=f"{tempdir}/")
        # run the method and ensure the file has been created in correct location
        # localDataService.writeCalibrationState(runNumber, calibration)
        localDataService._writeDefaultDiffCalTable(runNumber, useLiteMode)
        assert os.path.exists(tempdir + f"/{wnvf.fileVersion(version)}/" + filename + ".h5")
        # TODO we could in theory load the file and verify its contents here


def test_writeNormalizationState():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructNormalizationStatePath = mock.Mock(return_value=f"{tempdir}/")
        localDataService._getCurrentNormalizationRecord = mock.Mock()
        localDataService._getCurrentNormalizationRecord.return_value = Normalization.construct(
            {"seedRun": "123", "useLiteMode": True, "name": "test"}
        )
        normalization = Normalization.parse_file(Resource.getPath("/inputs/normalization/NormalizationParameters.json"))
        normalization.version = 1
        localDataService.writeNormalizationState(normalization, 1)
        assert os.path.exists(tempdir + "/v_0001/NormalizationParameters.json")


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
        stateRootPath = Path(f"{tmpDir}/") / stateId
        localDataService._constructCalibrationStatePath = mock.Mock(return_value=str(stateRootPath))
        localDataService._constructCalibrationStateRoot = mock.Mock(return_value=str(stateRootPath))

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


def test_readSamplePaths():
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock()
    localDataService._findMatchingFileList.return_value = [
        "/sample1.json",
        "/sample2.json",
    ]
    result = localDataService.readSamplePaths()
    assert len(result) == 2
    assert "/sample1.json" in result
    assert "/sample2.json" in result


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


def test_readNoSamplePaths():
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock()
    localDataService._findMatchingFileList.return_value = []

    with pytest.raises(RuntimeError) as e:
        localDataService.readSamplePaths()
    assert "No samples found" in str(e.value)


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
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        service = LocalDataService()
        stateId = "ab8704b0bc2a2342"
        stateRoot = Path(f"{tempdir}/{stateId}")
        service._constructCalibrationStateRoot = mock.Mock()
        service._constructCalibrationStateRoot.return_value = str(stateRoot)
        stateRoot.mkdir()
        shutil.copy(Path(Resource.getPath("inputs/pixel_grouping/groupingMap.json")), stateRoot)
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
