# Unit tests for `tests/util/state_helpers.py`
import shutil
import unittest.mock as mock
from pathlib import Path
from shutil import rmtree

import pytest
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import parse_file_as
from util.state_helpers import reduction_root_redirect, state_root_override, state_root_redirect


@pytest.fixture(autouse=True)
def _cleanup_directories():
    stateId = "ab8704b0bc2a2342"
    stateRootPath = Path(Config["instrument.calibration.powder.home"]) / stateId
    yield
    # teardown
    if stateRootPath.exists():
        shutil.rmtree(stateRootPath)


def initPVFileMock() -> mock.Mock:
    mock_ = mock.Mock()
    # 8X: seven required `readDetectorState` log entries:
    #   * generated stateId hex-digest: 'ab8704b0bc2a2342',
    #   * generated `DetectorInfo` matches that from 'inputs/calibration/CalibrationParameters.json'
    mock_.get.side_effect = [
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
    return mock_


@mock.patch.object(LocalDataService, "_writeDefaultDiffCalTable")
@mock.patch.object(LocalDataService, "_generateStateId")
@mock.patch.object(LocalDataService, "_defaultGroupingMapPath")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_enter(
    mockReadPVFile,
    mockReadInstrumentConfig,
    mockDefaultGroupingMapPath,
    mockGenerateStateId,
    mockWriteDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = initPVFileMock()

    testCalibrationData = parse_file_as(Calibration, Resource.getPath("inputs/calibration/CalibrationParameters.json"))
    mockReadInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig

    mockDefaultGroupingMapPath.return_value = Path(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))

    stateId = "ab8704b0bc2a2342"
    # NOTE delete the path first or the test can fail for confusing reasons
    expectedStateRootPath = Path(Config["instrument.calibration.powder.home"]) / stateId
    rmtree(expectedStateRootPath, ignore_errors=True)
    decodedKey = None
    mockGenerateStateId.return_value = (stateId, decodedKey)
    runNumber = "123456"
    stateName = "my happy state"
    useLiteMode = True
    with state_root_override(runNumber, stateName, useLiteMode) as stateRootPath:
        assert Path(stateRootPath) == expectedStateRootPath
        assert Path(stateRootPath).exists()
        assert Path(stateRootPath).joinpath("groupingMap.json").exists()
        versionString = wnvf.pathVersion(VERSION_DEFAULT)
        assert (Path(stateRootPath) / "lite" / "diffraction" / versionString / "CalibrationParameters.json").exists()


@mock.patch.object(LocalDataService, "_writeDefaultDiffCalTable")
@mock.patch.object(LocalDataService, "_defaultGroupingMapPath")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_exit(
    mockReadPVFile,
    mockReadInstrumentConfig,
    mockDefaultGroupingMapPath,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = initPVFileMock()

    testCalibrationData = parse_file_as(Calibration, Resource.getPath("inputs/calibration/CalibrationParameters.json"))
    mockReadInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig

    mockDefaultGroupingMapPath.return_value = Path(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))

    stateId = "ab8704b0bc2a2342"  # noqa: F841
    runNumber = "123456"
    stateName = "my happy state"
    useLiteMode = True

    stateRootPath = None
    with state_root_override(runNumber, stateName, useLiteMode) as rootPath:
        stateRootPath = rootPath
        assert Path(stateRootPath).exists()
    assert not Path(stateRootPath).exists()


@mock.patch.object(LocalDataService, "_writeDefaultDiffCalTable")
@mock.patch.object(LocalDataService, "_defaultGroupingMapPath")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_exit_no_delete(
    mockReadPVFile,
    mockReadInstrumentConfig,
    mockDefaultGroupingMapPath,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = initPVFileMock()

    testCalibrationData = parse_file_as(Calibration, Resource.getPath("inputs/calibration/CalibrationParameters.json"))
    mockReadInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig

    mockDefaultGroupingMapPath.return_value = Path(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))

    stateId = "ab8704b0bc2a2342"  # noqa: F841
    runNumber = "123456"
    stateName = "my happy state"
    useLiteMode = True

    stateRootPath = None
    with state_root_override(runNumber, stateName, useLiteMode, delete_at_exit=False) as rootPath:
        stateRootPath = rootPath
        assert Path(stateRootPath).exists()
    assert Path(stateRootPath).exists()
    shutil.rmtree(stateRootPath)


def test_state_root_redirect_no_stateid():
    localDataService = LocalDataService()
    oldSelf = localDataService._constructCalibrationStateRoot
    with state_root_redirect(localDataService) as tmpRoot:
        # make sure the path exists
        assert tmpRoot.path().exists()
        # make sure the data service's path points to the tmp directory
        assert localDataService._constructCalibrationStateRoot() == tmpRoot.path()
        assert localDataService._generateStateId()[0] == tmpRoot.path().parts[-1]
        # make sure a file can be added inside the directory -- can be any file
        # verify it can be found by data services and equals the value written
        indexer = localDataService.calibrationIndexer("xyz", True)
        tmpRoot.addFileAs(Resource.getPath("inputs/calibration/CalibrationParameters.json"), indexer.parametersPath(1))
        ans = localDataService.readCalibrationState("xyz", True, 1)
        assert ans == parse_file_as(Calibration, Resource.getPath("inputs/calibration/CalibrationParameters.json"))
        # make sure files can only be added inside the directory
        with pytest.raises(AssertionError):
            tmpRoot.addFileAs(
                Resource.getPath("inputs/calibration/CalibrationRecord_v0002.json"),
                "here",
            )
    # make sure the directory is deleted at exit
    assert not tmpRoot.path().exists()
    # make sure the construct state root method is restored on exit
    assert oldSelf == localDataService._constructCalibrationStateRoot


def test_state_root_redirect_with_stateid():
    stateId = "1234567890123456"
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        # make sure the root is pointing to this state ID
        assert localDataService._generateStateId() == (stateId, None)
        assert localDataService._constructCalibrationStateRoot() == tmpRoot.path()
        assert stateId == localDataService._constructCalibrationStateRoot().parts[-1]


def test_reduction_root_redirect_no_stateid():
    localDataService = LocalDataService()
    oldSelf = localDataService._constructReductionStateRoot
    with reduction_root_redirect(localDataService) as tmpRoot:
        # make sure the path exists
        assert tmpRoot.path().exists()
        # make sure the data service's path points to the tmp directory
        assert localDataService._constructReductionStateRoot() == tmpRoot.path()
        assert localDataService._generateStateId()[0] == tmpRoot.path().parts[-1]
    # make sure the directory is deleted at exit
    assert not tmpRoot.path().exists()
    # make sure the construct state root method is restored on exit
    assert oldSelf == localDataService._constructReductionStateRoot


def test_reduction_root_redirect_with_stateid():
    stateId = "1234567890123456"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        # make sure the root is pointing to this state ID
        assert localDataService._generateStateId() == (stateId, None)
        assert localDataService._constructReductionStateRoot() == tmpRoot.path()
        assert stateId == localDataService._constructReductionStateRoot().parts[-1]
