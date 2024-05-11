# Unit tests for `tests/util/state_helpers.py`
import shutil
import unittest.mock as mock
from pathlib import Path

import pytest
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config, Resource
from util.state_helpers import state_root_override


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
    # 4X: seven required `readDetectorState` log entries:
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
    ]
    return mock_


@mock.patch.object(LocalDataService, "_defaultGroupingMapPath")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_enter(mockReadPVFile, mockReadInstrumentConfig, mockDefaultGroupingMapPath):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = initPVFileMock()

    testCalibrationData = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
    mockReadInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig

    mockDefaultGroupingMapPath.return_value = Path(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))

    stateId = "ab8704b0bc2a2342"
    runNumber = "123456"
    stateName = "my happy state"
    useLiteMode = True
    with state_root_override(runNumber, stateName, useLiteMode) as stateRootPath:
        assert Path(stateRootPath) == Path(Config["instrument.calibration.powder.home"]) / stateId
        assert Path(stateRootPath).exists()
        assert Path(stateRootPath).joinpath("groupingMap.json").exists()
        assert (Path(stateRootPath) / "lite" / "diffraction" / "v_0001" / "CalibrationParameters.json").exists()


@mock.patch.object(LocalDataService, "_defaultGroupingMapPath")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_exit(mockReadPVFile, mockReadInstrumentConfig, mockDefaultGroupingMapPath):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = initPVFileMock()

    testCalibrationData = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
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


@mock.patch.object(LocalDataService, "_defaultGroupingMapPath")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_exit_no_delete(mockReadPVFile, mockReadInstrumentConfig, mockDefaultGroupingMapPath):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = initPVFileMock()

    testCalibrationData = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
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
