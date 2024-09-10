# Unit tests for `tests/util/state_helpers.py`
import shutil
import unittest.mock as mock
from pathlib import Path
from shutil import rmtree

import pytest
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from util.dao import DAOFactory
from util.state_helpers import reduction_root_redirect, state_root_override, state_root_redirect


@pytest.fixture(autouse=True)
def _cleanup_directories():
    stateId = "ab8704b0bc2a2342"
    stateRootPath = Path(Config["instrument.calibration.powder.home"]) / stateId
    yield
    # teardown
    if stateRootPath.exists():
        shutil.rmtree(stateRootPath)


def initPVFileMock():
    return {
        "entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value": [1.1],
        "entry/DASlogs/det_arc1/value": [1.0],
        "entry/DASlogs/det_arc2/value": [2.0],
        "entry/DASlogs/BL3:Det:TH:BL:Frequency/value": [1.2],
        "entry/DASlogs/BL3:Mot:OpticsPos:Pos/value": [1],
        "entry/DASlogs/det_lin1/value": [1.0],
        "entry/DASlogs/det_lin2/value": [2.0],
    }


@mock.patch.object(LocalDataService, "_writeDefaultDiffCalTable")
@mock.patch.object(LocalDataService, "generateStateId")
@mock.patch.object(LocalDataService, "_readDefaultGroupingMap")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_enter(
    mockReadPVFile,
    mockReadInstrumentConfig,
    mockReadDefaultGroupingMap,
    mockGenerateStateId,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = initPVFileMock()

    testCalibrationData = DAOFactory.calibrationParameters("57514", True, 1)
    mockReadInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    mockReadDefaultGroupingMap.return_value = DAOFactory.groupingMap_SNAP()

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
@mock.patch.object(LocalDataService, "_readDefaultGroupingMap")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_exit(
    mockReadPVFile,
    mockReadInstrumentConfig,
    mockReadDefaultGroupingMap,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = initPVFileMock()

    testCalibrationData = DAOFactory.calibrationParameters("57514", True, 1)
    mockReadInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    mockReadDefaultGroupingMap.return_value = DAOFactory.groupingMap_SNAP()

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
@mock.patch.object(LocalDataService, "_readDefaultGroupingMap")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_exit_no_delete(
    mockReadPVFile,
    mockReadInstrumentConfig,
    mockReadDefaultGroupingMap,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = initPVFileMock()

    testCalibrationData = DAOFactory.calibrationParameters("57514", True, 1)
    mockReadInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    mockReadDefaultGroupingMap.return_value = DAOFactory.groupingMap_SNAP()

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
    oldSelf = localDataService.constructCalibrationStateRoot
    with state_root_redirect(localDataService) as tmpRoot:
        # make sure the path exists
        assert tmpRoot.path().exists()
        # make sure the data service's path points to the tmp directory
        assert localDataService.constructCalibrationStateRoot() == tmpRoot.path()
        assert localDataService.generateStateId()[0] == tmpRoot.path().parts[-1]
        # make sure a file can be added inside the directory -- can be any file
        # verify it can be found by data services and equals the value written
        localDataService.calibrationExists = mock.Mock(return_value=True)
        expected = DAOFactory.calibrationParameters("xyz", True, 1)
        indexer = localDataService.calibrationIndexer("xyz", True)
        tmpRoot.saveObjectAt(expected, indexer.parametersPath(1))
        ans = localDataService.readCalibrationState("xyz", True, 1)
        assert ans == expected
        # make sure files can only be added inside the directory
        with pytest.raises(AssertionError):
            tmpRoot.saveObjectAt(expected, "here")
    # make sure the directory is deleted at exit
    assert not tmpRoot.path().exists()
    # make sure the construct state root method is restored on exit
    assert oldSelf == localDataService.constructCalibrationStateRoot


def test_state_root_redirect_with_stateid():
    stateId = "1234567890123456"
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        # make sure the root is pointing to this state ID
        assert localDataService.generateStateId() == (stateId, None)
        assert localDataService.constructCalibrationStateRoot() == tmpRoot.path()
        assert stateId == localDataService.constructCalibrationStateRoot().parts[-1]


def test_reduction_root_redirect_no_stateid():
    localDataService = LocalDataService()
    oldSelf = localDataService._constructReductionStateRoot
    with reduction_root_redirect(localDataService) as tmpRoot:
        # make sure the path exists
        assert tmpRoot.path().exists()
        # make sure the data service's path points to the tmp directory
        assert localDataService._constructReductionStateRoot() == tmpRoot.path()
        assert localDataService.generateStateId()[0] == tmpRoot.path().parts[-1]
    # make sure the directory is deleted at exit
    assert not tmpRoot.path().exists()
    # make sure the construct state root method is restored on exit
    assert oldSelf == localDataService._constructReductionStateRoot


def test_reduction_root_redirect_with_stateid():
    stateId = "1234567890123456"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        # make sure the root is pointing to this state ID
        assert localDataService.generateStateId() == (stateId, None)
        assert localDataService._constructReductionStateRoot() == tmpRoot.path()
        assert stateId == localDataService._constructReductionStateRoot().parts[-1]
